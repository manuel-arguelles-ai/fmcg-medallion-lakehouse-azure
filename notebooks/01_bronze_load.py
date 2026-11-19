# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Bronze — ingesta cruda CSV → Delta
# MAGIC Lee los CSV del Volume (landing) con **Auto Loader** y los guarda como tablas Delta en `bronze` **tal cual llegan**, añadiendo metadata de ingesta.
# MAGIC
# MAGIC - `load_mode = initial` → carga histórica.
# MAGIC - `load_mode = incremental` → el lote nuevo se anexa al histórico bronze y queda además en una tabla **staging**, que es lo que Silver procesa.
# MAGIC
# MAGIC Tres garantías de ingesta: el **esquema está declarado** (no se infiere), Auto Loader **recuerda qué archivos ya cargó** (ejecutar el notebook otra vez no repite nada) y la tabla de paso se arma con **lo que Bronze tiene y ella todavía no**, así que una ejecución interrumpida se recupera sola en el reintento.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: lee los dos parámetros del notebook y arma las rutas y los nombres de tabla
dbutils.widgets.dropdown("data_source", "orders", ["customers", "products", "gross_price", "orders"], "Data source")  # desplegable: qué fuente se carga
dbutils.widgets.dropdown("load_mode", "initial", ["initial", "incremental"], "Load mode")  # desplegable: carga inicial o lote nuevo
data_source = dbutils.widgets.get("data_source")  # valor elegido (el job lo envía como parámetro)
load_mode = dbutils.widgets.get("load_mode")

from pyspark.sql import functions as F

landing_path = f"{landing_root}/{load_mode}/{data_source}/landing/"  # donde esperan los archivos
checkpoint_path = f"{landing_root}/_checkpoints/bronze_{load_mode}_{data_source}"  # donde Auto Loader anota lo que ya cargó
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"  # tabla Bronze de esta fuente
staging_table = f"{catalog}.{bronze_schema}.staging_{data_source}"  # tabla de paso: solo el lote nuevo
print(landing_path, "->", bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esquema declarado
# MAGIC Bronze no limpia, pero tampoco adivina: cada fuente tiene sus columnas y tipos escritos. Los identificadores que llegan sucios se leen como texto para no perder el valor original.
# MAGIC
# MAGIC Si una fila no encaja en el esquema (un texto donde va un número, columnas de más), el valor no se pierde en silencio: Auto Loader lo guarda en la columna `_rescued_data` y Silver decide qué hacer con esa fila.

# COMMAND ----------

# QUÉ HACE: declara las columnas y los tipos de cada fuente, en vez de dejar que Spark los adivine
SCHEMAS = {
    "customers":   "customer_id INT, customer_name STRING, city STRING",
    "products":    "product_id STRING, product_name STRING, category STRING",  # el identificador llega sucio: se lee como texto
    "gross_price": "product_id INT, month STRING, price DOUBLE",
    "orders":      "order_id INT, order_placement_date STRING, customer_id STRING, product_id STRING, order_qty INT",
}
schema = SCHEMAS[data_source]  # el esquema de la fuente elegida
print(schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura con Auto Loader + metadata
# MAGIC Auto Loader (`cloudFiles`) anota en su *checkpoint* cada archivo que procesa. Con `availableNow` carga lo pendiente y se detiene: funciona como una carga por lotes, pero con memoria. Cada fila guarda de qué archivo vino, cuándo se leyó y con qué tipo de carga entró.

# COMMAND ----------

# QUÉ HACE: prepara la lectura de los archivos pendientes y les agrega de qué archivo vino cada fila y cuándo se leyó
inicio = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS t").first()["t"]  # hora de inicio: solo para informar cuántas filas entraron en esta ejecución

df = (spark.readStream.format("cloudFiles")  # cloudFiles = Auto Loader
      .option("cloudFiles.format", "csv")
      .option("header", True)  # la primera fila trae los nombres de columna
      .option("rescuedDataColumn", "_rescued_data")  # lo que no encaje en el esquema se guarda aquí en vez de perderse
      .schema(schema)  # esquema declarado, sin inferencia
      .load(landing_path)  # vigila la carpeta completa
      .withColumn("read_timestamp", F.current_timestamp())  # cuándo se leyó
      .withColumn("load_mode", F.lit(load_mode))  # con qué tipo de carga entró: inicial o incremental
      .select("*", "_metadata.file_name", "_metadata.file_size"))  # de qué archivo vino cada fila y cuánto pesaba

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escritura Delta
# MAGIC Bronze no limpia nada: guarda lo que llegó. La escritura y el *checkpoint* avanzan juntos, de modo que un archivo queda cargado exactamente una vez aunque la ejecución se interrumpa y se repita.

# COMMAND ----------

# QUÉ HACE: carga en la tabla Bronze lo que Auto Loader todavía no había procesado y cuenta cuántas filas entraron
(df.writeStream
   .option("checkpointLocation", checkpoint_path)  # la memoria de qué archivos ya se cargaron
   .trigger(availableNow=True)  # procesa lo pendiente y termina, como un lote
   .toTable(bronze_table)  # agrega las filas a la tabla; la crea si no existe
   .awaitTermination())  # espera a que la carga termine antes de seguir

bronze = spark.table(bronze_table)
n_nuevas = bronze.filter(F.col("read_timestamp") >= F.lit(inicio).cast("timestamp")).count()  # las filas que entraron en esta ejecución
if n_nuevas > 0:
    spark.sql(f"ALTER TABLE {bronze_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # deja registrados los cambios fila por fila
print("Filas nuevas en esta ejecución:", n_nuevas, "| filas en la tabla:", bronze.count())  # cifra de control: la validación la compara con lo esperado
if n_nuevas == 0:
    print("Sin archivos nuevos en landing: nada que cargar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de paso del lote nuevo
# MAGIC En la carga incremental, Silver procesa solo el lote nuevo, que se copia a una tabla *staging*. Qué es «nuevo» no se decide con la memoria de esta ejecución, sino comparando tablas: son las filas incrementales de Bronze **más recientes que todo lo que la tabla de paso ya contiene**.
# MAGIC
# MAGIC Por eso el paso se recupera solo: si una ejecución se corta después de cargar Bronze y antes de escribir la tabla de paso, el reintento no encuentra archivos nuevos, pero sí filas pendientes, y la escribe. Si no hay nada pendiente, la tabla queda como estaba.

# COMMAND ----------

# QUÉ HACE: en la carga incremental, pasa a la tabla de paso las filas que Bronze tiene y ella todavía no
if load_mode == "incremental":
    pendientes = bronze.filter(F.col("load_mode") == "incremental")  # solo lo que entró por la carga incremental
    if spark.catalog.tableExists(staging_table):
        ya_en_paso = spark.sql(f"SELECT CAST(MAX(read_timestamp) AS STRING) AS t FROM {staging_table}").first()["t"]  # lo más reciente que la tabla de paso ya contiene
        if ya_en_paso is not None:
            pendientes = pendientes.filter(F.col("read_timestamp") > F.lit(ya_en_paso).cast("timestamp"))  # pendiente = más reciente que eso
    n_pendientes = pendientes.count()
    if n_pendientes > 0:  # sin pendientes, la tabla de paso queda como estaba
        (pendientes.write.format("delta")
           .option("overwriteSchema", "true")
           .mode("overwrite")
           .saveAsTable(staging_table))  # el lote nuevo, solo, para que Silver procese únicamente eso
    print("Filas pendientes de pasar a Silver:", n_pendientes)

# COMMAND ----------

# QUÉ HACE: comprobación: cuántas filas aportó cada archivo
display(spark.table(bronze_table).groupBy("file_name").agg(
    F.count("*").alias("filas"),
    F.count("_rescued_data").alias("filas_malformadas")))
