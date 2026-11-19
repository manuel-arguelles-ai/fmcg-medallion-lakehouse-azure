# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Bronze — ingesta cruda CSV → Delta
# MAGIC Lee los CSV del Volume (landing) con **Auto Loader** y los guarda como tablas Delta en `bronze` **tal cual llegan**, añadiendo metadata de ingesta.
# MAGIC
# MAGIC - `load_mode = initial` → carga histórica.
# MAGIC - `load_mode = incremental` → el lote nuevo va a una tabla **staging** y además se anexa al histórico bronze.
# MAGIC
# MAGIC Dos garantías de ingesta: el **esquema está declarado** (no se infiere) y Auto Loader **recuerda qué archivos ya cargó**, así que ejecutar el notebook otra vez no repite nada.

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
# MAGIC Auto Loader (`cloudFiles`) anota en su *checkpoint* cada archivo que procesa. Con `availableNow` carga lo pendiente y se detiene: funciona como una carga por lotes, pero con memoria. `_metadata.file_name` y `_metadata.file_size` dan la trazabilidad de cada fila.

# COMMAND ----------

# QUÉ HACE: prepara la lectura de los archivos pendientes y les agrega de qué archivo vino cada fila y cuándo se leyó
inicio = spark.sql("SELECT current_timestamp() AS t").first()["t"]  # hora de inicio: sirve para saber qué filas entraron en esta ejecución

df = (spark.readStream.format("cloudFiles")  # cloudFiles = Auto Loader
      .option("cloudFiles.format", "csv")
      .option("header", True)  # la primera fila trae los nombres de columna
      .schema(schema)  # esquema declarado, sin inferencia
      .load(landing_path)  # vigila la carpeta completa
      .withColumn("read_timestamp", F.current_timestamp())  # cuándo se leyó
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

nuevas = spark.table(bronze_table).filter(F.col("read_timestamp") >= F.lit(inicio))  # las filas que entraron en esta ejecución
n_nuevas = nuevas.count()
if n_nuevas > 0:
    spark.sql(f"ALTER TABLE {bronze_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # deja registrados los cambios fila por fila
print("Filas nuevas en esta ejecución:", n_nuevas, "| filas en la tabla:", spark.table(bronze_table).count())  # cifra de control: debe coincidir con la lámina
if n_nuevas == 0:
    print("Sin archivos nuevos en landing: nada que procesar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de paso del lote nuevo
# MAGIC En la carga incremental, Silver debe procesar solo lo que llegó en esta ejecución: esas filas se copian a una tabla *staging*. Si no llegó nada, la tabla de paso queda como estaba.

# COMMAND ----------

# QUÉ HACE: en la carga incremental, copia el lote recién llegado a la tabla de paso
if load_mode == "incremental" and n_nuevas > 0:
    (nuevas.write.format("delta")
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(staging_table))  # el lote nuevo, solo, para que Silver procese únicamente eso
    print("Lote nuevo →", staging_table, "|", spark.table(staging_table).count(), "filas")

# COMMAND ----------

# QUÉ HACE: comprobación: cuántas filas aportó cada archivo
display(spark.table(bronze_table).groupBy("file_name").agg(F.count("*").alias("filas")))
