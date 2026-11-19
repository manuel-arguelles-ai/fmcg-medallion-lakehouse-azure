# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Bronze — ingesta cruda CSV → Delta
# MAGIC Lee los CSV del Volume (landing) y los guarda como tablas Delta en `bronze` **tal cual llegan**, añadiendo metadata de ingesta. Después mueve los archivos a `processed/`.
# MAGIC
# MAGIC - `load_mode = initial` → carga histórica.
# MAGIC - `load_mode = incremental` → el lote nuevo va a una tabla **staging** y además se anexa al histórico bronze.
# MAGIC
# MAGIC Dos garantías de ingesta: el **esquema está declarado** (no se infiere) y un archivo cuyo nombre ya está en la tabla **no se vuelve a cargar**, aunque reaparezca en `landing/`.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: lee los dos parámetros del notebook y arma las rutas y los nombres de tabla
dbutils.widgets.dropdown("data_source", "orders", ["customers", "products", "gross_price", "orders"], "Data source")  # desplegable: qué fuente se carga
dbutils.widgets.dropdown("load_mode", "initial", ["initial", "incremental"], "Load mode")  # desplegable: carga inicial o lote nuevo
data_source = dbutils.widgets.get("data_source")  # valor elegido (el job lo envía como parámetro)
load_mode = dbutils.widgets.get("load_mode")

from pyspark.sql import functions as F

landing_path = f"{landing_root}/{load_mode}/{data_source}/landing/"  # donde esperan los archivos nuevos
processed_path = f"{landing_root}/{load_mode}/{data_source}/processed/"  # donde quedan los ya cargados
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
# MAGIC ### ¿Hay archivos nuevos?
# MAGIC Un archivo es nuevo si su nombre todavía no está en la tabla Bronze. Si un archivo ya cargado reaparece en `landing/` (por ejemplo, porque una ejecución anterior falló antes de moverlo), se aparta sin volver a cargarlo.

# COMMAND ----------

# QUÉ HACE: separa los archivos nuevos de los que ya se cargaron antes, comparando por nombre
def mover_a_processed(f):
    destino = processed_path + f.name
    try:
        dbutils.fs.ls(destino)  # si el archivo no está en processed/, esta línea falla y pasa al except
        dbutils.fs.rm(f.path)  # ya estaba guardado: se borra la copia repetida
    except Exception:
        dbutils.fs.mv(f.path, destino)  # de landing/ a processed/

csv_files = [f for f in dbutils.fs.ls(landing_path) if f.name.endswith(".csv")]  # lista los CSV de la carpeta
ya_cargados = set()
if spark.catalog.tableExists(bronze_table):
    ya_cargados = {r["file_name"] for r in spark.table(bronze_table).select("file_name").distinct().collect()}  # nombres de archivo que ya están en la tabla
nuevos = [f for f in csv_files if f.name not in ya_cargados]  # los que faltan por cargar
repetidos = [f for f in csv_files if f.name in ya_cargados]  # los que reaparecieron: no se cargan otra vez
for f in repetidos:
    mover_a_processed(f)
print("Archivos en landing:", [f.name for f in csv_files])
print("Nuevos:", [f.name for f in nuevos], "| ya cargados, no se repiten:", [f.name for f in repetidos])
hay_nuevos = len(nuevos) > 0  # las celdas siguientes solo actúan si hay algo nuevo
if not hay_nuevos:
    print("Sin archivos nuevos en landing: nada que procesar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura cruda + metadata
# MAGIC `_metadata.file_name` y `_metadata.file_size` son columnas automáticas de Spark: trazabilidad gratis de qué archivo vino cada fila.

# COMMAND ----------

# QUÉ HACE: lee los CSV nuevos tal cual y les agrega de qué archivo vino cada fila y cuándo se leyó
if hay_nuevos:
    df = (spark.read.format("csv")
          .option("header", True)  # la primera fila trae los nombres de columna
          .schema(schema)  # esquema declarado, sin inferencia
          .load([f.path for f in nuevos])  # solo los archivos nuevos
          .withColumn("read_timestamp", F.current_timestamp())  # cuándo se leyó
          .select("*", "_metadata.file_name", "_metadata.file_size"))  # de qué archivo vino cada fila y cuánto pesaba

    print("Total filas:", df.count())  # cifra de control: debe coincidir con la lámina
    display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escritura Delta
# MAGIC Bronze no limpia nada: guarda lo que llegó. Con `delta.enableChangeDataFeed` queda listo para consumir cambios aguas abajo.

# COMMAND ----------

# QUÉ HACE: guarda en Delta: el lote nuevo va a una tabla de paso y todo se suma a la tabla Bronze
if hay_nuevos:
    if load_mode == "incremental":
        # lote nuevo → staging: solo lo que Silver debe procesar en esta ejecución
        (df.write.format("delta")
           .option("overwriteSchema", "true")
           .mode("overwrite")
           .saveAsTable(staging_table))  # el lote nuevo, solo, para que Silver procese únicamente eso
    if spark.catalog.tableExists(bronze_table):
        df.write.format("delta").mode("append").saveAsTable(bronze_table)  # la tabla ya existe: se suman las filas
    else:
        (df.write.format("delta")
           .option("delta.enableChangeDataFeed", "true")  # primera carga: deja registrados los cambios fila por fila
           .mode("overwrite")
           .saveAsTable(bronze_table))
    print("Escrito:", bronze_table, "| filas en la tabla:", spark.table(bronze_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover archivos landing → processed
# MAGIC El movimiento ordena la carpeta; la garantía de no repetir la da el control por nombre de archivo.

# COMMAND ----------

# QUÉ HACE: mueve los archivos ya cargados para dejar landing/ vacío
if hay_nuevos:
    for f in nuevos:
        mover_a_processed(f)
    print("Archivos movidos a processed/:", [f.name for f in nuevos])

# COMMAND ----------

# QUÉ HACE: comprobación: cuántas filas aportó cada archivo
if spark.catalog.tableExists(bronze_table):
    display(spark.table(bronze_table).groupBy("file_name").agg(F.count("*").alias("filas")))
