# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Bronze — ingesta cruda CSV → Delta
# MAGIC Lee los CSV del Volume (landing) y los guarda como tablas Delta en `bronze` **tal cual llegan**, añadiendo metadata de ingesta. Después mueve los archivos a `processed/` para que la próxima corrida solo procese lo nuevo.
# MAGIC
# MAGIC - `load_mode = initial` → carga histórica (overwrite de la tabla bronze).
# MAGIC - `load_mode = incremental` → el lote nuevo va a una tabla **staging** y además se anexa al histórico bronze.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

dbutils.widgets.dropdown("data_source", "orders", ["customers", "products", "gross_price", "orders"], "Data source")
dbutils.widgets.dropdown("load_mode", "initial", ["initial", "incremental"], "Load mode")
data_source = dbutils.widgets.get("data_source")
load_mode = dbutils.widgets.get("load_mode")

from pyspark.sql import functions as F

landing_path = f"{landing_root}/{load_mode}/{data_source}/landing/"
processed_path = f"{landing_root}/{load_mode}/{data_source}/processed/"
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
staging_table = f"{catalog}.{bronze_schema}.staging_{data_source}"
print(landing_path, "->", bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Hay archivos nuevos?
# MAGIC Si `landing/` está vacío no hay nada que procesar: el notebook termina sin tocar ninguna tabla.

# COMMAND ----------

csv_files = [f for f in dbutils.fs.ls(landing_path) if f.name.endswith(".csv")]
print("Archivos en landing:", [f.name for f in csv_files])
if not csv_files:
    dbutils.notebook.exit("Sin archivos nuevos en landing: nada que procesar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura cruda + metadata
# MAGIC `_metadata.file_name` y `_metadata.file_size` son columnas automáticas de Spark: trazabilidad gratis de qué archivo vino cada fila.

# COMMAND ----------

df = (spark.read.format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(landing_path)
      .withColumn("read_timestamp", F.current_timestamp())
      .select("*", "_metadata.file_name", "_metadata.file_size"))

print("Total filas:", df.count())
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escritura Delta
# MAGIC Bronze no limpia nada: guarda lo que llegó. Con `delta.enableChangeDataFeed` queda listo para consumir cambios aguas abajo.

# COMMAND ----------

if load_mode == "initial":
    (df.write.format("delta")
       .option("delta.enableChangeDataFeed", "true")
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(bronze_table))
    written = bronze_table
else:
    # lote nuevo → staging (lo que procesa Silver) + histórico bronze
    (df.write.format("delta")
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(staging_table))
    df.write.format("delta").mode("append").saveAsTable(bronze_table)
    written = staging_table
print("Escrito:", written)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover archivos landing → processed
# MAGIC Patrón del proyecto original: evita re-procesar el mismo archivo en la siguiente corrida.

# COMMAND ----------

for f in csv_files:
    dbutils.fs.mv(f.path, processed_path + f.name)
print("Archivos movidos a processed/:", [f.name for f in csv_files])

# COMMAND ----------

display(spark.table(bronze_table).groupBy("file_name").agg(F.count("*").alias("filas")))
