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
# MAGIC ### ¿Hay archivos nuevos?
# MAGIC Si `landing/` está vacío no hay nada que procesar: el notebook termina sin tocar ninguna tabla.

# COMMAND ----------

# QUÉ HACE: busca CSV nuevos en landing/; si no hay, termina aquí sin tocar nada
csv_files = [f for f in dbutils.fs.ls(landing_path) if f.name.endswith(".csv")]  # lista los CSV de la carpeta
print("Archivos en landing:", [f.name for f in csv_files])
if not csv_files:
    dbutils.notebook.exit("Sin archivos nuevos en landing: nada que procesar")  # corta la ejecución del notebook con un mensaje

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura cruda + metadata
# MAGIC `_metadata.file_name` y `_metadata.file_size` son columnas automáticas de Spark: trazabilidad gratis de qué archivo vino cada fila.

# COMMAND ----------

# QUÉ HACE: lee los CSV tal cual y les agrega de qué archivo vino cada fila y cuándo se leyó
df = (spark.read.format("csv")
      .option("header", True)  # la primera fila trae los nombres de columna
      .option("inferSchema", True)  # Spark deduce el tipo de cada columna
      .load(landing_path)  # lee todos los CSV de la carpeta
      .withColumn("read_timestamp", F.current_timestamp())  # cuándo se leyó
      .select("*", "_metadata.file_name", "_metadata.file_size"))  # de qué archivo vino cada fila y cuánto pesaba

print("Total filas:", df.count())  # cifra de control: debe coincidir con la lámina
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escritura Delta
# MAGIC Bronze no limpia nada: guarda lo que llegó. Con `delta.enableChangeDataFeed` queda listo para consumir cambios aguas abajo.

# COMMAND ----------

# QUÉ HACE: guarda en Delta: la carga inicial reemplaza la tabla; la incremental va a una tabla de paso y al histórico
if load_mode == "initial":
    (df.write.format("delta")
       .option("delta.enableChangeDataFeed", "true")  # deja registrados los cambios fila por fila
       .option("overwriteSchema", "true")  # permite reemplazar también las columnas
       .mode("overwrite")  # carga inicial: reemplaza todo
       .saveAsTable(bronze_table))
    written = bronze_table
else:
    # lote nuevo → staging (lo que procesa Silver) + histórico bronze
    (df.write.format("delta")
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(staging_table))  # el lote nuevo, solo, para que Silver procese únicamente eso
    df.write.format("delta").mode("append").saveAsTable(bronze_table)  # y además se suma al histórico de Bronze
    written = staging_table
print("Escrito:", written)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover archivos landing → processed
# MAGIC Patrón del proyecto original: evita re-procesar el mismo archivo en la siguiente corrida.

# COMMAND ----------

# QUÉ HACE: mueve los archivos ya cargados para que la próxima ejecución no los repita
for f in csv_files:
    dbutils.fs.mv(f.path, processed_path + f.name)  # de landing/ a processed/
print("Archivos movidos a processed/:", [f.name for f in csv_files])

# COMMAND ----------

# QUÉ HACE: comprobación: cuántas filas aportó cada archivo
display(spark.table(bronze_table).groupBy("file_name").agg(F.count("*").alias("filas")))
