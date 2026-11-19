# Databricks notebook source
# MAGIC %md
# MAGIC ## Prueba · ejecución cortada entre Bronze y la tabla de paso
# MAGIC Reproduce el corte: carga el lote incremental en Bronze (el *checkpoint* avanza) y se detiene **sin** escribir la tabla de paso. Después se ejecuta el resto del pipeline: si la validación da todas las cifras, el reintento recuperó el lote.

# COMMAND ----------

# MAGIC %run ../../notebooks/utilities

# COMMAND ----------

from pyspark.sql import functions as F

landing_path = f"{landing_root}/incremental/orders/landing/"
checkpoint_path = f"{landing_root}/_checkpoints/bronze_incremental_orders"
bronze_table = f"{catalog}.{bronze_schema}.orders"
antes = spark.table(bronze_table).count()

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "csv").option("header", True)
   .option("rescuedDataColumn", "_rescued_data")
   .schema("order_id INT, order_placement_date STRING, customer_id STRING, product_id STRING, order_qty INT")
   .load(landing_path)
   .withColumn("read_timestamp", F.current_timestamp())
   .withColumn("load_mode", F.lit("incremental"))
   .select("*", "_metadata.file_name", "_metadata.file_size")
   .writeStream.option("checkpointLocation", checkpoint_path)
   .trigger(availableNow=True).toTable(bronze_table).awaitTermination())

despues = spark.table(bronze_table).count()
hay_paso = spark.catalog.tableExists(f"{catalog}.{bronze_schema}.staging_orders")
print("Bronze antes:", antes, "| después:", despues, "| ¿existe la tabla de paso?:", hay_paso)
assert despues > antes and not hay_paso, "el corte no quedó reproducido"
raise Exception("CORTE SIMULADO: Bronze cargado, tabla de paso sin escribir")
