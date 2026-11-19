# Databricks notebook source
# MAGIC %md
# MAGIC ## Prueba · una fila que no encaja en el esquema no se pierde en silencio
# MAGIC Escribe un CSV de órdenes con una fila correcta, una con texto donde va la cantidad y una con una columna de más. Lo lee con las mismas opciones que `01_bronze_load` y comprueba que la fila con el tipo incorrecto queda señalada en `_rescued_data`, que es lo que la regla `fila_malformada` de Silver envía a cuarentena. No toca las tablas del pipeline y borra lo que crea.
# MAGIC
# MAGIC **Límite que esta prueba deja a la vista:** la fila con una columna de más no se señala. El lector CSV de Spark descarta los valores sobrantes y da la fila por buena.

# COMMAND ----------

# MAGIC %run ../../notebooks/utilities

# COMMAND ----------

from pyspark.sql import functions as F

carpeta = f"{landing_root}/_pruebas/fila_malformada"
tabla = f"{catalog}.{bronze_schema}.prueba_fila_malformada"
dbutils.fs.rm(carpeta, True)
spark.sql(f"DROP TABLE IF EXISTS {tabla}")
dbutils.fs.put(f"{carpeta}/datos/ordenes.csv",
               "order_id,order_placement_date,customer_id,product_id,order_qty\n"
               "1,2026/01/05,789101,25891101,10\n"
               "2,2026/01/05,789101,25891101,diez\n"
               "3,2026/01/05,789101,25891101,7,sobra\n", True)

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "csv").option("header", True)
   .option("rescuedDataColumn", "_rescued_data")
   .schema("order_id INT, order_placement_date STRING, customer_id STRING, product_id STRING, order_qty INT")
   .load(f"{carpeta}/datos/")
   .writeStream.option("checkpointLocation", f"{carpeta}/_checkpoint")
   .trigger(availableNow=True).toTable(tabla).awaitTermination())

filas = {r["order_id"]: r["_rescued_data"] for r in spark.table(tabla).collect()}
print(filas)
malas = spark.table(tabla).filter("NOT (_rescued_data IS NULL)").count()  # la condición de la regla fila_malformada, negada
spark.sql(f"DROP TABLE IF EXISTS {tabla}")
dbutils.fs.rm(f"{landing_root}/_pruebas", True)
assert len(filas) == 3 and filas[1] is None and filas[2] is not None and malas == 1, filas
assert filas[3] is None, "el lector ya señala las columnas de más: actualiza la documentación"
print("OK: 3 filas leídas; la de tipo incorrecto, señalada; la de una columna de más, no (límite conocido)")
