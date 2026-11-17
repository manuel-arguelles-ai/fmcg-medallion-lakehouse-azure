# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 · Silver pricing — normalización de `month` y precio único
# MAGIC La columna `month` llega en 4 formatos mezclados; se resuelve con `try_to_timestamp` + `coalesce` (el primero que parsea gana). Después dejamos **un solo precio por producto y mes**: si no, el join con la fact duplicaría ventas.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

from pyspark.sql import functions as F

# to_date tolerante: si el formato no calza devuelve NULL en vez de fallar
to_date_safe = lambda c, fmt: F.try_to_timestamp(F.col(c), F.lit(fmt)).cast("date")
from pyspark.sql.window import Window

df = spark.table(f"{catalog}.{bronze_schema}.gross_price")
display(df.select("month").distinct().limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Cuatro formatos de fecha → un solo tipo `date`

# COMMAND ----------

df_s = (df.withColumn("month", F.col("month").cast("string"))
          .withColumn("month",
    F.coalesce(
        to_date_safe("month", "yyyy/MM/dd"),
        to_date_safe("month", "dd/MM/yyyy"),
        to_date_safe("month", "yyyy-MM-dd"),
        to_date_safe("month", "dd-MM-yyyy"),
    ))
          .withColumn("product_id", F.col("product_id").cast("string")))

print("Fechas sin parsear:", df_s.filter(F.col("month").isNull()).count())
display(df_s.groupBy("month").count().orderBy("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Un precio por producto y mes
# MAGIC Hay productos con dos precios para el mismo mes. Regla acordada con negocio: se queda el **precio más alto** (lista vigente).

# COMMAND ----------

dups = df_s.groupBy("product_id", "month").count().filter("count > 1")
print("Combinaciones producto-mes con más de un precio:", dups.count())

w = Window.partitionBy("product_id", "month").orderBy(F.col("price").desc())
df_s = df_s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
print("Filas finales:", df_s.count())

# COMMAND ----------

(df_s.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")
   .saveAsTable(f"{catalog}.{silver_schema}.gross_price"))
print("OK →", f"{catalog}.{silver_schema}.gross_price")
