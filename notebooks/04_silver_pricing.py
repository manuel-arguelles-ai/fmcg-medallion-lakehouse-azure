# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 · Silver pricing — normalización de `month` y precio único
# MAGIC La columna `month` llega en 4 formatos mezclados; se resuelve con `try_to_timestamp` + `coalesce` (el primero que parsea gana). Después dejamos **un solo precio por producto y mes**: si no, el join con la fact duplicaría ventas.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: prepara una conversión de fechas que no falla y muestra cómo llega el mes
from pyspark.sql import functions as F

# to_date tolerante: si el formato no calza devuelve NULL en vez de fallar
to_date_safe = lambda c, fmt: F.try_to_timestamp(F.col(c), F.lit(fmt)).cast("date")  # se usa una vez por formato
from pyspark.sql.window import Window  # para numerar filas dentro de un grupo

df = spark.table(f"{catalog}.{bronze_schema}.gross_price")
display(df.select("month").distinct().limit(20))  # el «antes»: formatos mezclados

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Cuatro formatos de fecha → un solo tipo `date`

# COMMAND ----------

# QUÉ HACE: convierte el mes, que llega en cuatro formatos, a un solo tipo fecha
df_s = (df.withColumn("month", F.col("month").cast("string"))
          .withColumn("month",
    F.coalesce(  # se queda con el primer formato que logre convertir
        to_date_safe("month", "yyyy/MM/dd"),
        to_date_safe("month", "dd/MM/yyyy"),
        to_date_safe("month", "yyyy-MM-dd"),
        to_date_safe("month", "dd-MM-yyyy"),
    ))
          .withColumn("product_id", F.col("product_id").cast("string")))  # como texto, igual que en las demás tablas

print("Fechas sin parsear:", df_s.filter(F.col("month").isNull()).count())  # debe dar 0
display(df_s.groupBy("month").count().orderBy("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Un precio por producto y mes
# MAGIC Hay productos con dos precios para el mismo mes. Regla acordada con negocio: se queda el **precio más alto** (lista vigente).

# COMMAND ----------

# QUÉ HACE: deja un solo precio por producto y mes
dups = df_s.groupBy("product_id", "month").count().filter("count > 1")  # combinaciones con más de un precio
print("Combinaciones producto-mes con más de un precio:", dups.count())  # cifra de control: 24

w = Window.partitionBy("product_id", "month").orderBy(F.col("price").desc())  # dentro de cada producto y mes, del precio más alto al más bajo
df_s = df_s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")  # se queda con la primera fila de cada grupo
print("Filas finales:", df_s.count())  # 312 → 288

# COMMAND ----------

# QUÉ HACE: guarda la tabla Silver de precios
(df_s.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")
   .saveAsTable(f"{catalog}.{silver_schema}.gross_price"))
print("OK →", f"{catalog}.{silver_schema}.gross_price")
