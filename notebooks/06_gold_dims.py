# Databricks notebook source
# MAGIC %md
# MAGIC ## 06 · Gold — dimensiones del star schema
# MAGIC `dim_date` (generada), `dim_customers`, `dim_products`, `dim_pricing`. Las dimensiones se reconstruyen completas (overwrite); solo la fact usa MERGE incremental.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date — calendario generado
# MAGIC Cubre el año completo, no solo las fechas que ya tienen ventas: así las cargas incrementales futuras siempre encuentran su fecha.

# COMMAND ----------

# QUÉ HACE: genera el calendario del año completo, con una fila por día
from pyspark.sql import functions as F

dim_date = (spark.sql("SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS date")  # sequence genera las fechas; explode las convierte en filas
    .withColumn("date_key",   F.date_format("date", "yyyyMMdd").cast("int"))  # llave numérica, por ejemplo 20260308
    .withColumn("year",       F.year("date"))
    .withColumn("quarter",    F.quarter("date"))
    .withColumn("month",      F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day",        F.dayofmonth("date"))
    .withColumn("day_name",   F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7)))  # 1 = domingo, 7 = sábado

(dim_date.write.format("delta").option("overwriteSchema", "true").mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.dim_date"))
print("dim_date:", spark.table(f"{catalog}.{gold_schema}.dim_date").count(), "días")  # cifra de control: 365

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customers / dim_products / dim_pricing — desde Silver
# MAGIC `dim_customers` incluye el miembro **999999 · Cliente desconocido**: las órdenes con cliente inválido no se pierden, caen ahí.

# COMMAND ----------

# QUÉ HACE: construye las otras tres dimensiones a partir de Silver
unknown = spark.createDataFrame([("999999", "Cliente Desconocido", "Sin ciudad")],  # miembro «desconocido»: recibe las ventas con cliente inválido
                                "customer_id string, customer_name string, city string")
(spark.table(f"{catalog}.{silver_schema}.customers")
    .select("customer_id", "customer_name", "city")
    .unionByName(unknown)  # se agrega a los clientes reales: 184 + 1
    .write.format("delta").option("overwriteSchema", "true").mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.dim_customers"))

(spark.table(f"{catalog}.{silver_schema}.products")
    .select("product_code", "product_id", "division", "category", "product", "variant")  # solo las columnas que describen al producto
    .write.format("delta").option("overwriteSchema", "true").mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.dim_products"))

(spark.table(f"{catalog}.{silver_schema}.gross_price")
    .select("product_id", F.col("month").alias("price_month"), "price")  # el mes se renombra para que el cruce se lea claro
    .write.format("delta").option("overwriteSchema", "true").mode("overwrite")
    .saveAsTable(f"{catalog}.{gold_schema}.dim_pricing"))

print("dims OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUÉ HACE: lista las tablas de Gold para confirmar
# MAGIC SHOW TABLES IN gold;
