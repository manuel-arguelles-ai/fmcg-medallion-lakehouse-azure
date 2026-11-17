# Databricks notebook source
# MAGIC %md
# MAGIC ## 07 · Gold fact — FULL LOAD
# MAGIC `fact_sales` a grano de línea de orden: la silver de orders ya trae `product_code`; el precio viene de `dim_pricing` (join por producto + mes) y `revenue = quantity × price`.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

df = spark.sql(f"""
    SELECT o.order_id,
           o.order_placement_date AS date,
           o.customer_id          AS customer_code,
           o.product_code,
           o.product_id,
           o.order_qty            AS quantity,
           p.price,
           round(o.order_qty * p.price, 2) AS revenue
    FROM {catalog}.{silver_schema}.orders o
    LEFT JOIN {catalog}.{gold_schema}.dim_pricing p
           ON o.product_id = p.product_id
          AND trunc(o.order_placement_date, 'MM') = p.price_month
""")
print("fact rows:", df.count())
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Full load**: carga histórica → overwrite. (El incremental está en el notebook 08.)
# MAGIC
# MAGIC Nota de diseño: la primera versión de este notebook particionaba la tabla por fecha. Con este volumen eso crea cientos de archivos diminutos; preferimos una tabla sin particionar y `OPTIMIZE ... ZORDER` (notebook 09).

# COMMAND ----------

fact_table = f"{catalog}.{gold_schema}.fact_sales"
(df.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")
   .saveAsTable(fact_table))
print("OK →", fact_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT round(SUM(revenue), 2) AS revenue_usd, SUM(quantity) AS units, COUNT(*) AS order_lines
# MAGIC FROM fmcg.gold.fact_sales;
