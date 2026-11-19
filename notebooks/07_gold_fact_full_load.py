# Databricks notebook source
# MAGIC %md
# MAGIC ## 07 · Gold fact — FULL LOAD
# MAGIC `fact_sales` a grano de línea de orden: la silver de orders ya trae `product_code`; el precio viene de `dim_pricing` (join por producto + mes) y `revenue = quantity × price`.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: arma la tabla de ventas: una fila por línea de orden, con su precio y su revenue
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
print("fact rows:", df.count())  # cifra de control: 4.511
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Full load**: carga histórica → overwrite. (El incremental está en el notebook 08.)
# MAGIC
# MAGIC Nota de diseño: la primera versión de este notebook particionaba la tabla por fecha. Con este volumen eso crea cientos de archivos diminutos; preferimos una tabla sin particionar y `OPTIMIZE ... ZORDER` (notebook 09).

# COMMAND ----------

# QUÉ HACE: guarda fact_sales completa (carga inicial) y declara que ninguna venta puede quedar sin precio
fact_table = f"{catalog}.{gold_schema}.fact_sales"
(df.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")  # la carga inicial crea la versión 0 de la tabla
   .saveAsTable(fact_table))
# restricción de la tabla: una venta sin precio no entra (ni ahora ni en los lotes siguientes)
spark.sql(f"ALTER TABLE {fact_table} DROP CONSTRAINT IF EXISTS precio_no_nulo")
spark.sql(f"ALTER TABLE {fact_table} ADD CONSTRAINT precio_no_nulo CHECK (price IS NOT NULL)")  # un lote con una venta sin precio falla en vez de publicar revenue nulo
registrar_cargas(f"{gold_schema}.fact_sales", con_importes=True)
print("OK →", fact_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUÉ HACE: totales de control de la carga inicial
# MAGIC SELECT round(SUM(revenue), 2) AS revenue_usd, SUM(quantity) AS units, COUNT(*) AS order_lines
# MAGIC -- esperado: 648.109,79 · 92.491 · 4.511
# MAGIC FROM gold.fact_sales;
