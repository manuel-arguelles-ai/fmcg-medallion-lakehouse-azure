# Databricks notebook source
# MAGIC %md
# MAGIC ## 08 · Gold fact — INCREMENTAL con Delta MERGE ⭐
# MAGIC El clímax del taller. Llegó un archivo nuevo a `incremental/`; ya pasó por Bronze (notebook 01, `load_mode=incremental`) y Silver (notebook 05, `load_mode=incremental`). Ahora hacemos **MERGE** contra `fact_sales`: lo que ya existe y cambió se actualiza, lo nuevo se inserta. Correrlo dos veces produce el mismo resultado: **idempotencia**.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_new = spark.table(f"{catalog}.{silver_schema}.staging_orders")
print("Filas del lote nuevo (silver staging):", df_new.count())
display(df_new.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact del lote nuevo, con precio

# COMMAND ----------

df_fact_new = spark.sql(f"""
    SELECT o.order_id,
           o.order_placement_date AS date,
           o.customer_id          AS customer_code,
           o.product_code,
           o.product_id,
           o.order_qty            AS quantity,
           p.price,
           round(o.order_qty * p.price, 2) AS revenue
    FROM {catalog}.{silver_schema}.staging_orders o
    LEFT JOIN {catalog}.{gold_schema}.dim_pricing p
           ON o.product_id = p.product_id
          AND trunc(o.order_placement_date, 'MM') = p.price_month
""")
print("Filas a fusionar:", df_fact_new.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE upsert
# MAGIC Llave: `order_id + date + customer_code + product_code`. Solo actualiza si algo cambió de verdad.

# COMMAND ----------

fact_table = f"{catalog}.{gold_schema}.fact_sales"

def merge_fact():
    before = spark.table(fact_table).count()
    (DeltaTable.forName(spark, fact_table).alias("t")
        .merge(df_fact_new.alias("s"),
               "t.order_id = s.order_id AND t.date = s.date "
               "AND t.customer_code = s.customer_code AND t.product_code = s.product_code")
        .whenMatchedUpdateAll(condition="NOT (t.quantity <=> s.quantity AND t.price <=> s.price AND t.revenue <=> s.revenue)")
        .whenNotMatchedInsertAll()
        .execute())
    after = spark.table(fact_table).count()
    m = (DeltaTable.forName(spark, fact_table).history(1)
         .select("version", "operationMetrics").collect()[0])
    ins = int(m["operationMetrics"].get("numTargetRowsInserted", 0))
    upd = int(m["operationMetrics"].get("numTargetRowsUpdated", 0))
    print(f"fact antes: {before:,} → después: {after:,} | insertadas: {ins} | actualizadas: {upd} (versión Delta {m['version']})")
    return ins, upd

ins1, upd1 = merge_fact()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Y si lo corro otra vez? Idempotencia
# MAGIC Mismo lote, segunda corrida: **0 insertadas, 0 actualizadas**. Un reintento del job no duplica ventas.

# COMMAND ----------

ins2, upd2 = merge_fact()
assert (ins2, upd2) == (0, 0), "El MERGE no es idempotente"
print("Idempotente ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC **Historial Delta** — cada operación queda registrada:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, timestamp, operation,
# MAGIC        operationMetrics.numTargetRowsInserted AS insertadas,
# MAGIC        operationMetrics.numTargetRowsUpdated  AS actualizadas
# MAGIC FROM (DESCRIBE HISTORY fmcg.gold.fact_sales);
