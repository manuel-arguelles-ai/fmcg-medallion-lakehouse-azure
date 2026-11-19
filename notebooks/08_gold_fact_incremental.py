# Databricks notebook source
# MAGIC %md
# MAGIC ## 08 · Gold fact — INCREMENTAL con Delta MERGE ⭐
# MAGIC La pieza central del proyecto. Llegó un archivo nuevo a `incremental/`; ya pasó por Bronze (notebook 01, `load_mode=incremental`) y Silver (notebook 05, `load_mode=incremental`). Ahora hacemos **MERGE** contra `fact_sales`: lo que ya existe y cambió se actualiza, lo nuevo se inserta. Correrlo dos veces produce el mismo resultado: **idempotencia**.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: lee el lote nuevo que dejó Silver
from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_new = spark.table(f"{catalog}.{silver_schema}.staging_orders")  # lo escribió el notebook 05 en modo incremental
print("Filas del lote nuevo (silver staging):", df_new.count())  # cifra de control: 295
display(df_new.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact del lote nuevo, con precio

# COMMAND ----------

# QUÉ HACE: le pone precio y revenue al lote nuevo, igual que en la carga inicial
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

# QUÉ HACE: define el MERGE como función, para poder ejecutarlo dos veces, y lo ejecuta la primera vez
fact_table = f"{catalog}.{gold_schema}.fact_sales"

def merge_fact():
    before = spark.table(fact_table).count()  # filas antes de fusionar
    (DeltaTable.forName(spark, fact_table).alias("t")  # t = la tabla destino
        .merge(df_fact_new.alias("s"),  # s = el lote nuevo
               "t.order_id = s.order_id AND t.date = s.date "
               "AND t.customer_code = s.customer_code AND t.product_code = s.product_code")  # las cuatro columnas que identifican una venta
        .whenMatchedUpdateAll(condition="NOT (t.quantity <=> s.quantity AND t.price <=> s.price AND t.revenue <=> s.revenue)")  # ya existe: se actualiza solo si algo cambió (<=> trata a NULL como un valor)
        .whenNotMatchedInsertAll()  # no existe: se inserta
        .execute())
    after = spark.table(fact_table).count()  # filas después de fusionar
    m = (DeltaTable.forName(spark, fact_table).history(1)  # última entrada del historial
         .select("version", "operationMetrics").collect()[0])
    ins = int(m["operationMetrics"].get("numTargetRowsInserted", 0))  # cuántas filas insertó el MERGE
    upd = int(m["operationMetrics"].get("numTargetRowsUpdated", 0))  # cuántas actualizó
    print(f"fact antes: {before:,} → después: {after:,} | insertadas: {ins} | actualizadas: {upd} (versión Delta {m['version']})")
    return ins, upd

ins1, upd1 = merge_fact()  # primera ejecución: 275 insertadas, 20 actualizadas

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Y si lo corro otra vez? Idempotencia
# MAGIC Mismo lote, segunda corrida: **0 insertadas, 0 actualizadas**. Un reintento del job no duplica ventas.

# COMMAND ----------

# QUÉ HACE: segunda ejecución con el mismo lote: no debe cambiar nada
ins2, upd2 = merge_fact()
registrar_cargas(f"{gold_schema}.fact_sales", con_importes=True)
assert (ins2, upd2) == (0, 0), "El MERGE no es idempotente"  # si no da 0 y 0, el notebook falla a propósito
print("Idempotente ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC **Historial Delta** — cada operación queda registrada:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUÉ HACE: consulta el historial de la tabla: una fila por cada escritura
# MAGIC SELECT version, timestamp, operation,
# MAGIC        operationMetrics.numTargetRowsInserted AS insertadas,
# MAGIC        operationMetrics.numTargetRowsUpdated  AS actualizadas
# MAGIC -- versión 0 = carga inicial; después, los dos MERGE
# MAGIC FROM (DESCRIBE HISTORY gold.fact_sales);
