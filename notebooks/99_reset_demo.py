# Databricks notebook source
# MAGIC %md
# MAGIC ## 99 · Reset de la demo
# MAGIC Deja el proyecto como recién provisionado: borra las tablas de `bronze`, `silver` y `gold` y devuelve los CSV de `processed/` a `landing/`. Correr **antes** de cada ensayo o del taller en vivo.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

for schema in [bronze_schema, silver_schema, gold_schema]:
    for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t['tableName']}")
        print("drop", f"{catalog}.{schema}.{t['tableName']}")

# COMMAND ----------

sources = [("initial", s) for s in ["customers", "products", "gross_price", "orders"]] + [("incremental", "orders")]
for mode, s in sources:
    processed = f"{landing_root}/{mode}/{s}/processed/"
    landing = f"{landing_root}/{mode}/{s}/landing/"
    try:
        files = dbutils.fs.ls(processed)
    except Exception:
        files = []
    for f in files:
        dbutils.fs.mv(f.path, landing + f.name)
    print(f"{mode}/{s}: landing =", [f.name for f in dbutils.fs.ls(landing)])
