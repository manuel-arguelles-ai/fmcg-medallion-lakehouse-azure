# Databricks notebook source
# MAGIC %md
# MAGIC ## 99 · Reset de la demo
# MAGIC Deja el proyecto como recién provisionado: borra las tablas de `bronze`, `silver` y `gold` y devuelve los CSV de `processed/` a `landing/`. Correr **antes** de cada ensayo o del taller en vivo.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: borra todas las tablas de las tres capas
for schema in [bronze_schema, silver_schema, gold_schema]:
    for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():  # recorre las tablas del esquema
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t['tableName']}")  # borra la tabla
        print("drop", f"{catalog}.{schema}.{t['tableName']}")

# COMMAND ----------

# QUÉ HACE: devuelve los CSV de processed/ a landing/ para poder cargar otra vez desde cero
sources = [("initial", s) for s in ["customers", "products", "gross_price", "orders"]] + [("incremental", "orders")]  # las cinco carpetas de datos
for mode, s in sources:
    processed = f"{landing_root}/{mode}/{s}/processed/"
    landing = f"{landing_root}/{mode}/{s}/landing/"
    try:
        files = dbutils.fs.ls(processed)
    except Exception:  # si la carpeta no existe, no hay nada que mover
        files = []
    for f in files:
        dbutils.fs.mv(f.path, landing + f.name)  # de vuelta a landing/
    print(f"{mode}/{s}: landing =", [f.name for f in dbutils.fs.ls(landing)])
