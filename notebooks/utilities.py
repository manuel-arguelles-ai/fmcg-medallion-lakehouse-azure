# Databricks notebook source
# MAGIC %md
# MAGIC ## utilities — variables compartidas del proyecto
# MAGIC Todos los notebooks hacen `%run ./utilities` para heredarlas.

# COMMAND ----------
# Config de catálogo y schemas (medallion)
catalog = "fmcg"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# Raíz del Volume de landing
landing_root = "/Volumes/fmcg/landing/raw"

print("catalog:", catalog, "| landing:", landing_root)
