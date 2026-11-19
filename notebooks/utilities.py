# Databricks notebook source
# MAGIC %md
# MAGIC ## utilities — variables compartidas del proyecto
# MAGIC Todos los notebooks hacen `%run ./utilities` para heredarlas. El catálogo llega como parámetro: cada entorno del bundle (`dev`, `prod`) envía el suyo.

# COMMAND ----------
# Config de catálogo y schemas (medallion)
# QUÉ HACE: define los nombres que comparten todos los notebooks; el catálogo llega como parámetro
dbutils.widgets.text("catalog", "fmcg", "Catálogo")  # parámetro: el job de cada entorno envía su catálogo
catalog = dbutils.widgets.get("catalog")  # catálogo de Unity Catalog del proyecto
bronze_schema = "bronze"  # capa 1: lo que llegó, tal cual
silver_schema = "silver"  # capa 2: datos limpios y confiables
gold_schema = "gold"  # capa 3: modelo estrella para el negocio

# Raíz del Volume de landing
landing_root = f"/Volumes/{catalog}/landing/raw"  # carpeta del Volume donde llegan los CSV

spark.sql(f"USE CATALOG {catalog}")  # desde aquí, las consultas SQL nombran solo esquema.tabla
print("catalog:", catalog, "| landing:", landing_root)  # confirma en pantalla con qué valores se trabaja
