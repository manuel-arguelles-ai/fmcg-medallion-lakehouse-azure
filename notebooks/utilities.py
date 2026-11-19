# Databricks notebook source
# MAGIC %md
# MAGIC ## utilities — variables compartidas del proyecto
# MAGIC Todos los notebooks hacen `%run ./utilities` para heredarlas.

# COMMAND ----------
# Config de catálogo y schemas (medallion)
# QUÉ HACE: define los nombres que comparten todos los notebooks; si cambias de catálogo, solo se toca aquí
catalog = "fmcg"  # catálogo de Unity Catalog del proyecto
bronze_schema = "bronze"  # capa 1: lo que llegó, tal cual
silver_schema = "silver"  # capa 2: datos limpios y confiables
gold_schema = "gold"  # capa 3: modelo estrella para el negocio

# Raíz del Volume de landing
landing_root = "/Volumes/fmcg/landing/raw"  # carpeta del Volume donde llegan los CSV

print("catalog:", catalog, "| landing:", landing_root)  # confirma en pantalla con qué valores se trabaja
