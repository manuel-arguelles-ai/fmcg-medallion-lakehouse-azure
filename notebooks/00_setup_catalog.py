# Databricks notebook source
# MAGIC %md
# MAGIC ## 00 · Setup — catálogo, schemas y Volume de landing
# MAGIC Comprueba que el catálogo de Unity Catalog existe y crea los schemas medallion y el Volume que hace de landing zone (reemplaza al bucket S3 del proyecto original). El nombre del catálogo llega como parámetro (`fmcg` si no se indica).

# COMMAND ----------
# MAGIC %md
# MAGIC **Verificación previa**: el catálogo se provisiona una sola vez, fuera del pipeline, porque necesita una ubicación de almacenamiento. Si no existe, el notebook se detiene aquí con un mensaje claro en vez de fallar más adelante.

# COMMAND ----------
# QUÉ HACE: comprueba que el catálogo existe; si no, se detiene con un mensaje que dice cómo crearlo
dbutils.widgets.text("catalog", "fmcg", "Catálogo")
catalog = dbutils.widgets.get("catalog")  # catálogo que envía el job; fmcg si no se indica

existentes = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]  # los catálogos que este usuario puede ver
if catalog not in existentes:
    raise Exception(  # falla aquí, con la causa y la solución, y no tres tareas después
        f"El catálogo '{catalog}' no existe o no tienes permiso sobre él. "
        f"Créalo una vez con: databricks catalogs create {catalog} --storage-root <ubicación> "
        "y vuelve a ejecutar.")
spark.sql(f"USE CATALOG {catalog}")  # las celdas SQL de abajo trabajan dentro de este catálogo
print("catálogo en uso:", catalog)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- QUÉ HACE: crea un esquema por capa, más el de llegada de archivos
# MAGIC -- IF NOT EXISTS permite ejecutar la celda muchas veces sin error
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC -- esquema que contiene el Volume de llegada
# MAGIC CREATE SCHEMA IF NOT EXISTS landing;

# COMMAND ----------
# MAGIC %md
# MAGIC **Landing zone**: un Volume de UC es storage gestionado, sin mounts ni credenciales externas. Aquí subimos los CSV crudos (carpetas `initial/` e `incremental/`).

# COMMAND ----------
# MAGIC %sql
# MAGIC -- QUÉ HACE: crea la carpeta administrada donde se suben los CSV
# MAGIC -- reemplaza al bucket S3 del proyecto original
# MAGIC CREATE VOLUME IF NOT EXISTS landing.raw;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- QUÉ HACE: muestra los esquemas para confirmar que quedaron creados
# MAGIC SHOW SCHEMAS;

# COMMAND ----------
# MAGIC %md
# MAGIC Subida de archivos (una sola vez, desde la UI o CLI):
# MAGIC ```
# MAGIC databricks fs cp -r data/landing dbfs:/Volumes/<catálogo>/landing/raw
# MAGIC ```
# MAGIC Cada CSV debe quedar en `raw/<initial|incremental>/<fuente>/landing/`. El pipeline solo los lee: subirlos es un paso previo (ver el README).
