# Databricks notebook source
# MAGIC %md
# MAGIC ## 00 · Setup — catálogo, schemas y Volume de landing
# MAGIC Crea el catálogo Unity Catalog `fmcg`, los schemas medallion y el Volume que hace de landing zone (reemplaza al bucket S3 del proyecto original).

# COMMAND ----------
# MAGIC %md
# MAGIC **Verificación previa**: si el catálogo ya existe (provisionado por CLI/UI), saltamos el CREATE para no chocar con la validación de Default Storage del metastore.

# COMMAND ----------
# QUÉ HACE: comprueba si el catálogo ya existe y solo lo crea si falta
from pyspark.sql.utils import AnalysisException  # tipo de error que lanza Spark cuando un objeto no existe

catalog_exists = True  # se asume que existe hasta demostrar lo contrario
try:
    spark.sql("DESCRIBE CATALOG fmcg").collect()  # si el catálogo no existe, esta línea falla
except AnalysisException:  # falló: el catálogo no existe
    catalog_exists = False
print("catálogo fmcg existe:", catalog_exists)

if catalog_exists:  # ya estaba creado: no se toca
    print("OK: saltando CREATE CATALOG (ya provisionado)")
else:
    try:
        spark.sql("CREATE CATALOG fmcg")  # primera vez: se crea
    except AnalysisException as e:
        print("CREATE falló — provisionar vía CLI/UI:", e)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- IF NOT EXISTS permite ejecutar la celda muchas veces sin error
# MAGIC -- QUÉ HACE: crea un esquema por capa, más el de llegada de archivos
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;
# MAGIC -- esquema que contiene el Volume de llegada
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.landing;

# COMMAND ----------
# MAGIC %md
# MAGIC **Landing zone**: un Volume de UC es storage gestionado, sin mounts ni credenciales externas. Aquí subimos los CSV crudos (carpetas `initial/` e `incremental/`).

# COMMAND ----------
# MAGIC %sql
# MAGIC -- reemplaza al bucket S3 del proyecto original
# MAGIC -- QUÉ HACE: crea la carpeta administrada donde se suben los CSV
# MAGIC CREATE VOLUME IF NOT EXISTS fmcg.landing.raw;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- QUÉ HACE: muestra los esquemas para confirmar que quedaron creados
# MAGIC SHOW SCHEMAS IN fmcg;

# COMMAND ----------
# MAGIC %md
# MAGIC Subida de archivos (una sola vez, desde la UI o CLI):
# MAGIC ```
# MAGIC databricks fs cp data/landing/initial/customers/*.csv \
# MAGIC   dbfs:/Volumes/fmcg/landing/raw/initial/customers/ --recursive
# MAGIC ```
# MAGIC En el taller los CSV ya están subidos off-line: el en vivo solo los lee.
