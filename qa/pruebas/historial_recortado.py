# Databricks notebook source
# MAGIC %md
# MAGIC ## Prueba · la validación no necesita el historial de Delta
# MAGIC Delta recorta el historial de una tabla con el tiempo, y ese recorte no se puede forzar en el día. Esta prueba lo simula: sustituye `gold.fact_sales` y `silver.orders` por clones de sí mismas, cuyo historial empieza de cero y ya no contiene la carga inicial ni los `MERGE`. Después se ejecuta `98_validacion`: si da todas las cifras, es porque las lee del registro de cargas (`gold.load_log`) y no del historial.
# MAGIC
# MAGIC Ejecútala con el pipeline ya corrido. Los datos de las tablas no cambian.

# COMMAND ----------

# MAGIC %run ../../notebooks/utilities

# COMMAND ----------

for tabla in (f"{gold_schema}.fact_sales", f"{silver_schema}.orders"):
    antes = [h["operation"] for h in spark.sql(f"DESCRIBE HISTORY {catalog}.{tabla}").collect()]
    filas = spark.table(f"{catalog}.{tabla}").count()
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tabla}__clon DEEP CLONE {catalog}.{tabla}")
    spark.sql(f"DROP TABLE {catalog}.{tabla}")
    spark.sql(f"CREATE TABLE {catalog}.{tabla} DEEP CLONE {catalog}.{tabla}__clon")
    spark.sql(f"DROP TABLE {catalog}.{tabla}__clon")
    despues = [h["operation"] for h in spark.sql(f"DESCRIBE HISTORY {catalog}.{tabla}").collect()]
    print(tabla, "| historial antes:", len(antes), "versiones, con MERGE:", "MERGE" in antes, "| después:", despues)
    assert "MERGE" in antes, "ejecuta antes el pipeline completo"
    assert "MERGE" not in despues and len(despues) < len(antes), "el historial no quedó recortado"
    assert spark.table(f"{catalog}.{tabla}").count() == filas, "los datos cambiaron"

print("Historial sustituido. Ahora ejecuta 98_validacion: debe dar todas las cifras.")
