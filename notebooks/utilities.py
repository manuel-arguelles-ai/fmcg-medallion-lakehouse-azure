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

# COMMAND ----------
# MAGIC %md
# MAGIC ### Registro de cargas
# MAGIC Cada escritura de datos (carga completa o `MERGE`) se copia del historial de Delta a una tabla propia, `gold.load_log`. El historial de Delta se recorta con el tiempo; el registro no. La validación lee el registro.

# COMMAND ----------
# QUÉ HACE: define la función que copia al registro de cargas las escrituras que aún no están anotadas
load_log = f"{catalog}.{gold_schema}.load_log"  # tabla propia donde queda anotada cada carga

def registrar_cargas(tabla, con_importes=False):
    """Anota en el registro las escrituras de `tabla` que todavía no están. Se puede llamar las veces que haga falta."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {load_log} (tabla STRING, version BIGINT, momento TIMESTAMP, operacion STRING,
                  insertadas BIGINT, actualizadas BIGINT, filas BIGINT, unidades BIGINT, revenue DOUBLE)""")
    anotadas = {r["version"] for r in spark.sql(f"SELECT version FROM {load_log} WHERE tabla = '{tabla}'").collect()}  # las escrituras de esta tabla que ya están en el registro
    nuevas = 0
    for h in spark.sql(f"DESCRIBE HISTORY {catalog}.{tabla}").collect()[::-1]:  # recorre el historial de Delta, de la más antigua a la más reciente
        es_carga = h["operation"] == "MERGE" or "WRITE" in h["operation"] or "AS SELECT" in h["operation"]  # solo cargas de datos: se saltan OPTIMIZE, restricciones y propiedades
        if h["version"] in anotadas or not es_carga:
            continue
        importes = "SUM(quantity), round(SUM(revenue), 2)" if con_importes else "NULL, NULL"
        spark.sql(f"""INSERT INTO {load_log}
            SELECT '{tabla}', {h["version"]}, TIMESTAMP'{h["timestamp"]}', '{h["operation"]}',
                   {int(h["operationMetrics"].get("numTargetRowsInserted") or 0)}, {int(h["operationMetrics"].get("numTargetRowsUpdated") or 0)},
                   COUNT(*), {importes}
            FROM {catalog}.{tabla} VERSION AS OF {h["version"]}""")  # los totales se calculan sobre la tabla tal como quedó en esa versión
        nuevas += 1
    print(f"registro de cargas: {nuevas} escrituras nuevas de {tabla}")
