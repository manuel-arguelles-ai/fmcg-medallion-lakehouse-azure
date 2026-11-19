# Databricks notebook source
# MAGIC %md
# MAGIC ## 98 · Validación — el job falla si una cifra no coincide
# MAGIC Extrae las cifras reales de las tablas y del registro de cargas (`gold.load_log`) y las compara con las que produce el cálculo independiente (`qa/oraculo.py`). Si alguna difiere, el notebook termina con error y la ejecución del job queda en rojo.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./esperado

# COMMAND ----------

# QUÉ HACE: extrae las cifras reales de las tablas
import json
q = lambda s: spark.sql(s).collect()  # atajo: ejecuta una consulta y devuelve sus filas
m = {}  # aquí se acumulan todas las cifras
for t in ["bronze.customers", "bronze.products", "bronze.gross_price", "bronze.orders",
          "silver.customers", "silver.products", "silver.gross_price", "silver.orders",
          "gold.dim_date", "gold.dim_customers", "gold.dim_products", "gold.dim_pricing", "gold.fact_sales"]:
    m[t] = q(f"SELECT COUNT(*) c FROM {t}")[0]["c"]  # conteo de filas de cada tabla
m["cities"] = {r["city"]: r["c"] for r in q("SELECT city, COUNT(*) c FROM silver.customers GROUP BY city")}  # clientes por ciudad
m["categories"] = {r["category"]: r["c"] for r in q("SELECT category, COUNT(*) c FROM silver.products GROUP BY category")}
m["products_999999"] = q("SELECT COUNT(*) c FROM silver.products WHERE product_id='999999'")[0]["c"]  # productos marcados como inválidos
m["orders_cust_999999"] = q("SELECT COUNT(*) c FROM silver.orders WHERE customer_id='999999'")[0]["c"]
m["orders_null_dates"] = q("SELECT COUNT(*) c FROM silver.orders WHERE order_placement_date IS NULL")[0]["c"]  # fechas sin convertir: debe ser 0
m["orders_min_max"] = [str(x) for x in q("SELECT MIN(order_placement_date) a, MAX(order_placement_date) b FROM silver.orders")[0]]
r = q("SELECT COUNT(*) c, SUM(quantity) u, round(SUM(revenue),2) rev, SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) np, COUNT(DISTINCT order_id) o FROM gold.fact_sales")[0]
m["fact_now"] = {"rows": r["c"], "units": r["u"], "revenue": float(r["rev"]), "null_price": r["np"], "orders": r["o"]}  # totales de la tabla de ventas, hoy
registrar_cargas("gold.fact_sales", con_importes=True)  # pone el registro al día, por si una ejecución se cortó antes de anotar
registrar_cargas("silver.orders")
cargas = lambda tabla: q(f"SELECT * FROM gold.load_log WHERE tabla = '{tabla}' ORDER BY version DESC")  # las cargas anotadas de una tabla, de la más reciente a la más antigua
r = [c for c in cargas("gold.fact_sales") if c["operacion"] != "MERGE"][-1]
m["fact_v0"] = {"rows": r["filas"], "units": r["unidades"], "revenue": float(r["revenue"])}  # los totales de la primera carga completa, según el registro
m["history"] = [{"v": c["version"], "op": c["operacion"], "ins": c["insertadas"], "upd": c["actualizadas"]} for c in cargas("gold.fact_sales")]  # qué insertó y actualizó cada escritura
m["by_category"] = {r["category"]: float(r["rev"]) for r in q("""SELECT dp.category, round(SUM(f.revenue),2) rev FROM gold.fact_sales f
    JOIN gold.dim_products dp ON f.product_code = dp.product_code GROUP BY dp.category""")}
m["by_month"] = {str(r["m"])[:7]: float(r["rev"]) for r in q("SELECT date_trunc('MONTH', date) m, round(SUM(revenue),2) rev FROM gold.fact_sales GROUP BY 1")}
m["fact_orphans"] = {  # ventas que apuntan a una dimensión inexistente: debe ser 0
  "customer": q("SELECT COUNT(*) c FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_customers d ON f.customer_code=d.customer_id")[0]["c"],
  "product": q("SELECT COUNT(*) c FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_products d ON f.product_code=d.product_code")[0]["c"],
  "date": q("SELECT COUNT(*) c FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_date d ON f.date=d.date")[0]["c"]}
m["fact_dup_keys"] = q("SELECT COUNT(*) c FROM (SELECT 1 FROM gold.fact_sales GROUP BY order_id, date, customer_code, product_code HAVING COUNT(*)>1)")[0]["c"]  # ventas repetidas: debe ser 0
m["dim_products_dup_code"] = q("SELECT COUNT(*) c FROM (SELECT 1 FROM gold.dim_products GROUP BY product_code HAVING COUNT(*)>1)")[0]["c"]
m["silver_history"] = [{"v": c["version"], "op": c["operacion"], "ins": c["insertadas"], "upd": c["actualizadas"]} for c in cargas("silver.orders")]
m["silver_dup_keys"] = q("SELECT COUNT(*) c FROM (SELECT 1 FROM silver.orders GROUP BY order_id, order_placement_date, customer_id, product_code HAVING COUNT(*)>1)")[0]["c"]  # órdenes repetidas en Silver: debe ser 0
m["quarantine"] = {}  # filas en cuarentena por archivo y motivo
for r in q("SELECT file_name, motivo, COUNT(*) c FROM silver.orders_quarantine GROUP BY file_name, motivo"):
    m["quarantine"].setdefault(r["file_name"], {})[r["motivo"]] = r["c"]
m["malformed"] = {t: q(f"SELECT COUNT(_rescued_data) c FROM bronze.{t}")[0]["c"] for t in ["customers", "products", "gross_price", "orders"]}  # filas que no encajaron en el esquema, por tabla de Bronze
print(json.dumps(m, indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparación
# MAGIC Una línea por comprobación. Con una sola `FALLA`, el notebook lanza un error.

# COMMAND ----------

# QUÉ HACE: compara cada cifra con la esperada y hace fallar el notebook si alguna difiere
lineas = []
fallas = comparar(m, esperado, lineas.append)  # la función y las cifras esperadas vienen del notebook esperado
print("\n".join(lineas))
print(f"\n{len(lineas) - len(fallas)} de {len(lineas)} comprobaciones coinciden")
if fallas:
    raise Exception(f"Validación fallida: {len(fallas)} cifras no coinciden: {fallas}")  # el error deja la tarea, y el job, en rojo

# COMMAND ----------

# QUÉ HACE: todo coincide: devuelve las cifras a quien ejecutó el notebook
dbutils.notebook.exit(json.dumps(m))
