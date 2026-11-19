# Databricks notebook source
# MAGIC %md
# MAGIC ## esperado — cifras de control y reglas de comparación
# MAGIC **Archivo generado**: no lo edites a mano. Sale de `qa/esperado.json` (el cálculo independiente) y de `qa/comparar.py`, con `python3 tools/sincronizar_validacion.py`. El notebook `98_validacion` lo carga con `%run`.

# COMMAND ----------
import json

esperado = json.loads(r"""
{
 "bronze.customers": 196,
 "bronze.products": 66,
 "bronze.gross_price": 312,
 "silver.customers": 184,
 "gold.dim_customers": 185,
 "cities": {
  "New Delhi": 51,
  "Bengaluru": 48,
  "Sin ciudad": 15,
  "Hyderabad": 70
 },
 "silver.products": 60,
 "gold.dim_products": 60,
 "products_999999": 12,
 "categories": {
  "Granola & Cereals": 9,
  "Electrolyte Mix": 8,
  "Protein Bars": 11,
  "Healthy Snacks": 11,
  "Energy Bars": 9,
  "Recovery Dairy": 12
 },
 "silver.gross_price": 288,
 "gold.dim_pricing": 288,
 "gold.dim_date": 365,
 "bronze.orders_initial": 5050,
 "bronze.orders": 5380,
 "silver.orders_initial": 4511,
 "fact_v0": {
  "rows": 4511,
  "units": 92491,
  "revenue": 648109.79
 },
 "merge": {
  "staging_rows": 295,
  "ins": 275,
  "upd": 20
 },
 "fact_now": {
  "rows": 4786,
  "units": 97986,
  "revenue": 684393.99,
  "null_price": 0,
  "orders": 4786
 },
 "silver.orders": 4786,
 "gold.fact_sales": 4786,
 "orders_cust_999999": 230,
 "orders_min_max": [
  "2026-01-01",
  "2026-06-15"
 ],
 "orders_null_dates": 0,
 "by_category": {
  "Electrolyte Mix": 92450.01,
  "Protein Bars": 164356.64,
  "Granola & Cereals": 82248.28,
  "Recovery Dairy": 168404.36,
  "Healthy Snacks": 77905.63,
  "Energy Bars": 99029.07
 },
 "by_month": {
  "2026-03": 138641.13,
  "2026-01": 130892.26,
  "2026-02": 122936.29,
  "2026-05": 128821.44,
  "2026-04": 127435.37,
  "2026-06": 35667.5
 },
 "quality": {
  "orders_initial": {
   "llegan": 5050,
   "sin_cantidad": 293,
   "tras_sin_cantidad": 4757,
   "tras_repetidas": 4709,
   "producto_inexistente": 198,
   "validas": 4511
  },
  "orders_incremental": {
   "llegan": 330,
   "sin_cantidad": 16,
   "tras_sin_cantidad": 314,
   "tras_repetidas": 305,
   "producto_inexistente": 10,
   "validas": 295
  },
  "quarantine": {
   "orders_batch.csv": {
    "sin_cantidad": 293,
    "producto_inexistente": 198
   },
   "orders_incremental_batch.csv": {
    "sin_cantidad": 16,
    "producto_inexistente": 10
   }
  }
 }
}
""")

# COMMAND ----------
def comparar(real, esp, out=print):
    """Devuelve la lista de comprobaciones que fallan. `out` recibe una línea por comprobación."""
    fallas = []

    def chk(name, a, b, tol=0):
        good = abs(a - b) <= tol if isinstance(a, (int, float)) and isinstance(b, (int, float)) else a == b
        if not good:
            fallas.append(name)
        out(f"{'OK   ' if good else 'FALLA'} {name} | real: {a} | esperado: {b}")

    for k in ["bronze.customers", "bronze.products", "bronze.gross_price", "bronze.orders", "silver.customers", "silver.products",
              "silver.gross_price", "silver.orders", "gold.dim_date", "gold.dim_customers", "gold.dim_products", "gold.dim_pricing",
              "gold.fact_sales", "cities", "categories", "products_999999", "orders_cust_999999", "orders_null_dates", "orders_min_max"]:
        chk(k, real[k], esp[k])
    for part in ["fact_v0", "fact_now"]:
        for k, v in esp[part].items():
            chk(f"{part}.{k}", real[part][k], v, 0.05 if k == "revenue" else 0)
    for part in ["by_category", "by_month"]:
        chk(part + " (claves)", sorted(real[part]), sorted(esp[part]))
        for k, v in esp[part].items():
            chk(f"{part}.{k}", real[part].get(k, -1), v, 0.05)
    # historial de MERGE: el primero aplica el lote; todos los posteriores (reintentos, re-ejecuciones) no cambian nada
    for capa, hist in (("gold", "history"), ("silver", "silver_history")):
        merges = [h for h in real[hist] if h["op"] == "MERGE"][::-1]
        chk(f"{capa}: al menos un MERGE en el historial", len(merges) >= 1, True)
        if merges:
            chk(f"{capa}: primer MERGE, insertadas", int(merges[0]["ins"]), esp["merge"]["ins"])
            chk(f"{capa}: primer MERGE, actualizadas", int(merges[0]["upd"]), esp["merge"]["upd"])
            chk(f"{capa}: MERGE posteriores sin cambios", sum(int(h["ins"]) + int(h["upd"]) for h in merges[1:]), 0)
    chk("gold: la idempotencia se probó (2 o más MERGE)", len([h for h in real["history"] if h["op"] == "MERGE"]) >= 2, True)
    for k, v in real["fact_orphans"].items():
        chk(f"ventas sin dimensión {k}", v, 0)
    chk("llaves duplicadas en fact", real["fact_dup_keys"], 0)
    chk("llaves duplicadas en silver.orders", real["silver_dup_keys"], 0)
    chk("product_code duplicado en dim", real["dim_products_dup_code"], 0)
    for archivo, motivos in esp["quality"]["quarantine"].items():
        for motivo, n in motivos.items():
            chk(f"cuarentena {archivo} · {motivo}", real["quarantine"].get(archivo, {}).get(motivo, 0), n)
    return fallas
