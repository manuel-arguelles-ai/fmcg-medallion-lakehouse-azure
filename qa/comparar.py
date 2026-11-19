#!/usr/bin/env python3
"""Compara la salida del notebook 98_validacion contra qa/esperado.json.

    python3 qa/comparar.py salida_98.json                 # un archivo con el JSON que imprime el notebook 98
    python3 qa/comparar.py --run <id> [--profile <perfil>] # o el id de la ejecución de la tarea, vía la CLI de Databricks

Termina con código 0 si las 50 cifras coinciden y con 1 si alguna difiere."""
import json, os, subprocess, sys
AQUI = os.path.dirname(os.path.abspath(__file__))
if "--run" in sys.argv:
    cmd = ["databricks", "jobs", "get-run-output", sys.argv[sys.argv.index("--run") + 1], "-o", "json"]
    if "--profile" in sys.argv:
        cmd += ["-p", sys.argv[sys.argv.index("--profile") + 1]]
    real = json.loads(json.loads(subprocess.check_output(cmd))["notebook_output"]["result"])
else:
    real = json.load(open(sys.argv[1], encoding="utf-8"))
esp = json.load(open(os.path.join(AQUI, "esperado.json"), encoding="utf-8"))
ok = True
def chk(name, a, b, tol=0):
    global ok
    good = abs(a - b) <= tol if isinstance(a, (int, float)) and isinstance(b, (int, float)) else a == b
    ok &= good; print(("OK  " if good else "FALLA"), name, "| real:", a, "| esperado:", b)
for k in ["bronze.customers", "bronze.products", "bronze.gross_price", "bronze.orders", "silver.customers", "silver.products",
          "silver.gross_price", "silver.orders", "gold.dim_date", "gold.dim_customers", "gold.dim_products", "gold.dim_pricing",
          "gold.fact_sales", "cities", "categories", "products_999999", "orders_cust_999999", "orders_null_dates", "orders_min_max"]:
    chk(k, real[k], esp[k])
for part in ["fact_v0", "fact_now"]:
    for k, v in esp[part].items(): chk(f"{part}.{k}", real[part][k], v, 0.05 if k == "revenue" else 0)
for part in ["by_category", "by_month"]:
    chk(part + " (claves)", sorted(real[part]), sorted(esp[part]))
    for k, v in esp[part].items(): chk(f"{part}.{k}", real[part].get(k, -1), v, 0.05)
merges = [h for h in real["history"] if h["op"] == "MERGE"][::-1]
chk("merge 1 insertadas", int(merges[0]["ins"]), esp["merge"]["ins"]); chk("merge 1 actualizadas", int(merges[0]["upd"]), esp["merge"]["upd"])
chk("merge 2 (idempotencia) ins+upd", int(merges[1]["ins"]) + int(merges[1]["upd"]), 0)
chk("nº de MERGE en historial", len(merges), 2)
for k, v in real["fact_orphans"].items(): chk(f"huérfanos fact→dim {k}", v, 0)
chk("llaves duplicadas en fact", real["fact_dup_keys"], 0); chk("product_code duplicado en dim", real["dim_products_dup_code"], 0)
print("\nRESULTADO:", "TODO OK" if ok else "HAY DIFERENCIAS"); sys.exit(0 if ok else 1)
