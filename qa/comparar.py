#!/usr/bin/env python3
"""Compara la salida del notebook 98_validacion contra qa/esperado.json.

    python3 qa/comparar.py salida_98.json                 # un archivo con el JSON que devuelve el notebook 98
    python3 qa/comparar.py --run <id> [--profile <perfil>] # o el id de la ejecución de la tarea, vía la CLI de Databricks

Termina con código 0 si todas las cifras coinciden y con 1 si alguna difiere.
La función `comparar` es la misma que usa el notebook 98 dentro del job (tools/sincronizar_validacion.py la copia)."""
import json, os, subprocess, sys


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


if __name__ == "__main__":
    AQUI = os.path.dirname(os.path.abspath(__file__))
    if "--run" in sys.argv:
        cmd = ["databricks", "jobs", "get-run-output", sys.argv[sys.argv.index("--run") + 1], "-o", "json"]
        if "--profile" in sys.argv:
            cmd += ["-p", sys.argv[sys.argv.index("--profile") + 1]]
        real = json.loads(json.loads(subprocess.check_output(cmd))["notebook_output"]["result"])
    else:
        real = json.load(open(sys.argv[1], encoding="utf-8"))
    esp = json.load(open(os.path.join(AQUI, "esperado.json"), encoding="utf-8"))
    n = [0]

    def linea(s):
        n[0] += 1
        print(s)

    fallas = comparar(real, esp, linea)
    print(f"\nRESULTADO: {n[0] - len(fallas)} de {n[0]} comprobaciones coinciden ·", "TODO OK" if not fallas else f"HAY DIFERENCIAS: {fallas}")
    sys.exit(1 if fallas else 0)
