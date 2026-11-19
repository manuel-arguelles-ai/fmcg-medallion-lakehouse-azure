#!/usr/bin/env python3
"""Genera notebooks/esperado.py a partir de qa/esperado.json y de la función `comparar` de qa/comparar.py.
Así el notebook 98 valida dentro del job con las mismas cifras y las mismas reglas que la comparación local.

    python3 tools/sincronizar_validacion.py
"""
import json
import os

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
esp = json.load(open(os.path.join(RAIZ, "qa", "esperado.json"), encoding="utf-8"))
src = open(os.path.join(RAIZ, "qa", "comparar.py"), encoding="utf-8").read()
funcion = src[src.index("def comparar("):src.index('if __name__ == "__main__":')].rstrip()
nb = f'''# Databricks notebook source
# MAGIC %md
# MAGIC ## esperado — cifras de control y reglas de comparación
# MAGIC **Archivo generado**: no lo edites a mano. Sale de `qa/esperado.json` (el cálculo independiente) y de `qa/comparar.py`, con `python3 tools/sincronizar_validacion.py`. El notebook `98_validacion` lo carga con `%run`.

# COMMAND ----------
import json

esperado = json.loads(r"""
{json.dumps(esp, indent=1, ensure_ascii=False)}
""")

# COMMAND ----------
{funcion}
'''
open(os.path.join(RAIZ, "notebooks", "esperado.py"), "w", encoding="utf-8").write(nb)
print("notebooks/esperado.py actualizado")
