#!/usr/bin/env python3
"""Comentarios orientativos de los notebooks del taller.

    python3 tools/comentar_notebooks.py            # los agrega (no cambia el código: se comprueba con el AST)
    python3 tools/comentar_notebooks.py --quitar   # los elimina y deja los notebooks como estaban

Dos tipos de comentario:
  · cabecera de celda  →  «# QUÉ HACE: …» antes de la primera línea de la celda
  · comentario al final de la línea  →  «código  # explicación»
En las celdas %sql el comentario se escribe con «--».
"""
import ast
import os
import sys

NB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks")
MARCA = "QUÉ HACE: "

# archivo -> ([(inicio de la primera línea de la celda, cabecera)], [(inicio de la línea, comentario)])
A = {
"utilities": ([
    ('catalog = "fmcg"', "define los nombres que comparten todos los notebooks; si cambias de catálogo, solo se toca aquí"),
], [
    ('catalog = "fmcg"', "catálogo de Unity Catalog del proyecto"),
    ('bronze_schema', "capa 1: lo que llegó, tal cual"),
    ('silver_schema', "capa 2: datos limpios y confiables"),
    ('gold_schema', "capa 3: modelo estrella para el negocio"),
    ('landing_root', "carpeta del Volume donde llegan los CSV"),
    ('print("catalog:"', "confirma en pantalla con qué valores se trabaja"),
]),
"00_setup_catalog": ([
    ("from pyspark.sql.utils", "comprueba si el catálogo ya existe y solo lo crea si falta"),
    ("CREATE SCHEMA IF NOT EXISTS fmcg.bronze", "crea un esquema por capa, más el de llegada de archivos"),
    ("CREATE VOLUME", "crea la carpeta administrada donde se suben los CSV"),
    ("SHOW SCHEMAS", "muestra los esquemas para confirmar que quedaron creados"),
], [
    ("from pyspark.sql.utils", "tipo de error que lanza Spark cuando un objeto no existe"),
    ("catalog_exists = True", "se asume que existe hasta demostrar lo contrario"),
    ('spark.sql("DESCRIBE CATALOG fmcg")', "si el catálogo no existe, esta línea falla"),
    ("except AnalysisException:", "falló: el catálogo no existe"),
    ("if catalog_exists:", "ya estaba creado: no se toca"),
    ('spark.sql("CREATE CATALOG fmcg")', "primera vez: se crea"),
    ("CREATE SCHEMA IF NOT EXISTS fmcg.bronze", "IF NOT EXISTS permite ejecutar la celda muchas veces sin error"),
    ("CREATE SCHEMA IF NOT EXISTS fmcg.landing", "esquema que contiene el Volume de llegada"),
    ("CREATE VOLUME", "reemplaza al bucket S3 del proyecto original"),
]),
"01_bronze_load": ([
    ("dbutils.widgets.dropdown", "lee los dos parámetros del notebook y arma las rutas y los nombres de tabla"),
    ("csv_files =", "busca CSV nuevos en landing/; si no hay, termina aquí sin tocar nada"),
    ("df = (spark.read", "lee los CSV tal cual y les agrega de qué archivo vino cada fila y cuándo se leyó"),
    ('if load_mode == "initial":', "guarda en Delta: la carga inicial reemplaza la tabla; la incremental va a una tabla de paso y al histórico"),
    ("for f in csv_files:", "mueve los archivos ya cargados para que la próxima ejecución no los repita"),
    ("display(spark.table(bronze_table)", "comprobación: cuántas filas aportó cada archivo"),
], [
    ('dbutils.widgets.dropdown("data_source"', "desplegable: qué fuente se carga"),
    ('dbutils.widgets.dropdown("load_mode"', "desplegable: carga inicial o lote nuevo"),
    ('data_source = dbutils', "valor elegido (el job lo envía como parámetro)"),
    ("landing_path =", "donde esperan los archivos nuevos"),
    ("processed_path =", "donde quedan los ya cargados"),
    ("bronze_table =", "tabla Bronze de esta fuente"),
    ("staging_table =", "tabla de paso: solo el lote nuevo"),
    ("csv_files =", "lista los CSV de la carpeta"),
    ("dbutils.notebook.exit", "corta la ejecución del notebook con un mensaje"),
    ('.option("header", True)', "la primera fila trae los nombres de columna"),
    ('.option("inferSchema", True)', "Spark deduce el tipo de cada columna"),
    (".load(landing_path)", "lee todos los CSV de la carpeta"),
    ('.withColumn("read_timestamp"', "cuándo se leyó"),
    ('.select("*", "_metadata.file_name"', "de qué archivo vino cada fila y cuánto pesaba"),
    ('print("Total filas:"', "cifra de control: debe coincidir con la lámina"),
    ('.option("delta.enableChangeDataFeed", "true")', "deja registrados los cambios fila por fila"),
    ('.option("overwriteSchema", "true")', "permite reemplazar también las columnas"),
    ('.mode("overwrite")', "carga inicial: reemplaza todo"),
    (".saveAsTable(staging_table))", "el lote nuevo, solo, para que Silver procese únicamente eso"),
    ('df.write.format("delta").mode("append")', "y además se suma al histórico de Bronze"),
    ("dbutils.fs.mv", "de landing/ a processed/"),
]),
"02_silver_customers": ([
    ("from pyspark.sql import functions as F", "lee la tabla Bronze de clientes y muestra cómo llegó"),
    ("dup =", "busca clientes repetidos y se queda con una fila por cliente"),
    ('print("Nombres con espacios', "limpia el nombre: sin espacios sobrantes y con mayúscula inicial"),
    ('display(df_s.groupBy("city").count().orderBy("city"))', "el «antes»: todas las formas en que llegó escrita la ciudad"),
    ("city_mapping = {", "corrige las ciudades mal escritas y anula las que no son válidas"),
    ("customer_city_fix =", "completa las ciudades que faltan: cuatro confirmadas por negocio y el resto «Sin ciudad»"),
    ('df_s = df_s.withColumn("customer_id"', "fija el tipo del identificador y guarda la tabla Silver"),
], [
    ("df = spark.table", "punto de partida: lo que dejó Bronze"),
    ('print("Filas bronze:"', "cifra de control: 196"),
    ("dup =", "identificadores que aparecen más de una vez"),
    ("df_s = df.dropDuplicates", "una sola fila por customer_id"),
    ('print("Antes:"', "196 → 184"),
    ('df_s = df_s.withColumn("customer_name"', "trim quita espacios; initcap pone mayúscula inicial"),
    ('"Bengaluruu": "Bengaluru"', "forma mal escrita → forma correcta"),
    ("allowed =", "lista permitida: las únicas tres ciudades válidas"),
    ('df_s = (df_s.withColumn("city", F.trim', "primero quita espacios"),
    (".replace(city_mapping", "luego aplica el diccionario"),
    ('.withColumn("city", F.when(F.col("city").isin(allowed)', "lo que no está en la lista queda en NULL"),
    ("customer_city_fix =", "decisión de negocio, no de datos"),
    ("df_fix =", "la convierte en una tabla pequeña para poder cruzarla"),
    ('df_s = (df_s.join(df_fix', "left: conserva todos los clientes"),
    ('.withColumn("city", F.coalesce', "primer valor no nulo: ciudad, corrección o «Sin ciudad»"),
    ('.drop("fixed_city"))', "la columna auxiliar ya no hace falta"),
    ('df_s = df_s.withColumn("customer_id"', "como texto, igual que en las órdenes"),
    ('.mode("overwrite")', "Silver de clientes se reconstruye completa cada vez"),
    ('print("OK →"', "cifra de control: 184"),
]),
"03_silver_products": ([
    ("from pyspark.sql import functions as F", "lee los productos de Bronze y quita los repetidos"),
    ('.withColumn("category", F.initcap("category"))', "normaliza categoría y nombre, y separa la variante que viene entre paréntesis"),
    ('.withColumn("product_name", F.regexp_replace("product_name", "(?i)Protien"', "corrige el error «Protien» en el nombre y en la categoría"),
    ('df_s = df_s.withColumn("division"', "deriva la división comercial, genera un código estable por producto y marca los identificadores inválidos"),
    ("(df_s.write", "guarda la tabla Silver de productos"),
], [
    ("df_s = df.dropDuplicates", "una fila por product_id"),
    ('print("Antes:"', "66 → 60"),
    ('.withColumn("category", F.initcap("category"))', "«energy bars» → «Energy Bars»"),
    ('.withColumn("variant"', "extrae lo que está entre paréntesis"),
    ('.withColumn("product_name", F.initcap(F.trim', "nombre sin la variante"),
    ('.withColumn("product_name", F.concat', "nombre final: «Producto (Variante)»"),
    ('.withColumn("product_name", F.regexp_replace("product_name", "(?i)Protien"', "(?i) = sin distinguir mayúsculas"),
    ('F.when(F.col("category") == "Energy Bars"', "regla de negocio: cada categoría pertenece a una división"),
    ('.otherwise("Other"))', "lo no previsto queda visible, no se pierde"),
    ('.withColumn("product_code"', "huella del nombre: el mismo producto recibe siempre el mismo código"),
    ('F.when(F.col("product_id").cast("string").rlike', "solo dígitos = identificador válido"),
    ('.otherwise(F.lit("999999")))', "inválido: se marca, no se borra"),
    ('df_s = df_s.select("product_code"', "orden final de columnas"),
    ('print("OK →"', "cifra de control: 60"),
]),
"04_silver_pricing": ([
    ("from pyspark.sql import functions as F", "prepara una conversión de fechas que no falla y muestra cómo llega el mes"),
    ('df_s = (df.withColumn("month"', "convierte el mes, que llega en cuatro formatos, a un solo tipo fecha"),
    ("dups =", "deja un solo precio por producto y mes"),
    ("(df_s.write", "guarda la tabla Silver de precios"),
], [
    ("to_date_safe =", "se usa una vez por formato"),
    ("from pyspark.sql.window", "para numerar filas dentro de un grupo"),
    ('display(df.select("month")', "el «antes»: formatos mezclados"),
    ("F.coalesce(", "se queda con el primer formato que logre convertir"),
    ('.withColumn("product_id"', "como texto, igual que en las demás tablas"),
    ('print("Fechas sin parsear:"', "debe dar 0"),
    ("dups =", "combinaciones con más de un precio"),
    ('print("Combinaciones', "cifra de control: 24"),
    ("w = Window", "dentro de cada producto y mes, del precio más alto al más bajo"),
    ('df_s = df_s.withColumn("rn"', "se queda con la primera fila de cada grupo"),
    ('print("Filas finales:"', "312 → 288"),
]),
"05_silver_orders": ([
    ("from pyspark.sql import functions as F", "prepara las herramientas y lee el parámetro de modo de carga"),
    ('src = (f"{catalog}', "elige de dónde leer: todo Bronze o solo el lote nuevo"),
    ('df = df.filter(F.col("order_qty").isNotNull())', "descarta las órdenes sin cantidad y marca los clientes inválidos"),
    ('df = df.withColumn("order_placement_date",', "deja todas las fechas en un solo formato"),
    ("df = df.dropDuplicates", "quita filas repetidas y descarta las órdenes cuyo producto no existe"),
    ('silver_table =', "guarda en Silver: reemplaza todo en la carga inicial o fusiona el lote nuevo"),
], [
    ("from delta.tables import DeltaTable", "necesario para MERGE"),
    ('if load_mode == "incremental"', "lote nuevo: la tabla de paso"),
    ("n_bronze =", "cifra de control: 5.050 en la carga inicial"),
    ('df = df.filter(F.col("order_qty").isNotNull())', "sin cantidad no hay venta que medir"),
    ('F.when(F.col("customer_id").rlike', "solo dígitos = cliente válido"),
    ('.otherwise(F.lit("999999"))))', "cliente inválido: se marca, la venta se conserva"),
    ('F.regexp_replace(F.col("order_placement_date")', "quita el día de la semana del inicio"),
    ("F.coalesce(", "prueba los cuatro formatos; gana el primero que convierte"),
    ('print("Fechas sin parsear:"', "debe dar 0"),
    ("df = df.dropDuplicates", "misma orden, fecha, cliente, producto y cantidad = fila repetida"),
    ("df_products =", "maestro de productos ya limpio"),
    ('df = df.join(df_products', "inner: sin producto en el maestro, la orden se descarta"),
    ('print("Tras dedup:"', "cifra de control: 4.709 → 4.511"),
    ('.mode("overwrite")', "carga inicial: reemplaza todo"),
    ('.mode("overwrite").saveAsTable(f"{catalog}.{silver_schema}.staging_orders"))', "el lote limpio que leerá el notebook 08"),
    (".merge(df.alias(\"new\"),", "condición: las cuatro columnas que identifican una orden"),
    (".whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())", "si existe, actualiza; si no, inserta"),
]),
"06_gold_dims": ([
    ("from pyspark.sql import functions as F", "genera el calendario del año completo, con una fila por día"),
    ("unknown =", "construye las otras tres dimensiones a partir de Silver"),
    ("SHOW TABLES IN fmcg.gold", "lista las tablas de Gold para confirmar"),
], [
    ('dim_date = (spark.sql', "sequence genera las fechas; explode las convierte en filas"),
    ('.withColumn("date_key"', "llave numérica, por ejemplo 20260308"),
    ('.withColumn("is_weekend"', "1 = domingo, 7 = sábado"),
    ('print("dim_date:"', "cifra de control: 365"),
    ("unknown =", "miembro «desconocido»: recibe las ventas con cliente inválido"),
    (".unionByName(unknown)", "se agrega a los clientes reales: 184 + 1"),
    ('.select("product_code", "product_id"', "solo las columnas que describen al producto"),
    ('.select("product_id", F.col("month").alias("price_month")', "el mes se renombra para que el cruce se lea claro"),
]),
"07_gold_fact_full_load": ([
    ('df = spark.sql(f"""', "arma la tabla de ventas: una fila por línea de orden, con su precio y su revenue"),
    ("fact_table =", "guarda fact_sales completa (carga inicial)"),
    ("SELECT round(SUM(revenue), 2) AS revenue_usd, SUM(quantity)", "totales de control: deben coincidir con la lámina"),
], [
    ('print("fact rows:"', "cifra de control: 4.511"),
    ('.mode("overwrite")', "la carga inicial crea la versión 0 de la tabla"),
    ("FROM fmcg.gold.fact_sales;", "esperado: 648.109,79 · 92.491 · 4.511"),
]),
"08_gold_fact_incremental": ([
    ("from pyspark.sql import functions as F", "lee el lote nuevo que dejó Silver"),
    ('df_fact_new = spark.sql(f"""', "le pone precio y revenue al lote nuevo, igual que en la carga inicial"),
    ("fact_table =", "define el MERGE como función, para poder ejecutarlo dos veces, y lo ejecuta la primera vez"),
    ("ins2, upd2 = merge_fact()", "segunda ejecución con el mismo lote: no debe cambiar nada"),
    ("SELECT version, timestamp, operation,", "consulta el historial de la tabla: una fila por cada escritura"),
], [
    ("df_new =", "lo escribió el notebook 05 en modo incremental"),
    ('print("Filas del lote nuevo', "cifra de control: 295"),
    ("before =", "filas antes de fusionar"),
    ('(DeltaTable.forName(spark, fact_table).alias("t")', "t = la tabla destino"),
    ('.merge(df_fact_new.alias("s"),', "s = el lote nuevo"),
    ('"AND t.customer_code = s.customer_code', "las cuatro columnas que identifican una venta"),
    (".whenMatchedUpdateAll(condition=", "ya existe: se actualiza solo si algo cambió (<=> trata a NULL como un valor)"),
    (".whenNotMatchedInsertAll()", "no existe: se inserta"),
    ("after =", "filas después de fusionar"),
    ("m = (DeltaTable.forName(spark, fact_table).history(1)", "última entrada del historial"),
    ("ins =", "cuántas filas insertó el MERGE"),
    ("upd =", "cuántas actualizó"),
    ("ins1, upd1 = merge_fact()", "primera ejecución: 275 insertadas, 20 actualizadas"),
    ("assert (ins2, upd2) == (0, 0)", "si no da 0 y 0, el notebook falla a propósito"),
    ("FROM (DESCRIBE HISTORY fmcg.gold.fact_sales);", "versión 0 = carga inicial; después, los dos MERGE"),
]),
"09_optimize_and_kpis": ([
    ("OPTIMIZE fmcg.gold.fact_sales", "compacta los archivos de la tabla y los ordena por producto"),
    ("SELECT 'versión 0 (full load)'", "compara la tabla como estaba al inicio con la actual"),
], [
    ("OPTIMIZE fmcg.gold.fact_sales", "ZORDER acelera los filtros por product_id"),
    ("FROM fmcg.gold.fact_sales VERSION AS OF 0", "la tabla tal como quedó en su primera escritura"),
    ("SELECT 'actual'", "esperado: 4.511 filas antes, 4.786 ahora"),
    ("JOIN fmcg.gold.dim_products dp ON f.product_code = dp.product_code", "la dimensión aporta el nombre y la categoría"),
    ("GROUP BY 1 ORDER BY 1;", "1 = la primera columna del SELECT"),
]),
"98_validacion": ([
    ("import json", "extrae las cifras reales de las tablas para compararlas con el cálculo independiente (qa/oraculo.py)"),
], [
    ("q = lambda s:", "atajo: ejecuta una consulta y devuelve sus filas"),
    ("m = {}", "aquí se acumulan todas las cifras"),
    ('m[t] = q(f"SELECT COUNT(*)', "conteo de filas de cada tabla"),
    ('m["cities"]', "clientes por ciudad"),
    ('m["products_999999"]', "productos marcados como inválidos"),
    ('m["orders_null_dates"]', "fechas sin convertir: debe ser 0"),
    ('m["fact_now"]', "totales de la tabla de ventas, hoy"),
    ('m["fact_v0"]', "los mismos totales en la versión 0"),
    ('m["history"]', "qué insertó y actualizó cada escritura"),
    ('m["fact_orphans"]', "ventas que apuntan a una dimensión inexistente: debe ser 0"),
    ('m["fact_dup_keys"]', "ventas repetidas: debe ser 0"),
    ("dbutils.notebook.exit(json.dumps(m))", "devuelve las cifras a quien ejecutó el notebook"),
]),
"99_reset_demo": ([
    ("for schema in", "borra todas las tablas de las tres capas"),
    ("sources =", "devuelve los CSV de processed/ a landing/ para poder cargar otra vez desde cero"),
], [
    ('for t in spark.sql(f"SHOW TABLES', "recorre las tablas del esquema"),
    ('spark.sql(f"DROP TABLE IF EXISTS', "borra la tabla"),
    ("sources =", "las cinco carpetas de datos"),
    ("except Exception:", "si la carpeta no existe, no hay nada que mover"),
    ("dbutils.fs.mv", "de vuelta a landing/"),
]),
"run_all": ([
    ("counts = {}", "cuenta las filas de las 13 tablas como resumen final"),
], [
    ('counts[t] = spark.table', "filas de cada tabla"),
]),
}


def celdas_python(src):
    """Código Python de las celdas que no son «magic», para comparar su AST."""
    out = []
    for c in src.split("# COMMAND ----------"):
        lineas = [l for l in c.splitlines() if not l.startswith("# Databricks notebook source")]
        if any(l.startswith("# MAGIC") for l in lineas):
            continue
        out.append(ast.dump(ast.parse("\n".join(lineas))))
    return out


def poner(src, cabeceras, lineas):
    L = src.split("\n")
    usados = set()

    def buscar(clave, libres=True):
        for i, l in enumerate(L):
            if MARCA in l or (libres and i in usados):
                continue
            magic = l.startswith("# MAGIC ")
            if (l[8:] if magic else l).strip().startswith(clave):
                return i, magic
        raise SystemExit(f"no encuentro la línea: {clave!r}")

    for clave, texto in lineas:
        i, magic = buscar(clave)
        assert not L[i].rstrip().endswith("\\"), L[i]
        if magic and L[i].rstrip().endswith(";"):
            # en SQL, un comentario después del «;» final quedaría como sentencia vacía: va en su propia línea, antes
            L.insert(i, "# MAGIC -- " + texto)
            usados = {u + 1 if u >= i else u for u in usados} | {i, i + 1}
            continue
        L[i] = L[i].rstrip() + ("  -- " if magic else "  # ") + texto
        usados.add(i)
    for clave, texto in cabeceras:
        i, magic = buscar(clave, libres=False)
        L.insert(i, ("# MAGIC -- " if magic else "# ") + MARCA + texto)
    return "\n".join(L)


def quitar(src, cabeceras, lineas):
    propios = {"# MAGIC -- " + t for _, t in lineas}
    L = [l for l in src.split("\n") if MARCA not in l and l not in propios]
    textos = sorted((t for _, t in lineas), key=len, reverse=True)
    for i, l in enumerate(L):
        for t in textos:
            for sep in ("  # ", "  -- "):
                if l.endswith(sep + t):
                    L[i] = l[: -len(sep + t)]
    return "\n".join(L)


modo_quitar = "--quitar" in sys.argv
for nombre, (cab, lin) in A.items():
    ruta = os.path.join(NB, nombre + ".py")
    antes = open(ruta, encoding="utf-8").read()
    limpio = quitar(antes, cab, lin)
    despues = limpio if modo_quitar else poner(limpio, cab, lin)
    assert celdas_python(limpio) == celdas_python(despues), f"{nombre}: el código cambió"
    assert quitar(despues, cab, lin) == limpio, f"{nombre}: quitar no devuelve el original"
    open(ruta, "w", encoding="utf-8").write(despues)
    print(f"{nombre:28s} cabeceras {0 if modo_quitar else len(cab):2d} · comentarios de línea {0 if modo_quitar else len(lin):2d}")
