#!/usr/bin/env python3
"""Comentarios orientativos de los notebooks del proyecto.

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
    ('dbutils.widgets.text("catalog"', "define los nombres que comparten todos los notebooks; el catálogo llega como parámetro"),
    ("load_log =", "define la función que copia al registro de cargas las escrituras que aún no están anotadas"),
], [
    ('dbutils.widgets.text("catalog"', "parámetro: el job de cada entorno envía su catálogo"),
    ('catalog = dbutils', "catálogo de Unity Catalog del proyecto"),
    ('bronze_schema', "capa 1: lo que llegó, tal cual"),
    ('silver_schema', "capa 2: datos limpios y confiables"),
    ('gold_schema', "capa 3: modelo estrella para el negocio"),
    ('landing_root', "carpeta del Volume donde llegan los CSV"),
    ('spark.sql(f"USE CATALOG', "desde aquí, las consultas SQL nombran solo esquema.tabla"),
    ('print("catalog:"', "confirma en pantalla con qué valores se trabaja"),
    ("load_log =", "tabla propia donde queda anotada cada carga"),
    ("anotadas =", "las escrituras de esta tabla que ya están en el registro"),
    ("for h in spark.sql(f\"DESCRIBE HISTORY", "recorre el historial de Delta, de la más antigua a la más reciente"),
    ("es_carga =", "solo cargas de datos: se saltan OPTIMIZE, restricciones y propiedades"),
    ("FROM {catalog}.{tabla} VERSION AS OF", "los totales se calculan sobre la tabla tal como quedó en esa versión"),
]),
"00_setup_catalog": ([
    ('dbutils.widgets.text("catalog"', "comprueba que el catálogo existe; si no, se detiene con un mensaje que dice cómo crearlo"),
    ("CREATE SCHEMA IF NOT EXISTS bronze", "crea un esquema por capa, más el de llegada de archivos"),
    ("CREATE VOLUME", "crea la carpeta administrada donde se suben los CSV"),
    ("SHOW SCHEMAS", "muestra los esquemas para confirmar que quedaron creados"),
], [
    ('catalog = dbutils', "catálogo que envía el job; fmcg si no se indica"),
    ("existentes =", "los catálogos que este usuario puede ver"),
    ("raise Exception(", "falla aquí, con la causa y la solución, y no tres tareas después"),
    ('spark.sql(f"USE CATALOG', "las celdas SQL de abajo trabajan dentro de este catálogo"),
    ("CREATE SCHEMA IF NOT EXISTS bronze", "IF NOT EXISTS permite ejecutar la celda muchas veces sin error"),
    ("CREATE SCHEMA IF NOT EXISTS landing", "esquema que contiene el Volume de llegada"),
    ("CREATE VOLUME", "reemplaza al bucket S3 del proyecto original"),
]),
"01_bronze_load": ([
    ("dbutils.widgets.dropdown", "lee los dos parámetros del notebook y arma las rutas y los nombres de tabla"),
    ("SCHEMAS = {", "declara las columnas y los tipos de cada fuente, en vez de dejar que Spark los adivine"),
    ("inicio =", "prepara la lectura de los archivos pendientes y les agrega de qué archivo vino cada fila y cuándo se leyó"),
    ("(df.writeStream", "carga en la tabla Bronze lo que Auto Loader todavía no había procesado y cuenta cuántas filas entraron"),
    ('if load_mode == "incremental":', "en la carga incremental, pasa a la tabla de paso las filas que Bronze tiene y ella todavía no"),
    ("display(spark.table(bronze_table)", "comprobación: cuántas filas aportó cada archivo"),
], [
    ('dbutils.widgets.dropdown("data_source"', "desplegable: qué fuente se carga"),
    ('dbutils.widgets.dropdown("load_mode"', "desplegable: carga inicial o lote nuevo"),
    ('data_source = dbutils', "valor elegido (el job lo envía como parámetro)"),
    ("landing_path =", "donde esperan los archivos"),
    ("checkpoint_path =", "donde Auto Loader anota lo que ya cargó"),
    ("bronze_table =", "tabla Bronze de esta fuente"),
    ("staging_table =", "tabla de paso: solo el lote nuevo"),
    ('"products":', "el identificador llega sucio: se lee como texto"),
    ("schema = SCHEMAS", "el esquema de la fuente elegida"),
    ("inicio =", "hora de inicio: solo para informar cuántas filas entraron en esta ejecución"),
    ('df = (spark.readStream.format("cloudFiles")', "cloudFiles = Auto Loader"),
    ('.option("header", True)', "la primera fila trae los nombres de columna"),
    ('.option("rescuedDataColumn"', "lo que no encaje en el esquema se guarda aquí en vez de perderse"),
    (".schema(schema)", "esquema declarado, sin inferencia"),
    (".load(landing_path)", "vigila la carpeta completa"),
    ('.withColumn("read_timestamp"', "cuándo se leyó"),
    ('.withColumn("load_mode"', "con qué tipo de carga entró: inicial o incremental"),
    ('.select("*", "_metadata.file_name"', "de qué archivo vino cada fila y cuánto pesaba"),
    ('.option("checkpointLocation", checkpoint_path)', "la memoria de qué archivos ya se cargaron"),
    (".trigger(availableNow=True)", "procesa lo pendiente y termina, como un lote"),
    (".toTable(bronze_table)", "agrega las filas a la tabla; la crea si no existe"),
    (".awaitTermination())", "espera a que la carga termine antes de seguir"),
    ("n_nuevas =", "las filas que entraron en esta ejecución"),
    ('print("Filas nuevas', "cifra de control: la validación la compara con lo esperado"),
    ('spark.sql(f"ALTER TABLE {bronze_table} SET TBLPROPERTIES', "deja registrados los cambios fila por fila"),
    ('pendientes = bronze.filter', "solo lo que entró por la carga incremental"),
    ("ya_en_paso =", "lo más reciente que la tabla de paso ya contiene"),
    ("pendientes = pendientes.filter", "pendiente = más reciente que eso"),
    ("if n_pendientes > 0:", "sin pendientes, la tabla de paso queda como estaba"),
    (".saveAsTable(staging_table))", "el lote nuevo, solo, para que Silver procese únicamente eso"),
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
    ("df_s = (df_s", "normaliza categoría y nombre, y separa la variante que viene entre paréntesis"),
    ("df_s = (df_s", "corrige el error «Protien» en el nombre y en la categoría"),
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
    ('.withColumn("product_code"', "huella del nombre: el mismo nombre produce siempre el mismo código"),
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
    ("REGLAS = {", "declara las reglas de calidad y la función que aparta las filas que no las cumplen"),
    ('df = aplicar_regla(df, "fila_malformada")', "aplica las dos primeras reglas y marca los clientes inválidos"),
    ('df = df.withColumn("order_placement_date",', "deja todas las fechas en un solo formato"),
    ('df = df.withColumn("product_id"', "deja una sola fila por orden: si la llave se repite, gana la más reciente"),
    ("df_products =", "trae el código de producto y aplica la segunda regla"),
    ("quarantine_table =", "guarda las filas rechazadas con su motivo"),
    ('silver_table =', "guarda en Silver y declara las restricciones de la tabla"),
], [
    ("from delta.tables import DeltaTable", "necesario para MERGE"),
    ("from pyspark.sql.window", "para ordenar filas dentro de un grupo"),
    ('if load_mode == "incremental"', "lote nuevo: la tabla de paso"),
    ("n_bronze =", "cifra de control: 5.050 en la carga inicial"),
    ('"fila_malformada":', "la fila no encajó en el esquema de Bronze: no se puede confiar en sus valores"),
    ('"sin_cantidad":', "sin cantidad no hay venta que medir"),
    ('registrar_cargas(f"{silver_schema}.orders")', "anota esta carga en el registro que lee la validación"),
    ('"producto_inexistente":', "sin producto en el maestro no hay precio"),
    ("rechazadas = []", "aquí se acumulan las filas apartadas"),
    ('malas = df.filter', "las que NO cumplen la regla"),
    ('F.col("order_qty").cast("int").alias("order_qty"), F.col("file_name"), F.lit(nombre).alias("motivo")))', "con el nombre de la regla como motivo"),
    ("return df.filter(REGLAS[nombre])", "siguen adelante las que sí la cumplen"),
    ('F.when(F.col("customer_id").rlike', "solo dígitos = cliente válido"),
    ('.otherwise(F.lit("999999"))))', "cliente inválido: se marca, la venta se conserva"),
    ('print("Sin qty nula:"', "cifra de control: 4.757"),
    ('F.regexp_replace(F.col("order_placement_date")', "quita el día de la semana del inicio"),
    ("F.coalesce(", "prueba los cuatro formatos; gana el primero que convierte"),
    ('print("Fechas sin parsear:"', "debe dar 0"),
    ('ultima = (Window.partitionBy', "agrupa por la llave de negocio"),
    ('.orderBy(F.col("read_timestamp").desc()', "la leída más tarde queda primera"),
    ('df = df.withColumn("rn"', "se queda con la primera de cada grupo"),
    ('print("Tras quitar repetidas:", n_dedup)', "cifra de control: 4.709"),
    ('df = aplicar_regla(df.join(df_products', "left: la orden sin producto queda con código nulo y la regla la aparta"),
    ('print("Tras quitar repetidas:", n_dedup, "| con producto válido:"', "cifra de control: 4.709 → 4.511"),
    ("df_q =", "las rechazadas por las dos reglas"),
    ('if load_mode == "initial" or not', "carga inicial: la cuarentena se reemplaza"),
    ('.delete(F.col("file_name").isin(archivos)))', "lote nuevo: primero se quitan las filas previas de ese archivo"),
    ('df_q.write.format("delta").mode("append")', "y se agregan las de esta carga"),
    ('.mode("overwrite")\n       .saveAsTable(silver_table))', "carga inicial: reemplaza todo"),
    ('.mode("overwrite").saveAsTable(f"{catalog}.{silver_schema}.staging_orders"))', "el lote limpio que leerá el notebook 08"),
    (".merge(df.alias(\"new\"),", "condición: las cuatro columnas que identifican una orden"),
    (".whenMatchedUpdateAll(condition=", "si existe, se actualiza solo cuando cambió la cantidad"),
    (".whenNotMatchedInsertAll().execute())", "si no existe, se inserta"),
    ('spark.sql(f"ALTER TABLE {silver_table} DROP CONSTRAINT', "permite repetir la celda sin error"),
    ('spark.sql(f"ALTER TABLE {silver_table} ADD CONSTRAINT', "desde aquí, Delta rechaza cualquier fila que la incumpla"),
]),
"06_gold_dims": ([
    ("from pyspark.sql import functions as F", "genera el calendario del año completo, con una fila por día"),
    ("unknown =", "construye las otras tres dimensiones a partir de Silver"),
    ("SHOW TABLES IN gold", "lista las tablas de Gold para confirmar"),
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
    ("fact_table =", "guarda fact_sales completa (carga inicial) y declara que ninguna venta puede quedar sin precio"),
    ("SELECT round(SUM(revenue), 2) AS revenue_usd, SUM(quantity)", "totales de control de la carga inicial"),
], [
    ('print("fact rows:"', "cifra de control: 4.511"),
    ('.mode("overwrite")', "la carga inicial crea la versión 0 de la tabla"),
    ('spark.sql(f"ALTER TABLE {fact_table} ADD CONSTRAINT', "un lote con una venta sin precio falla en vez de publicar revenue nulo"),
    ("FROM gold.fact_sales;", "esperado: 648.109,79 · 92.491 · 4.511"),
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
    ("FROM (DESCRIBE HISTORY gold.fact_sales);", "versión 0 = carga inicial; después, los dos MERGE"),
]),
"09_optimize_and_kpis": ([
    ("OPTIMIZE gold.fact_sales", "compacta los archivos de la tabla y los ordena por producto"),
    ("SELECT 'versión 0 (full load)'", "compara la tabla como estaba al inicio con la actual"),
], [
    ("OPTIMIZE gold.fact_sales", "ZORDER acelera los filtros por product_id"),
    ("FROM gold.fact_sales VERSION AS OF 0", "la tabla tal como quedó en su primera escritura"),
    ("SELECT 'actual'", "esperado: 4.511 filas antes, 4.786 ahora"),
    ("JOIN gold.dim_products dp ON f.product_code = dp.product_code", "la dimensión aporta el nombre y la categoría"),
    ("GROUP BY 1 ORDER BY 1;", "1 = la primera columna del SELECT"),
]),
"98_validacion": ([
    ("import json", "extrae las cifras reales de las tablas"),
    ("lineas = []", "compara cada cifra con la esperada y hace fallar el notebook si alguna difiere"),
    ("dbutils.notebook.exit", "todo coincide: devuelve las cifras a quien ejecutó el notebook"),
], [
    ("q = lambda s:", "atajo: ejecuta una consulta y devuelve sus filas"),
    ("m = {}", "aquí se acumulan todas las cifras"),
    ('m[t] = q(f"SELECT COUNT(*)', "conteo de filas de cada tabla"),
    ('m["cities"]', "clientes por ciudad"),
    ('m["products_999999"]', "productos marcados como inválidos"),
    ('m["orders_null_dates"]', "fechas sin convertir: debe ser 0"),
    ('m["fact_now"]', "totales de la tabla de ventas, hoy"),
    ('registrar_cargas("gold.fact_sales"', "pone el registro al día, por si una ejecución se cortó antes de anotar"),
    ("cargas = lambda tabla:", "las cargas anotadas de una tabla, de la más reciente a la más antigua"),
    ('m["fact_v0"]', "los totales de la primera carga completa, según el registro"),
    ('m["malformed"]', "filas que no encajaron en el esquema, por tabla de Bronze"),
    ('m["history"]', "qué insertó y actualizó cada escritura"),
    ('m["fact_orphans"]', "ventas que apuntan a una dimensión inexistente: debe ser 0"),
    ('m["fact_dup_keys"]', "ventas repetidas: debe ser 0"),
    ('m["silver_dup_keys"]', "órdenes repetidas en Silver: debe ser 0"),
    ('m["quarantine"] = {}', "filas en cuarentena por archivo y motivo"),
    ("fallas = comparar(m, esperado, lineas.append)", "la función y las cifras esperadas vienen del notebook esperado"),
    ("raise Exception", "el error deja la tarea, y el job, en rojo"),
]),
"99_reset_demo": ([
    ("for schema in", "borra todas las tablas de las tres capas"),
    ("# la memoria de Auto Loader", "borra los checkpoints"),
    ("# compatibilidad con la versión anterior", "si quedaron archivos en processed/, los devuelve a landing/"),
], [
    ('for t in spark.sql(f"SHOW TABLES', "recorre las tablas del esquema"),
    ('spark.sql(f"DROP TABLE IF EXISTS', "borra la tabla"),
    ("dbutils.fs.rm", "True = borra la carpeta con todo su contenido"),
    ("sources =", "las cinco carpetas de datos"),
    ("except Exception:", "si la carpeta no existe, no hay nada que mover"),
    ("dbutils.fs.mv", "de vuelta a landing/"),
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
    cuerpo = lambda l: (l[8:] if l.startswith("# MAGIC ") else l).strip()

    def coincide(i, clave):
        """La clave puede tener varias líneas: deben coincidir líneas consecutivas (inicio de cada una)."""
        partes = clave.split("\n")
        return all(i + k < len(L) and cuerpo(L[i + k]).startswith(p.strip()) for k, p in enumerate(partes))

    for clave, texto in lineas:
        partes = clave.split("\n")
        i = next((j for j in range(len(L)) if MARCA not in L[j] and (j + len(partes) - 1) not in usados and coincide(j, clave)), None)
        if i is None:
            raise SystemExit(f"no encuentro la línea: {clave!r}")
        i += len(partes) - 1  # el comentario va en la última línea de la clave
        magic = L[i].startswith("# MAGIC ")
        assert not L[i].rstrip().endswith("\\"), L[i]
        if magic and L[i].rstrip().endswith(";"):
            # en SQL, un comentario después del «;» final quedaría como sentencia vacía: va en su propia línea, antes
            L.insert(i, "# MAGIC -- " + texto)
            usados = {u + 1 if u >= i else u for u in usados} | {i, i + 1}
            continue
        L[i] = L[i].rstrip() + ("  -- " if magic else "  # ") + texto
        usados.add(i)
    propios = {"# MAGIC -- " + t for _, t in lineas}
    for clave, texto in cabeceras:
        i = next((j for j in range(len(L)) if MARCA not in L[j] and MARCA not in L[j - 1] and coincide(j, clave)), None)
        if i is None:
            raise SystemExit(f"no encuentro la celda: {clave!r}")
        magic = L[i].startswith("# MAGIC ")
        while L[i - 1] in propios:  # la cabecera va antes de los comentarios propios de esa línea
            i -= 1
        if MARCA in L[i - 1]:
            raise SystemExit(f"celda con dos cabeceras: {clave!r}")
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
