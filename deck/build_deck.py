#!/usr/bin/env python3
"""Genera deck/index.html del taller «Construye tu portafolio de datos End-to-End».
Uso: python3 deck/build_deck.py   (las cifras salen de qa/esperado.json, validado contra el pipeline real)"""
import html
import json
import os

AQUI = os.path.dirname(os.path.abspath(__file__))
E = json.load(open(os.path.join(AQUI, "..", "qa", "esperado.json")))


def n(v, dec=0):
    """Número con formato español: 4.511 · 648.109,79"""
    s = f"{v:,.{dec}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


REFS = {
    "repo": ("Repositorio de referencia · FMCG Medallion Lakehouse (takadlag)", "https://github.com/takadlag/databricks-medallion-lakehouse-fmcg"),
    "medallion": ("Microsoft Learn · Arquitectura medallion en Azure Databricks", "https://learn.microsoft.com/azure/databricks/lakehouse/medallion"),
    "volumes": ("Microsoft Learn · Volúmenes de Unity Catalog", "https://learn.microsoft.com/azure/databricks/volumes/"),
    "merge": ("Microsoft Learn · Upsert en una tabla Delta con MERGE", "https://learn.microsoft.com/azure/databricks/delta/merge"),
    "optimize": ("Microsoft Learn · OPTIMIZE: compactación de archivos de datos", "https://learn.microsoft.com/azure/databricks/delta/optimize"),
    "history": ("Microsoft Learn · Historial de tablas Delta y time travel", "https://learn.microsoft.com/azure/databricks/delta/history"),
    "dashboards": ("Microsoft Learn · Dashboards de AI/BI", "https://learn.microsoft.com/azure/databricks/dashboards/"),
    "jobs": ("Microsoft Learn · Lakeflow Jobs", "https://learn.microsoft.com/azure/databricks/jobs/"),
    "compute": ("Microsoft Learn · Configuración de cómputo (nodo único)", "https://learn.microsoft.com/azure/databricks/compute/configure"),
}


GH = "https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure"
NB = {"00": "00_setup_catalog", "01": "01_bronze_load", "02": "02_silver_customers", "03": "03_silver_products",
      "04": "04_silver_pricing", "05": "05_silver_orders", "06": "06_gold_dims", "07": "07_gold_fact_full_load",
      "08": "08_gold_fact_incremental", "09": "09_optimize_and_kpis", "98": "98_validacion", "99": "99_reset_demo"}
# Enlaces al workspace y al portal: solo existen en la copia del presentador (archivo local, fuera del repositorio).
_priv = os.path.join(AQUI, "links.local.json")
PRIV = json.load(open(_priv)) if os.path.exists(_priv) else {}


def gh(ruta=""):
    return GH + ("/" + ruta if ruta else "")


# lámina -> (enlaces al recurso real [etiqueta, clave de PRIV], enlaces al repositorio [etiqueta, ruta])
def _nb(*nums):
    return [(n_, "nb" + n_) for n_ in nums], [(n_, f"blob/main/notebooks/{NB[n_]}.py") for n_ in nums]


def _mas(par, vivos=(), repo=()):
    return par[0] + list(vivos), par[1] + list(repo)


ENLACES = {
    1: ([], [("repositorio del taller", "")]),
    9: ([], [("nuestra versión", "")]),
    11: ([("Volume raw", "volume")], [("gen_data.py", "blob/main/data/gen_data.py"), ("CSV", "tree/main/data/landing")]),
    12: ([("carpeta del proyecto", "carpeta")], [("notebooks/", "tree/main/notebooks")]),
    13: ([("Volume raw", "volume"), ("contenedor fmcg en Azure", "adls")], [("CSV", "tree/main/data/landing")]),
    14: _mas(_nb("00", "01"), [("esquema bronze", "bronze")]),
    15: _mas(_nb("02", "03", "04"), [("esquema silver", "silver")]),
    16: _mas(_nb("02"), [("silver.customers", "silver.customers")]),
    17: _mas(_nb("05"), [("silver.orders", "silver.orders")]),
    18: _mas(_nb("06"), [("esquema gold", "gold")]),
    19: _mas(_nb("07"), [("fact_sales", "fact")]),
    20: ([("catálogo fmcg", "catalogo")], []),
    21: _mas(_nb("08"), [("fact_sales", "fact")]),
    22: _mas(_nb("08"), [("historial", "historial")]),
    23: _mas(_nb("09"), [("historial", "historial")]),
    24: ([("linaje de fact_sales", "linaje")], []),
    25: _mas(_nb("09"), [("dashboard", "dashboard")]),
    26: ([("job", "job")], [("notebooks/", "tree/main/notebooks")]),
    27: ([("corrida", "corrida"), ("job", "job")], []),
    29: _mas(_nb("98"), repo=[("oraculo.py", "blob/main/qa/oraculo.py"), ("esperado.json", "blob/main/qa/esperado.json")]),
    31: ([], [("repositorio", ""), ("guía paso a paso", "blob/main/README.md")]),
    32: ([], [("repositorio", "")]),
}


def enlaces(i):
    vivos, repo = ENLACES.get(i, ([], []))
    a = lambda t, u: f"<a href='{html.escape(u, quote=True)}' target='_blank' rel='noopener'>{t}</a>"
    grupos = []
    v = [a(t, PRIV[k]) for t, k in vivos if k in PRIV]
    if v:
        grupos.append("<span><b>Abrir</b>" + "".join(v) + "</span>")
    if repo:
        grupos.append("<span><b>GitHub</b>" + "".join(a(t, gh(r)) for t, r in repo) + "</span>")
    return f"<span class='lk'>{''.join(grupos)}</span>" if grupos else ""


def mk(*keys):
    return "".join(
        f"<a class='mk' href='{REFS[k][1]}' target='_blank' rel='noopener' title='{html.escape(REFS[k][0])}' "
        f"aria-label='Fuente: {html.escape(REFS[k][0])}'>★</a>" for k in keys)


def code(s):
    return "<pre><code>" + html.escape(s.strip("\n")) + "</code></pre>"


def table(head, rows, num_cols=()):
    th = "".join(f"<th>{h}</th>" for h in head)
    body = ""
    for r in rows:
        body += "<tr>" + "".join(f"<td class='num'>{c}</td>" if i in num_cols else f"<td>{c}</td>" for i, c in enumerate(r)) + "</tr>"
    return f"<table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"


def ul(*items):
    return "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"


def pasos(items, extra=""):
    out = f"<div class='flow {extra}'>"
    for it in items:
        cls, titulo, texto = it[0], it[1], it[2]
        pie = f"<em>{it[3]}</em>" if len(it) > 3 else ""
        out += f"<div class='paso {cls}'><b>{titulo}</b><span>{texto}</span>{pie}</div>"
    return out + "</div>"


def kpis(items, extra=""):
    return f"<div class='kpis {extra}'>" + "".join(f"<div class='kpi'><b>{a}</b><span>{b}</span></div>" for a, b in items) + "</div>"


f0, f1, mg = E["fact_v0"], E["fact_now"], E["merge"]
cat_top = sorted(E["by_category"].items(), key=lambda kv: -kv[1])

# (clase, tiempo, kicker, título, cuerpo, nota del presentador)
S = []


def slide(cls, t, kicker, titulo, cuerpo, nota=""):
    S.append((cls, t, kicker, titulo, cuerpo, nota))


slide("hero", "0-3", "Taller solidario · Datanilo Community · 19 de noviembre de 2026",
      "En dos horas construimos un lakehouse completo: de CSV sucios a un dashboard",
      "<p class='sub'>Construye tu portafolio de datos End-to-End · Databricks en Azure, con Claude como copiloto</p>"
      + ul("Manuel Argüelles · AI Analytics Engineer",
           "Hoy miras cómo se construye, paso por paso; el material para repetirlo se comparte al terminar",
           "Todo lo recaudado va a la Navidad de los niños de Carabayllo"))

slide("", "3-6", "La causa",
      "Tu aporte de hoy es la Navidad de los niños de Carabayllo",
      kpis([("S/ 20", "Aporte solidario por Yape<br>967 048 578 · Junior Candela"),
            ("US$ 10", "Aporte desde el exterior por PayPal<br>paypal.me/leandrojt"),
            ("2 h", "En vivo por Zoom · 7:00 a 9:00 pm, hora de Perú")], "tres")
      + "<p class='note'>Organiza Datanilo Community junto con otras comunidades de datos de la región. Si ya aportaste, gracias: comparte el taller con alguien que quiera armar su portafolio.</p>")

slide("", "6-10", "Por qué un portafolio",
      "Un portafolio convence cuando muestra un problema resuelto de punta a punta",
      "<div class='cols'><div class='col'><h3>Lo que suele verse</h3>"
      + ul("Una lista de cursos y certificados", "Notebooks sueltos con datasets ya limpios", "Un gráfico final sin el camino que llevó a él")
      + "</div><div class='col'><h3>Lo que construimos hoy</h3>"
      + ul("Datos que llegan sucios, como en el trabajo real", "Decisiones explicadas: por qué cada capa, cada regla y cada llave",
           "Un resultado que cualquiera puede volver a ejecutar y comprobar")
      + "</div></div><p class='note'>La pregunta que responde un buen portafolio no es «qué herramientas conoces», sino «qué harías con mis datos el primer mes».</p>")

slide("table", "10-13", "La ruta de hoy",
      "Al terminar tendrás 13 tablas, una carga incremental que no duplica y un dashboard",
      table(["Min", "Bloque", "Notebook", "Qué queda construido"], [
          ["0–31", "El método con Claude, el proyecto de referencia y los datos", "—", "El plan: qué adaptamos y por qué"],
          ["31–42", "Setup y Bronze", "<code>00</code> · <code>01</code>", "4 tablas Bronze con su trazabilidad"],
          ["42–62", "Silver: limpieza y reglas de negocio", "<code>02</code> a <code>05</code>", "4 tablas Silver confiables"],
          ["62–74", "Gold: modelo estrella y carga inicial", "<code>06</code> · <code>07</code>", "4 dimensiones y <code>fact_sales</code>"],
          ["74–87", "Carga incremental con MERGE", "<code>01</code> · <code>05</code> · <code>08</code>", "Lote nuevo fusionado, sin duplicar"],
          ["87–102", "Optimización, dashboard y orquestación", "<code>09</code>", "Dashboard y job de punta a punta"],
          ["102–120", "Lo que falló, cómo se verificó y tu siguiente paso", "—", "Tu plan para replicarlo"],
      ])
      + "<p class='note'>Son 13 tablas finales más dos temporales que usa la carga incremental. Hay dos pausas para preguntas: al terminar Silver y al terminar el MERGE.</p>")

slide("", "13-17", "El método",
      "Claude acelera lo mecánico; el diseño y la verificación siguen siendo tuyos",
      pasos([
          ("", "1 · Referencia", "Le das un proyecto real como punto de partida, no una hoja en blanco.", "Tú eliges cuál"),
          ("", "2 · Decisiones", "Le pides opciones y eliges: dónde corre, qué se adapta, qué se elimina.", "Tú decides"),
          ("", "3 · Construcción", "Genera datos de prueba, notebooks y scripts. Aquí es donde más tiempo ahorra.", "Claude escribe"),
          ("", "4 · Verificación", "Todo se ejecuta y se compara contra un cálculo independiente.", "Tú exiges evidencia"),
          ("", "5 · Registro", "Cada error y cada decisión queda anotado para explicarlo después.", "Tu portafolio"),
      ], "cinco")
      + "<p class='note'>En cada bloque de la demo verás una línea «Con Claude»: qué hizo el asistente en ese paso y qué quedó en manos de la persona.</p>",
      "Mostrar en pantalla PROJECT.md y STATE.md como ejemplo de «registro».")

slide("", "", "La herramienta",
      "Claude trabaja dentro del proyecto: lee los archivos, ejecuta comandos y ve los errores reales",
      "<div class='cols'><div class='col'><h3>Cómo se usó aquí</h3>"
      + ul("Claude Code en la terminal, dentro de la carpeta del proyecto",
           "Con acceso a la línea de comandos de Databricks: importa notebooks, lanza el job y lee el resultado de cada tarea",
           "Cuando algo falla recibe el mensaje de error completo, sin que nadie se lo copie")
      + "</div><div class='col'><h3>Las reglas que recibió antes de empezar</h3>"
      + ul("Nunca iniciar sesión en Azure por mí ni mostrar credenciales en pantalla",
           "Todo lo que crea, modifica o borra en la nube se muestra antes como comando exacto",
           "Lo que no se ejecutó no se da por hecho: se anota como pendiente")
      + "</div></div><p class='note'>Las reglas viven en un archivo de texto del proyecto que el asistente lee al comenzar cada sesión. También puedes trabajar copiando y pegando en un chat: es más lento y el asistente no ve los errores por sí mismo, pero los hábitos de la lámina final aplican igual.</p>",
      "Mostrar el archivo de reglas (AGENTS.md) y STATE.md.")

slide("table", "", "El reparto del trabajo",
      "Cinco pedidos armaron el proyecto, y en cada uno la decisión final fue humana",
      table(["Lo que pedí", "Lo que hizo Claude", "Lo que decidí o corregí"], [
          ["Estudia este repositorio y dime si sirve de referencia para un taller de 2 horas", "Resumió fases y tecnologías; avisó que no trae datos y que apunta a tablas que no existen", "Usarlo como base y recortar el alcance"],
          ["No te adelantes: dime qué necesitas saber antes de construir", "Tres preguntas: dónde corre, qué alcance tiene la demo y qué material se comparte", "Mi suscripción de Azure, demo completa en vivo"],
          ["Genera datos de prueba que obliguen a aplicar cada limpieza", "Un generador con semilla fija: duplicados, fechas mezcladas, identificadores inválidos", "Revisar que cada defecto tuviera su regla"],
          ["Adapta los notebooks de AWS a Azure", "11 notebooks: del bucket S3 a un Volume, rutas relativas, un job que los encadena", "Quitar la capa que dependía de tablas inexistentes"],
          ["Termina los pendientes, bien validados", "Encontró la causa del bloqueo, corrigió 9 defectos y comparó 50 cifras contra un cálculo aparte", "Qué se muestra en vivo y qué se cuenta"],
      ])
      + "<p class='note'>Los pedidos están resumidos; el último es literal. Ninguno es un «prompt mágico»: lo que cambia el resultado es el contexto que se entrega y la verificación que se exige después.</p>")

slide("table", "", "Vocabulario",
      "Seis términos que vas a oír hoy, en una línea cada uno",
      table(["Término", "Qué es"], [
          ["Tabla Delta", "Una tabla guardada como archivos, con un historial de versiones: cada escritura queda registrada y se puede consultar"],
          ["Unity Catalog", "El catálogo de Databricks: organiza los datos en catálogo → esquema → tabla y controla quién accede a qué"],
          ["Volume", "Una carpeta de archivos administrada por el catálogo. Aquí llegan los CSV antes de convertirse en tablas"],
          ["MERGE", "Una sola operación que actualiza las filas que ya existen e inserta las que faltan (también se le dice upsert)"],
          ["Modelo estrella", "Una tabla central con los hechos que se miden (ventas) rodeada de tablas que los describen (cliente, producto, fecha)"],
          ["Cluster y serverless", "Dos formas de tener cómputo: una máquina que tú configuras y enciendes, o cómputo que Databricks entrega al momento"],
      ]))

slide("table", "17-20", "El punto de partida",
      "Partimos de un proyecto público en AWS y lo adaptamos a Azure con Unity Catalog" + mk("repo", "volumes"),
      table(["Proyecto de referencia (AWS)", "Nuestra versión (Azure)", "Por qué el cambio"], [
          ["Archivos leídos de un bucket S3 de AWS (<code>s3://…</code>)", "Archivos en un Volume de Unity Catalog", "El acceso lo gobierna el catálogo, sin credenciales en los notebooks"],
          ["Datos en un S3 privado, no incluidos", "Datos sintéticos con un generador propio", "Cualquiera puede reproducirlo desde cero"],
          ["Un paso final que escribe en tablas de una «empresa matriz» que el repo no incluye", "Eliminado: el modelo es autocontenido", "Un portafolio debe poder ejecutarse completo"],
          ["La validación de datos figura como trabajo futuro", "Validación contra un cálculo independiente", "Para poder afirmar que los números son correctos"],
      ])
      + "<p class='note'>Adaptar un proyecto existente y explicar cada diferencia es lo que harás al llegar a un equipo con código heredado.</p>")

slide("", "20-23", "La arquitectura",
      "Medallion separa tres preguntas: qué llegó, qué es confiable y qué responde al negocio" + mk("medallion"),
      pasos([
          ("bronce", "Bronze · qué llegó", "Los CSV tal cual, con fecha de lectura y nombre del archivo de origen.",
           f"4 tablas · {n(E['bronze.orders_initial'])} órdenes crudas en la carga inicial"),
          ("plata", "Silver · qué es confiable", "Sin duplicados, con tipos correctos, fechas normalizadas y reglas de negocio aplicadas.",
           f"4 tablas · {n(E['silver.orders_initial'])} órdenes válidas en la carga inicial"),
          ("oro", "Gold · qué responde al negocio", "Modelo estrella: una tabla de hechos de ventas y cuatro dimensiones.",
           "5 tablas · listas para el dashboard"),
      ])
      + "<p class='note'>Catálogo <code>fmcg</code> con los esquemas <code>bronze</code>, <code>silver</code>, <code>gold</code> y <code>landing</code>. El caso: una distribuidora de alimentos saludables con clientes, productos, lista de precios mensual y órdenes. Las ciudades (Bengaluru, Hyderabad, Nueva Delhi) vienen del proyecto de referencia, que es de la India.</p>")

slide("table", "23-28", "Los datos",
      "Los datos vienen sucios a propósito: sin suciedad no hay nada que demostrar",
      table(["Fuente", "Filas", "Qué trae mal", "Dónde se resuelve"], [
          ["<code>customers</code>", n(E["bronze.customers"]), "12 duplicados, nombres con espacios y minúsculas, 11 variantes de ciudad mal escritas, ciudades vacías", "notebook <code>02</code>"],
          ["<code>products</code>", n(E["bronze.products"]), f"6 duplicados, categorías en minúscula, «Protien» por «Protein», {E['products_999999']} identificadores no numéricos", "notebook <code>03</code>"],
          ["<code>gross_price</code>", n(E["bronze.gross_price"]), "Mes en 4 formatos de fecha distintos, 24 combinaciones producto-mes con dos precios", "notebook <code>04</code>"],
          ["<code>orders</code>", n(E["bronze.orders_initial"]), "293 sin cantidad, 256 con cliente inválido, fechas en 4 formatos (1.219 con el día de la semana delante), 50 filas repetidas", "notebook <code>05</code>"],
          ["<code>orders</code> incremental", "330", "Lo mismo, más 20 correcciones de cantidad sobre órdenes que ya existen", "notebooks <code>01</code>, <code>05</code> y <code>08</code>"],
      ], num_cols=(1,))
      + "<p class='note'>El generador (<code>data/gen_data.py</code>) usa una semilla fija: siempre produce los mismos archivos, y por eso los resultados se pueden comprobar.</p>")

slide("", "", "El proyecto en el workspace",
      "Todo el proyecto cabe en una carpeta: 14 notebooks numerados y un dashboard",
      "<div class='shot par'><img src='assets/ui-notebooks.png' alt='Carpeta taller-portafolio-datos en el workspace de Databricks con los notebooks 00 a 09, 98, 99, run_all, utilities y el dashboard'>"
      + ul("El número del nombre es el orden de ejecución: <code>00</code> prepara el catálogo, <code>01</code> a <code>09</code> construyen las capas",
           "<code>utilities</code> guarda los nombres compartidos; todos lo cargan con <code>%run</code>",
           "<code>98</code> valida, <code>99</code> deja todo en cero y <code>run_all</code> lo ejecuta completo")
      + "</div>")

slide("", "", "Dónde están los datos",
      "Los CSV llegan a un Volume, que en Azure es un contenedor de Data Lake",
      "<div class='shot dos'><img src='assets/ui-volume.png' alt='El Volume raw en el Explorador de catálogos de Databricks, carpeta initial/orders/processed con el archivo orders_batch.csv'><img src='assets/ui-adls.png' alt='El mismo almacenamiento en el portal de Azure: contenedor fmcg con la ruta administrada por Unity Catalog'>"
      + "</div><p class='note'>A la izquierda, el Volume en Databricks; a la derecha, el mismo almacenamiento en el portal de Azure. Unity Catalog administra las rutas: por eso en Azure se ven identificadores largos. El archivo está en <code>processed/</code> porque Bronze ya lo cargó.</p>")

slide("", "28-40", "Demo · notebooks 00 y 01",
      "Bronze guarda lo que llegó, tal cual, y anota de qué archivo vino cada fila" + mk("volumes"),
      "<div class='code-wrap'>" + code('''
df = (spark.read.format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(landing_path)
      .withColumn("read_timestamp", F.current_timestamp())
      .select("*", "_metadata.file_name",
                   "_metadata.file_size"))

df.write.format("delta") \\
  .option("delta.enableChangeDataFeed", "true") \\
  .mode("overwrite").saveAsTable(bronze_table)
''') + ul("Un solo notebook parametrizado sirve para las 4 fuentes y para los dos modos de carga",
           "<code>_metadata</code> da la trazabilidad sin escribir código extra",
           "Al terminar, el archivo pasa de <code>landing/</code> a <code>processed/</code>: la siguiente corrida solo ve lo nuevo",
           "Si no hay archivos nuevos, el notebook termina sin tocar nada")
      + "</div><p class='note'><strong>Con Claude:</strong> cambió las rutas del bucket por las del Volume y agregó el control de «sin archivos nuevos». <code>delta.enableChangeDataFeed</code> deja la tabla lista para que otro proceso lea solo los cambios.</p>",
      "Correr 00 (ya provisionado, tarda segundos) y 01 cuatro veces cambiando el widget data_source: customers, products, gross_price, orders.")

slide("table", "40-48", "Demo · notebooks 02 a 04",
      "En Silver cada regla de negocio se vuelve código con un antes y un después",
      table(["Tabla", "Bronze", "Silver", "Qué se aplicó"], [
          ["<code>customers</code>", n(E["bronze.customers"]), n(E["silver.customers"]),
           f"Sin duplicados; ciudades corregidas con diccionario y lista permitida; 4 ciudades confirmadas por negocio; {E['cities']['Sin ciudad']} quedan como «Sin ciudad»"],
          ["<code>products</code>", n(E["bronze.products"]), n(E["silver.products"]),
           "Categoría y nombre normalizados; división comercial derivada; <code>product_code</code> con SHA-256; identificadores inválidos → <code>999999</code>"],
          ["<code>gross_price</code>", n(E["bronze.gross_price"]), n(E["silver.gross_price"]),
           "Mes convertido a fecha; un solo precio por producto y mes (se conserva el más alto)"],
      ], num_cols=(1, 2))
      + "<p class='note'><strong>Con Claude:</strong> adaptó las transformaciones y agregó los conteos de control. Qué hacer con un dato malo (corregirlo, marcarlo o descartarlo) lo decide negocio; en el portafolio, cada regla debe poder explicarse en una frase. <code>product_code</code> es una huella del nombre: el mismo producto recibe siempre el mismo código.</p>",
      "En 02 mostrar el display de ciudades antes y después del diccionario. En 04 remarcar: sin el precio único, el join con la fact duplicaría ventas.")

slide("", "", "Demo · notebook 02",
      "El diccionario reduce catorce formas de escribir la ciudad a tres ciudades válidas",
      "<div class='shot par'><img src='assets/nb-silver-ciudad.png' alt='Notebook 02 ejecutado: el diccionario de ciudades y el conteo resultante con Bengaluru, Hyderabad, New Delhi y nulos'>"
      + ul("El diccionario corrige las variantes conocidas: mayúsculas, espacios y errores de tipeo",
           "La lista permitida convierte en nulo todo lo que no sea una ciudad válida",
           "Después, cuatro clientes reciben la ciudad que confirmó negocio y el resto queda como «Sin ciudad»")
      + "</div>")

slide("", "48-55", "Demo · notebook 05",
      "La tabla de órdenes reúne los cuatro problemas clásicos de cualquier fuente transaccional",
      "<div class='code-wrap'>" + code('''
# "Sunday, March 08, 2026" -> "March 08, 2026"
df = df.withColumn("order_placement_date",
    F.regexp_replace("order_placement_date",
                     r"^[A-Za-z]+,\\s*", ""))

# cuatro formatos: gana el primero que parsea
df = df.withColumn("order_placement_date",
    F.coalesce(
        to_date_safe("order_placement_date", "yyyy/MM/dd"),
        to_date_safe("order_placement_date", "dd-MM-yyyy"),
        to_date_safe("order_placement_date", "dd/MM/yyyy"),
        to_date_safe("order_placement_date", "MMMM dd, yyyy")))
''') + table(["Paso", "Filas"], [
          ["Llegan de Bronze", n(5050)],
          ["Sin cantidad nula", n(4757)],
          ["Sin duplicados", n(4709)],
          ["Con producto válido", f"<strong>{n(E['silver.orders_initial'])}</strong>"],
      ], num_cols=(1,))
      + "</div><p class='note'>De las órdenes que siguen en pie, 241 tienen cliente inválido: se conservan, marcadas con el cliente <code>999999</code>. Las 198 órdenes cuyo producto no existe en el maestro se descartan, porque no hay cómo ponerles precio. <strong>Con Claude:</strong> mantuvo la regla del proyecto de referencia (marcar, no borrar) y agregó el conteo de cada paso.</p>")

slide("", "55-65", "Demo · notebook 06",
      "Gold es un modelo estrella: una tabla de ventas y cuatro dimensiones que la explican",
      pasos([
          ("oro", "dim_date", "Calendario generado para todo el año, no solo para los días con ventas.", f"{n(E['gold.dim_date'])} filas"),
          ("oro", "dim_customers", "Cliente y ciudad, más el miembro <code>999999</code> «Cliente desconocido».", f"{n(E['gold.dim_customers'])} filas"),
          ("oro", "dim_products", "División, categoría, producto y variante.", f"{n(E['gold.dim_products'])} filas"),
          ("oro", "dim_pricing", "Precio vigente por producto y mes.", f"{n(E['gold.dim_pricing'])} filas"),
      ], "cuatro")
      + "<p class='sub'><code>fact_sales</code>: una fila por línea de orden, con cantidad, precio y <code>revenue = quantity × price</code>.</p>"
      + "<p class='note'>El calendario cubre fechas que todavía no tienen ventas, y existe un cliente «desconocido» para que ninguna venta quede sin su dimensión. Comprobado: ninguna venta apunta a un cliente, producto o fecha que no exista. <strong>Con Claude:</strong> al revisar los cruces detectó que las ventas con cliente inválido quedaban sin dimensión y propuso el cliente «desconocido».</p>")

slide("", "65-75", "Demo · notebook 07",
      f"La carga inicial deja {n(f0['rows'])} líneas de venta y {n(f0['revenue'] / 1000, 0)} mil dólares de revenue",
      kpis([(n(f0["rows"]), "líneas de orden en <code>fact_sales</code>"),
            (n(f0["units"]), "unidades vendidas"),
            ("US$ " + n(f0["revenue"], 2), "revenue de enero a mayo de 2026")], "tres")
      + code('''
SELECT o.order_id, o.order_placement_date AS date, o.customer_id AS customer_code,
       o.product_code, o.product_id, o.order_qty AS quantity, p.price,
       round(o.order_qty * p.price, 2) AS revenue
FROM fmcg.silver.orders o
LEFT JOIN fmcg.gold.dim_pricing p
       ON o.product_id = p.product_id
      AND trunc(o.order_placement_date, 'MM') = p.price_month
''')
      + "<p class='note'>El precio se busca por producto y por mes de la orden. Todas las líneas encontraron precio: 0 nulos.</p>")

slide("", "", "El catálogo",
      "El catálogo queda con tres esquemas y las tablas de cada capa",
      "<div class='shot solo'><img src='assets/ui-catalogo.png' alt='Explorador de catálogos: esquemas bronze, silver y gold del catálogo fmcg con cinco tablas cada uno'>"
      + "</div><p class='note'>Captura tomada al final del pipeline: <code>staging_orders</code> aparece en Bronze y en Silver porque es la tabla de paso del lote incremental. Sin esas dos, son las 13 tablas del proyecto.</p>")

slide("", "75-83", "Demo · notebook 08",
      "MERGE resuelve la carga incremental: actualiza lo que cambió e inserta lo nuevo" + mk("merge"),
      "<div class='shot par'><img src='assets/nb-merge.png' alt='Notebook 08 ejecutado: la función merge_fact y su salida, fact antes 4.511, después 4.786, insertadas 275, actualizadas 20'>" + table(["Lote incremental", "Filas"], [
          ["Llegan en el archivo", "330"],
          ["Válidas después de Silver", n(mg["staging_rows"])],
          ["Insertadas (órdenes nuevas)", f"<strong>{mg['ins']}</strong>"],
          ["Actualizadas (cantidad corregida)", f"<strong>{mg['upd']}</strong>"],
          ["<code>fact_sales</code> después", f"{n(f0['rows'])} → <strong>{n(f1['rows'])}</strong>"],
      ], num_cols=(1,))
      + "</div><p class='note'>Las cuatro columnas de la condición identifican una venta: si coinciden, es la misma fila. <code>&lt;=&gt;</code> compara dos valores tratando a NULL como un valor más; con eso solo se reescriben las filas que cambiaron. <strong>Con Claude:</strong> agregó esa condición al preparar la prueba de la segunda corrida.</p>",
      "Antes de 08: correr 01 con load_mode=incremental y 05 con load_mode=incremental.")

slide("", "83-90", "Demo · notebook 08",
      "Correrlo dos veces no cambia nada: 0 insertadas y 0 actualizadas",
      "<div class='shot par'><img src='assets/nb-idempotencia.png' alt='Notebook 08 ejecutado: segunda corrida con 0 insertadas y 0 actualizadas, y el historial Delta con las tres versiones de fact_sales'>"
      + ul("Los jobs fallan a medias y se reintentan: una carga que duplica en el reintento termina inflando las ventas",
           "Idempotente significa que repetir la operación deja el mismo resultado",
           "Aquí la garantizan la llave completa y la condición del MERGE; además, el archivo ya procesado pasa a <code>processed/</code> y no vuelve a entrar")
      + "</div>"
      + "<p class='note'>Si alguien te pregunta en una entrevista qué pasa si tu pipeline corre dos veces, esta es la demostración.</p>")

slide("", "90-97", "Demo · notebook 09",
      "La tabla se compacta y, aun así, puedes volver a cualquier versión anterior" + mk("optimize", "history"),
      "<div class='shot par'><img src='assets/nb-timetravel.png' alt='Notebook 09 ejecutado: consulta con VERSION AS OF 0 y su resultado, 4.511 filas en la versión 0 y 4.786 en la actual'>"
      + ul(f"Versión 0, la carga inicial: {n(f0['rows'])} filas y US$ {n(f0['revenue'], 2)}", f"Actual, tras el MERGE: {n(f1['rows'])} filas y US$ {n(f1['revenue'], 2)}", "<code>OPTIMIZE</code> junta archivos pequeños; <code>ZORDER</code> los ordena por la columna que más se filtra",
                      "Cada escritura crea una versión: el historial, que ya viste en el notebook <code>08</code>, dice qué operación la creó y cuántas filas tocó") + "</div>")

slide("", "", "Linaje",
      "Unity Catalog registra solo de dónde viene cada tabla y quién la usa",
      "<div class='shot par'><img src='assets/ui-linaje.png' alt='Pestaña Linaje de fact_sales: tablas de origen, notebooks y el job que la leen o la escriben'>"
      + ul("Ascendente: de qué tablas y notebooks sale <code>fact_sales</code>",
           "Descendente: qué notebooks, jobs y dashboards la consumen",
           "Nadie lo documentó a mano: se arma con cada ejecución",
           "Es lo primero que se mira antes de cambiar una tabla")
      + "</div>")

slide("", "97-103", "Demo · dashboard",
      "El dashboard cierra el ciclo: la tabla Gold responde las preguntas del negocio" + mk("dashboards"),
      "<div class='shot'><img src='assets/dashboard.png' alt='Dashboard FMCG · Ventas (Gold) con revenue total, unidades, órdenes, top 10 productos y revenue por categoría'>"
      + ul(f"Revenue total: US$ {n(f1['revenue'], 2)}",
           f"{n(f1['units'])} unidades en {n(f1['rows'])} líneas de orden",
           f"{cat_top[0][0]} y {cat_top[1][0]} lideran las categorías",
           "Más abajo, la tendencia mensual cae en junio: el lote incremental solo trae 15 días",
           "Las consultas son las mismas del notebook <code>09</code>; Claude armó el dashboard a partir de ellas")
      + "</div>")

slide("table", "103-108", "Orquestación",
      "Un pipeline de portafolio también se orquesta: 16 tareas en un job de 3 minutos y medio" + mk("jobs", "compute"),
      table(["Etapa", "Tareas del job", "Notebook"], [
          ["Setup", "<code>t00_setup</code>", "<code>00_setup_catalog</code>"],
          ["Bronze", "<code>t01_customers</code> · <code>t01_products</code> · <code>t01_pricing</code> · <code>t01_orders</code>", "<code>01_bronze_load</code> con 4 parámetros"],
          ["Silver", "<code>t02</code> · <code>t03</code> · <code>t04</code> · <code>t05_orders</code>", "<code>02</code> a <code>05</code>"],
          ["Gold", "<code>t06_dims</code> · <code>t07_fact_full</code>", "<code>06</code> · <code>07</code>"],
          ["Incremental", "<code>t01_orders_incr</code> · <code>t05_orders_incr</code> · <code>t08_fact_incr</code>", "<code>01</code> · <code>05</code> · <code>08</code>"],
          ["Cierre", "<code>t09_optimize</code> · <code>t98_validacion</code>", "<code>09</code> · <code>98</code>"],
      ])
      + "<p class='note'>Tiempos medidos el 17 de noviembre: 3 min 34 s en un cluster de nodo único (Standard_D4ds_v7, 4 núcleos) y unos 8 min en serverless, incluido el reset. Con <code>99_reset_demo</code> todo vuelve al punto de partida para ensayar otra vez.</p>")

slide("", "", "Orquestación",
      "La corrida completa: 16 tareas en verde, cada una espera a la anterior",
      "<div class='shot par'><img src='assets/ui-corrida.png' alt='Ejecución del job fmcg-medallion-pipeline en vista de lista: 16 tareas satisfactorias con su duración y la tarea de la que dependen'>"
      + ul("16 tareas, todas satisfactorias, en 3 minutos y 38 segundos", "«Depende de» encadena las tareas: si una falla, las siguientes no corren", "Una ejecución fallida se repara desde la tarea que falló, sin repetir las anteriores", "Cada tarea guarda el notebook ejecutado con sus salidas")
      + "</div>")

slide("table", "108-112", "Lo que falló",
      "Lo que no salió a la primera también va al portafolio",
      table(["Síntoma", "Primera hipótesis", "Causa real", "Cómo se encontró"], [
          ["Las tareas de Bronze quedaban «en ejecución» durante horas sin escribir nada", "Falta de capacidad en la región de Azure (hubo faltantes reales, pero no causaban el cuelgue)",
           "La máquina se creó sin máquinas auxiliares y sin activar el modo «nodo único»: arrancaba, pero sin nada que ejecutara el trabajo", "Leyendo la configuración del cluster, no reintentando"],
          ["Silver fallaba al convertir fechas", "—", "La función usada no existe en esa versión de PySpark", "El error lo decía; se cambió por una equivalente"],
          ["Riesgo de ventas duplicadas", "—", "24 combinaciones producto-mes con dos precios", "Revisando los datos antes de hacer el join"],
          ["La demostración de idempotencia no se podía repetir", "—", "El notebook borraba la tabla intermedia al terminar", "Ejecutando la demo completa dos veces"],
      ])
      + "<p class='note'>La primera hipótesis sobre el cluster también la propuso el asistente, parecía razonable y era falsa: costó una madrugada de reintentos. Antes de reintentar, pide la evidencia que distingue una causa de otra.</p>")

slide("", "112-115", "Verificación",
      "No le creas al pipeline: compáralo contra un cálculo independiente",
      "<div class='cols'><div class='col'><h3>Cómo se verificó</h3>"
      + ul("Un script en Python puro, sin Spark, recalcula desde los CSV lo que debe salir (<code>qa/oraculo.py</code>)",
           "El notebook <code>98_validacion</code> extrae las cifras reales de las tablas",
           "Se comparan 50 cifras: conteos por tabla, revenue por categoría y por mes, filas del MERGE, llaves duplicadas y huérfanos")
      + "</div><div class='col'><h3>Resultado</h3>"
      + ul("50 de 50 coinciden en cinco corridas: cluster de nodo único (tres veces), serverless y el orquestador <code>run_all</code>",
           "El script comparte las reglas pero no el motor: detecta errores de implementación, no una regla mal elegida",
           "Es la respuesta a «¿cómo sabes que está bien?»")
      + "</div></div><p class='note'>Pedirle al asistente que escriba la verificación por un camino distinto al del pipeline es una de las mejores formas de usarlo.</p>")

slide("", "115-117", "Trabajar con Claude",
      "Cinco hábitos mantienen el control de tu lado cuando trabajas con un asistente",
      ul("<strong>Dale contexto real:</strong> un repositorio de referencia, tus restricciones y lo que ya decidiste",
         "<strong>Pídele opciones, no solo código:</strong> las decisiones de diseño las tomas tú y las anotas",
         "<strong>Exige ejecución:</strong> código que no corrió es una hipótesis, no un resultado",
         "<strong>Desconfía de la primera explicación:</strong> pide la evidencia que la confirma o la descarta",
         "<strong>Deja registro:</strong> el estado, los errores y las decisiones por escrito son la mitad del portafolio"))

slide("", "117-119", "Para repetirlo",
      "Para repetirlo necesitas un workspace con Unity Catalog y el material del taller",
      pasos([
          ("", "1 · Entorno", "Un workspace de Databricks con Unity Catalog y permiso para crear un catálogo.", "El del taller: Azure Databricks Premium, creado con la prueba gratuita de Azure"),
          ("", "2 · Datos", "Genera los CSV con <code>data/gen_data.py</code> y súbelos al Volume.", "5 archivos, menos de 1 MB"),
          ("", "3 · Notebooks", "Importa la carpeta <code>notebooks/</code> y ejecútalos en el orden de la guía: el lote incremental repite <code>01</code> y <code>05</code> antes del <code>08</code>.", "O usa <code>run_all</code>, que hace todo"),
          ("", "4 · Comprueba", "Compara tus cifras con las de este deck: deben coincidir.", f"{n(f1['rows'])} filas · US$ {n(f1['revenue'], 2)}"),
      ], "cuatro")
      + "<p class='note'>El taller corre en un workspace Premium de Azure Databricks creado con la prueba gratuita de Azure (Azure Free Trial): ahí se probó todo, en DBR 16.4 y en serverless. El material (notebooks, datos, verificación y una guía paso a paso) está en <a href='" + GH + "' target='_blank' rel='noopener'>github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure</a>.</p>")

slide("hero", "119-120", "Tu siguiente paso",
      "Cambia el rubro, conserva la arquitectura y publícalo con tus decisiones explicadas",
      ul("Elige datos de un negocio que conozcas: una botica, un colegio, una empresa de transporte",
         "Mantén las tres capas, el modelo estrella, la carga incremental y la verificación",
         "Escribe un README que cuente qué decidiste y qué falló: eso es lo que se lee en una entrevista")
      + "<p class='sub'>Manuel Argüelles · linkedin.com/in/manuel-arguelles-ai · analizodatos.com</p>"
      + "<p class='note'>Gracias por sumarte a la Navidad de los niños de Carabayllo. Aporte solidario: Yape 967 048 578 (Junior Candela) · paypal.me/leandrojt</p>")

# ---------- render ----------
TIEMPOS = ['0-3', '3-5', '5-8', '8-10', '10-13', '13-16', '16-20', '20-23', '23-26', '26-28', '28-30', '30-31', '31-32', '32-42', '42-50', '50-54', '54-62', '62-69', '69-73', '73-74', '74-83', '83-87', '87-92', '92-93', '93-98', '98-101', '101-102', '102-106', '106-109', '109-113', '113-116', '116-120']
assert len(TIEMPOS) == len(S), len(S)
S = [(c, TIEMPOS[i], k, t, b, nt) for i, (c, _, k, t, b, nt) in enumerate(S)]
total = len(S)
secs = []
for i, (cls, t, kicker, titulo, cuerpo, nota) in enumerate(S, 1):
    aside = f"<aside class='pres' hidden>{html.escape(nota)}</aside>" if nota else ""
    secs.append(
        f"<section class='slide {cls}' id='s{i:02d}' data-n='{i}' data-time='{t}'>"
        f"<p class='kicker'><span>{kicker}</span>{enlaces(i)}</p><h2>{titulo}</h2>{cuerpo}{aside}"
        f"<footer class='foot'><span>Taller solidario · Portafolio de datos End-to-End</span><span>{i}/{total}</span></footer></section>")

def _li(t, u):
    return f"<li>{html.escape(t)} — <a href='{html.escape(u, quote=True)}' target='_blank' rel='noopener'>{html.escape(u if len(u) < 90 else u[:87] + '…')}</a></li>"


rec = [("Repositorio del taller", GH), ("Guía paso a paso", gh("blob/main/README.md")), ("Notebooks", gh("tree/main/notebooks")),
       ("Datos y generador", gh("tree/main/data")), ("Verificación", gh("tree/main/qa"))]
ETQ = {"carpeta": "Carpeta del proyecto en el workspace", "volume": "Volume raw", "adls": "Contenedor fmcg en el portal de Azure",
       "catalogo": "Catálogo fmcg", "fact": "Tabla gold.fact_sales", "historial": "Historial de fact_sales", "linaje": "Linaje de fact_sales",
       "dashboard": "Dashboard FMCG · Ventas (Gold)", "job": "Job fmcg-medallion-pipeline", "corrida": "Última corrida del job"}
rec += [(t, PRIV[k]) for k, t in ETQ.items() if k in PRIV] + [(f"Notebook {v}", PRIV["nb" + k]) for k, v in NB.items() if "nb" + k in PRIV]
recursos = "".join(_li(t, u) for t, u in rec)
refs = "".join(f"<li>{html.escape(t)} — <a href='{u}' target='_blank' rel='noopener'>{u}</a></li>" for t, u in REFS.values())
doc = f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Portafolio de datos End-to-End · Taller solidario</title>
<meta name="description" content="Taller solidario Datanilo Community: un lakehouse medallion completo en Databricks sobre Azure, de CSV sucios a un dashboard, con Claude como copiloto.">
<link rel="icon" href="data:,">
<link rel="stylesheet" href="deck.css">
</head>
<body class="tema-oscuro">
<div id="deck">
{chr(10).join(secs)}
</div>
<div id="bar" class="bar"></div>
<nav id="ctl" aria-label="Controles de la presentación">
  <button id="bPrev" aria-label="Slide anterior">←</button>
  <button id="bNext" aria-label="Slide siguiente">→</button>
  <span id="progress">1 / {total}</span>
  <button id="bTema" aria-pressed="true" aria-label="Cambiar tema">Tema: oscuro</button>
  <span id="escalaValor" hidden>100%</span>
  <button id="bMenos" aria-label="Reducir tipografía">A−</button>
  <button id="bMas" aria-label="Aumentar tipografía">A+</button>
  <button id="bMarcas" aria-expanded="false" aria-controls="refsPanel" aria-label="Abrir fuentes">★ Fuentes</button>
</nav>
<aside id="refsPanel" aria-label="Fuentes del taller">
  <button id="refsClose">Cerrar ✕</button>
  <h3>Fuentes del taller</h3>
  <ol>{refs}</ol>
  <h3>Recursos del proyecto</h3>
  <ul>{recursos}</ul>
</aside>
<script src="deck.js"></script>
</body>
</html>
"""
open(os.path.join(AQUI, "index.html"), "w", encoding="utf-8").write(doc)
print("slides:", total)
for i, s in enumerate(S, 1):
    print(f"{i:2d}. [{s[1]:>7}] {html.unescape(s[3].split('<a ')[0])}")
