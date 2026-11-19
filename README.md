# FMCG Medallion Lakehouse en Azure Databricks

[![CI](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml/badge.svg)](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml)

🇪🇸 **Español** · 🇺🇸 [English](README.en.md)

La insignia `CI` comprueba que los datos y las cifras son reproducibles y que la evidencia guardada corresponde al código actual. No ejecuta el pipeline en Databricks ni puede comprobar que esa evidencia sea auténtica: confirma que corresponde a los notebooks actuales. El flujo de despliegue aparece en verde aunque se omita por falta de secretos.

> Adaptación a Azure y Unity Catalog del proyecto público [databricks-medallion-lakehouse-fmcg](https://github.com/takadlag/databricks-medallion-lakehouse-fmcg) (AWS). Qué viene del original y qué aporta esta versión: [Origen y autoría](#origen-y-autoría).

Un pipeline de datos de punta a punta para una distribuidora de alimentos de consumo masivo (FMCG): recibe archivos CSV con errores reales de captura, los limpia con reglas de negocio explícitas, los modela en estrella y los publica en un dashboard. Cada lote nuevo de órdenes se integra con `MERGE` sin duplicar ventas, y todas las cifras se comprueban contra un cálculo independiente.

| | |
|---|---|
| **Resultado** | 13 tablas Delta en tres capas (más dos de paso y una de cuarentena) · 4.786 líneas de venta · US$ 684.393,99 de revenue · 60 de 60 comprobaciones dentro del job |
| **Stack** | Azure Databricks · Unity Catalog · Delta Lake · Auto Loader · PySpark · Spark SQL · Lakeflow Jobs · Asset Bundles · AI/BI Dashboards · ADLS Gen2 |
| **Tiempo de ejecución** | Unos 5 minutos de principio a fin en serverless ([5 min 22 s en la última ejecución](qa/evidencia/ejecucion_job.json)) |
| **Datos** | Sintéticos y reproducibles: 5 archivos CSV, 5.954 filas, generados con semilla fija |

![Dashboard «FMCG · Ventas (Gold)»](docs/img/dashboard.png)

**Lo que demuestra este proyecto**

- **Carga incremental que no duplica:** un `MERGE` con llave de negocio que, ejecutado dos veces con el mismo lote, inserta 0 y actualiza 0. El pipeline completo se puede volver a ejecutar sin preparar nada.
- **Cifras verificadas:** un cálculo independiente en Python puro produce las cifras de control y el job falla por sí mismo si alguna no coincide; además, un flujo automático comprueba en cada cambio que ese cálculo sigue dando lo mismo y que coincide con la salida guardada de la última ejecución.
- **Reglas explícitas para los datos malos:** cada caso se corrige, se marca o va a una tabla de cuarentena con su motivo.

**Para verlo sin ejecutar nada:** la [comparación de las 60 comprobaciones](qa/evidencia/comparacion.txt), el [registro de la última ejecución](qa/evidencia/ejecucion_job.json) con la duración de cada tarea, la tabla de [qué pasa con cada dato malo](#qué-pasa-con-cada-dato-malo) y la [última verificación automática](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml).

## Contenido

[El problema](#el-problema) · [Arquitectura](#arquitectura) · [Stack](#stack) · [Modelo de datos](#modelo-de-datos) · [El pipeline, capa por capa](#el-pipeline-capa-por-capa) · [Carga incremental](#carga-incremental-e-idempotencia) · [Verificación](#verificación) · [Orquestación](#orquestación) · [Decisiones de diseño](#decisiones-de-diseño) · [Cómo ejecutarlo](#cómo-ejecutarlo) · [Estructura](#estructura-del-repositorio) · [Limitaciones](#limitaciones-y-siguientes-pasos) · [Origen y autoría](#origen-y-autoría)

## El problema

Una distribuidora recibe cada mes cuatro archivos de sus sistemas: clientes, productos, lista de precios y órdenes. Los archivos traen estos problemas:

| Fuente | Filas | Qué trae mal |
|---|---:|---|
| `customers` | 196 | 12 duplicados, nombres con espacios y minúsculas, 11 formas erróneas de escribir tres ciudades, ciudades vacías |
| `products` | 66 | 6 duplicados, categorías en minúscula, «Protien» por «Protein», 14 filas con identificador no numérico (12 productos distintos) |
| `gross_price` | 312 | El mes en 4 formatos de fecha distintos, 24 combinaciones producto-mes con dos precios |
| `orders` | 5.050 | 293 sin cantidad, 256 con cliente inválido, fechas en 4 formatos, 50 filas repetidas |
| `orders` (lote nuevo) | 330 | Lo mismo, más 20 correcciones de cantidad sobre órdenes que ya existen |

Estas son las cifras de los archivos tal como llegan. Más abajo, [qué pasa con cada dato malo](#qué-pasa-con-cada-dato-malo) muestra cuántas filas afecta cada regla: no siempre coinciden, porque una misma fila puede tener dos problemas y solo cuenta en la primera regla que la alcanza.

Las ciudades (Bengaluru, Hyderabad, New Delhi) y las categorías de producto vienen del proyecto original, que es de la India.

El negocio pregunta cuánto se vendió, de qué categoría y cómo evoluciona mes a mes. Para responder con cifras confiables hay que decidir qué hacer con cada dato malo, y poder demostrar que el resultado es correcto.

## Arquitectura

```mermaid
flowchart LR
    CSV["5 archivos CSV"] --> V[("Volume<br/>landing.raw")]
    V -- "01" --> B[("Bronze<br/>lo que llegó")]
    B -- "02–05" --> S[("Silver<br/>lo confiable")]
    S -- "06–08" --> G[("Gold<br/>modelo estrella")]
    G --> D["Dashboard"]
```

Los números sobre las flechas son los notebooks de cada paso. Todo vive en el catálogo `fmcg` de Unity Catalog, sobre ADLS Gen2; un job de 16 tareas ejecuta la cadena completa y termina comparando 60 cifras de control contra un cálculo independiente: si alguna difiere, el job falla.

Cada capa responde una pregunta distinta:

- **Bronze, qué llegó.** Los CSV tal cual, más la fecha de lectura y el archivo de origen de cada fila.
- **Silver, qué es confiable.** Sin duplicados, con tipos correctos, fechas normalizadas y las reglas de negocio aplicadas.
- **Gold, qué responde al negocio.** Un modelo estrella listo para consultar.

Los archivos llegan a un Volume de Unity Catalog, que en Azure es un contenedor de Data Lake Storage Gen2. El acceso lo gobierna el catálogo: los notebooks no llevan credenciales ni rutas de almacenamiento.

| El Volume en Databricks | El mismo almacenamiento en Azure |
|---|---|
| ![Volume raw](docs/img/ui-volume.png) | ![Contenedor en ADLS](docs/img/ui-adls.png) |

## Stack

Azure Databricks con Unity Catalog, Delta Lake sobre ADLS Gen2, Auto Loader, PySpark y Spark SQL, Lakeflow Jobs desplegados con Asset Bundles y AI/BI Dashboards.

<details>
<summary>Ver la tecnología de cada capa y para qué se usa</summary>

| Capa | Tecnología | Para qué |
|---|---|---|
| Plataforma | Azure Databricks (Premium), DBR 16.4 LTS y serverless | Cómputo y entorno de trabajo |
| Gobierno | Unity Catalog: catálogo, esquemas, Volume, linaje | Organización y trazabilidad |
| Almacenamiento | Delta Lake sobre ADLS Gen2 | Tablas con transacciones e historial de versiones |
| Transformación | PySpark y Spark SQL | Limpieza, reglas de negocio y modelado |
| Ingesta | Auto Loader (`cloudFiles`) con esquema declarado | Cargar cada archivo exactamente una vez |
| Carga incremental | Delta `MERGE` | Insertar lo nuevo y actualizar lo que cambió |
| Optimización | `OPTIMIZE` + `ZORDER` | Compactar archivos y acelerar filtros |
| Orquestación | Lakeflow Jobs | 16 tareas encadenadas con parámetros |
| Consumo | AI/BI Dashboards (Lakeview) | Indicadores de ventas |
| Verificación | Python puro, sin Spark | Cálculo independiente de las cifras esperadas |

</details>

## Modelo de datos

```mermaid
erDiagram
    dim_date ||--o{ fact_sales : "date"
    dim_customers ||--o{ fact_sales : "customer_code"
    dim_products ||--o{ fact_sales : "product_code"
    dim_pricing ||--o{ fact_sales : "product_id + mes"
    fact_sales {
        string order_id
        date date
        string customer_code
        string product_code
        string product_id
        int quantity
        double price
        double revenue
    }
    dim_date {
        date date
        int date_key
        int year
        int quarter
        int month
        string month_name
        int day
        string day_name
        boolean is_weekend
    }
    dim_customers {
        string customer_id
        string customer_name
        string city
    }
    dim_products {
        string product_code
        string product_id
        string division
        string category
        string product
        string variant
    }
    dim_pricing {
        string product_id
        date price_month
        double price
    }
```

- **Grano de `fact_sales`:** una fila por línea de orden. La llave es `order_id + date + customer_code + product_code`.
- **`revenue = quantity × price`**, con el precio vigente para ese producto en el mes de la orden.
- **`dim_customers` tiene 185 filas y Silver 184:** la fila adicional es el miembro `999999 · Cliente Desconocido`, que recibe las ventas cuyo cliente llegó inválido.
- **`dim_date` cubre el año completo**, no solo los días con ventas, para que los lotes futuros siempre encuentren su fecha.

Unity Catalog registra el linaje sin que nadie lo documente: de qué tablas y notebooks sale `fact_sales` y quién la consume.

![Linaje de fact_sales](docs/img/ui-linaje.png)

## El pipeline, capa por capa

Catorce notebooks, más uno generado con las cifras esperadas; los numerados van en orden de ejecución: `00` prepara el catálogo, `01` carga Bronze, `02` a `05` limpian en Silver, `06` a `08` construyen Gold y `09` optimiza. Están en [`notebooks/`](notebooks/).

<details>
<summary>Ver qué hace cada notebook y cuántas filas entran y salen</summary>

| Notebook | Qué hace | Entra → sale |
|---|---|---|
| [`00_setup_catalog`](notebooks/00_setup_catalog.py) | Crea el catálogo, los esquemas y el Volume | — |
| [`01_bronze_load`](notebooks/01_bronze_load.py) | Un solo notebook con parámetros para las 4 fuentes y los dos modos de carga. Ingesta con Auto Loader y esquema declarado: recuerda qué archivos ya cargó, y cada fila lleva su archivo de origen y su fecha de lectura | 196 · 66 · 312 · 5.050 |
| [`02_silver_customers`](notebooks/02_silver_customers.py) | Quita duplicados, normaliza nombres, corrige ciudades con un diccionario y una lista permitida | 196 → 184 |
| [`03_silver_products`](notebooks/03_silver_products.py) | Normaliza categoría y nombre, deriva la división comercial, genera un código determinista por producto (SHA-256 del nombre) | 66 → 60 |
| [`04_silver_pricing`](notebooks/04_silver_pricing.py) | Convierte el mes a fecha y deja un solo precio por producto y mes | 312 → 288 |
| [`05_silver_orders`](notebooks/05_silver_orders.py) | Aplica las reglas de calidad declaradas y envía a cuarentena las filas que no las cumplen, marca clientes inválidos, normaliza fechas y deja una fila por orden (gana la última) | 5.050 → 4.757 → 4.709 → 4.511 |
| [`06_gold_dims`](notebooks/06_gold_dims.py) | Construye las cuatro dimensiones | 365 · 185 · 60 · 288 |
| [`07_gold_fact_full_load`](notebooks/07_gold_fact_full_load.py) | Carga inicial de `fact_sales` con precio y revenue | 4.511 filas · US$ 648.109,79 |
| [`08_gold_fact_incremental`](notebooks/08_gold_fact_incremental.py) | Fusiona el lote nuevo con `MERGE` y demuestra la idempotencia | 4.511 → 4.786 |
| [`09_optimize_and_kpis`](notebooks/09_optimize_and_kpis.py) | `OPTIMIZE ZORDER`, consulta de una versión anterior y las consultas del dashboard | — |
| [`98_validacion`](notebooks/98_validacion.py) | Extrae las cifras reales, las compara con las esperadas y falla si alguna difiere | 60 comprobaciones |
| [`99_reset_demo`](notebooks/99_reset_demo.py) | Borra las tablas y la memoria de Auto Loader, para empezar desde cero | — |

</details>

Las celdas de código llevan un comentario que dice qué hacen, y las líneas clave tienen una explicación al final.

Al terminar, el esquema Silver queda así: las cuatro tablas limpias, la tabla de paso del lote nuevo y la de cuarentena.

![Esquema silver con la tabla orders_quarantine](docs/img/ui-catalogo.png)

### Qué pasa con cada dato malo

| Caso | Filas | Qué se hace | Por qué |
|---|---:|---|---|
| Orden sin cantidad | 293 | Va a cuarentena | Sin cantidad no hay venta que medir |
| Orden repetida | 48 | Se conserva la última leída | Misma orden, fecha, cliente y producto; si las dos vienen en el mismo archivo, gana la de mayor cantidad. El archivo trae 50; las otras 2 no tenían cantidad y ya salieron en el paso anterior |
| Orden con cliente inválido | 230 | Se conserva, con cliente `999999` | La venta ocurrió; solo falta saber de quién. La cifra es de la tabla final, con el lote nuevo incluido; el archivo inicial trae 256 y parte de esas filas sale por otra regla |
| Orden con producto inexistente | 198 | Va a cuarentena | Sin producto no hay precio para calcular el revenue |
| Producto con identificador no numérico | 12 | Se conserva, con `product_id` `999999` | El producto existe y se identifica por `product_code`. Los 12 comparten ese `product_id`, así que no tienen precio ni órdenes |
| Cliente sin ciudad válida | 19 | 4 reciben una ciudad confirmada (supuesto del caso) y 15 quedan como «Sin ciudad» | No se inventa un dato que nadie confirmó |

Las cifras de órdenes sin cantidad, repetidas y con producto inexistente corresponden a la carga inicial. El cálculo independiente las produce paso por paso (5.050 → 4.757 → 4.709 → 4.511) y el job comprueba las filas en cuarentena por archivo y motivo: 293 y 198 del archivo inicial, 16 y 10 del lote nuevo.

## Carga incremental e idempotencia

Llega un archivo nuevo con 330 órdenes. Después de Silver quedan 295 válidas: 275 son órdenes nuevas y 20 corrigen la cantidad de órdenes que ya existían.

```python
(DeltaTable.forName(spark, fact_table).alias("t")
    .merge(df_fact_new.alias("s"),
           "t.order_id = s.order_id AND t.date = s.date "
           "AND t.customer_code = s.customer_code AND t.product_code = s.product_code")
    .whenMatchedUpdateAll(condition="NOT (t.quantity <=> s.quantity AND t.price <=> s.price AND t.revenue <=> s.revenue)")
    .whenNotMatchedInsertAll()
    .execute())
```

| | Insertadas | Actualizadas | `fact_sales` |
|---|---:|---:|---:|
| Primera ejecución | 275 | 20 | 4.511 → 4.786 |
| Segunda ejecución, mismo lote | 0 | 0 | 4.786 |

Los jobs fallan a medias y se reintentan. Una carga que duplica en el reintento infla las ventas; esta no cambia nada la segunda vez.

- **La cantidad queda fuera de la llave a propósito.** Las 20 correcciones del lote cambian la cantidad de una orden existente: con la cantidad en la llave serían 20 ventas nuevas, no 20 actualizaciones.
- **La condición con `<=>`** compara tratando a `NULL` como un valor más, así que solo se reescriben las filas que de verdad cambiaron.
- **Silver y Gold** usan `MERGE` con una condición de cambio: solo reescriben una fila si su cantidad (o su precio) cambió. Gold lo prueba en el propio notebook, que falla si la segunda ejecución no da 0 y 0; la validación comprueba además que, en ambas capas, todo `MERGE` posterior al primero no cambió nada.
- **Bronze** usa Auto Loader: su *checkpoint* anota cada archivo procesado y avanza junto con la escritura, así que un archivo queda cargado exactamente una vez aunque la ejecución se interrumpa y se repita.
- **Probado, no solo diseñado:** en serverless, una segunda ejecución completa sin reset terminó en verde con las mismas 60 comprobaciones, y lo mismo el orquestador `run_all` en un cluster. Con una orden inesperada añadida en un archivo nuevo, el job terminó en rojo y señaló las 11 cifras afectadas (esa prueba se hizo con la versión anterior de la ingesta). De estas pruebas se conserva el registro del resultado, no las salidas completas.
- **Qué prueba cada cosa:** la idempotencia del `MERGE` la demuestra el notebook `08`, que lo ejecuta dos veces con el mismo lote. Una re-ejecución completa sin reset llega al mismo resultado por otro camino: Bronze no carga nada nuevo y Silver y la tabla de hechos se reconstruyen desde todo Bronze.
- **Límites conocidos:** si la ejecución se interrumpe justo después de que Auto Loader cargue un lote nuevo y antes de escribir su tabla de paso, el reintento no la reescribe y ese lote no llega a Silver hasta la siguiente carga completa. Y la validación lee el historial de las tablas (primer `MERGE`, versión 0): vale mientras ese historial se conserve.

| El `MERGE` ejecutado | Segunda ejecución e historial de la tabla |
|---|---|
| ![MERGE](docs/img/nb-merge.png) | ![Idempotencia](docs/img/nb-idempotencia.png) |

Cada escritura crea una versión de la tabla, y se puede volver a consultar mientras dure la retención del historial: `VERSION AS OF 0` devuelve las 4.511 filas de la carga inicial aun después del `MERGE` y de la compactación.

![Consulta de la versión 0 de fact_sales](docs/img/nb-timetravel.png)

## Verificación

1. [`qa/oraculo.py`](qa/oraculo.py) recalcula desde los CSV, en Python puro y sin Spark, todo lo que el pipeline debe producir. El resultado está en [`qa/esperado.json`](qa/esperado.json).
2. El notebook `98_validacion` extrae las cifras reales de las tablas y las compara, dentro del job, con las esperadas. Si una sola difiere, lanza un error y la ejecución queda en rojo.
3. Son 60 comprobaciones: filas por tabla, clientes por ciudad, productos por categoría, revenue por categoría y por mes, historial de `MERGE` en Silver y en Gold, llaves duplicadas, ventas sin dimensión y filas en cuarentena por archivo y motivo.

Resultado: 60 de 60. La salida real de la última ejecución del job (en serverless, desplegado con el bundle) y su comparación están guardadas en [`qa/evidencia/`](qa/evidencia/), junto con el registro de las pruebas adicionales.

Un flujo de GitHub Actions ([`ci.yml`](.github/workflows/ci.yml)) repite en cada cambio lo que no necesita Databricks: regenera los datos y comprueba que salen idénticos, recalcula las cifras esperadas y las compara con las publicadas, comprueba que el notebook de validación usa esas mismas cifras, compara la salida guardada de la última ejecución, y comprueba que esa evidencia corresponde a los notebooks actuales y que sus comentarios no alteran el código Python.

El cálculo independiente comparte las reglas de negocio pero no el motor. Detecta errores de implementación; no detecta una regla mal elegida.

```bash
python3 qa/oraculo.py                      # recalcula las cifras esperadas desde los CSV
python3 qa/comparar.py salida_98.json      # compara la salida del notebook 98; termina con error si algo difiere
python3 qa/comparar.py --run <id_tarea>    # lo mismo, leyendo la ejecución con la CLI de Databricks
```

`salida_98.json` es el JSON que imprime la última celda del notebook `98_validacion`: cópialo a un archivo. Si usas el job, `<id_tarea>` es el identificador de la ejecución de la tarea `t98_validacion`.

## Orquestación

Un job de Lakeflow encadena 16 tareas: setup, cuatro cargas Bronze con parámetros distintos sobre el mismo notebook, Silver, Gold, el lote incremental, la optimización y la validación. Si una tarea falla, las siguientes no corren y la ejecución se repara desde la que falló.

![Ejecución del job: 16 tareas](docs/img/ui-corrida.png)

El job está definido como Databricks Asset Bundle en [`databricks.yml`](databricks.yml) (y como JSON en [`jobs/`](jobs/fmcg-medallion-pipeline.json)); la definición del dashboard está en [`dashboard/fmcg-ventas-gold.lvdash.json`](dashboard/fmcg-ventas-gold.lvdash.json).

## Decisiones de diseño

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| Volume de Unity Catalog para la llegada de archivos | Montar un bucket o un contenedor con credenciales | El acceso lo gobierna el catálogo y los notebooks no llevan secretos |
| Marcar clientes inválidos como `999999` y conservar la venta | Descartar la orden | La venta ocurrió; perderla distorsiona el revenue. El miembro «desconocido» la mantiene visible |
| `MERGE` con llave de cuatro columnas y condición de cambio | Agregar filas (`append`) o reemplazar todo | Reintentar no duplica, y solo se reescribe lo que cambió |
| Verificación con un cálculo independiente | Confiar en que el notebook corrió sin error | Que corra no prueba que el número sea correcto |

La tabla completa de decisiones, lo que no salió a la primera y todas las limitaciones están en [`docs/DISENO.md`](docs/DISENO.md).

## Cómo ejecutarlo

Necesitas un workspace de Databricks con Unity Catalog y permiso para crear un catálogo llamado `fmcg`. Probado en Azure Databricks Premium (creado con la prueba gratuita de Azure), en serverless y en un cluster de un solo nodo con DBR 16.4 LTS.

1. **Importa** la carpeta `notebooks/` a tu workspace. Al importar, Databricks quita la extensión `.py` del nombre.
   ```bash
   databricks workspace import-dir notebooks /Users/<tu_usuario>/fmcg-medallion
   ```
2. **Ejecuta** `00_setup_catalog`. Si tu metastore no permite `CREATE CATALOG` sin ubicación, crea el catálogo `fmcg` desde el Explorador de catálogos y vuelve a ejecutar.
3. **Sube los CSV** de `data/landing/` al Volume, respetando las carpetas:
   ```bash
   databricks fs cp -r data/landing dbfs:/Volumes/fmcg/landing/raw
   databricks fs ls dbfs:/Volumes/fmcg/landing/raw/initial/orders/landing   # debe listar orders_batch.csv
   ```
   Requiere la [CLI de Databricks](https://learn.microsoft.com/azure/databricks/dev-tools/cli/) autenticada (`databricks auth login`). También puedes subirlos desde el Explorador de catálogos.
4. **Ejecuta** `run_all`, que corre todo el pipeline en orden y termina con la validación: si una cifra no coincide, falla.

El pipeline se puede volver a ejecutar sin preparar nada: una segunda ejecución completa deja las mismas cifras. `99_reset_demo` solo hace falta para empezar desde cero. El paso a paso notebook por notebook, las opciones de cómputo y cómo crear el job y el dashboard están en [`docs/EJECUCION.md`](docs/EJECUCION.md).

## Estructura del repositorio

```
├── notebooks/     14 notebooks en formato fuente de Databricks (00–09, 98, 99, run_all, utilities) y `esperado`, generado
├── data/          gen_data.py y los 5 CSV ya generados (carga inicial e incremental)
├── qa/            oraculo.py (cálculo independiente), esperado.json (cifras esperadas), comparar.py y evidencia/ de la última ejecución
├── databricks.yml definición del job como Databricks Asset Bundle
├── jobs/          la misma definición en JSON
├── dashboard/     definición del dashboard de ventas
├── tools/         utilidad para quitar o poner los comentarios de los notebooks
├── docs/          guía de ejecución, decisiones de diseño y limitaciones, y capturas del README
└── .github/       verificación automática en cada cambio
```

## Limitaciones y siguientes pasos

- **Los datos son sintéticos.** Están diseñados para ejercitar cada regla de limpieza; no tienen el volumen ni las sorpresas de un sistema real. Por ejemplo, ningún cliente con identificador numérico falta en el maestro: la validación lo detectaría como venta sin dimensión, pero el pipeline no tiene una regla para ese caso. El precio más alto como precio vigente también es una regla del caso, no una verdad general.
- **Auto Loader corre por lotes** (`availableNow`), no como proceso continuo, y los archivos se quedan en `landing/` sin archivarse. Si se borran las tablas sin borrar los *checkpoints*, no vuelve a cargar nada: por eso el reset borra ambos. Los entornos `dev` y `prod` del bundle comparten el catálogo `fmcg`, y `prod` no se ha desplegado.
- **Silver y las dimensiones se reconstruyen completas** y no guardan historial de cambios (SCD tipo 2). Solo las órdenes y la tabla de hechos son incrementales.
- **La validación compara contra cifras fijas** porque los datos son fijos. Con datos reales harían falta reglas relativas (rangos, variaciones entre cargas), no igualdades. El despliegue desde GitHub está definido, pero no se ha ejecutado: necesita los secretos de un service principal.

Siguientes pasos: ejecutar el despliegue desde GitHub con un service principal, validación con reglas relativas entre cargas, expectativas de calidad con Lakeflow Declarative Pipelines, alertas ante fallos, catálogo distinto por entorno y SCD tipo 2 en clientes.

La lista completa está en [`docs/DISENO.md`](docs/DISENO.md#limitaciones-y-siguientes-pasos).

## Origen y autoría

Este proyecto parte de [databricks-medallion-lakehouse-fmcg](https://github.com/takadlag/databricks-medallion-lakehouse-fmcg), de Tanmay Kadlag, construido sobre AWS. De ahí vienen el caso de negocio, la estructura de notebooks por capa y las reglas de limpieza.

| Del proyecto original | Aporte de esta versión |
|---|---|
| Caso FMCG y estructura medallion | Migración de AWS/S3 a Azure con Volumes de Unity Catalog |
| Reglas de limpieza de Silver | Generador de datos sintéticos reproducible (el original no incluye datos) |
| Carga incremental con `MERGE` | Condición de cambio y prueba de idempotencia repetible |
| | Modelo autocontenido: se eliminó el paso que escribía en tablas que el original no incluye |
| | Verificación contra un cálculo independiente (60 comprobaciones) dentro del job, que falla si una cifra difiere |
| | Reglas de calidad declaradas, tabla de cuarentena y restricciones en las tablas |
| | Ingesta con esquema declarado y control de archivos ya cargados; pipeline re-ejecutable sin reset |
| | Job de 16 tareas, dashboard, notebook de reset y notebooks comentados |

Se presentó en el taller solidario «Construye tu portafolio de datos End-to-End» de Datanilo Community (noviembre de 2026). Las láminas del taller están en la rama [`taller`](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/tree/taller/deck).

**Manuel Argüelles** · AI Analytics Engineer · [LinkedIn](https://www.linkedin.com/in/manuel-arguelles-ai/) · [analizodatos.com](https://analizodatos.com)

Licencia [MIT](LICENSE).
