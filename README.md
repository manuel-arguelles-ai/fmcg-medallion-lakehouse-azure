# FMCG Medallion Lakehouse · material del taller

Proyecto del taller solidario «Construye tu portafolio de datos End-to-End» (Datanilo Community, 19-nov-2026). Un lakehouse completo en Databricks con Unity Catalog: de CSV sucios a un dashboard, con carga incremental por MERGE.

Adaptado del proyecto público [databricks-medallion-lakehouse-fmcg](https://github.com/takadlag/databricks-medallion-lakehouse-fmcg) (AWS/S3) a Azure Databricks con Volumes de Unity Catalog.

## Qué incluye

| Ruta | Qué es |
|---|---|
| `data/gen_data.py` | Generador de datos sintéticos con suciedad intencional (semilla fija) |
| `data/landing/` | Los 5 CSV ya generados: carga inicial e incremental |
| `notebooks/` | `00` setup · `01` Bronze · `02`–`05` Silver · `06`–`08` Gold · `09` optimización y KPIs · `98` validación · `99` reset · `run_all` orquestador |
| `qa/oraculo.py` | Recalcula en Python puro las cifras que debe producir el pipeline |
| `qa/esperado.json` | Las cifras esperadas |
| `deck/` | Las láminas del taller (`index.html`) |

## Requisitos

- Un workspace de Databricks con Unity Catalog y permiso para crear un catálogo llamado `fmcg` (o que alguien lo cree por ti).
- Cómputo: serverless, o un cluster de nodo único. Si creas el cluster a mano, elige «Nodo único» en la configuración: un cluster con 0 workers sin ese modo arranca sin ejecutores y los notebooks se quedan colgados.
- Probado en un workspace **Premium** de Azure Databricks creado con la prueba gratuita de Azure (Azure Free Trial), en DBR 16.4 y en serverless. Es el mismo entorno del taller. Unity Catalog requiere el nivel Premium.

## Pasos

1. Importa la carpeta `notebooks/` a tu workspace. Los notebooks deben quedar sin la extensión `.py` en el nombre (todos usan `%run ./utilities`).
2. Ejecuta `00_setup_catalog`. Si tu metastore no permite `CREATE CATALOG` sin ubicación, crea el catálogo `fmcg` desde el Explorador de catálogos y vuelve a ejecutar.
3. Sube los CSV al Volume respetando las carpetas:
   ```
   /Volumes/fmcg/landing/raw/initial/customers/landing/customers_batch.csv
   /Volumes/fmcg/landing/raw/initial/products/landing/products_batch.csv
   /Volumes/fmcg/landing/raw/initial/gross_price/landing/prices_batch.csv
   /Volumes/fmcg/landing/raw/initial/orders/landing/orders_batch.csv
   /Volumes/fmcg/landing/raw/incremental/orders/landing/orders_incremental_batch.csv
   ```
4. Ejecuta en orden:
   - `01_bronze_load` cuatro veces, con `load_mode = initial` y `data_source` = `customers`, `products`, `gross_price`, `orders`
   - `02_silver_customers`, `03_silver_products`, `04_silver_pricing`
   - `05_silver_orders` con `load_mode = initial`
   - `06_gold_dims`, `07_gold_fact_full_load`
   - `01_bronze_load` con `data_source = orders`, `load_mode = incremental`
   - `05_silver_orders` con `load_mode = incremental`
   - `08_gold_fact_incremental`, `09_optimize_and_kpis`

   O ejecuta `run_all`, que hace todo lo anterior. `run_all` parte de cero: si ya lo corriste, ejecuta antes `99_reset_demo`.
5. Comprueba con `98_validacion` y compara contra `qa/esperado.json`.

Para empezar de nuevo: `99_reset_demo` borra las tablas y devuelve los CSV a `landing/`.

## Cifras que deben salir

| Tabla | Filas |
|---|---|
| `bronze.customers` · `products` · `gross_price` · `orders` | 196 · 66 · 312 · 5.380 (5.050 tras la carga inicial) |
| `silver.customers` · `products` · `gross_price` · `orders` | 184 · 60 · 288 · 4.786 (4.511 tras la carga inicial) |
| `gold.dim_date` · `dim_customers` · `dim_products` · `dim_pricing` | 365 · 185 · 60 · 288 |
| `gold.fact_sales` | 4.511 tras la carga inicial · 4.786 tras el MERGE (275 insertadas, 20 actualizadas) |

Revenue: US$ 648.109,79 tras la carga inicial y US$ 684.393,99 al final. La segunda corrida del MERGE inserta 0 y actualiza 0.

## Hazlo tuyo

Cambia el rubro por uno que conozcas y conserva la arquitectura: tres capas, modelo estrella, carga incremental idempotente y una verificación independiente. En tu README cuenta qué decidiste y qué falló.
