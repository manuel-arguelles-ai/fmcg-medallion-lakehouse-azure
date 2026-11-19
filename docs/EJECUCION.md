# Ejecución detallada

[← Volver al README](../README.md)

## Ejecución paso a paso, notebook por notebook

Sigue primero los pasos 1 a 3 del [README](../README.md#cómo-ejecutarlo): importar los notebooks, ejecutar `00_setup_catalog` y subir los CSV. `run_all` hace en una sola ejecución todo lo que sigue; esta guía es para ir notebook por notebook.

Las rutas del Volume deben quedar así:

```
/Volumes/fmcg/landing/raw/initial/customers/landing/customers_batch.csv
/Volumes/fmcg/landing/raw/initial/products/landing/products_batch.csv
/Volumes/fmcg/landing/raw/initial/gross_price/landing/prices_batch.csv
/Volumes/fmcg/landing/raw/initial/orders/landing/orders_batch.csv
/Volumes/fmcg/landing/raw/incremental/orders/landing/orders_incremental_batch.csv
```

Orden de ejecución:

1. `01_bronze_load` cuatro veces, con `load_mode = initial` y `data_source` = `customers`, `products`, `gross_price`, `orders`
2. `02_silver_customers`, `03_silver_products`, `04_silver_pricing`
3. `05_silver_orders` con `load_mode = initial`
4. `06_gold_dims`, `07_gold_fact_full_load`
5. `01_bronze_load` con `data_source = orders` y `load_mode = incremental`
6. `05_silver_orders` con `load_mode = incremental`
7. `08_gold_fact_incremental`, `09_optimize_and_kpis`

Al terminar, ejecuta `98_validacion`: compara las cifras y falla si alguna no coincide.

Los parámetros se eligen en los desplegables de la parte superior de cada notebook (`data_source` y `load_mode`).

Para empezar desde cero, `99_reset_demo` borra las tablas y la memoria de Auto Loader. No hace falta para repetir el pipeline: sin archivos nuevos, Bronze no carga nada y el resto deja las mismas cifras.

## Cómputo, job, dashboard y comentarios de los notebooks

- **Cómputo:** serverless o un cluster de un solo nodo. Si creas el cluster a mano, elige «Nodo único»: un cluster con 0 workers sin ese modo arranca sin ejecutores y los notebooks se quedan colgados.
- **Job con bundle:** `databricks bundle deploy -t dev` crea el job a partir de `databricks.yml`, en serverless, y `databricks bundle run -t dev fmcg_medallion_pipeline` lo ejecuta. `dev` escribe en el catálogo `fmcg_dev` y `prod` en `fmcg`: crea antes el que vayas a usar y sube los CSV a su Volume (`dbfs:/Volumes/<catálogo>/landing/raw`). Para apuntar a otro, añade `--var catalog=<catálogo>` a ambos comandos.
- **Job con JSON:** está definido para un cluster existente. En `jobs/fmcg-medallion-pipeline.json`, reemplaza `<CARPETA_DEL_PROYECTO>` (por ejemplo `/Users/<tu_usuario>/fmcg-medallion`) y `<ID_DE_TU_CLUSTER>` y créalo con `databricks jobs create --json @jobs/fmcg-medallion-pipeline.json`.
- **Dashboard:** importa `dashboard/fmcg-ventas-gold.lvdash.json` desde Workspace → Importar y asígnale un SQL warehouse. Sus consultas leen del catálogo `fmcg`: si usas otro, cambia el nombre en los conjuntos de datos.
- **Pruebas adicionales:** los notebooks de `qa/pruebas/` cargan `../../notebooks/utilities`, así que necesitan el repositorio completo en el workspace (una carpeta Git o los archivos que despliega el bundle), no solo la carpeta `notebooks/`.
- **Comentarios:** `python3 tools/comentar_notebooks.py --quitar` elimina los comentarios orientativos de los notebooks sin tocar el código; sin el argumento, los vuelve a poner.
- **Datos:** `python3 data/gen_data.py` regenera los mismos CSV (semilla fija).
