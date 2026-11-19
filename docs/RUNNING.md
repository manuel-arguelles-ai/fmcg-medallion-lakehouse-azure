# Detailed run guide

[← Back to the README](../README.en.md)

## Step-by-step run, notebook by notebook

The Volume paths must end up like this:

```
/Volumes/fmcg/landing/raw/initial/customers/landing/customers_batch.csv
/Volumes/fmcg/landing/raw/initial/products/landing/products_batch.csv
/Volumes/fmcg/landing/raw/initial/gross_price/landing/prices_batch.csv
/Volumes/fmcg/landing/raw/initial/orders/landing/orders_batch.csv
/Volumes/fmcg/landing/raw/incremental/orders/landing/orders_incremental_batch.csv
```

Run order:

1. `01_bronze_load` four times, with `load_mode = initial` and `data_source` = `customers`, `products`, `gross_price`, `orders`
2. `02_silver_customers`, `03_silver_products`, `04_silver_pricing`
3. `05_silver_orders` with `load_mode = initial`
4. `06_gold_dims`, `07_gold_fact_full_load`
5. `01_bronze_load` with `data_source = orders` and `load_mode = incremental`
6. `05_silver_orders` with `load_mode = incremental`
7. `08_gold_fact_incremental`, `09_optimize_and_kpis`

Parameters are chosen in the dropdowns at the top of each notebook (`data_source` and `load_mode`).

To start over, `99_reset_demo` drops the tables and returns the CSV files to `landing/`. Always run it before repeating the pipeline: with no files in `landing/`, Bronze finishes without loading anything, and the query on version 0 of `fact_sales` assumes that version is the initial load.

## Compute, job, dashboard and notebook comments

- **Compute:** serverless or a single-node cluster. If you create the cluster by hand, choose "Single node": a cluster with 0 workers without that mode starts with no executors and the notebooks hang.
- **Job:** it is defined for an existing cluster. In `jobs/fmcg-medallion-pipeline.json`, replace `<CARPETA_DEL_PROYECTO>` (project folder) and `<ID_DE_TU_CLUSTER>` (your cluster id) and create it with `databricks jobs create --json @jobs/fmcg-medallion-pipeline.json`.
- **Dashboard:** import `dashboard/fmcg-ventas-gold.lvdash.json` from Workspace → Import and assign it a SQL warehouse.
- **Comments:** `python3 tools/comentar_notebooks.py --quitar` removes the guiding comments from the notebooks without touching the code; without the argument, it puts them back.
- **Data:** `python3 data/gen_data.py` regenerates the same CSV files (fixed seed).
