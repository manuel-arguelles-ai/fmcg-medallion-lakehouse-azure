# Detailed run guide

[← Back to the README](../README.en.md)

## Step-by-step run, notebook by notebook

First follow steps 1 to 3 of the [README](../README.en.md#how-to-run-it): import the notebooks, run `00_setup_catalog` and upload the CSV files. `run_all` does everything below in a single run; this guide is for going notebook by notebook.

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

When it finishes, run `98_validacion`: it compares the figures and fails if any of them does not match.

Parameters are chosen in the dropdowns at the top of each notebook (`data_source` and `load_mode`).

To start from scratch, `99_reset_demo` drops the tables and Auto Loader's memory. It is not needed to repeat the pipeline: with no new files, Bronze loads nothing and the rest leaves the same figures.

## Compute, job, dashboard and notebook comments

- **Compute:** serverless or a single-node cluster. If you create the cluster by hand, choose "Single node": a cluster with 0 workers without that mode starts with no executors and the notebooks hang.
- **Job with the bundle:** `databricks bundle deploy -t dev` creates the job from `databricks.yml`, on serverless, and `databricks bundle run -t dev fmcg_medallion_pipeline` runs it. `dev` writes to the `fmcg_dev` catalog and `prod` to `fmcg`; to point at another one, add `--var catalog=<catalog>` to both commands.
- **Job with JSON:** it is defined for an existing cluster. In `jobs/fmcg-medallion-pipeline.json`, replace `<CARPETA_DEL_PROYECTO>` (project folder) (for example `/Users/<your_user>/fmcg-medallion`) and `<ID_DE_TU_CLUSTER>` (your cluster id) and create it with `databricks jobs create --json @jobs/fmcg-medallion-pipeline.json`.
- **Dashboard:** import `dashboard/fmcg-ventas-gold.lvdash.json` from Workspace → Import and assign it a SQL warehouse.
- **Comments:** `python3 tools/comentar_notebooks.py --quitar` removes the guiding comments from the notebooks without touching the code; without the argument, it puts them back.
- **Data:** `python3 data/gen_data.py` regenerates the same CSV files (fixed seed).
