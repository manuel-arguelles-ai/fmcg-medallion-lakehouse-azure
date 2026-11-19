# FMCG Medallion Lakehouse on Azure Databricks

[![CI](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml/badge.svg)](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml)

🇪🇸 [Español](README.md) · 🇺🇸 **English**

The `CI` badge checks that the data and the figures are reproducible and that the stored evidence matches the current code. It does not run the pipeline on Databricks, and it cannot check that the evidence is authentic: it confirms that it matches the current notebooks. The `despliegue` workflow does run it: with a service principal it deploys the bundle to `prod` and runs the whole pipeline, which turns red if a figure does not match ([recorded run](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/runs/44535633142)). In a fork without those secrets, the workflow is skipped and still shows green.

> An Azure and Unity Catalog adaptation of the public project [databricks-medallion-lakehouse-fmcg](https://github.com/takadlag/databricks-medallion-lakehouse-fmcg) (AWS). What comes from the original and what this version adds: [Origin and authorship](#origin-and-authorship).

An end-to-end data pipeline for a fast-moving consumer goods (FMCG) distributor: it takes CSV files with real-world data entry errors, cleans them with explicit business rules, models them as a star schema and publishes them in a dashboard. Each new batch of orders is integrated with `MERGE` without duplicating sales, and every figure is checked against an independent calculation.

| | |
|---|---|
| **Result** | 13 Delta tables across three layers (plus two staging tables and one quarantine table) · 4,786 sales lines · US$ 684,393.99 in revenue · 60 of 60 checks inside the job |
| **Stack** | Azure Databricks · Unity Catalog · Delta Lake · Auto Loader · PySpark · Spark SQL · Lakeflow Jobs · Asset Bundles · AI/BI Dashboards · ADLS Gen2 |
| **Run time** | About 5 minutes end to end on serverless ([5 min 22 s in the latest run](qa/evidencia/ejecucion_job.json)) |
| **Data** | Synthetic and reproducible: 5 CSV files, 5,954 rows, generated with a fixed seed |

![Dashboard "FMCG · Ventas (Gold)"](docs/img/dashboard.png)

**What this project demonstrates**

- **An incremental load that does not duplicate:** a `MERGE` on a business key that, run twice with the same batch, inserts 0 and updates 0. The whole pipeline can be run again with no preparation.
- **Verified figures:** an independent calculation in plain Python produces the control figures and the job fails by itself if any of them does not match; in addition, an automated workflow checks on every change that the calculation still gives the same result and that it matches the stored output of the latest run.
- **Explicit rules for bad data:** each case is fixed, flagged or sent to a quarantine table with its reason.

**To see it without running anything:** the [comparison of the 60 checks](qa/evidencia/comparacion.txt), the [record of the latest run](qa/evidencia/ejecucion_job.json) with the duration of each task, the table of [what happens to each bad record](#what-happens-to-each-bad-record) and the [latest automated check](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/workflows/ci.yml).

The notebooks, their comments and the screenshots are in Spanish.

## Contents

[The problem](#the-problem) · [Architecture](#architecture) · [Stack](#stack) · [Data model](#data-model) · [The pipeline, layer by layer](#the-pipeline-layer-by-layer) · [Incremental load](#incremental-load-and-idempotency) · [Verification](#verification) · [Orchestration](#orchestration) · [Design decisions](#design-decisions) · [How to run it](#how-to-run-it) · [Structure](#repository-structure) · [Limitations](#limitations-and-next-steps) · [Origin and authorship](#origin-and-authorship)

## The problem

A distributor receives four files from its systems every month: customers, products, price list and orders. The files come with these problems:

| Source | Rows | What is wrong |
|---|---:|---|
| `customers` | 196 | 12 duplicates, names with stray spaces and lowercase, 11 misspellings of three cities, empty cities |
| `products` | 66 | 6 duplicates, lowercase categories, "Protien" instead of "Protein", 14 rows with a non-numeric identifier (12 distinct products) |
| `gross_price` | 312 | The month in 4 different date formats, 24 product-month pairs with two prices |
| `orders` | 5,050 | 293 without quantity, 256 with an invalid customer, dates in 4 formats, 50 repeated rows |
| `orders` (new batch) | 330 | The same, plus 20 quantity corrections on orders that already exist |

These are the figures of the files as they arrive. Further down, [what happens to each bad record](#what-happens-to-each-bad-record) shows how many rows each rule affects: they do not always match, because one row can have two problems and it only counts under the first rule that reaches it.

The cities (Bengaluru, Hyderabad, New Delhi) and the product categories come from the original project, which is from India.

The business asks how much was sold, in which category, and how it evolves month by month. Answering with trustworthy figures means deciding what to do with each bad record, and being able to prove the result is correct.

## Architecture

```mermaid
flowchart LR
    CSV["5 CSV files"] --> V[("Volume<br/>landing.raw")]
    V -- "01" --> B[("Bronze<br/>what arrived")]
    B -- "02–05" --> S[("Silver<br/>what is trustworthy")]
    S -- "06–08" --> G[("Gold<br/>star schema")]
    G --> D["Dashboard"]
```

The numbers on the arrows are the notebooks of each step. Everything lives in the `fmcg` Unity Catalog catalog, on ADLS Gen2; a 16-task job runs the whole chain and ends by comparing 60 control figures against an independent calculation: if any of them differs, the job fails.

Each layer answers a different question:

- **Bronze, what arrived.** The CSV files as they are, plus the read timestamp and the source file of every row.
- **Silver, what is trustworthy.** No duplicates, correct types, normalized dates and business rules applied.
- **Gold, what answers the business.** A star schema ready to query.

Files land in a Unity Catalog Volume, which on Azure is a Data Lake Storage Gen2 container. Access is governed by the catalog: the notebooks carry no credentials and no storage paths.

| The Volume in Databricks | The same storage in Azure |
|---|---|
| ![Volume raw](docs/img/ui-volume.png) | ![ADLS container](docs/img/ui-adls.png) |

## Stack

Azure Databricks with Unity Catalog, Delta Lake on ADLS Gen2, Auto Loader, PySpark and Spark SQL, Lakeflow Jobs deployed with Asset Bundles and AI/BI Dashboards.

<details>
<summary>See the technology of each layer and what it is for</summary>

| Layer | Technology | Purpose |
|---|---|---|
| Platform | Azure Databricks (Premium), DBR 16.4 LTS and serverless | Compute and workspace |
| Governance | Unity Catalog: catalog, schemas, Volume, lineage | Organization and traceability |
| Storage | Delta Lake on ADLS Gen2 | Transactional tables with version history |
| Transformation | PySpark and Spark SQL | Cleaning, business rules and modeling |
| Ingestion | Auto Loader (`cloudFiles`) with a declared schema | Load each file exactly once |
| Incremental load | Delta `MERGE` | Insert what is new, update what changed |
| Optimization | `OPTIMIZE` + `ZORDER` | Compact files and speed up filters |
| Orchestration | Lakeflow Jobs | 16 chained tasks with parameters |
| Consumption | AI/BI Dashboards (Lakeview) | Sales indicators |
| Verification | Plain Python, no Spark | Independent calculation of the expected figures |

</details>

## Data model

```mermaid
erDiagram
    dim_date ||--o{ fact_sales : "date"
    dim_customers ||--o{ fact_sales : "customer_code"
    dim_products ||--o{ fact_sales : "product_code"
    dim_pricing ||--o{ fact_sales : "product_id + month"
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

- **Grain of `fact_sales`:** one row per order line. The key is `order_id + date + customer_code + product_code`.
- **`revenue = quantity × price`**, using the price in force for that product in the month of the order.
- **`dim_customers` has 185 rows and Silver has 184:** the extra row is the member `999999 · Cliente Desconocido` (unknown customer), which receives the sales whose customer arrived invalid.
- **`dim_date` covers the whole year**, not only the days with sales, so future batches always find their date.

Unity Catalog records lineage without anyone documenting it: which tables and notebooks `fact_sales` comes from, and who consumes it.

![Lineage of fact_sales](docs/img/ui-linaje.png)

## The pipeline, layer by layer

Fourteen notebooks, plus a generated one with the expected figures; the numbered ones go in run order: `00` prepares the catalog, `01` loads Bronze, `02` to `05` clean in Silver, `06` to `08` build Gold and `09` optimizes. They are in [`notebooks/`](notebooks/).

<details>
<summary>See what each notebook does and how many rows go in and out</summary>

| Notebook | What it does | In → out |
|---|---|---|
| [`00_setup_catalog`](notebooks/00_setup_catalog.py) | Checks that the catalog exists and creates the schemas and the Volume | — |
| [`01_bronze_load`](notebooks/01_bronze_load.py) | A single parameterized notebook for the 4 sources and both load modes. Ingestion with Auto Loader and a declared schema: it remembers which files it already loaded, and every row carries its source file and read timestamp | 196 · 66 · 312 · 5,050 |
| [`02_silver_customers`](notebooks/02_silver_customers.py) | Removes duplicates, normalizes names, fixes cities with a dictionary and an allow-list | 196 → 184 |
| [`03_silver_products`](notebooks/03_silver_products.py) | Normalizes category and name, derives the commercial division, generates a deterministic code per product (SHA-256 of the name) | 66 → 60 |
| [`04_silver_pricing`](notebooks/04_silver_pricing.py) | Converts the month to a date and keeps a single price per product and month | 312 → 288 |
| [`05_silver_orders`](notebooks/05_silver_orders.py) | Applies the declared quality rules and sends the rows that fail them to quarantine, flags invalid customers, normalizes dates and keeps one row per order (last one wins) | 5,050 → 4,757 → 4,709 → 4,511 |
| [`06_gold_dims`](notebooks/06_gold_dims.py) | Builds the four dimensions | 365 · 185 · 60 · 288 |
| [`07_gold_fact_full_load`](notebooks/07_gold_fact_full_load.py) | Initial load of `fact_sales` with price and revenue | 4,511 rows · US$ 648,109.79 |
| [`08_gold_fact_incremental`](notebooks/08_gold_fact_incremental.py) | Merges the new batch with `MERGE` and demonstrates idempotency | 4,511 → 4,786 |
| [`09_optimize_and_kpis`](notebooks/09_optimize_and_kpis.py) | `OPTIMIZE ZORDER`, a query on an earlier version and the dashboard queries | — |
| [`98_validacion`](notebooks/98_validacion.py) | Extracts the actual figures, compares them with the expected ones and fails if any differs | 60 checks |
| [`99_reset_demo`](notebooks/99_reset_demo.py) | Drops the tables and Auto Loader's memory, to start from scratch | — |

</details>

Code cells carry a comment saying what they do, and the key lines have a trailing explanation.

When it finishes, the Silver schema looks like this: the four clean tables, the staging table of the new batch and the quarantine table.

![Silver schema with the orders_quarantine table](docs/img/ui-catalogo.png)

### What happens to each bad record

| Case | Rows | What is done | Why |
|---|---:|---|---|
| Order without quantity | 293 | Sent to quarantine | Without a quantity there is no sale to measure |
| Repeated order | 48 | The last one read is kept | Same order, date, customer and product; if both come in the same file, the larger quantity wins. The file has 50; the other 2 had no quantity and were already dropped in the previous step |
| Order with an invalid customer | 230 | Kept, with customer `999999` | The sale happened; only the buyer is unknown. The figure is from the final table, new batch included; the initial file has 256 and part of those rows are dropped by another rule |
| Order with an unknown product | 198 | Sent to quarantine | Without a product there is no price to compute revenue |
| Product with a non-numeric identifier | 12 | Kept, with `product_id` `999999` | The product exists and is identified by `product_code`. All 12 share that `product_id`, so they have no price and no orders |
| Customer without a valid city | 19 | 4 get a confirmed city (an assumption of the case) and 15 stay as "Sin ciudad" (no city) | No value is invented that nobody confirmed |

The counts for orders without quantity, repeated orders and unknown products refer to the initial load. The independent calculation produces them step by step (5,050 → 4,757 → 4,709 → 4,511) and the job checks the quarantined rows per file and reason: 293 and 198 from the initial file, 16 and 10 from the new batch.

## Incremental load and idempotency

A new file arrives with 330 orders. After Silver, 295 are valid: 275 are new orders and 20 correct the quantity of orders that already existed.

```python
(DeltaTable.forName(spark, fact_table).alias("t")
    .merge(df_fact_new.alias("s"),
           "t.order_id = s.order_id AND t.date = s.date "
           "AND t.customer_code = s.customer_code AND t.product_code = s.product_code")
    .whenMatchedUpdateAll(condition="NOT (t.quantity <=> s.quantity AND t.price <=> s.price AND t.revenue <=> s.revenue)")
    .whenNotMatchedInsertAll()
    .execute())
```

| | Inserted | Updated | `fact_sales` |
|---|---:|---:|---:|
| First run | 275 | 20 | 4,511 → 4,786 |
| Second run, same batch | 0 | 0 | 4,786 |

Jobs fail halfway and get retried. A load that duplicates on retry inflates sales; this one changes nothing the second time.

- **Quantity is left out of the key on purpose.** The 20 corrections in the batch change the quantity of an existing order: with quantity in the key they would be 20 new sales, not 20 updates.
- **The `<=>` condition** compares treating `NULL` as just another value, so only the rows that really changed are rewritten.
- **Silver and Gold** use `MERGE` with a change condition: a row is only rewritten if its quantity (or its price) changed. Gold proves it in the notebook itself, which fails if the second run does not give 0 and 0; validation also checks that, in both layers, every `MERGE` after the first one changed nothing.
- **Bronze** uses Auto Loader: its checkpoint records every processed file and advances together with the write, so a file is loaded exactly once even if the run is interrupted and repeated.
- **Tested, not just designed:** on serverless, a second full run without a reset ended green with the same 60 checks, and so did the `run_all` orchestrator on a cluster. With an unexpected order added in a new file, the job ended in red and pointed at the 11 affected figures (that test was done with the previous version of the ingestion). For these tests the result is recorded, not the full outputs.
- **What proves what:** the idempotency of `MERGE` is shown by notebook `08`, which runs it twice with the same batch. A full re-run without a reset reaches the same result by another path: Bronze loads nothing new, and Silver and the fact table are rebuilt from all of Bronze.
- **Recovery after an interruption:** the staging table is built from the incremental Bronze rows that are newer than the ones it already holds, not from the memory of the run. If the run is interrupted after loading Bronze and before writing it, the retry rebuilds it; this is tested with [`qa/pruebas/corte_bronze.py`](qa/pruebas/corte_bronze.py) and the 60 figures.
- **Known limit:** validation reads the table history (first `MERGE`, version 0): it holds for as long as that history is retained.

| The `MERGE`, executed | Second run and table history |
|---|---|
| ![MERGE](docs/img/nb-merge.png) | ![Idempotency](docs/img/nb-idempotencia.png) |

Every write creates a version of the table, and it can be queried again for as long as the history is retained: `VERSION AS OF 0` returns the 4,511 rows of the initial load even after the `MERGE` and the compaction.

![Query on version 0 of fact_sales](docs/img/nb-timetravel.png)

## Verification

1. [`qa/oraculo.py`](qa/oraculo.py) recomputes from the CSV files, in plain Python and without Spark, everything the pipeline must produce. The result is in [`qa/esperado.json`](qa/esperado.json).
2. The `98_validacion` notebook extracts the actual figures from the tables and compares them, inside the job, with the expected ones. If a single one differs, it raises an error and the run ends in red.
3. There are 60 checks: rows per table, customers per city, products per category, revenue per category and per month, `MERGE` history in Silver and in Gold, duplicate keys, sales without a dimension and quarantined rows per file and reason.

Result: 60 of 60. The actual output of the latest job run (on serverless, deployed with the bundle) and its comparison are stored in [`qa/evidencia/`](qa/evidencia/), together with the record of the additional tests.

A GitHub Actions workflow ([`ci.yml`](.github/workflows/ci.yml)) repeats on every change what does not need Databricks: it regenerates the data and checks it comes out identical, recomputes the expected figures and compares them with the published ones, checks that the validation notebook uses those same figures, compares the stored output of the latest run, and checks that this evidence matches the current notebooks and that their comments do not alter the Python code.

The independent calculation shares the business rules but not the engine. It detects implementation errors; it does not detect a badly chosen rule.

```bash
python3 qa/oraculo.py                      # recomputes the expected figures from the CSV files
python3 qa/comparar.py salida_98.json      # compares the output of notebook 98; exits with an error if anything differs
python3 qa/comparar.py --run <task_run_id> # the same, reading the run through the Databricks CLI
```

`salida_98.json` is the JSON printed by the last cell of the `98_validacion` notebook: copy it to a file. If you use the job, `<task_run_id>` is the run id of the `t98_validacion` task.

## Orchestration

A Lakeflow job chains 16 tasks: setup, four Bronze loads with different parameters on the same notebook, Silver, Gold, the incremental batch, optimization and validation. If a task fails, the following ones do not run, and the run is repaired from the task that failed.

![Job run: 16 tasks](docs/img/ui-corrida.png)

The job is defined as a Databricks Asset Bundle in [`databricks.yml`](databricks.yml) (and as JSON in [`jobs/`](jobs/fmcg-medallion-pipeline.json)); the dashboard definition is in [`dashboard/fmcg-ventas-gold.lvdash.json`](dashboard/fmcg-ventas-gold.lvdash.json).

## Design decisions

| Decision | Discarded alternative | Why |
|---|---|---|
| Unity Catalog Volume for file landing | Mounting a bucket or container with credentials | Access is governed by the catalog and the notebooks carry no secrets |
| Flag invalid customers as `999999` and keep the sale | Drop the order | The sale happened; losing it distorts revenue. The "unknown" member keeps it visible |
| `MERGE` with a four-column key and a change condition | Append rows or overwrite everything | A retry does not duplicate, and only what changed is rewritten |
| Verification against an independent calculation | Trusting that the notebook ran without errors | Running does not prove the number is correct |

The full decision table, what did not work the first time and all the limitations are in [`docs/DESIGN.md`](docs/DESIGN.md).

## How to run it

You need a Databricks workspace with Unity Catalog and permission to create a catalog named `fmcg`. Tested on Azure Databricks Premium (created with the Azure free trial), on serverless and on a single-node cluster with DBR 16.4 LTS.

1. **Import** the `notebooks/` folder into your workspace. On import, Databricks drops the `.py` extension from the name.
   ```bash
   databricks workspace import-dir notebooks /Users/<your_user>/fmcg-medallion
   ```
2. **Create the catalog** `fmcg` from Catalog Explorer (or with `databricks catalogs create`) and **run** `00_setup_catalog`, which creates the schemas and the Volume. If the catalog does not exist, the notebook stops and says how to create it. To use another name, change the `catalog` parameter.
3. **Upload the CSV files** from `data/landing/` to the Volume, keeping the folders:
   ```bash
   databricks fs cp -r data/landing dbfs:/Volumes/fmcg/landing/raw
   databricks fs ls dbfs:/Volumes/fmcg/landing/raw/initial/orders/landing   # must list orders_batch.csv
   ```
   It requires the [Databricks CLI](https://learn.microsoft.com/azure/databricks/dev-tools/cli/), authenticated (`databricks auth login`). You can also upload them from Catalog Explorer.
4. **Run** `run_all`, which executes the whole pipeline in order and ends with the validation: if a figure does not match, it fails.

The pipeline can be run again with no preparation: a second full run leaves the same figures. `99_reset_demo` is only needed to start from scratch. The notebook-by-notebook walkthrough, the compute options and how to create the job and the dashboard are in [`docs/RUNNING.md`](docs/RUNNING.md).

## Repository structure

```
├── notebooks/     14 notebooks in Databricks source format (00–09, 98, 99, run_all, utilities) and `esperado`, generated
├── data/          gen_data.py and the 5 generated CSV files (initial and incremental load)
├── qa/            oraculo.py (independent calculation), esperado.json (expected figures), comparar.py and evidencia/ from the latest run
├── databricks.yml job definition as a Databricks Asset Bundle
├── jobs/          the same definition as JSON
├── dashboard/     definition of the sales dashboard
├── tools/         utility to remove or restore the notebook comments
├── docs/          run guide, design decisions and limitations, and README screenshots
└── .github/       automated verification on every change
```

## Limitations and next steps

- **The data is synthetic.** It is designed to exercise every cleaning rule; it has neither the volume nor the surprises of a real system. For example, no customer with a numeric identifier is missing from the master: validation would flag it as a sale without a dimension, but the pipeline has no rule for that case. The highest price as the price in force is also a rule of this case, not a general truth.
- **Auto Loader runs in batches** (`availableNow`), not as a continuous process, and the files stay in `landing/` without being archived. If the tables are dropped without dropping the checkpoints, nothing is loaded again: that is why the reset removes both. Each bundle target writes to its own catalog: `prod` to `fmcg` (deployed and run from GitHub) and `dev` to `fmcg_dev`, which must be created first and was not created in this project: `dev` was tested pointing at `fmcg` with `--var catalog=fmcg`.
- **Silver and the dimensions are rebuilt in full** and keep no change history (SCD type 2). Only orders and the fact table are incremental.
- **Validation compares against fixed figures** because the data is fixed. With real data it would need relative rules (ranges, variation between loads), not equalities.

Next steps: validation with relative rules between loads that does not depend on table history, quality expectations with Lakeflow Declarative Pipelines, alerts on failure, creating and testing the `dev` catalog and SCD type 2 on customers.

The full list is in [`docs/DESIGN.md`](docs/DESIGN.md#limitations-and-next-steps).

## Origin and authorship

This project starts from [databricks-medallion-lakehouse-fmcg](https://github.com/takadlag/databricks-medallion-lakehouse-fmcg), by Tanmay Kadlag, built on AWS. The business case, the per-layer notebook structure and the cleaning rules come from there.

| From the original project | Contribution of this version |
|---|---|
| FMCG case and medallion structure | Migration from AWS/S3 to Azure with Unity Catalog Volumes |
| Silver cleaning rules | Reproducible synthetic data generator (the original ships no data) |
| Incremental load with `MERGE` | Change condition and a repeatable idempotency proof |
| | Self-contained model: the step that wrote to tables the original does not include was removed |
| | Verification against an independent calculation (60 checks) inside the job, which fails if a figure differs |
| | Declared quality rules, quarantine table and table constraints |
| | Ingestion with a declared schema and tracking of loaded files; pipeline re-runnable without a reset |
| | 16-task job, dashboard, reset notebook and commented notebooks |

It was presented at the charity workshop "Construye tu portafolio de datos End-to-End" by Datanilo Community (November 2026). The workshop slides (in Spanish) are in the [`taller`](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/tree/taller/deck) branch.

**Manuel Argüelles** · AI Analytics Engineer · [LinkedIn](https://www.linkedin.com/in/manuel-arguelles-ai/) · [analizodatos.com](https://analizodatos.com)

[MIT](LICENSE) license.
