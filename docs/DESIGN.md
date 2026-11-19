# Design decisions and limitations

[← Back to the README](../README.en.md)

## Design decisions

| Decision | Discarded alternative | Why |
|---|---|---|
| Unity Catalog Volume for file landing | Mounting a bucket or container with credentials | Access is governed by the catalog and the notebooks carry no secrets |
| Flag invalid customers as `999999` and keep the sale | Drop the order | The sale happened; losing it distorts revenue. The "unknown" member keeps it visible |
| Send to quarantine the orders whose product is not in the master | Keep them without a price, or drop them without a trace | Without a product there is no price and no revenue to compute |
| A single price per product and month, the highest | Keep both | With two prices, the join with orders would duplicate sales |
| `MERGE` with a four-column key and a change condition | Append rows or overwrite everything | A retry does not duplicate, and only what changed is rewritten |
| Unpartitioned fact table with `OPTIMIZE ZORDER` | Partition by date | At this volume, partitioning creates one tiny file per date, more than 150 |
| Dimensions rebuilt in full on every run | `MERGE` on dimensions too | They are small and their history is not needed in this case |
| Calendar generated for the whole year | Only dates with sales | Future batches always find their date |
| Verification against an independent calculation | Trusting that the notebook ran without errors | Running does not prove the number is correct |

### What did not work the first time

- **Tasks "running" for hours without writing anything.** The first hypothesis was a capacity shortage in the region. The real cause: the cluster was created with 0 workers and without single-node mode, so it started with nothing to execute the work. It was found by reading the cluster configuration, not by retrying.
- **Risk of duplicated sales.** There were 24 product-month pairs with two prices. It was detected by inspecting the data before doing the join.
- **The idempotency proof could not be repeated.** The notebook dropped the intermediate table when it finished. It surfaced when running the full demonstration twice.

## Limitations and next steps

- **The data is synthetic.** It is designed to exercise every cleaning rule; it has neither the volume nor the surprises of a real system. For example, no customer with a numeric identifier is missing from the master: validation would flag it as a sale without a dimension, but the pipeline has no rule for that case. The highest price as the price in force is also a rule of this case, not a general truth.
- **Auto Loader runs in batches** (`availableNow`), not as a continuous process, and the files stay in `landing/` without being archived. If the tables are dropped without dropping the checkpoints, nothing is loaded again: that is why the reset removes both.
- **If the same order arrives twice in the same file with different quantities**, the larger quantity wins: across files the last one read wins, but within a file there is no arrival order. For customers and products, deduplication does not choose which row survives.
- **The 12 products with an invalid identifier share `product_id = 999999`.** Today they have no price and no orders; if an order arrived with that identifier, the join on `product_id` would multiply it by 12.
- **Silver and the dimensions are rebuilt in full** and keep no change history (SCD type 2). Only orders and the fact table are incremental.
- **Validation compares against fixed figures** because the data is fixed. With real data it would need relative rules (ranges, variation between loads), not equalities.
- **Deployment from GitHub was executed once, on `prod`:** the [`deploy.yml`](../.github/workflows/deploy.yml) workflow deployed the bundle with a service principal and ran the pipeline from scratch ([recorded run](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/runs/44541323573)). The `dev` target writes to its own catalog, `fmcg_dev`: it was run end to end from the CLI, with 66 of 66 checks, without writing to `fmcg`. Without the service principal secrets, the workflow is skipped.
- **Quality rules are declared in the notebook** (a dictionary of rules and `CHECK` constraints on the tables), not in a framework such as Lakeflow Declarative Pipelines, which would also keep the compliance history. Only orders have a quarantine.
- **The price list only covers January to June 2026.** A later order would end up with a null price and revenue; validation counts them, but there is no rule for that case.
- **The job is a linear chain**, although the four Bronze loads are independent and could run in parallel.
- **The rules "confirmed by the business"** (the four cities and the highest price) are assumptions of the case: the data is synthetic.
- **Others:** natural keys without surrogate keys, amounts in `DOUBLE` instead of `DECIMAL`, `ZORDER` applied without measuring its effect (below 5,000 rows no difference is expected), Change Data Feed enabled with no consumer and calendar fixed to 2026.

Next steps: validation with relative rules between loads, detection of rows with extra columns, quality expectations with Lakeflow Declarative Pipelines, alerts on failure and SCD type 2 on customers.
