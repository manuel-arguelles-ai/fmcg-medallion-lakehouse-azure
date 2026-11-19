# Databricks notebook source
# MAGIC %md
# MAGIC ## 09 · Optimización + KPIs
# MAGIC `OPTIMIZE ... ZORDER BY (product_id)` compacta y co-localiza los archivos por producto (acelera los filtros del dashboard) + time travel + las queries del dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ZORDER acelera los filtros por product_id
# MAGIC -- QUÉ HACE: compacta los archivos de la tabla y los ordena por producto
# MAGIC OPTIMIZE fmcg.gold.fact_sales ZORDER BY (product_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel — la tabla antes y después de la carga incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUÉ HACE: compara la tabla como estaba al inicio con la actual
# MAGIC SELECT 'versión 0 (full load)' AS momento, COUNT(*) AS filas, round(SUM(revenue), 2) AS revenue_usd
# MAGIC FROM fmcg.gold.fact_sales VERSION AS OF 0  -- la tabla tal como quedó en su primera escritura
# MAGIC UNION ALL
# MAGIC -- esperado: 4.511 filas antes, 4.786 ahora
# MAGIC SELECT 'actual', COUNT(*), round(SUM(revenue), 2) FROM fmcg.gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPIs del dashboard (mismas queries que Databricks SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 1 · Revenue total y unidades
# MAGIC SELECT round(SUM(revenue), 2) AS revenue_usd, SUM(quantity) AS total_units, COUNT(DISTINCT order_id) AS orders
# MAGIC FROM fmcg.gold.fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 2 · Top 10 productos por revenue
# MAGIC SELECT dp.product, dp.category, round(SUM(f.revenue), 2) AS revenue_usd
# MAGIC FROM fmcg.gold.fact_sales f
# MAGIC JOIN fmcg.gold.dim_products dp ON f.product_code = dp.product_code  -- la dimensión aporta el nombre y la categoría
# MAGIC GROUP BY dp.product, dp.category
# MAGIC ORDER BY revenue_usd DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 3 · Ventas por categoría
# MAGIC SELECT dp.category, round(SUM(f.revenue), 2) AS revenue_usd, SUM(f.quantity) AS units
# MAGIC FROM fmcg.gold.fact_sales f
# MAGIC JOIN fmcg.gold.dim_products dp ON f.product_code = dp.product_code
# MAGIC GROUP BY dp.category ORDER BY revenue_usd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 4 · Tendencia mensual
# MAGIC SELECT date_trunc('MONTH', date) AS month, round(SUM(revenue), 2) AS revenue_usd
# MAGIC FROM fmcg.gold.fact_sales
# MAGIC -- 1 = la primera columna del SELECT
# MAGIC GROUP BY 1 ORDER BY 1;
