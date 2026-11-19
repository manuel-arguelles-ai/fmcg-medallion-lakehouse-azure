# Databricks notebook source
# MAGIC %md
# MAGIC ## Orquestador — ejecuta el pipeline completo en orden y lo valida
# MAGIC No toca Azure directamente: llama a cada notebook con sus parámetros.
# MAGIC Correr con **Run all** en cómputo Serverless.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 0. Setup (catálogo, schemas, volume)

# COMMAND ----------
# MAGIC %run ./00_setup_catalog

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Bronze — carga inicial (4 fuentes)

# COMMAND ----------
# MAGIC %run ./01_bronze_load $data_source=customers $load_mode=initial

# COMMAND ----------
# MAGIC %run ./01_bronze_load $data_source=products $load_mode=initial

# COMMAND ----------
# MAGIC %run ./01_bronze_load $data_source=gross_price $load_mode=initial

# COMMAND ----------
# MAGIC %run ./01_bronze_load $data_source=orders $load_mode=initial

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Silver

# COMMAND ----------
# MAGIC %run ./02_silver_customers

# COMMAND ----------
# MAGIC %run ./03_silver_products

# COMMAND ----------
# MAGIC %run ./04_silver_pricing

# COMMAND ----------
# MAGIC %run ./05_silver_orders $load_mode=initial

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Gold — dims + fact full load

# COMMAND ----------
# MAGIC %run ./06_gold_dims

# COMMAND ----------
# MAGIC %run ./07_gold_fact_full_load

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Carga incremental (staging → silver → MERGE en gold)

# COMMAND ----------
# MAGIC %run ./01_bronze_load $data_source=orders $load_mode=incremental

# COMMAND ----------
# MAGIC %run ./05_silver_orders $load_mode=incremental

# COMMAND ----------
# MAGIC %run ./08_gold_fact_incremental

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Optimización + KPIs

# COMMAND ----------
# MAGIC %run ./09_optimize_and_kpis

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Validación — termina con error si una cifra no coincide

# COMMAND ----------

# MAGIC %run ./98_validacion
