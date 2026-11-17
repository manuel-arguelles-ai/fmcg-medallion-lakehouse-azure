# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 · Silver products — dedup, casing, typo `Protien`, división y `product_code`
# MAGIC Además genera un `product_code` determinista (SHA-256 del nombre) y marca los `product_id` inválidos como `999999`.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{catalog}.{bronze_schema}.products")
df_s = df.dropDuplicates(["product_id"])
print("Antes:", df.count(), "→ después:", df_s.count())
display(df_s.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Title-case de categoría y nombre + variante

# COMMAND ----------

df_s = (df_s
    .withColumn("category", F.initcap("category"))
    .withColumn("variant", F.initcap(F.regexp_extract("product_name", r"\((.*?)\)", 1)))
    .withColumn("product_name", F.initcap(F.trim(F.regexp_replace("product_name", r"\s*\(.*\)\s*$", ""))))
    .withColumn("product_name", F.concat("product_name", F.lit(" ("), "variant", F.lit(")"))))
display(df_s.groupBy("category").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Typo `Protien` → `Protein` (regex case-insensitive)

# COMMAND ----------

df_s = (df_s
    .withColumn("product_name", F.regexp_replace("product_name", "(?i)Protien", "Protein"))
    .withColumn("category",     F.regexp_replace("category",     "(?i)Protien", "Protein")))
display(df_s.groupBy("category").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. División comercial y product_code

# COMMAND ----------

df_s = df_s.withColumn("division",
    F.when(F.col("category") == "Energy Bars",       "Nutrition Bars")
     .when(F.col("category") == "Protein Bars",      "Nutrition Bars")
     .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
     .when(F.col("category") == "Recovery Dairy",    "Dairy & Recovery")
     .when(F.col("category") == "Healthy Snacks",    "Healthy Snacks")
     .when(F.col("category") == "Electrolyte Mix",   "Hydration & Electrolytes")
     .otherwise("Other"))

df_s = (df_s
    .withColumn("product_code", F.sha2(F.col("product_name").cast("string"), 256))
    .withColumn("product_id",
        F.when(F.col("product_id").cast("string").rlike("^[0-9]+$"), F.col("product_id").cast("string"))
         .otherwise(F.lit("999999")))
    .withColumnRenamed("product_name", "product"))

df_s = df_s.select("product_code", "division", "category", "product", "variant",
                   "product_id", "read_timestamp", "file_name", "file_size")
display(df_s.limit(10))

# COMMAND ----------

(df_s.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")
   .saveAsTable(f"{catalog}.{silver_schema}.products"))
print("OK →", f"{catalog}.{silver_schema}.products", "|", spark.table(f"{catalog}.{silver_schema}.products").count(), "filas")
