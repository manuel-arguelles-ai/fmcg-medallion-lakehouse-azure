# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 · Silver customers — dedup, limpieza y calidad
# MAGIC Transformaciones: dedup por `customer_id`, trim + title-case de nombres, corrección de typos de ciudad con diccionario, ciudad de 4 clientes confirmada por negocio, resto de nulos marcados como `Sin ciudad`, cast final a string.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{catalog}.{bronze_schema}.customers")
print("Filas bronze:", df.count())
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Deduplicación

# COMMAND ----------

dup = df.groupBy("customer_id").count().filter("count > 1")
display(dup)

df_s = df.dropDuplicates(["customer_id"])
print("Antes:", df.count(), "→ después:", df_s.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Trim + title-case del nombre

# COMMAND ----------

print("Nombres con espacios sobrantes:",
      df_s.filter(F.col("customer_name") != F.trim(F.col("customer_name"))).count())
df_s = df_s.withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
display(df_s.select("customer_id", "customer_name").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Typos de ciudad → diccionario + lista blanca
# MAGIC Primero vemos qué llegó; lo que no queda en la lista permitida pasa a `NULL` (y se resuelve en el paso 4).

# COMMAND ----------

display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

city_mapping = {
    "Bengaluruu": "Bengaluru", "Bengalore": "Bengaluru", "bengaluru": "Bengaluru", "BENGALURU": "Bengaluru",
    "Hyderabadd": "Hyderabad", "Hyderbad": "Hyderabad", "hyderabad": "Hyderabad",
    "NewDelhi": "New Delhi", "NewDheli": "New Delhi", "NewDelhee": "New Delhi",
    "new delhi": "New Delhi",
}
allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_s = (df_s.withColumn("city", F.trim(F.col("city")))
            .replace(city_mapping, subset=["city"])
            .withColumn("city", F.when(F.col("city").isin(allowed), F.col("city"))))
display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ciudades faltantes
# MAGIC Cuatro clientes corporativos tienen ciudad confirmada por negocio (fix puntual por `customer_id`: decisión de negocio, no de datos). El resto de nulos se marca como `Sin ciudad` para no perderlos en los reportes.

# COMMAND ----------

customer_city_fix = {789403: "New Delhi", 789420: "Bengaluru", 789521: "Hyderabad", 789603: "Hyderabad"}
df_fix = spark.createDataFrame([(k, v) for k, v in customer_city_fix.items()], "customer_id int, fixed_city string")

df_s = (df_s.join(df_fix, "customer_id", "left")
            .withColumn("city", F.coalesce("city", "fixed_city", F.lit("Sin ciudad")))
            .drop("fixed_city"))
display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Cast + escritura Silver

# COMMAND ----------

df_s = df_s.withColumn("customer_id", F.col("customer_id").cast("string"))

(df_s.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")
   .saveAsTable(f"{catalog}.{silver_schema}.customers"))
print("OK →", f"{catalog}.{silver_schema}.customers", "|", spark.table(f"{catalog}.{silver_schema}.customers").count(), "filas")
