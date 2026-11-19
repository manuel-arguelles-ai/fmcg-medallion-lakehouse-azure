# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 · Silver customers — dedup, limpieza y calidad
# MAGIC Transformaciones: dedup por `customer_id`, trim + title-case de nombres, corrección de typos de ciudad con diccionario, ciudad de 4 clientes confirmada por negocio, resto de nulos marcados como `Sin ciudad`, cast final a string.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: lee la tabla Bronze de clientes y muestra cómo llegó
from pyspark.sql import functions as F

df = spark.table(f"{catalog}.{bronze_schema}.customers")  # punto de partida: lo que dejó Bronze
print("Filas bronze:", df.count())  # cifra de control: 196
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Deduplicación

# COMMAND ----------

# QUÉ HACE: busca clientes repetidos y se queda con una fila por cliente
dup = df.groupBy("customer_id").count().filter("count > 1")  # identificadores que aparecen más de una vez
display(dup)

df_s = df.dropDuplicates(["customer_id"])  # una sola fila por customer_id
print("Antes:", df.count(), "→ después:", df_s.count())  # 196 → 184

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Trim + title-case del nombre

# COMMAND ----------

# QUÉ HACE: limpia el nombre: sin espacios sobrantes y con mayúscula inicial
print("Nombres con espacios sobrantes:",
      df_s.filter(F.col("customer_name") != F.trim(F.col("customer_name"))).count())
df_s = df_s.withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))  # trim quita espacios; initcap pone mayúscula inicial
display(df_s.select("customer_id", "customer_name").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Typos de ciudad → diccionario + lista blanca
# MAGIC Primero vemos qué llegó; lo que no queda en la lista permitida pasa a `NULL` (y se resuelve en el paso 4).

# COMMAND ----------

# QUÉ HACE: el «antes»: todas las formas en que llegó escrita la ciudad
display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

# QUÉ HACE: corrige las ciudades mal escritas y anula las que no son válidas
city_mapping = {
    "Bengaluruu": "Bengaluru", "Bengalore": "Bengaluru", "bengaluru": "Bengaluru", "BENGALURU": "Bengaluru",  # forma mal escrita → forma correcta
    "Hyderabadd": "Hyderabad", "Hyderbad": "Hyderabad", "hyderabad": "Hyderabad",
    "NewDelhi": "New Delhi", "NewDheli": "New Delhi", "NewDelhee": "New Delhi",
    "new delhi": "New Delhi",
}
allowed = ["Bengaluru", "Hyderabad", "New Delhi"]  # lista permitida: las únicas tres ciudades válidas

df_s = (df_s.withColumn("city", F.trim(F.col("city")))  # primero quita espacios
            .replace(city_mapping, subset=["city"])  # luego aplica el diccionario
            .withColumn("city", F.when(F.col("city").isin(allowed), F.col("city"))))  # lo que no está en la lista queda en NULL
display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ciudades faltantes
# MAGIC Cuatro clientes corporativos tienen ciudad confirmada por negocio (fix puntual por `customer_id`: decisión de negocio, no de datos). El resto de nulos se marca como `Sin ciudad` para no perderlos en los reportes.

# COMMAND ----------

# QUÉ HACE: completa las ciudades que faltan: cuatro confirmadas por negocio y el resto «Sin ciudad»
customer_city_fix = {789403: "New Delhi", 789420: "Bengaluru", 789521: "Hyderabad", 789603: "Hyderabad"}  # decisión de negocio, no de datos
df_fix = spark.createDataFrame([(k, v) for k, v in customer_city_fix.items()], "customer_id int, fixed_city string")  # la convierte en una tabla pequeña para poder cruzarla

df_s = (df_s.join(df_fix, "customer_id", "left")  # left: conserva todos los clientes
            .withColumn("city", F.coalesce("city", "fixed_city", F.lit("Sin ciudad")))  # primer valor no nulo: ciudad, corrección o «Sin ciudad»
            .drop("fixed_city"))  # la columna auxiliar ya no hace falta
display(df_s.groupBy("city").count().orderBy("city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Cast + escritura Silver

# COMMAND ----------

# QUÉ HACE: fija el tipo del identificador y guarda la tabla Silver
df_s = df_s.withColumn("customer_id", F.col("customer_id").cast("string"))  # como texto, igual que en las órdenes

(df_s.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .option("overwriteSchema", "true")
   .mode("overwrite")  # Silver de clientes se reconstruye completa cada vez
   .saveAsTable(f"{catalog}.{silver_schema}.customers"))
print("OK →", f"{catalog}.{silver_schema}.customers", "|", spark.table(f"{catalog}.{silver_schema}.customers").count(), "filas")  # cifra de control: 184
