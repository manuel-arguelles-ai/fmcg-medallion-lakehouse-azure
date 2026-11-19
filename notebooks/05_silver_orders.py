# Databricks notebook source
# MAGIC %md
# MAGIC ## 05 · Silver orders — la más completa
# MAGIC Filtra nulos de `order_qty`, marca `customer_id` inválido como `999999`, normaliza la fecha (quita el prefijo de día de semana + 4 formatos), dedup por llave natural y agrega el `product_code` con un join a products.
# MAGIC
# MAGIC - `load_mode = initial` → procesa la tabla bronze completa y **sobrescribe** silver.
# MAGIC - `load_mode = incremental` → procesa solo la staging y hace **MERGE** contra silver.

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# QUÉ HACE: prepara las herramientas y lee el parámetro de modo de carga
from pyspark.sql import functions as F

# to_date tolerante: si el formato no calza devuelve NULL en vez de fallar
to_date_safe = lambda c, fmt: F.try_to_timestamp(F.col(c), F.lit(fmt)).cast("date")
from delta.tables import DeltaTable  # necesario para MERGE

dbutils.widgets.dropdown("load_mode", "initial", ["initial", "incremental"], "Load mode")
load_mode = dbutils.widgets.get("load_mode")

# QUÉ HACE: elige de dónde leer: todo Bronze o solo el lote nuevo
src = (f"{catalog}.{bronze_schema}.staging_orders"
       if load_mode == "incremental"  # lote nuevo: la tabla de paso
       else f"{catalog}.{bronze_schema}.orders")
df = spark.table(src)
n_bronze = df.count()  # cifra de control: 5.050 en la carga inicial
print("Fuente:", src, "| filas:", n_bronze)
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Filtrar qty nula + limpiar customer_id

# COMMAND ----------

# QUÉ HACE: descarta las órdenes sin cantidad y marca los clientes inválidos
df = df.filter(F.col("order_qty").isNotNull())  # sin cantidad no hay venta que medir
df = (df.withColumn("order_qty", F.col("order_qty").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("customer_id",
            F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))  # solo dígitos = cliente válido
             .otherwise(F.lit("999999"))))  # cliente inválido: se marca, la venta se conserva
print("Sin qty nula:", df.count(), "| customer_id inválidos → 999999:",
      df.filter("customer_id = '999999'").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Normalizar fecha: quitar "Sunday, " y probar 4 formatos

# COMMAND ----------

# QUÉ HACE: deja todas las fechas en un solo formato
df = df.withColumn("order_placement_date",
        F.regexp_replace(F.col("order_placement_date").cast("string"), r"^[A-Za-z]+,\s*", ""))  # quita el día de la semana del inicio
df = df.withColumn("order_placement_date",
        F.coalesce(  # prueba los cuatro formatos; gana el primero que convierte
            to_date_safe("order_placement_date", "yyyy/MM/dd"),
            to_date_safe("order_placement_date", "dd-MM-yyyy"),
            to_date_safe("order_placement_date", "dd/MM/yyyy"),
            to_date_safe("order_placement_date", "MMMM dd, yyyy"),
        ))
print("Fechas sin parsear:", df.filter(F.col("order_placement_date").isNull()).count())  # debe dar 0
display(df.agg(F.min("order_placement_date").alias("min"), F.max("order_placement_date").alias("max")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Dedup por llave natural + product_code
# MAGIC El `inner join` con products descarta las órdenes cuyo `product_id` no existe en el maestro.

# COMMAND ----------

# QUÉ HACE: quita filas repetidas y descarta las órdenes cuyo producto no existe
df = df.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])  # misma orden, fecha, cliente, producto y cantidad = fila repetida
df = df.withColumn("product_id", F.col("product_id").cast("string"))
n_dedup = df.count()

df_products = spark.table(f"{catalog}.{silver_schema}.products").select("product_id", "product_code")  # maestro de productos ya limpio
df = df.join(df_products, "product_id", "inner")  # inner: sin producto en el maestro, la orden se descarta
df = df.select("order_id", "order_placement_date", "customer_id", "product_id", "order_qty",
               "product_code", "read_timestamp", "file_name", "file_size")
print("Tras dedup:", n_dedup, "| tras join con products:", df.count())  # cifra de control: 4.709 → 4.511

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Escritura Silver: overwrite en la carga inicial, MERGE idempotente en la incremental

# COMMAND ----------

# QUÉ HACE: guarda en Silver: reemplaza todo en la carga inicial o fusiona el lote nuevo
silver_table = f"{catalog}.{silver_schema}.orders"
if load_mode == "initial":
    (df.write.format("delta")
       .option("delta.enableChangeDataFeed", "true")
       .option("overwriteSchema", "true")
       .mode("overwrite")  # carga inicial: reemplaza todo
       .saveAsTable(silver_table))
else:
    # staging de silver: el lote limpio que consume el notebook 08 (fact incremental)
    (df.write.format("delta").option("overwriteSchema", "true")
       .mode("overwrite").saveAsTable(f"{catalog}.{silver_schema}.staging_orders"))  # el lote limpio que leerá el notebook 08
    # MERGE con llave completa: la misma corrida 2 veces = mismo resultado (idempotente)
    (DeltaTable.forName(spark, silver_table).alias("silver")
        .merge(df.alias("new"),  # condición: las cuatro columnas que identifican una orden
               "silver.order_id = new.order_id AND silver.order_placement_date = new.order_placement_date "
               "AND silver.customer_id = new.customer_id AND silver.product_code = new.product_code")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())  # si existe, actualiza; si no, inserta
print("OK →", silver_table, "|", spark.table(silver_table).count(), "filas")
