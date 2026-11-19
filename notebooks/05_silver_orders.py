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
from pyspark.sql.window import Window  # para ordenar filas dentro de un grupo

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
# MAGIC ### Reglas de calidad declaradas
# MAGIC Cada regla tiene un nombre y una condición que la fila debe cumplir. Las filas que no la cumplen no se pierden: van a la tabla de cuarentena `silver.orders_quarantine` con el motivo.

# COMMAND ----------

# QUÉ HACE: declara las reglas de calidad y la función que aparta las filas que no las cumplen
REGLAS = {
    "fila_malformada":      "_rescued_data IS NULL",  # la fila no encajó en el esquema de Bronze: no se puede confiar en sus valores
    "sin_cantidad":         "order_qty IS NOT NULL",  # sin cantidad no hay venta que medir
    "producto_inexistente": "product_code IS NOT NULL",  # sin producto en el maestro no hay precio
}
rechazadas = []  # aquí se acumulan las filas apartadas

def aplicar_regla(df, nombre):
    """Devuelve las filas que cumplen la regla y aparta las que no."""
    malas = df.filter(f"NOT ({REGLAS[nombre]})")  # las que NO cumplen la regla
    rechazadas.append(malas.select(
        F.col("order_id"), F.col("order_placement_date").cast("string").alias("order_placement_date"),
        F.col("customer_id").cast("string").alias("customer_id"), F.col("product_id").cast("string").alias("product_id"),
        F.col("order_qty").cast("int").alias("order_qty"), F.col("file_name"), F.lit(nombre).alias("motivo")))  # con el nombre de la regla como motivo
    return df.filter(REGLAS[nombre])  # siguen adelante las que sí la cumplen

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Reglas «fila_malformada» y «sin_cantidad» + limpiar customer_id
# MAGIC Primero se apartan las filas que no encajaron en el esquema de Bronze (`_rescued_data` con contenido); después, las que no traen cantidad.

# COMMAND ----------

# QUÉ HACE: aplica las dos primeras reglas y marca los clientes inválidos
df = aplicar_regla(df, "fila_malformada")
df = aplicar_regla(df, "sin_cantidad")
df = (df.withColumn("order_qty", F.col("order_qty").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("customer_id",
            F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))  # solo dígitos = cliente válido
             .otherwise(F.lit("999999"))))  # cliente inválido: se marca, la venta se conserva
print("Sin qty nula:", df.count(), "| customer_id inválidos → 999999:",  # cifra de control: 4.757
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
# MAGIC ### 3. Una fila por orden: gana la última
# MAGIC La llave de negocio es orden + fecha + cliente + producto. Si la misma llave llega más de una vez (una fila repetida o una corrección de cantidad), se conserva la que se leyó al final.

# COMMAND ----------

# QUÉ HACE: deja una sola fila por orden: si la llave se repite, gana la más reciente
df = df.withColumn("product_id", F.col("product_id").cast("string"))
ultima = (Window.partitionBy("order_id", "order_placement_date", "customer_id", "product_id")  # agrupa por la llave de negocio
                .orderBy(F.col("read_timestamp").desc(), F.col("file_name").desc(), F.col("order_qty").desc()))  # la leída más tarde queda primera
df = df.withColumn("rn", F.row_number().over(ultima)).filter("rn = 1").drop("rn")  # se queda con la primera de cada grupo
n_dedup = df.count()
print("Tras quitar repetidas:", n_dedup)  # cifra de control: 4.709

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Regla «producto_inexistente» + product_code
# MAGIC El cruce con products trae el `product_code`. Una orden cuyo producto no está en el maestro no tiene cómo recibir precio: va a cuarentena.

# COMMAND ----------

# QUÉ HACE: trae el código de producto y aplica la segunda regla
df_products = spark.table(f"{catalog}.{silver_schema}.products").select("product_id", "product_code")
df = aplicar_regla(df.join(df_products, "product_id", "left"), "producto_inexistente")  # left: la orden sin producto queda con código nulo y la regla la aparta
df = df.select("order_id", "order_placement_date", "customer_id", "product_id", "order_qty",
               "product_code", "read_timestamp", "file_name", "file_size")
print("Tras quitar repetidas:", n_dedup, "| con producto válido:", df.count())  # cifra de control: 4.709 → 4.511

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Cuarentena
# MAGIC Las filas rechazadas se guardan con su motivo y su archivo de origen. Repetir la carga de un archivo reemplaza sus filas en cuarentena, no las duplica.

# COMMAND ----------

# QUÉ HACE: guarda las filas rechazadas con su motivo
quarantine_table = f"{catalog}.{silver_schema}.orders_quarantine"
from functools import reduce
df_q = reduce(lambda a, b: a.unionByName(b), rechazadas).withColumn("quarantined_at", F.current_timestamp())  # las rechazadas por las dos reglas
if load_mode == "initial" or not spark.catalog.tableExists(quarantine_table):  # carga inicial: la cuarentena se reemplaza
    df_q.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(quarantine_table)
else:
    archivos = [r["file_name"] for r in spark.table(src).select("file_name").distinct().collect()]
    (DeltaTable.forName(spark, quarantine_table)
        .delete(F.col("file_name").isin(archivos)))  # lote nuevo: primero se quitan las filas previas de ese archivo
    df_q.write.format("delta").mode("append").saveAsTable(quarantine_table)  # y se agregan las de esta carga
display(spark.table(quarantine_table).groupBy("file_name", "motivo").count().orderBy("file_name", "motivo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Escritura Silver: reemplazo en la carga inicial, MERGE en la incremental
# MAGIC El `MERGE` solo reescribe una fila si su cantidad cambió, así que repetirlo con el mismo lote no modifica nada. Al final, dos restricciones de la tabla impiden que entre una fila sin cantidad o sin fecha.

# COMMAND ----------

# QUÉ HACE: guarda en Silver y declara las restricciones de la tabla
silver_table = f"{catalog}.{silver_schema}.orders"
if load_mode == "initial":
    (df.write.format("delta")
       .option("delta.enableChangeDataFeed", "true")
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(silver_table))  # carga inicial: reemplaza todo
else:
    # staging de silver: el lote limpio que consume el notebook 08 (fact incremental)
    (df.write.format("delta").option("overwriteSchema", "true")
       .mode("overwrite").saveAsTable(f"{catalog}.{silver_schema}.staging_orders"))  # el lote limpio que leerá el notebook 08
    (DeltaTable.forName(spark, silver_table).alias("silver")
        .merge(df.alias("new"),  # condición: las cuatro columnas que identifican una orden
               "silver.order_id = new.order_id AND silver.order_placement_date = new.order_placement_date "
               "AND silver.customer_id = new.customer_id AND silver.product_code = new.product_code")
        .whenMatchedUpdateAll(condition="NOT (silver.order_qty <=> new.order_qty)")  # si existe, se actualiza solo cuando cambió la cantidad
        .whenNotMatchedInsertAll().execute())  # si no existe, se inserta

for nombre, condicion in {"cantidad_no_nula": "order_qty IS NOT NULL", "fecha_no_nula": "order_placement_date IS NOT NULL"}.items():
    spark.sql(f"ALTER TABLE {silver_table} DROP CONSTRAINT IF EXISTS {nombre}")  # permite repetir la celda sin error
    spark.sql(f"ALTER TABLE {silver_table} ADD CONSTRAINT {nombre} CHECK ({condicion})")  # desde aquí, Delta rechaza cualquier fila que la incumpla
registrar_cargas(f"{silver_schema}.orders")  # anota esta carga en el registro que lee la validación
print("OK →", silver_table, "|", spark.table(silver_table).count(), "filas")
