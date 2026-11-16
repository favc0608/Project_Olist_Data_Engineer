import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL SILVER - CLIENTES SCD TIPO 2 (historia de direcciones por persona)      APLICADO 2026-11-16
# entrada: erp/silver/customers (SCD1, una fila por customer_id = por pedido)
#          erp/silver/orders    (fecha de compra de cada pedido)
# salida:  erp/silver/customers_scd2 (Delta)
#
# En Olist customer_id cambia en cada pedido; la persona es customer_unique_id. Cada pedido "fotografía"
# la dirección del cliente en la fecha de compra. Se ordenan esas fotos por fecha y se abre una versión
# nueva solo cuando cambia la dirección (zip, ciudad o estado).
#   valid_from = fecha de compra del primer pedido con esa dirección (no la hora del job)
#   valid_to   = fecha de compra en que apareció la dirección siguiente (nulo si es la vigente)
#   is_current = valid_to es nulo
#   sk_cliente = sha2(customer_unique_id + valid_from): clave sustituta estable para gold
# Los hechos se unen con: valid_from <= order_purchase_timestamp < coalesce(valid_to, '9999-12-31')
#
# Escritura: se recalcula la historia completa (determinista) y se aplica con MERGE por sk_cliente:
# inserta versiones nuevas, cierra las que dejaron de estar vigentes y borra las que un pedido atrasado
# volvió inválidas. Re-ejecutar no cambia nada (idempotente).
# Columnas SCD2 con los mismos nombres que sellers: valid_from, valid_to, is_current, row_hash.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_CLIENTES = "s3://trabajo-integrador-g1/erp/silver/customers/"
RUTA_PEDIDOS = "s3://trabajo-integrador-g1/erp/silver/orders/"
RUTA_SCD2 = "s3://trabajo-integrador-g1/erp/silver/customers_scd2/"
ATRIBUTOS = ["customer_zip_code_prefix", "customer_city", "customer_state"]

# 2. LEER SILVER
clientes = spark.read.format("delta").load(RUTA_CLIENTES)
# En Olist customer_id es 1:1 con el pedido; por si un lote trae dos pedidos con el mismo customer_id,
# se toma la primera compra (si no, el join duplicaría la foto del cliente).
pedidos = (spark.read.format("delta").load(RUTA_PEDIDOS)
           .groupBy("customer_id").agg(F.min("order_purchase_timestamp").alias("order_purchase_timestamp")))
total_clientes = clientes.count()
print(f"Clientes (customer_id) en silver: {total_clientes}")
if total_clientes == 0:
    raise ValueError("ERROR: silver customers está vacío.")

# 3. FECHA DE VIGENCIA DE CADA FOTO (compra del pedido; si el pedido aún no está en silver, la ingesta)
fotos = (clientes.join(pedidos, "customer_id", "left")
         .withColumn("vigencia_estimada", F.col("order_purchase_timestamp").isNull())
         .withColumn("fecha_foto", F.coalesce(F.col("order_purchase_timestamp").cast("timestamp"),
                                              F.col("fecha_ingesta").cast("timestamp")))
         .withColumn("attr_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>"))
                                                             for c in ATRIBUTOS]), 256)))
estimadas = fotos.filter("vigencia_estimada").count()
print(f"Fotos sin pedido en silver (vigencia = fecha de ingesta): {estimadas}")
if fotos.filter(F.col("fecha_foto").isNull()).limit(1).count():
    raise ValueError("ERROR: hay clientes sin fecha de compra ni de ingesta.")

# 4. DETECTAR CAMBIOS DE DIRECCION POR PERSONA
orden = Window.partitionBy("customer_unique_id").orderBy("fecha_foto", "customer_id")
versiones = (fotos
    .withColumn("_hash_previo", F.lag("attr_hash").over(orden))
    .filter(F.col("_hash_previo").isNull() | (F.col("_hash_previo") != F.col("attr_hash")))
    .withColumn("valid_from", F.col("fecha_foto"))
    .withColumn("valid_to", F.lead("fecha_foto").over(Window.partitionBy("customer_unique_id").orderBy("fecha_foto", "customer_id")))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("version", F.row_number().over(Window.partitionBy("customer_unique_id").orderBy("fecha_foto", "customer_id")))
    .withColumn("sk_cliente", F.sha2(F.concat_ws("||", "customer_unique_id", F.col("valid_from").cast("string")), 256))
    .select("sk_cliente", "customer_unique_id", F.col("customer_id").alias("customer_id_origen"), *ATRIBUTOS,
            "valid_from", "valid_to", "is_current", "version", "vigencia_estimada",
            F.col("attr_hash").alias("row_hash"))
    .withColumn("fecha_procesamiento_silver", F.current_timestamp())
    .withColumn("capa_datos", F.lit("silver"))
    .cache())

# 5. VALIDACIONES
personas = clientes.select("customer_unique_id").distinct().count()
vigentes = versiones.filter("is_current").count()
total_versiones = versiones.count()
print(f"Personas: {personas} | versiones: {total_versiones} | con cambios de dirección: {total_versiones - personas}")
if vigentes != personas:
    raise ValueError(f"ERROR: {vigentes} versiones vigentes para {personas} personas (debe haber una por persona).")
if versiones.groupBy("sk_cliente").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: sk_cliente duplicada (dos fotos de la misma persona en el mismo instante).")
if versiones.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).limit(1).count():
    raise ValueError("ERROR: versiones con valid_to <= valid_from.")

# 6. ESCRIBIR EN DELTA (MERGE por clave sustituta)
if DeltaTable.isDeltaTable(spark, RUTA_SCD2):
    (DeltaTable.forPath(spark, RUTA_SCD2).alias("t")
        .merge(versiones.alias("s"), "t.sk_cliente = s.sk_cliente")
        # cierra la versión (valid_to) o corrige atributos si cambiaron
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash OR NOT (t.valid_to <=> s.valid_to) "
                                        "OR t.is_current <> s.is_current OR t.version <> s.version")
        .whenNotMatchedInsertAll()                 # versiones nuevas
        .whenNotMatchedBySourceDelete()            # versiones que un pedido atrasado dejó sin efecto
        .execute())
    print("MERGE SCD2 aplicado.")
else:
    versiones.write.format("delta").mode("overwrite").save(RUTA_SCD2)
    print("Primera carga: tabla SCD2 creada.")

# 7. VALIDAR EL RESULTADO
final = spark.read.format("delta").load(RUTA_SCD2)
if final.filter("is_current").groupBy("customer_unique_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: personas con más de una versión vigente.")
print(f"Versiones en silver SCD2: {final.count()} | vigentes: {final.filter('is_current').count()}")
print("ETL SILVER CLIENTES SCD2 FINALIZADO CORRECTAMENTE.")
versiones.unpersist()
job.commit()
