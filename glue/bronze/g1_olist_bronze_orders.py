import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - PEDIDOS (orders)            PROPUESTA 2026-11-15
# Cambios respecto del job vigente:
#   - valida encabezados, claves y fecha de compra ANTES de escribir (antes guardaba cualquier CSV)
#   - descarta filas idénticas repetidas dentro del lote (reenvíos de SQS / archivos subidos dos veces)
#   - mismo esquema que antes (las 8 columnas + _source_file, _ingested_at): el silver no cambia
# Sigue usando el job bookmark: solo lee archivos nuevos.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
BUCKET = "trabajo-integrador-g1"
LANDING = f"s3://{BUCKET}/erp/landing/orders/"
BRONZE = f"s3://{BUCKET}/erp/bronze/orders/"
COLUMNAS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]

# 2. LEER SOLO LOS ARCHIVOS NUEVOS (bookmark)
dyf = glue.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [LANDING], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="landing_orders",
)
df = dyf.toDF()

if len(df.columns) == 0 or df.rdd.isEmpty():
    print("No hay archivos nuevos. Nada que procesar.")
else:
    # 3. VALIDAR ENCABEZADOS (mismo conjunto de columnas que Olist)
    print("Columnas leidas:", df.columns)
    faltantes = [c for c in COLUMNAS if c not in df.columns]
    sobrantes = [c for c in df.columns if c not in COLUMNAS]
    if faltantes or sobrantes:
        raise ValueError(f"ERROR: encabezados incorrectos. Faltan: {faltantes}. Sobran: {sobrantes}")
    df = df.select(*[F.trim(F.col(c).cast("string")).alias(c) for c in COLUMNAS])

    # 4. VALIDAR CLAVE Y FECHA DE COMPRA (definen la fila y su partición)
    total = df.count()
    sin_clave = df.filter(F.col("order_id").isNull() | (F.col("order_id") == "")).count()
    sin_compra = df.filter(F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss").isNull()).count()
    print(f"Filas leidas: {total} | sin order_id: {sin_clave} | fecha de compra invalida: {sin_compra}")
    if sin_clave or sin_compra:
        raise ValueError("ERROR: hay pedidos sin order_id o con order_purchase_timestamp invalido.")

    # 5. TRAZABILIDAD Y PARTICION (periodo de negocio = mes de compra)
    bronze = (df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.substring("order_purchase_timestamp", 1, 4))
        .withColumn("month", F.substring("order_purchase_timestamp", 6, 2)))

    # 6. DESCARTAR FILAS IDENTICAS DENTRO DEL LOTE (distintos estados del mismo pedido se conservan)
    bronze = bronze.dropDuplicates(COLUMNAS)
    escritas = bronze.count()
    print(f"Filas identicas descartadas: {total - escritas}")

    # 7. ESCRIBIR EN BRONZE
    bronze.write.mode("append").partitionBy("year", "month").parquet(BRONZE)
    print("Filas escritas en bronze:", escritas)

job.commit()
