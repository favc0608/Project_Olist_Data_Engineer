import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - RESEÑAS (order_reviews) DEL ERP
# landing: erp/landing/reviews/year=/month=/*.csv (lotes mensuales y CSV del simulador)
# bronze:  erp/bronze/reviews/year=/month=/ (Parquet append, partición por fecha de la reseña)
# Mismo patrón que orders: el job bookmark lee solo los archivos nuevos.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
BUCKET = "trabajo-integrador-g1"
LANDING = f"s3://{BUCKET}/erp/landing/reviews/"
BRONZE = f"s3://{BUCKET}/erp/bronze/reviews/"
COLUMNAS = ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp"]

# 2. LEER SOLO LOS ARCHIVOS NUEVOS (bookmark)
# multiLine: los comentarios del CSV histórico de Olist traen saltos de línea dentro de comillas
dyf = glue.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [LANDING], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "multiLine": True, "escaper": '"'},
    transformation_ctx="landing_reviews",
)
df = dyf.toDF()

if len(df.columns) == 0 or df.rdd.isEmpty():
    print("No hay archivos nuevos. Nada que procesar.")
else:
    # 3. VALIDAR ENCABEZADOS Y CLAVES
    print("Columnas leidas:", df.columns)
    faltantes = [c for c in COLUMNAS if c not in df.columns]
    if faltantes:
        raise ValueError(f"ERROR: faltan columnas en landing: {faltantes}")
    total = df.count()
    sin_clave = df.filter(F.col("review_id").isNull() | F.col("order_id").isNull()).count()
    sin_fecha = df.filter(F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss").isNull()).count()
    print(f"Filas leidas: {total} | sin clave: {sin_clave} | sin fecha de creacion valida: {sin_fecha}")
    if sin_clave or sin_fecha:
        raise ValueError("ERROR: hay reseñas sin review_id/order_id o con review_creation_date invalida.")

    # 4. TRAZABILIDAD Y PARTICION (periodo de negocio = fecha de la reseña)
    bronze = (df.select(*COLUMNAS)
              .withColumn("_source_file", F.input_file_name())
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("year", F.substring("review_creation_date", 1, 4))
              .withColumn("month", F.substring("review_creation_date", 6, 2)))

    # 5. ESCRIBIR EN BRONZE
    bronze.write.mode("append").partitionBy("year", "month").parquet(BRONZE)
    print("Filas escritas en bronze:", total)

job.commit()
