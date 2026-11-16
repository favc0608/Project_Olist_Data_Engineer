import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - TRADUCCION DE CATEGORIAS (category_translation)          2026-11-16
# landing: erp/landing/category_translation/year=AAAA/month=MM/*.csv  (CSV original de Olist, con BOM UTF-8)
# bronze:  erp/bronze/category_translation/ (Parquet append, partición year/month de la ruta)
# Bookmark: solo lee archivos nuevos. Sin sys.exit (Glue 5.1 lo marca como fallo).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
LANDING = "s3://trabajo-integrador-g1/erp/landing/category_translation/"
BRONZE = "s3://trabajo-integrador-g1/erp/bronze/category_translation/"
COLUMNAS = ["product_category_name", "product_category_name_english"]

# 2. LEER SOLO ARCHIVOS NUEVOS (bookmark)
dyf = glue.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [LANDING], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="landing_category_translation",
)
df = dyf.toDF()

if len(df.columns) == 0 or df.rdd.isEmpty():
    print("No hay archivos nuevos. Nada que procesar.")
else:
    # 3. ENCABEZADOS (el CSV de Olist trae BOM: '﻿product_category_name')
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])
    print("Columnas leidas:", df.columns)
    if sorted(df.columns) != sorted(COLUMNAS):
        raise ValueError(f"ERROR: encabezados incorrectos. Esperado {COLUMNAS}, recibido {df.columns}")
    df = df.select(*[F.trim(F.col(c).cast("string")).alias(c) for c in COLUMNAS])

    # 4. VALIDAR CLAVE
    total = df.count()
    sin_clave = df.filter(F.col("product_category_name").isNull() | (F.col("product_category_name") == "")).count()
    print(f"Filas: {total} | sin categoría: {sin_clave}")
    if sin_clave:
        raise ValueError("ERROR: filas sin product_category_name.")

    # 5. TRAZABILIDAD Y PARTICION
    bronze = (df.withColumn("_source_file", F.input_file_name())
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("year", F.regexp_extract("_source_file", r"year=(\d{4})", 1))
                .withColumn("month", F.regexp_extract("_source_file", r"month=(\d{2})", 1)))
    if bronze.filter((F.col("year") == "") | (F.col("month") == "")).limit(1).count():
        raise ValueError("ERROR: archivo fuera de year=AAAA/month=MM/.")

    # 6. ESCRIBIR
    bronze.write.mode("append").partitionBy("year", "month").parquet(BRONZE)
    print("Filas escritas en bronze:", total)

job.commit()
