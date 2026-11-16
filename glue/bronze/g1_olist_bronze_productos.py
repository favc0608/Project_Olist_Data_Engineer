import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    col,
    regexp_extract
)

# ==========================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - PRODUCTOS            APLICADO 2026-11-16 (auditoría, ficha E1-07)
# Único cambio respecto del job vigente (script del 2026-11-14):
#   - sin sys.exit(0): si no hay archivos nuevos el job termina normalmente (if/else), así el
#     orquestador lo ve como exitoso sin depender de cómo Glue trate SystemExit.
#   - se valida que year/month salgan de la ruta (antes un archivo fuera de year=/month= se escribía
#     en una partición vacía sin avisar).
# ==========================================

# 1. Inicializar AWS Glue
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 2. Definir las rutas de Amazon S3
RUTA_LANDING = (
    "s3://trabajo-integrador-g1/"
    "erp/landing/products/"
)

RUTA_BRONZE = (
    "s3://trabajo-integrador-g1/"
    "erp/bronze/products/"
)

# 3. Leer los CSV nuevos de Landing (DynamicFrame + transformation_ctx para que funcione el bookmark)
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [RUTA_LANDING], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="landing_products",
)

if dyf.count() == 0:
    print("No hay archivos nuevos en Landing")
else:
    df = dyf.toDF()

    # DynamicFrame lee todo como string, se restauran los tipos numéricos
    columnas_int = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for c in columnas_int:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("int"))

    # 4. Validaciones iniciales
    total = df.count()

    if "product_id" not in df.columns:
        raise ValueError("No existe la columna product_id")

    print(f"Productos leidos desde Landing: {total}")
    print(f"Columnas originales: {len(df.columns)}")

    # 5. Agregar metadatos de ingesta (year y month se extraen de la ruta del archivo)
    df_bronze = (
        df.withColumn("fecha_ingesta", current_timestamp())
          .withColumn("archivo_origen", input_file_name())
          .withColumn("origen_datos", lit("olist"))
          .withColumn("year", regexp_extract(col("archivo_origen"), r"year=(\d{4})", 1))
          .withColumn("month", regexp_extract(col("archivo_origen"), r"month=(\d{2})", 1))
    )

    sin_particion = df_bronze.filter((col("year") == "") | (col("month") == "")).count()
    if sin_particion > 0:
        raise ValueError(f"ERROR: {sin_particion} registros sin year/month en la ruta")

    # 6. Guardar en formato Parquet (append + partición por year/month)
    (
        df_bronze.write
        .mode("append")
        .option("compression", "snappy")
        .partitionBy("year", "month")
        .parquet(RUTA_BRONZE)
    )

    print("Bronze de productos generado correctamente")

# 7. Finalizar el Job (guarda el bookmark)
job.commit()
