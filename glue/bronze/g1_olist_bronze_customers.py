import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - CLIENTES (customers)        APLICADO 2026-11-16
# Cambios respecto del job vigente:
#   - un customer_id repetido en el lote ya NO detiene el job: se conserva la fila del archivo más
#     reciente y se informa cuántas se descartaron (con cargas incrementales y reenvíos va a pasar)
#   - sin sys.exit(0): si no hay archivos nuevos el job termina normalmente (if/else), así el
#     orquestador lo ve como exitoso sin depender de cómo Glue trate SystemExit
#   - origen_datos: "colab" para los archivos de la Lambda del simulador, "olist" para los lotes
#   - zip con ceros a la izquierda desde bronze (nunca como número)
# ==========================================

# 1. INICIALIZAR AWS GLUE Y APACHE SPARK
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 2. RUTAS Y COLUMNAS
ruta_landing = "s3://trabajo-integrador-g1/erp/landing/customers/"
ruta_bronze = "s3://trabajo-integrador-g1/erp/bronze/customers/"
columnas_esperadas = ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                      "customer_city", "customer_state"]
# Archivos de la Lambda: customers_AAAA_MM_<AAAAMMDDTHHMMSS>_<id>.csv
patron_colab = r"customers_\d{4}_\d{2}_\d{8}T\d{6}_[0-9a-z]+\.csv$"

# 3. LEER SOLO ARCHIVOS NUEVOS (BOOKMARK)
dyf_clientes = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [ruta_landing], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="landing_customers",
)
df_clientes = dyf_clientes.toDF()

if len(df_clientes.columns) == 0 or df_clientes.rdd.isEmpty():
    print("No hay archivos nuevos")
else:
    # 4. VALIDACION DE ENCABEZADOS
    if sorted(df_clientes.columns) != sorted(columnas_esperadas):
        raise ValueError(f"Encabezados incorrectos. Esperado: {columnas_esperadas}. Recibido: {df_clientes.columns}")
    df_clientes = (df_clientes
                   .select([F.trim(F.col(c).cast("string")).alias(c) for c in columnas_esperadas])
                   .withColumn("customer_zip_code_prefix", F.lpad("customer_zip_code_prefix", 5, "0"))
                   .withColumn("archivo_origen", F.input_file_name()))
    print("OK: Encabezados correctos")

    # 5. DATA QUALITY - IDENTIFICADORES (esto sí detiene el job: una fila sin clave no se puede cargar)
    cantidad_invalidos = df_clientes.filter(
        F.col("customer_id").isNull() | (F.col("customer_id") == "")
        | F.col("customer_unique_id").isNull() | (F.col("customer_unique_id") == "")
    ).count()
    if cantidad_invalidos > 0:
        raise ValueError(f"ERROR: {cantidad_invalidos} clientes tienen identificadores invalidos")
    print("OK: Identificadores validos")

    # 6. DUPLICADOS DENTRO DEL LOTE: se conserva el archivo más reciente (el nombre lleva la hora)
    total = df_clientes.count()
    ventana = Window.partitionBy("customer_id").orderBy(F.col("archivo_origen").desc())
    df_clientes = (df_clientes.withColumn("_rn", F.row_number().over(ventana))
                   .filter(F.col("_rn") == 1).drop("_rn"))
    unicos = df_clientes.count()
    print(f"Clientes leidos: {total} | duplicados descartados: {total - unicos}")

    # 7. TRAZABILIDAD Y PARTICIONES (year/month desde la ruta)
    df_bronze = (df_clientes
        .withColumn("year", F.regexp_extract("archivo_origen", r"year=(\d{4})", 1))
        .withColumn("month", F.regexp_extract("archivo_origen", r"month=(\d{2})", 1))
        .withColumn("fecha_ingesta", F.current_timestamp())
        .withColumn("origen_datos", F.when(F.col("archivo_origen").rlike(patron_colab), "colab").otherwise("olist")))
    sin_particion = df_bronze.filter((F.col("year") == "") | (F.col("month") == "")).count()
    if sin_particion > 0:
        raise ValueError(f"ERROR: {sin_particion} registros sin year/month en la ruta")

    # 8. ESCRIBIR EN BRONZE (INCREMENTAL)
    df_bronze.write.mode("append").partitionBy("year", "month").parquet(ruta_bronze)
    print(f"OK: Ingesta Bronze completada ({unicos} clientes)")

# 9. FINALIZAR JOB (guarda el bookmark)
job.commit()
