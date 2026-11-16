import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    row_number
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL SILVER - PRODUCTOS (SCD TIPO 1, MERGE POR product_id)
# 2026-11-16: se agrega row_hash (columnas de negocio) y el MERGE solo actualiza si cambió;
# el resto del job es el del dueño (script del 2026-11-14, respaldo en scripts/_respaldo/2026-11-16/).
# ============================================

# 1. INICIALIZAR AWS GLUE
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 2. RUTAS DE AMAZON S3
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/products/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/products/"

COLUMNAS_NEGOCIO = [
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

# 3. LEER DATOS DESDE BRONZE
df = spark.read.parquet(RUTA_BRONZE)
total_bronze = df.count()
print("========== SILVER PRODUCTOS ==========")
print(f"Registros Bronze: {total_bronze}")
if total_bronze == 0:
    raise ValueError("Bronze de productos esta vacio")
if "product_id" not in df.columns:
    raise ValueError("No existe product_id en Bronze")

# 4. NORMALIZAR IDENTIFICADOR
df = df.withColumn("product_id", trim(col("product_id").cast("string")))

# 5. IDENTIFICAR Y ELIMINAR IDS INVALIDOS
ids_invalidos = df.filter(col("product_id").isNull() | (col("product_id") == "")).count()
df = df.filter(col("product_id").isNotNull() & (col("product_id") != ""))
print(f"IDs invalidos: {ids_invalidos}")

# 6. DEDUPLICACION POR PRODUCT_ID (la ingesta más reciente)
duplicados = df.groupBy("product_id").count().filter(col("count") > 1).count()
print(f"IDs con duplicados: {duplicados}")
ventana = Window.partitionBy("product_id").orderBy(col("fecha_ingesta").desc())
df = df.withColumn("rn", row_number().over(ventana)).filter(col("rn") == 1).drop("rn")

# 7. CONVERSION DE TIPOS DE DATOS
for nombre in ["product_name_lenght", "product_description_lenght", "product_photos_qty"]:
    if nombre in df.columns:
        df = df.withColumn(nombre, col(nombre).cast(IntegerType()))
for nombre in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
    if nombre in df.columns:
        df = df.withColumn(nombre, col(nombre).cast(DoubleType()))

# 8. LIMPIEZA DE CATEGORIAS
if "product_category_name" in df.columns:
    df = df.withColumn("product_category_name", trim(col("product_category_name")))

# 9. HASH DE NEGOCIO Y METADATOS DE SILVER
partes = [F.coalesce(col(c).cast("string"), lit("<NULL>")) for c in COLUMNAS_NEGOCIO if c in df.columns]
df_silver = (
    df.withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))
      .withColumn("fecha_procesamiento_silver", current_timestamp())
      .withColumn("capa_datos", lit("silver"))
)

# 10. VALIDACION FINAL
total_silver = df_silver.count()
nulos_id = df_silver.filter(col("product_id").isNull() | (col("product_id") == "")).count()
if nulos_id > 0:
    raise ValueError("Validacion fallida: product_id invalido")
if df_silver.select("product_id").distinct().count() != total_silver:
    raise ValueError("Validacion fallida: product_id no es unico")
print(f"Registros Silver: {total_silver}")

# 11. ESCRIBIR EN DELTA (MERGE POR PRODUCT_ID, solo si cambió el hash)
if DeltaTable.isDeltaTable(spark, RUTA_SILVER):
    tabla_silver = DeltaTable.forPath(spark, RUTA_SILVER)
    if "row_hash" not in tabla_silver.toDF().columns:
        # la condición del MERGE usa row_hash: hay que agregar la columna antes (autoMerge no alcanza)
        spark.sql(f"ALTER TABLE delta.`{RUTA_SILVER}` ADD COLUMNS (row_hash STRING)")
        tabla_silver = DeltaTable.forPath(spark, RUTA_SILVER)
        print("Columna row_hash agregada a silver products.")
    (
        tabla_silver.alias("t")
        .merge(df_silver.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdateAll(condition="t.row_hash IS NULL OR t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("MERGE ejecutado en Silver de productos (solo filas nuevas o con cambios)")
else:
    df_silver.write.format("delta").mode("overwrite").save(RUTA_SILVER)
    print("Silver de productos creado por primera vez")

print("Silver de productos generado correctamente")
print("=======================================")

# 12. FINALIZAR JOB
job.commit()
