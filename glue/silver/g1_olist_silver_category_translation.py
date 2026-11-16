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
# ETL SILVER - TRADUCCION DE CATEGORIAS (SCD TIPO 1, MERGE por product_category_name)    2026-11-16
# bronze: erp/bronze/category_translation/  ->  silver: erp/silver/category_translation/ (Delta)
# El dataset no traduce 2 categorías que sí usan los productos (perfilado 2026-10-16): se completan con una
# traducción del grupo marcada origen_datos = 'complemento_g1' (si un lote futuro las trae, gana el lote).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/category_translation/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/category_translation/"
COMPLEMENTO = [("pc_gamer", "pc_gamer"),
               ("portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_and_food_preparers")]

# 2. LEER BRONZE
bronze = spark.read.parquet(RUTA_BRONZE)
total = bronze.count()
print(f"Registros bronze: {total}")
if total == 0:
    raise ValueError("ERROR: bronze de category_translation está vacío.")

# 3. LIMPIEZA Y DEDUPLICACION (la carga más reciente por categoría)
w = Window.partitionBy("product_category_name").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
lote = (bronze
        .withColumn("product_category_name", F.lower(F.trim("product_category_name")))
        .withColumn("product_category_name_english", F.lower(F.trim("product_category_name_english")))
        .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
        .select("product_category_name", "product_category_name_english", "_source_file", "_ingested_at",
                F.lit("olist").alias("origen_datos")))

# 4. COMPLETAR CATEGORIAS SIN TRADUCCION EN EL DATASET
complemento = (spark.createDataFrame(COMPLEMENTO, ["product_category_name", "product_category_name_english"])
               .join(lote.select("product_category_name"), "product_category_name", "left_anti")
               .withColumn("_source_file", F.lit("complemento_g1"))
               .withColumn("_ingested_at", F.current_timestamp())
               .withColumn("origen_datos", F.lit("complemento_g1")))
silver = (lote.unionByName(complemento)
          .withColumn("row_hash", F.sha2(F.coalesce(F.col("product_category_name_english"), F.lit("<NULL>")), 256))
          .withColumn("fecha_procesamiento_silver", F.current_timestamp())
          .withColumn("capa_datos", F.lit("silver")))

# 5. VALIDACIONES
sin_traduccion = silver.filter(F.col("product_category_name_english").isNull() | (F.col("product_category_name_english") == "")).count()
filas = silver.count()
print(f"Categorías: {filas} (complemento: {complemento.count()}) | sin traducción: {sin_traduccion}")
if sin_traduccion:
    raise ValueError("ERROR: categorías sin traducción al inglés.")
if silver.groupBy("product_category_name").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: categoría duplicada.")

# 6. ESCRIBIR (SCD1: MERGE que actualiza solo si cambió la traducción)
if DeltaTable.isDeltaTable(spark, RUTA_SILVER):
    (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
        .merge(silver.alias("s"), "t.product_category_name = s.product_category_name")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGE aplicado.")
else:
    silver.write.format("delta").mode("overwrite").save(RUTA_SILVER)
    print("Primera carga: tabla Delta creada.")

print(f"Categorías en silver: {spark.read.format('delta').load(RUTA_SILVER).count()}")
print("ETL SILVER CATEGORY_TRANSLATION FINALIZADO CORRECTAMENTE.")
job.commit()
