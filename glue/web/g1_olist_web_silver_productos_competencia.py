import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL SILVER - PRODUCTOS DE LA COMPETENCIA (SCD TIPO 1)
# bronze: web/bronze/precios/
# silver: web/silver/productos_competencia/ (Delta, una fila por tienda + sku_loja)
#
# Ficha descriptiva del producto en cada tienda y su cruce con el catálogo Olist (sku_olist, product_id).
# SCD1: se sobrescribe con la captura más reciente, solo si cambia row_hash (mismo patrón que orders).
# primera_captura / ultima_captura dicen desde cuándo y hasta cuándo se vio el producto en la tienda.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
BUCKET = "trabajo-integrador-g1"
RUTA_BRONZE = f"s3://{BUCKET}/web/bronze/precios/"
RUTA_SILVER = f"s3://{BUCKET}/web/silver/productos_competencia/"
CLAVE = ["tienda", "sku_loja"]
ATRIBUTOS = ["sku_olist", "product_id", "ean", "titulo", "url", "partnumber", "localizacao", "plus",
             "rating", "avaliacoes", "vendidos", "texto_estoque"]
s3 = boto3.client("s3")


def existe(prefijo):
    return s3.list_objects_v2(Bucket=BUCKET, Prefix=prefijo, MaxKeys=1).get("KeyCount", 0) > 0


def ejecutar():
    # 2. LEER BRONZE (volumen pequeño: se relee completo y el MERGE solo cambia lo distinto)
    if not existe("web/bronze/precios/"):
        print("Bronze vacío. Nada que procesar.")
        return
    bronze = spark.read.parquet(RUTA_BRONZE).filter(F.col("reglas_ok"))
    total = bronze.count()
    print(f"Filas de bronze con reglas OK: {total}")
    if total == 0:
        print("No hay capturas confiables. Nada que procesar.")
        return

    # 3. VALIDACIONES
    if bronze.filter(F.col("sku_loja").isNull() | F.col("tienda").isNull()).limit(1).count():
        raise ValueError("ERROR: hay filas sin clave (tienda, sku_loja).")
    sin_cruce = bronze.filter(F.col("sku_olist").isNull()).select(*CLAVE).distinct().count()
    print(f"Productos sin cruce con el catálogo Olist: {sin_cruce}")

    # 4. ÚLTIMA VERSIÓN DE CADA PRODUCTO + PRIMERA/ÚLTIMA CAPTURA
    w = Window.partitionBy(*CLAVE).orderBy(F.col("capturado_en").desc(), F.col("_ingested_at").desc())
    ultima = (bronze.withColumn("_fila", F.row_number().over(w)).filter("_fila = 1")
              .select(*CLAVE, *ATRIBUTOS))
    fechas = bronze.groupBy(*CLAVE).agg(F.min("capturado_en").alias("primera_captura"),
                                        F.max("capturado_en").alias("ultima_captura"),
                                        F.countDistinct("run_id").alias("capturas"))
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in ATRIBUTOS + ["ultima_captura"]]
    df_silver = (ultima.join(fechas, CLAVE)
                 .withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))
                 .withColumn("fecha_procesamiento_silver", F.current_timestamp())
                 .withColumn("capa_datos", F.lit("silver")))
    n = df_silver.count()
    if df_silver.groupBy(*CLAVE).count().filter("count > 1").limit(1).count():
        raise ValueError("ERROR: claves duplicadas antes de escribir.")

    # 5. ESCRIBIR EN SILVER (SCD1: MERGE que sobrescribe solo si cambió el hash)
    if existe("web/silver/productos_competencia/_delta_log/"):
        (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
         .merge(df_silver.alias("s"), "t.tienda = s.tienda AND t.sku_loja = s.sku_loja")
         .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
         .whenNotMatchedInsertAll()
         .execute())
        print("MERGE aplicado sobre Silver existente.")
    else:
        df_silver.write.format("delta").mode("overwrite").partitionBy("tienda").save(RUTA_SILVER)
        print("Silver creado.")

    total_silver = spark.read.format("delta").load(RUTA_SILVER).count()
    if total_silver < n:
        raise ValueError(f"ERROR: silver tiene {total_silver} filas y se esperaban al menos {n}.")
    print(f"Productos en silver: {total_silver}")
    print("ETL SILVER PRODUCTOS COMPETENCIA (SCD1) FINALIZADO CORRECTAMENTE.")


# 6. EJECUTAR
ejecutar()
job.commit()
