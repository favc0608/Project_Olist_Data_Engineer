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
# ETL SILVER - RESEÑAS (order_reviews) DEL ERP - SCD TIPO 1
# bronze: erp/bronze/reviews/
# silver: erp/silver/reviews/ (Delta, partición year/month de review_creation_date)
# Clave: review_id + order_id (en Olist review_id se repite entre pedidos).
# MERGE que sobrescribe solo si cambia row_hash (mismo patrón que orders y order_items).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
BUCKET = "trabajo-integrador-g1"
RUTA_BRONZE = f"s3://{BUCKET}/erp/bronze/reviews/"
RUTA_SILVER = f"s3://{BUCKET}/erp/silver/reviews/"
RUTA_SILVER_ORDERS = f"s3://{BUCKET}/erp/silver/orders/"
CLAVE = ["review_id", "order_id"]
COLUMNAS_HASH = ["review_score", "review_comment_title", "review_comment_message",
                 "review_creation_date", "review_answer_timestamp"]
s3 = boto3.client("s3")


def existe(prefijo):
    return s3.list_objects_v2(Bucket=BUCKET, Prefix=prefijo, MaxKeys=1).get("KeyCount", 0) > 0


def ejecutar():
    # 2. LEER BRONZE
    if not existe("erp/bronze/reviews/"):
        print("Bronze vacío. Nada que procesar.")
        return
    df = spark.read.parquet(RUTA_BRONZE).drop("year", "month")
    total = df.count()
    print(f"Registros Bronze: {total}")

    # 3. TIPOS Y LIMPIEZA
    df = (df
          .withColumn("review_score", F.col("review_score").cast("int"))
          .withColumn("review_creation_date", F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("review_comment_title", F.nullif(F.trim("review_comment_title"), F.lit("")))
          .withColumn("review_comment_message",
                      F.nullif(F.trim(F.regexp_replace("review_comment_message", r"\s+", " ")), F.lit(""))))

    # 4. VALIDACIONES
    if df.filter(F.col("review_id").isNull() | F.col("order_id").isNull()).limit(1).count():
        raise ValueError("ERROR: hay reseñas sin clave.")
    fuera_rango = df.filter(~F.col("review_score").between(1, 5) | F.col("review_score").isNull()).count()
    if fuera_rango:
        raise ValueError(f"ERROR: {fuera_rango} reseñas con puntaje fuera de 1-5.")
    if df.filter(F.col("review_creation_date").isNull()).limit(1).count():
        raise ValueError("ERROR: hay reseñas con fecha de creación inválida.")
    respuesta_antes = df.filter(F.col("review_answer_timestamp") < F.col("review_creation_date")).count()
    print(f"Respuestas anteriores a la creación (se conservan, se reportan): {respuesta_antes}")

    # 5. DEDUPLICAR: la versión más reciente de cada reseña
    w = Window.partitionBy(*CLAVE).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    df = df.withColumn("_fila", F.row_number().over(w)).filter("_fila = 1").drop("_fila")
    unicos = df.count()
    print(f"Duplicados eliminados: {total - unicos}")

    # 6. INTEGRIDAD CON PEDIDOS (informativa: la reseña puede llegar antes que el pedido a silver)
    if existe("erp/silver/orders/_delta_log/"):
        pedidos = spark.read.format("delta").load(RUTA_SILVER_ORDERS).select("order_id")
        huerfanas = df.join(pedidos, "order_id", "left_anti").count()
        print(f"Reseñas sin pedido en silver orders: {huerfanas}")

    # 7. HASH, TRAZABILIDAD Y PARTICIÓN
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in COLUMNAS_HASH]
    df_silver = (df
                 .withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))
                 .withColumn("year", F.year("review_creation_date"))
                 .withColumn("month", F.month("review_creation_date"))
                 .withColumn("fecha_procesamiento_silver", F.current_timestamp())
                 .withColumn("capa_datos", F.lit("silver")))

    # 8. ESCRIBIR EN SILVER (SCD tipo 1)
    if existe("erp/silver/reviews/_delta_log/"):
        (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
         .merge(df_silver.alias("s"), "t.review_id = s.review_id AND t.order_id = s.order_id")
         .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
         .whenNotMatchedInsertAll()
         .execute())
        print("MERGE aplicado sobre Silver existente.")
    else:
        df_silver.write.format("delta").mode("overwrite").partitionBy("year", "month").save(RUTA_SILVER)
        print("Silver creado.")

    # 9. VALIDAR EL RESULTADO
    silver = spark.read.format("delta").load(RUTA_SILVER)
    if silver.groupBy(*CLAVE).count().filter("count > 1").limit(1).count():
        raise ValueError("ERROR: claves duplicadas en silver.")
    print(f"Reseñas en silver: {silver.count()}")
    print("ETL SILVER REVIEWS FINALIZADO CORRECTAMENTE.")


# 10. EJECUTAR
ejecutar()
job.commit()
