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
# ETL SILVER - PRECIOS DE LA COMPETENCIA (SCD TIPO 2)
# bronze:  web/bronze/precios/ (una fila por producto, tienda y captura)
# silver:  web/silver/precios_competencia/ (Delta, una versión por cada cambio de precio/stock)
# control: web/silver/_control/precios_competencia/ (última captura procesada)
#
# SCD2: clave = tienda + sku_loja. Atributos historizados: precios, descuento, cuotas y stock.
# valid_from = momento de la captura en que se vio el cambio (no la hora del job): reprocesar da lo mismo.
# valid_to = captura en que apareció la versión siguiente (nulo si vigente). is_current = valid_to nulo.
# Solo se usan capturas cuyas reglas del scraper quedaron OK (reglas_ok = true).
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
RUTA_SILVER = f"s3://{BUCKET}/web/silver/precios_competencia/"
RUTA_CONTROL = f"s3://{BUCKET}/web/silver/_control/precios_competencia/"

CLAVE = ["tienda", "sku_loja"]
ATRIBUTOS_SCD2 = ["precio_brl", "precio_lista_brl", "descuento_pct", "precio_usd", "precio_lista_usd",
                  "precio_cartao_brl", "cuotas", "valor_cuota", "en_stock"]
DESCRIPTIVOS = ["sku_olist", "product_id", "ean", "precio_olist", "diferencia_pct", "run_id", "version_sitio"]
s3 = boto3.client("s3")


def existe(prefijo):
    return s3.list_objects_v2(Bucket=BUCKET, Prefix=prefijo, MaxKeys=1).get("KeyCount", 0) > 0


def agregar_hash(df):
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in ATRIBUTOS_SCD2]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))


def ejecutar():
    # 2. CAPTURAS NUEVAS (posteriores a la última procesada)
    if not existe("web/bronze/precios/"):
        print("Bronze vacío. Nada que procesar.")
        return

    hay_silver = existe("web/silver/precios_competencia/_delta_log/")
    ultima = None
    if existe("web/silver/_control/precios_competencia/"):
        ultima = spark.read.parquet(RUTA_CONTROL).agg(F.max("ultima_captura")).first()[0]
    print(f"Última captura procesada: {ultima}")

    bronze = spark.read.parquet(RUTA_BRONZE)
    if ultima is not None:
        bronze = bronze.filter(F.col("capturado_en") > F.lit(ultima))
    total_nuevas = bronze.count()
    descartadas = bronze.filter(~F.coalesce(F.col("reglas_ok"), F.lit(False))).count()
    print(f"Filas nuevas en bronze: {total_nuevas} | descartadas por reglas no OK: {descartadas}")

    snap = bronze.filter(F.col("reglas_ok"))
    # Una fila por clave y captura (bronze ya deduplica por corrida; esto protege contra reprocesos)
    w_dup = Window.partitionBy(*CLAVE, "capturado_en").orderBy(F.col("_ingested_at").desc())
    snap = (snap.withColumn("_fila", F.row_number().over(w_dup)).filter("_fila = 1").drop("_fila")
            .select(*CLAVE, "capturado_en", *ATRIBUTOS_SCD2, *DESCRIPTIVOS))
    snap = agregar_hash(snap).withColumn("_origen", F.lit("nuevo")).cache()
    n_snap = snap.count()

    if n_snap == 0:
        print("No hay capturas nuevas confiables. Nada que procesar.")
        return

    # 3. VALIDACIONES
    if snap.filter(F.col("precio_brl").isNull() | (F.col("precio_brl") <= 0)).limit(1).count():
        raise ValueError("ERROR: hay precios nulos o no positivos.")
    if snap.filter(F.col("descuento_pct") < 0).limit(1).count():
        raise ValueError("ERROR: hay descuentos negativos.")

    # 4. DETECTAR CAMBIOS
    # Se une la versión vigente de silver con las capturas nuevas y, en orden de captura,
    # se conserva solo la captura donde el hash cambia respecto de la anterior.
    columnas = [*CLAVE, "capturado_en", *ATRIBUTOS_SCD2, *DESCRIPTIVOS, "row_hash", "_origen"]
    union = snap.select(*columnas)
    if hay_silver:
        vigentes = (spark.read.format("delta").load(RUTA_SILVER).filter("is_current")
                    .withColumnRenamed("valid_from", "capturado_en")
                    .withColumnRenamed("run_id_origen", "run_id")
                    .withColumn("_origen", F.lit("actual")))
        union = union.unionByName(vigentes.select(*columnas))

    w = Window.partitionBy(*CLAVE).orderBy("capturado_en")
    cambios = (union.withColumn("_hash_previo", F.lag("row_hash").over(w))
               .filter((F.col("_origen") == "nuevo")
                       & (F.col("_hash_previo").isNull() | (F.col("_hash_previo") != F.col("row_hash"))))
               .drop("_hash_previo"))
    w_c = Window.partitionBy(*CLAVE).orderBy("capturado_en")
    versiones = (cambios
                 .withColumn("valid_to", F.lead("capturado_en").over(w_c))
                 .withColumnRenamed("capturado_en", "valid_from")
                 .withColumnRenamed("run_id", "run_id_origen")
                 .withColumn("is_current", F.col("valid_to").isNull())
                 .withColumn("fecha_procesamiento_silver", F.current_timestamp())
                 .withColumn("year", F.year("valid_from"))
                 .withColumn("month", F.month("valid_from"))
                 .drop("_origen")
                 .cache())
    n_versiones = versiones.count()
    print(f"Capturas nuevas confiables: {n_snap} | versiones nuevas (cambios): {n_versiones}")

    # 5. ESCRIBIR EN SILVER (2 pasos, como sellers: MERGE que cierra + append de versiones)
    if n_versiones > 0:
        if hay_silver:
            # a) cerrar la versión vigente de las claves que cambiaron, con la primera captura del cambio
            cierres = versiones.groupBy(*CLAVE).agg(F.min("valid_from").alias("nuevo_valid_from"))
            (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
             .merge(cierres.alias("s"), "t.tienda = s.tienda AND t.sku_loja = s.sku_loja AND t.is_current = true")
             .whenMatchedUpdate(set={"valid_to": "s.nuevo_valid_from", "is_current": "false",
                                     "fecha_procesamiento_silver": "current_timestamp()"})
             .execute())
            # b) agregar las versiones nuevas
            versiones.write.format("delta").mode("append").save(RUTA_SILVER)
        else:
            versiones.write.format("delta").mode("overwrite").partitionBy("year", "month").save(RUTA_SILVER)

    # 6. VALIDAR EL RESULTADO
    silver = spark.read.format("delta").load(RUTA_SILVER)
    vigentes_dup = silver.filter("is_current").groupBy(*CLAVE).count().filter("count > 1").limit(1).count()
    if vigentes_dup:
        raise ValueError("ERROR: hay claves con más de una versión vigente.")
    if silver.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).limit(1).count():
        raise ValueError("ERROR: hay versiones con valid_to <= valid_from.")

    # 7. REGISTRAR LA ÚLTIMA CAPTURA PROCESADA (después de escribir)
    maxima = snap.agg(F.max("capturado_en")).first()[0]
    (spark.createDataFrame([(maxima, n_snap, n_versiones, args.get("JOB_RUN_ID", ""))],
                           "ultima_captura timestamp, capturas int, versiones int, job_run_id string")
     .withColumn("procesado_en", F.current_timestamp())
     .coalesce(1).write.mode("append").parquet(RUTA_CONTROL))

    total = silver.count()
    actuales = silver.filter("is_current").count()
    print(f"Silver: {total} versiones | {actuales} vigentes | última captura: {maxima}")
    print("ETL SILVER PRECIOS COMPETENCIA (SCD2) FINALIZADO CORRECTAMENTE.")
    snap.unpersist()
    versiones.unpersist()


# 8. EJECUTAR
ejecutar()
job.commit()
