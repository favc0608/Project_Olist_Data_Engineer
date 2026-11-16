import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL BRONZE - PRECIOS DE LA COMPETENCIA (fuente web)
# landing: web/landing/precios/year=/month=/day=/<tienda>-<run_id>.jsonl.gz
#          (Lambda g1-olist-web-scraper: una línea por producto listado y tienda)
# control: web/landing/control/year=/month=/day=/<run_id>.json (resultado de las reglas R1-R13)
# bronze:  web/bronze/precios/year=/month=/ (Parquet, partición por fecha de captura)
#          web/bronze/_control/archivos_precios/ (archivos de landing ya procesados)
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
PREFIJO_LANDING = "web/landing/precios/"
PREFIJO_CONTROL_RUN = "web/landing/control/year="
RUTA_BRONZE = f"s3://{BUCKET}/web/bronze/precios/"
RUTA_CONTROL = f"s3://{BUCKET}/web/bronze/_control/archivos_precios/"
TIENDAS = ["mercadoabierto", "selva", "compree", "ridley"]

# Esquema explícito: cada tienda trae un subconjunto de estas columnas (las demás quedan nulas)
ESQUEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("modo", T.StringType()),
    T.StructField("capturado_en", T.StringType()), T.StructField("version_sitio", T.StringType()),
    T.StructField("tienda", T.StringType()), T.StructField("sku_loja", T.StringType()),
    T.StructField("ean", T.StringType()), T.StructField("url", T.StringType()),
    T.StructField("titulo", T.StringType()), T.StructField("pagina", T.IntegerType()),
    T.StructField("precio_brl", T.DoubleType()), T.StructField("precio_lista_brl", T.DoubleType()),
    T.StructField("precio_usd", T.DoubleType()), T.StructField("precio_lista_usd", T.DoubleType()),
    T.StructField("precio_max_brl", T.DoubleType()), T.StructField("precio_cartao_brl", T.DoubleType()),
    T.StructField("precio_jsonld", T.DoubleType()), T.StructField("descuento_pct", T.IntegerType()),
    T.StructField("cuotas", T.IntegerType()), T.StructField("valor_cuota", T.DoubleType()),
    T.StructField("texto_estoque", T.StringType()), T.StructField("en_stock", T.BooleanType()),
    T.StructField("rating", T.DoubleType()), T.StructField("avaliacoes", T.IntegerType()),
    T.StructField("vendidos", T.IntegerType()), T.StructField("localizacao", T.StringType()),
    T.StructField("plus", T.BooleanType()), T.StructField("partnumber", T.StringType()),
    T.StructField("ficha_ok", T.BooleanType()), T.StructField("precio_ficha_brl", T.DoubleType()),
    T.StructField("fretes_json", T.StringType()), T.StructField("otros_vendedores_json", T.StringType()),
    T.StructField("sku_olist", T.StringType()), T.StructField("product_id", T.StringType()),
    T.StructField("precio_olist", T.DoubleType()), T.StructField("diferencia_pct", T.DoubleType()),
])
s3 = boto3.client("s3")


def existe(prefijo):
    return s3.list_objects_v2(Bucket=BUCKET, Prefix=prefijo, MaxKeys=1).get("KeyCount", 0) > 0


def listar(prefijo, sufijo):
    claves = []
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefijo):
        claves += [o["Key"] for o in pagina.get("Contents", []) if o["Key"].endswith(sufijo)]
    return claves


# 2. ARCHIVOS NUEVOS EN LANDING
# Control por archivo (igual que los eventos del ERP): verificable con una consulta y re-ejecutable sin riesgo.
archivos_landing = [f"s3://{BUCKET}/{k}" for k in listar(PREFIJO_LANDING, ".jsonl.gz")]
existe_bronze = existe("web/bronze/precios/")
procesados = set()
if existe("web/bronze/_control/archivos_precios/"):
    procesados = {f["archivo"] for f in spark.read.parquet(RUTA_CONTROL).select("archivo").collect()}
nuevos = sorted(set(archivos_landing) - procesados)
print(f"Archivos en landing: {len(archivos_landing)} | ya procesados: {len(procesados)} | nuevos: {len(nuevos)}")


def resultado_reglas():
    """run_id -> (ok de la corrida, {tienda: ok}) desde los archivos de control del scraper."""
    estado = {}
    for clave in listar(PREFIJO_CONTROL_RUN, ".json"):
        try:
            control = json.loads(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read())
        except ValueError:
            continue
        estado[control.get("run_id")] = (bool(control.get("ok")),
                                         {i["tienda"]: bool(i["ok"]) for i in control.get("informes", [])})
    return estado


def procesar(nuevos):
    # 3. LEER
    df = (spark.read.schema(ESQUEMA).json(nuevos)
          .withColumn("_source_file", F.input_file_name())
          .withColumn("capturado_en", F.to_timestamp("capturado_en"))
          .cache())
    total_leidos = df.count()
    print(f"Filas leídas: {total_leidos}")

    # 4. VALIDACIONES (falla el job si landing no cumple lo que promete el scraper)
    archivos_leidos = df.select("_source_file").distinct().count()
    if archivos_leidos != len(nuevos):
        raise ValueError(f"ERROR: se esperaban {len(nuevos)} archivos y se leyeron {archivos_leidos}.")
    invalidas = df.filter(
        F.col("run_id").isNull() | F.col("capturado_en").isNull() | F.col("sku_loja").isNull()
        | ~F.col("tienda").isin(TIENDAS) | F.col("precio_brl").isNull() | (F.col("precio_brl") <= 0)
    )
    n_invalidas = invalidas.count()
    print(f"Filas inválidas: {n_invalidas}")
    if n_invalidas > 0:
        invalidas.select("_source_file", "tienda", "sku_loja", "precio_brl").show(10, truncate=False)
        raise ValueError("ERROR: hay filas sin clave, tienda desconocida o precio inválido.")

    # 5. DEDUPLICAR: un producto por tienda y corrida
    ventana = Window.partitionBy("run_id", "tienda", "sku_loja").orderBy(F.col("_source_file"))
    df_unicos = df.withColumn("_fila", F.row_number().over(ventana)).filter("_fila = 1").drop("_fila")
    duplicados = total_leidos - df_unicos.count()
    if existe_bronze:
        existentes = spark.read.parquet(RUTA_BRONZE).select("run_id", "tienda", "sku_loja").distinct()
        antes = df_unicos.count()
        df_unicos = df_unicos.join(existentes, ["run_id", "tienda", "sku_loja"], "left_anti")
        duplicados += antes - df_unicos.count()
    print(f"Duplicados descartados: {duplicados}")

    # 6. RESULTADO DE LAS REGLAS DEL SCRAPER (para que silver use solo capturas confiables)
    reglas = resultado_reglas()
    filas_reglas = [(run, tienda, ok_t) for run, (_, por_tienda) in reglas.items() for tienda, ok_t in por_tienda.items()]
    df_reglas = spark.createDataFrame(filas_reglas, "run_id string, tienda string, reglas_ok boolean")
    df_bronze = (
        df_unicos.join(F.broadcast(df_reglas), ["run_id", "tienda"], "left")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format("capturado_en", "yyyy"))
        .withColumn("month", F.date_format("capturado_en", "MM"))
        .cache()
    )
    total_escribir = df_bronze.count()
    sin_control = df_bronze.filter(F.col("reglas_ok").isNull()).count()
    if sin_control:
        print(f"AVISO: {sin_control} filas sin archivo de control (reglas_ok nulo).")

    leidos_por_archivo = {f["_source_file"]: f["count"] for f in df.groupBy("_source_file").count().collect()}
    escritos_por_archivo = {f["_source_file"]: f["count"] for f in df_bronze.groupBy("_source_file").count().collect()}
    resumen = df_bronze.groupBy("tienda").agg(F.count("*").alias("filas"),
                                              F.sum(F.col("reglas_ok").cast("int")).alias("reglas_ok")).collect()

    # 7. ESCRIBIR EN BRONZE
    if total_escribir > 0:
        df_bronze.write.mode("append").partitionBy("year", "month").parquet(RUTA_BRONZE)

    # 8. REGISTRAR ARCHIVOS PROCESADOS (después de escribir; si falla antes, se reprocesan sin duplicar)
    control = spark.createDataFrame(
        [(a, int(n), int(escritos_por_archivo.get(a, 0)), args.get("JOB_RUN_ID", ""))
         for a, n in leidos_por_archivo.items()],
        "archivo string, filas_leidas int, filas_escritas int, job_run_id string",
    ).withColumn("procesado_en", F.current_timestamp())
    control.coalesce(1).write.mode("append").parquet(RUTA_CONTROL)

    print(f"Filas escritas en bronze: {total_escribir}")
    for fila in resumen:
        print(f"  {fila['tienda']:<16} {fila['filas']:>6} filas | con reglas OK: {fila['reglas_ok']}")
    print("ETL BRONZE PRECIOS WEB FINALIZADO CORRECTAMENTE.")
    df_bronze.unpersist()
    df.unpersist()


# 9. EJECUTAR
if nuevos:
    procesar(nuevos)
else:
    print("No hay archivos nuevos. Nada que procesar.")

job.commit()
