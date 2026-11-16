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
# ETL SILVER - VENDEDORES (sellers) - SCD TIPO 2                 2026-11-16
# Reemplaza al job "\tg1_olist_silver_sellers" (nombre con tabulador, fuera del pipeline). Correcciones:
#   - valid_from = inicio del PERIODO del lote (year/month de bronze), no la hora del job: la historia es
#     reproducible (re-ejecutar da el mismo resultado)
#   - seller_zip_code_prefix como texto de 5 dígitos (antes int: se perdían los ceros a la izquierda)
#   - historia completa recalculada desde bronze y aplicada con MERGE por sk_vendedor (idempotente; un lote
#     atrasado reordena la historia)
#   - mismas columnas SCD2 que clientes: valid_from, valid_to, is_current, version, row_hash + clave sustituta
# Unión con hechos: valid_from <= fecha_compra < coalesce(valid_to, '9999-12-31').
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/sellers/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/sellers/"
ATRIBUTOS = ["seller_zip_code_prefix", "seller_city", "seller_state"]

# 2. LEER BRONZE (todas las cargas: la historia se recalcula completa)
bronze = spark.read.parquet(RUTA_BRONZE)
total_bronze = bronze.count()
print(f"Registros Bronze: {total_bronze}")
if total_bronze == 0:
    raise ValueError("ERROR: Bronze de sellers está vacío.")
faltantes = [c for c in ["seller_id", *ATRIBUTOS, "_source_file", "_ingested_at", "year", "month"] if c not in bronze.columns]
if faltantes:
    raise ValueError(f"ERROR: Faltan columnas en bronze: {faltantes}")

# 3. LIMPIEZA Y NORMALIZACION
df = (bronze
      .withColumn("seller_id", F.trim(F.col("seller_id").cast("string")))
      .withColumn("seller_zip_code_prefix", F.lpad(F.trim(F.col("seller_zip_code_prefix").cast("string")), 5, "0"))
      .withColumn("seller_city", F.lower(F.trim(F.col("seller_city").cast("string"))))
      .withColumn("seller_state", F.upper(F.trim(F.col("seller_state").cast("string"))))
      .withColumn("periodo", F.to_timestamp(F.format_string("%04d-%02d-01", F.col("year").cast("int"),
                                                              F.col("month").cast("int")))))

# 4. VALIDACIONES
invalidos = df.filter(F.col("seller_id").isNull() | (F.col("seller_id") == "") | F.col("periodo").isNull()).count()
zips_malos = df.filter(~F.col("seller_zip_code_prefix").rlike(r"^\d{5}$")).count()
estados_malos = df.filter(F.length("seller_state") != 2).count()
print(f"Sin seller_id o periodo: {invalidos} | zip inválido: {zips_malos} | estado inválido: {estados_malos}")
if invalidos or zips_malos or estados_malos:
    raise ValueError("ERROR: vendedores con clave, periodo, zip o estado inválidos.")

# 5. UNA FOTO POR VENDEDOR Y PERIODO (si el mismo periodo llegó dos veces, gana la carga más reciente)
w_foto = Window.partitionBy("seller_id", "periodo").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
fotos = (df.withColumn("_rn", F.row_number().over(w_foto)).filter("_rn = 1").drop("_rn")
         .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in ATRIBUTOS]), 256)))

# 6. VERSIONES: nueva fila solo cuando cambian zip, ciudad o estado
orden = Window.partitionBy("seller_id").orderBy("periodo")
versiones = (fotos
    .withColumn("_hash_previo", F.lag("row_hash").over(orden))
    .filter(F.col("_hash_previo").isNull() | (F.col("_hash_previo") != F.col("row_hash")))
    .withColumn("valid_from", F.col("periodo"))
    .withColumn("valid_to", F.lead("periodo").over(Window.partitionBy("seller_id").orderBy("periodo")))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("version", F.row_number().over(Window.partitionBy("seller_id").orderBy("periodo")))
    .withColumn("sk_vendedor", F.sha2(F.concat_ws("||", "seller_id", F.col("valid_from").cast("string")), 256))
    .withColumn("fecha_procesamiento_silver", F.current_timestamp())
    .withColumn("capa_datos", F.lit("silver"))
    .select("sk_vendedor", "seller_id", *ATRIBUTOS, "valid_from", "valid_to", "is_current", "version", "row_hash",
            "_source_file", "_ingested_at", "fecha_procesamiento_silver", "capa_datos")
    .cache())

vendedores = fotos.select("seller_id").distinct().count()
total = versiones.count()
vigentes = versiones.filter("is_current").count()
print(f"Vendedores: {vendedores} | versiones: {total} | con cambios: {total - vendedores}")
if vigentes != vendedores:
    raise ValueError(f"ERROR: {vigentes} versiones vigentes para {vendedores} vendedores.")
if versiones.groupBy("sk_vendedor").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: sk_vendedor duplicada.")

# 7. ESCRIBIR (primera vez con el esquema nuevo: se reemplaza la tabla del job anterior, cuyo valid_from no era
#    reproducible y cuyo zip era int; luego MERGE por sk_vendedor)
tabla_nueva = DeltaTable.isDeltaTable(spark, RUTA_SILVER) and "sk_vendedor" in DeltaTable.forPath(spark, RUTA_SILVER).toDF().columns
if tabla_nueva:
    (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
        .merge(versiones.alias("s"), "t.sk_vendedor = s.sk_vendedor")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash OR NOT (t.valid_to <=> s.valid_to) "
                                        "OR t.is_current <> s.is_current OR t.version <> s.version")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())
    print("MERGE SCD2 aplicado.")
else:
    versiones.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(RUTA_SILVER)
    print("Tabla SCD2 de sellers (re)creada con el esquema nuevo.")

# 8. VALIDAR EL RESULTADO
final = spark.read.format("delta").load(RUTA_SILVER)
if final.filter("is_current").groupBy("seller_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: vendedores con más de una versión vigente.")
print(f"Versiones en silver: {final.count()} | vigentes: {final.filter('is_current').count()}")
print("ETL SILVER SELLERS SCD2 FINALIZADO CORRECTAMENTE.")
versiones.unpersist()
job.commit()
