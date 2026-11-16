import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL SILVER - CLIENTES (SCD TIPO 1, MERGE POR customer_id)   APLICADO 2026-11-16
# Cambios respecto del job vigente:
#   - row_hash: el MERGE solo actualiza si cambió algún dato (antes reescribía todas las filas en cada
#     corrida y el historial Delta crecía sin motivo)
#   - desempate por archivo más reciente cuando dos filas tienen la misma fecha_ingesta (mismo lote)
#   - mismas validaciones que el original
# Este silver sigue siendo SCD1 por customer_id (una fila por pedido, como Olist). La historia de
# direcciones de cada persona está en g1_olist_silver_customers_scd2 (por customer_unique_id).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/customers/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/customers/"
COLUMNAS_CLIENTE = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
COLUMNAS_HASH = [c for c in COLUMNAS_CLIENTE if c != "customer_id"]

# 2. LEER DATOS BRONZE
df_bronze = spark.read.parquet(RUTA_BRONZE)
total_bronze = df_bronze.count()
print(f"Registros Bronze: {total_bronze}")
if total_bronze == 0:
    raise ValueError("ERROR: Bronze no contiene registros.")

# 3. VALIDAR ESTRUCTURA
columnas_faltantes = [c for c in COLUMNAS_CLIENTE + ["fecha_ingesta", "archivo_origen"] if c not in df_bronze.columns]
if columnas_faltantes:
    raise ValueError(f"ERROR: Faltan columnas: {columnas_faltantes}")

# 4. LIMPIEZA Y NORMALIZACION
df_silver = df_bronze
for columna in COLUMNAS_CLIENTE:
    df_silver = df_silver.withColumn(columna, F.trim(F.col(columna).cast(StringType())))
df_silver = (df_silver
    .withColumn("customer_city", F.lower(F.col("customer_city")))
    .withColumn("customer_state", F.upper(F.col("customer_state")))
    .withColumn("customer_zip_code_prefix", F.lpad(F.col("customer_zip_code_prefix"), 5, "0")))

# 5. VALIDAR CAMPOS OBLIGATORIOS Y CODIGO POSTAL
condicion_invalida = None
for columna in COLUMNAS_CLIENTE:
    condicion = F.col(columna).isNull() | (F.col(columna) == "")
    condicion_invalida = condicion if condicion_invalida is None else condicion_invalida | condicion
registros_invalidos = df_silver.filter(condicion_invalida).count()
print(f"Registros invalidos: {registros_invalidos}")
if registros_invalidos > 0:
    raise ValueError("ERROR: Existen clientes con campos obligatorios vacios.")
codigos_invalidos = df_silver.filter(~F.col("customer_zip_code_prefix").rlike(r"^\d{5}$")).count()
print(f"Codigos postales invalidos: {codigos_invalidos}")
if codigos_invalidos > 0:
    raise ValueError("ERROR: Existen codigos postales con formato invalido.")

# 6. DEDUPLICAR POR customer_id (la carga más reciente; si empatan, el archivo más reciente)
ventana = Window.partitionBy("customer_id").orderBy(F.col("fecha_ingesta").desc(), F.col("archivo_origen").desc())
df_silver = df_silver.withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")

# 7. HASH DE NEGOCIO, TRAZABILIDAD Y COLUMNAS FINALES
partes = [F.coalesce(F.col(c), F.lit("<NULL>")) for c in COLUMNAS_HASH]
df_silver = (df_silver
    .withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))
    .withColumn("fecha_procesamiento_silver", F.current_timestamp())
    .withColumn("capa_datos", F.lit("silver"))
    .select(*COLUMNAS_CLIENTE, "row_hash", "fecha_ingesta", "origen_datos", "fecha_procesamiento_silver", "capa_datos"))
total_silver = df_silver.count()
print(f"Registros a cargar en Silver: {total_silver}")

# 8. ESCRIBIR EN DELTA (SCD1: MERGE que actualiza solo si cambió el hash)
if DeltaTable.isDeltaTable(spark, RUTA_SILVER):
    tabla = DeltaTable.forPath(spark, RUTA_SILVER)
    if "row_hash" not in tabla.toDF().columns:
        # Primera corrida con la versión nueva: se agrega la columna antes del MERGE (la condición la usa;
        # autoMerge no basta porque la condición se resuelve contra el esquema actual). Filas viejas: row_hash nulo
        # -> se actualizan una vez y quedan con su hash.
        spark.sql(f"ALTER TABLE delta.`{RUTA_SILVER}` ADD COLUMNS (row_hash STRING)")
        tabla = DeltaTable.forPath(spark, RUTA_SILVER)
        print("Columna row_hash agregada a silver customers.")
    (tabla.alias("t")
        .merge(df_silver.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll(condition="t.row_hash IS NULL OR t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGE aplicado (solo filas nuevas o con cambios).")
else:
    df_silver.write.format("delta").mode("overwrite").save(RUTA_SILVER)
    print("Primera carga: tabla Delta creada.")

# 9. VALIDAR EL RESULTADO
final = spark.read.format("delta").load(RUTA_SILVER)
if final.groupBy("customer_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: customer_id duplicado en silver.")
print(f"Clientes en silver: {final.count()}")
print("ETL SILVER CLIENTES FINALIZADO CORRECTAMENTE.")

job.commit()
