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
# ETL SILVER - PEDIDOS (orders)
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/orders/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/orders/"

COLUMNAS_PEDIDO = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

COLUMNAS_FECHA = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

CAMPOS_OBLIGATORIOS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_estimated_delivery_date",
]

ESTADOS_VALIDOS = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
]

# 2. LEER DATOS BRONZE
print("Leyendo pedidos desde Bronze...")
df_bronze = spark.read.parquet(RUTA_BRONZE)

total_bronze = df_bronze.count()
print(f"Registros Bronze: {total_bronze}")

if total_bronze == 0:
    raise ValueError("ERROR: Bronze no contiene registros.")

# 3. VALIDAR ESTRUCTURA
columnas_faltantes = [
    c for c in COLUMNAS_PEDIDO + ["_ingested_at"]
    if c not in df_bronze.columns
]
if columnas_faltantes:
    raise ValueError(f"ERROR: Faltan columnas: {columnas_faltantes}")

# year/month de Bronze vienen de la carpeta; se recalculan mas abajo
df_silver = df_bronze.drop("year", "month")

# 4. LIMPIEZA Y NORMALIZACION
print("Aplicando limpieza de datos...")

for c in COLUMNAS_PEDIDO:
    df_silver = df_silver.withColumn(c, F.trim(F.col(c).cast(StringType())))
    df_silver = df_silver.withColumn(
        c, F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

df_silver = df_silver.withColumn("order_status", F.lower(F.col("order_status")))

# 5. VALIDAR CAMPOS OBLIGATORIOS
condicion_invalida = None
for c in CAMPOS_OBLIGATORIOS:
    cond = F.col(c).isNull()
    condicion_invalida = cond if condicion_invalida is None else condicion_invalida | cond

registros_invalidos = df_silver.filter(condicion_invalida).count()
print(f"Registros con campos obligatorios vacios: {registros_invalidos}")
if registros_invalidos > 0:
    raise ValueError("ERROR: Existen pedidos con campos obligatorios vacios.")

# 6. VALIDAR ESTADOS
estados_invalidos = df_silver.filter(
    ~F.col("order_status").isin(ESTADOS_VALIDOS)
).count()
print(f"Estados invalidos: {estados_invalidos}")
if estados_invalidos > 0:
    raise ValueError("ERROR: Existen estados de pedido no reconocidos.")

# 7. CONVERTIR FECHAS A TIMESTAMP Y VALIDAR
fechas_invalidas = 0
for c in COLUMNAS_FECHA:
    parseada = F.to_timestamp(F.col(c))
    fechas_invalidas += df_silver.filter(
        F.col(c).isNotNull() & parseada.isNull()
    ).count()
    df_silver = df_silver.withColumn(c, parseada)

print(f"Fechas con formato invalido: {fechas_invalidas}")
if fechas_invalidas > 0:
    raise ValueError("ERROR: Existen fechas con formato invalido.")

# Aviso (no bloquea): entregas anteriores a la compra
incoherentes = df_silver.filter(
    F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp")
).count()
print(f"AVISO - entregas anteriores a la compra: {incoherentes}")

# 8. DEDUPLICAR POR ORDER_ID (nos quedamos con el estado mas avanzado y, si empatan, la carga mas reciente)
# Las ventas del simulador llegan como varias filas del mismo pedido (created, approved, shipped...).
# Si un mismo lote de bronze trae varias, todas tienen el mismo _ingested_at: el estado decide.
# El estado de un pedido de Olist solo avanza, asi que el mas avanzado es siempre el vigente.
RANGO_ESTADO = (F.when(F.col("order_status").isin("canceled", "unavailable"), 6)
                .when(F.col("order_status") == "delivered", 5)
                .when(F.col("order_status") == "shipped", 4)
                .when(F.col("order_status") == "processing", 3)
                .when(F.col("order_status") == "invoiced", 2)
                .when(F.col("order_status") == "approved", 1)
                .otherwise(0))
w = Window.partitionBy("order_id").orderBy(RANGO_ESTADO.desc(), F.col("_ingested_at").desc(),
                                           F.col("_source_file").desc())
df_silver = (
    df_silver
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
total_unicos = df_silver.count()
print(f"Pedidos unicos: {total_unicos} (duplicados descartados: {total_bronze - total_unicos})")

# 9. HASH DE LA FILA (detecta si un pedido realmente cambio)
# Se calcula sobre todas las columnas de negocio menos la clave (order_id).
# Los nulos se reemplazan por un marcador para que no se confundan con vacios.
def agregar_hash(df, columnas):
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columnas]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))

COLUMNAS_HASH = [c for c in COLUMNAS_PEDIDO if c != "order_id"]
df_silver = agregar_hash(df_silver, COLUMNAS_HASH)

# 10. TRAZABILIDAD SILVER
df_silver = (
    df_silver
    .withColumn("year", F.year("order_purchase_timestamp"))
    .withColumn("month", F.month("order_purchase_timestamp"))
    .withColumn("fecha_procesamiento_silver", F.current_timestamp())
    .withColumn("capa_datos", F.lit("silver"))
)

# 11. ESCRIBIR EN SILVER (SCD tipo 1: MERGE que sobrescribe solo si cambio el hash)
print("Escribiendo pedidos en Silver...")

if DeltaTable.isDeltaTable(spark, RUTA_SILVER):
    (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
        .merge(df_silver.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGE aplicado sobre Silver existente.")
else:
    (df_silver.write.format("delta")
        .mode("errorifexists")
        .partitionBy("year", "month")
        .save(RUTA_SILVER))
    print("Primera carga de Silver creada.")

total_silver = spark.read.format("delta").load(RUTA_SILVER).count()
print(f"Registros totales en Silver: {total_silver}")
print("ETL SILVER PEDIDOS FINALIZADO CORRECTAMENTE.")

job.commit()