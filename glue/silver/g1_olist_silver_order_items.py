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
# ETL SILVER - ITEMS DE PEDIDO (order_items)
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
RUTA_BRONZE = "s3://trabajo-integrador-g1/erp/bronze/order_items/"
RUTA_SILVER = "s3://trabajo-integrador-g1/erp/silver/order_items/"
RUTA_SILVER_ORDERS = "s3://trabajo-integrador-g1/erp/silver/orders/"

COLUMNAS_ITEM = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

# columna -> tipo destino
CONVERSIONES = {
    "order_item_id": "int",
    "shipping_limit_date": "timestamp",
    "price": "decimal(10,2)",
    "freight_value": "decimal(10,2)",
}

CAMPOS_OBLIGATORIOS = COLUMNAS_ITEM + ["year", "month"]

# 2. LEER DATOS BRONZE
print("Leyendo order_items desde Bronze...")
df_bronze = spark.read.parquet(RUTA_BRONZE)

total_bronze = df_bronze.count()
print(f"Registros Bronze: {total_bronze}")

if total_bronze == 0:
    raise ValueError("ERROR: Bronze no contiene registros.")

# 3. VALIDAR ESTRUCTURA
columnas_faltantes = [
    c for c in COLUMNAS_ITEM + ["_ingested_at", "year", "month"]
    if c not in df_bronze.columns
]
if columnas_faltantes:
    raise ValueError(f"ERROR: Faltan columnas: {columnas_faltantes}")

# En order_items el year/month NO se puede recalcular desde una fecha propia
# (shipping_limit_date no es la fecha de compra), asi que se conservan los
# de Bronze, que vienen de la carpeta del archivo (mes del pedido).
df_silver = (
    df_bronze
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.col("month").cast("int"))
)

# 4. LIMPIEZA Y NORMALIZACION
print("Aplicando limpieza de datos...")

for c in COLUMNAS_ITEM:
    df_silver = df_silver.withColumn(c, F.trim(F.col(c).cast(StringType())))
    df_silver = df_silver.withColumn(
        c, F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

# 5. VALIDAR CAMPOS OBLIGATORIOS
condicion_invalida = None
for c in CAMPOS_OBLIGATORIOS:
    cond = F.col(c).isNull()
    condicion_invalida = cond if condicion_invalida is None else condicion_invalida | cond

registros_invalidos = df_silver.filter(condicion_invalida).count()
print(f"Registros con campos obligatorios vacios: {registros_invalidos}")
if registros_invalidos > 0:
    raise ValueError("ERROR: Existen items con campos obligatorios vacios.")

# 6. CONVERTIR TIPOS Y VALIDAR
conversiones_invalidas = 0
for c, tipo in CONVERSIONES.items():
    convertida = F.col(c).cast(tipo)
    conversiones_invalidas += df_silver.filter(
        F.col(c).isNotNull() & convertida.isNull()
    ).count()
    df_silver = df_silver.withColumn(c, convertida)

print(f"Valores con tipo invalido: {conversiones_invalidas}")
if conversiones_invalidas > 0:
    raise ValueError("ERROR: Existen valores que no se pueden convertir al tipo esperado.")

# 7. VALIDAR REGLAS DE NEGOCIO
valores_invalidos = df_silver.filter(
    (F.col("price") < 0) | (F.col("freight_value") < 0) | (F.col("order_item_id") < 1)
).count()
print(f"Items con precio/flete negativo o item_id < 1: {valores_invalidos}")
if valores_invalidos > 0:
    raise ValueError("ERROR: Existen items con valores fuera de rango.")

# 8. DEDUPLICAR POR ORDER_ID + ORDER_ITEM_ID (carga mas reciente)
w = Window.partitionBy("order_id", "order_item_id").orderBy(F.col("_ingested_at").desc())
df_silver = (
    df_silver
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
total_unicos = df_silver.count()
print(f"Items unicos: {total_unicos} (duplicados descartados: {total_bronze - total_unicos})")

# Aviso (no bloquea): items cuyo pedido aun no esta en Silver orders
if DeltaTable.isDeltaTable(spark, RUTA_SILVER_ORDERS):
    pedidos = spark.read.format("delta").load(RUTA_SILVER_ORDERS).select("order_id")
    huerfanos = df_silver.join(pedidos, "order_id", "left_anti").count()
    print(f"AVISO - items sin pedido en Silver orders: {huerfanos}")
else:
    print("AVISO - Silver orders aun no existe; se omite la validacion de pedidos.")

# 9. HASH DE LA FILA (detecta si un item realmente cambio)
# Se calcula sobre las columnas de negocio menos la clave (order_id + order_item_id).
# Los nulos se reemplazan por un marcador para que no se confundan con vacios.
def agregar_hash(df, columnas):
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columnas]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *partes), 256))

COLUMNAS_HASH = ["product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
df_silver = agregar_hash(df_silver, COLUMNAS_HASH)

# 10. TRAZABILIDAD SILVER
df_silver = (
    df_silver
    .withColumn("fecha_procesamiento_silver", F.current_timestamp())
    .withColumn("capa_datos", F.lit("silver"))
)

# 11. ESCRIBIR EN SILVER (SCD tipo 1: MERGE que sobrescribe solo si cambio el hash)
print("Escribiendo order_items en Silver...")

if DeltaTable.isDeltaTable(spark, RUTA_SILVER):
    (DeltaTable.forPath(spark, RUTA_SILVER).alias("t")
        .merge(
            df_silver.alias("s"),
            "t.order_id = s.order_id AND t.order_item_id = s.order_item_id",
        )
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
print("ETL SILVER ORDER_ITEMS FINALIZADO CORRECTAMENTE.")

job.commit()