import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL GOLD - FACT SALES (una fila por ítem vendido)                      versión 2026-11-16
# Cambios respecto de la versión del 2026-11-15 (mismas columnas, solo se agregan):
#   - Glue 5.1 + Delta: lee silver por ruta Delta (no depende del esquema del catálogo) y escribe Delta con
#     MERGE por (order_id, order_item_id) en vez de Parquet overwrite
#   - sk_cliente: versión SCD2 del cliente vigente a la fecha de compra (silver customers_scd2); ciudad y estado
#     del cliente salen de ESA versión (antes: la dirección actual)
#   - sk_vendedor + ciudad/estado del vendedor: versión SCD2 vigente a la fecha de compra (silver sellers)
#   - product_category_name_english (silver category_translation)
# Si la compra es anterior a la primera versión conocida, se usa la primera versión (sk_*_estimada = true).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. CONFIGURACION
S = "s3://trabajo-integrador-g1/erp/silver/"
DESTINO = "s3://trabajo-integrador-g1/erp/gold/fact_sales/"


def leer(entidad):
    df = spark.read.format("delta").load(S + entidad + "/")
    print(f"silver {entidad}: {df.count()} filas")
    return df


# 2. LECTURA DE SILVER
orders = leer("orders").select("order_id", "customer_id", "order_status", "order_purchase_timestamp",
                               "order_approved_at", "order_delivered_customer_date", "order_estimated_delivery_date")
items = leer("order_items").select("order_id", "order_item_id", "product_id", "seller_id", "price",
                                   "freight_value", "shipping_limit_date")
customers = leer("customers").select("customer_id", "customer_unique_id")
clientes_scd2 = leer("customers_scd2").select("customer_unique_id", "sk_cliente", "customer_city", "customer_state",
                                              "valid_from", "valid_to", "version")
vendedores_scd2 = leer("sellers").select("seller_id", "sk_vendedor", "seller_city", "seller_state",
                                         "valid_from", "valid_to", "version")
products = leer("products").select("product_id", "product_category_name")
traduccion = leer("category_translation").select("product_category_name", "product_category_name_english")

# 3. VALIDACIONES DE CLAVES
for df, claves, nombre in [(orders, ["order_id"], "orders"), (items, ["order_id", "order_item_id"], "order_items"),
                           (customers, ["customer_id"], "customers"), (products, ["product_id"], "products")]:
    if df.filter(" OR ".join(f"{c} IS NULL" for c in claves)).limit(1).count():
        raise ValueError(f"ERROR: {nombre} tiene claves nulas {claves}.")
    if df.groupBy(*claves).count().filter("count > 1").limit(1).count():
        raise ValueError(f"ERROR: {nombre} tiene claves duplicadas {claves}.")

# 4. HECHO BASE (misma lógica que la versión anterior: items + pedido + producto)
base = (items.alias("i")
        .join(orders.alias("o"), "order_id", "left")
        .join(customers.alias("c"), "customer_id", "left")
        .join(products.alias("p"), "product_id", "left")
        .join(traduccion.alias("t"), "product_category_name", "left"))


def version_vigente(df, scd2, clave, sk, atributos, alias):
    """Une cada ítem con la versión SCD2 vigente a la fecha de compra (o la primera si la compra es anterior)."""
    cand = df.join(scd2.alias(alias), clave, "left")
    elegible = ((F.col(f"{alias}.valid_from") <= F.col("order_purchase_timestamp"))
                & (F.col(f"{alias}.valid_to").isNull() | (F.col("order_purchase_timestamp") < F.col(f"{alias}.valid_to"))))
    w = Window.partitionBy("order_id", "order_item_id").orderBy(elegible.desc(), F.col(f"{alias}.version").asc_nulls_last())
    return (cand.withColumn(f"_{sk}_ok", elegible)
                .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
                .withColumn(f"{sk}_estimada", F.col(sk).isNotNull() & ~F.coalesce(F.col(f"_{sk}_ok"), F.lit(False)))
                .drop(f"_{sk}_ok", F.col(f"{alias}.valid_from"), F.col(f"{alias}.valid_to"), F.col(f"{alias}.version")))


con_cliente = version_vigente(base, clientes_scd2, "customer_unique_id", "sk_cliente",
                              ["customer_city", "customer_state"], "cs")
con_vendedor = version_vigente(con_cliente, vendedores_scd2, "seller_id", "sk_vendedor",
                               ["seller_city", "seller_state"], "vs")

# 5. COLUMNAS FINALES (las de la versión anterior + las nuevas al final)
fact = (con_vendedor.select(
        "order_id", "order_item_id", "customer_id", "customer_unique_id", "product_id", "product_category_name",
        "seller_id", "order_status",
        F.col("order_purchase_timestamp").alias("fecha_compra"),
        F.col("order_approved_at").alias("fecha_aprobacion"),
        F.col("order_delivered_customer_date").alias("fecha_entrega"),
        F.col("order_estimated_delivery_date").alias("fecha_entrega_estimada"),
        F.col("shipping_limit_date").alias("fecha_limite_envio"),
        F.col("customer_city").alias("ciudad_cliente"),
        F.col("customer_state").alias("estado_cliente"),
        F.col("price").cast(DecimalType(18, 2)).alias("precio_producto"),
        F.col("freight_value").cast(DecimalType(18, 2)).alias("costo_flete"),
        "sk_cliente", "sk_cliente_estimada", "sk_vendedor", "sk_vendedor_estimada",
        F.col("seller_city").alias("ciudad_vendedor"), F.col("seller_state").alias("estado_vendedor"),
        "product_category_name_english")
    .withColumn("cantidad", F.lit(1))
    .withColumn("importe_total", (F.coalesce(F.col("precio_producto"), F.lit(0))
                                  + F.coalesce(F.col("costo_flete"), F.lit(0))).cast(DecimalType(18, 2)))
    .withColumn("fecha", F.to_date("fecha_compra"))
    .withColumn("anio", F.year("fecha_compra"))
    .withColumn("mes", F.month("fecha_compra"))
    .withColumn("trimestre", F.quarter("fecha_compra"))
    .withColumn("fecha_procesamiento_gold", F.current_timestamp())
    .withColumn("capa_datos", F.lit("gold"))
    .cache())

# 6. VALIDACIONES FINALES
total_items, total_gold = items.count(), fact.count()
print(f"Order items: {total_items} | fact sales: {total_gold}")
if total_gold != total_items:
    raise ValueError("ERROR: la cantidad de filas gold no coincide con order_items.")
if fact.groupBy("order_id", "order_item_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: fact_sales con claves duplicadas.")
fact.agg(F.count("sk_cliente").alias("con_sk_cliente"), F.count("sk_vendedor").alias("con_sk_vendedor"),
         F.sum(F.col("sk_cliente_estimada").cast("int")).alias("cliente_estimado"),
         F.count("product_category_name_english").alias("con_categoria_en"),
         F.sum("importe_total").alias("importe_total")).show(truncate=False)

# 7. ESCRIBIR EN DELTA (MERGE: inserta, actualiza y quita ítems que ya no están en silver)
if DeltaTable.isDeltaTable(spark, DESTINO):
    (DeltaTable.forPath(spark, DESTINO).alias("t")
        .merge(fact.alias("s"), "t.order_id = s.order_id AND t.order_item_id = s.order_item_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())
    print("MERGE aplicado en gold fact_sales.")
else:
    fact.write.format("delta").mode("overwrite").save(DESTINO)
    print("Primera carga Delta de gold fact_sales.")

print(f"Filas en gold fact_sales: {spark.read.format('delta').load(DESTINO).count()}")
print("ETL GOLD FACT SALES FINALIZADO CORRECTAMENTE.")
fact.unpersist()
job.commit()
