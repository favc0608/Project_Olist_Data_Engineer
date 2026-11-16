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
# ETL GOLD - FACT ENTREGAS (una fila por pedido)                         2026-11-16
# Pregunta de negocio recomendada: ¿dónde y por qué llegan tarde los pedidos y cuánto cuesta en reseñas?
# Tiempos del ciclo del pedido (en días), atraso contra la fecha estimada, ruta vendedor -> cliente, flete.
#   llego_tarde = fecha de entrega (día) > fecha estimada (día)      (solo pedidos entregados)
#   sk_cliente  = versión SCD2 del cliente vigente a la compra        (gold_dim_cliente_scd2)
#   sk_vendedor = versión SCD2 del vendedor principal (ítem 1)        (gold_dim_vendedor_scd2)
# Tabla nueva: no reemplaza nada de lo que usa Power BI. Salida Delta con MERGE por order_id.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

S = "s3://trabajo-integrador-g1/erp/silver/"
DESTINO = "s3://trabajo-integrador-g1/erp/gold/fact_entregas/"
REGION = {"AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
          "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
          "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
          "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
          "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
          "PR": "Sul", "RS": "Sul", "SC": "Sul"}
mapa_region = F.create_map(*[x for k, v in REGION.items() for x in (F.lit(k), F.lit(v))])


def silver(entidad):
    return spark.read.format("delta").load(S + entidad + "/")


def dias(hasta, desde):
    """Días (con decimales) entre dos timestamps; nulo si falta alguno."""
    return F.round((F.col(hasta).cast("long") - F.col(desde).cast("long")) / 86400.0, 2)


# 1. LECTURA
orders = silver("orders").select("order_id", "customer_id", "order_status", "order_purchase_timestamp",
                                 "order_approved_at", "order_delivered_carrier_date",
                                 "order_delivered_customer_date", "order_estimated_delivery_date")
items = silver("order_items").select("order_id", "order_item_id", "seller_id", "price", "freight_value")
customers = silver("customers").select("customer_id", "customer_unique_id")
cli_scd2 = silver("customers_scd2").select("customer_unique_id", "sk_cliente", F.col("customer_state").alias("estado_cliente"),
                                           F.col("customer_city").alias("ciudad_cliente"), "valid_from", "valid_to", "version")
ven_scd2 = silver("sellers").select("seller_id", "sk_vendedor", F.col("seller_state").alias("estado_vendedor"),
                                    F.col("seller_city").alias("ciudad_vendedor"), "valid_from", "valid_to", "version")
if orders.groupBy("order_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: silver orders con order_id duplicado.")

# 2. AGREGADOS POR PEDIDO (ítems, vendedores, valor y flete) + vendedor principal (ítem 1)
agg = (items.groupBy("order_id")
       .agg(F.count("*").alias("cantidad_items"),
            F.countDistinct("seller_id").alias("cantidad_vendedores"),
            F.sum("price").cast(DecimalType(18, 2)).alias("valor_productos"),
            F.sum("freight_value").cast(DecimalType(18, 2)).alias("valor_flete")))
principal = (items.withColumn("_rn", F.row_number().over(Window.partitionBy("order_id").orderBy("order_item_id")))
             .filter("_rn = 1").select("order_id", F.col("seller_id").alias("seller_id_principal")))

base = (orders.join(customers, "customer_id", "left").join(agg, "order_id", "left").join(principal, "order_id", "left"))


def version_vigente(df, scd2, clave_df, clave_scd2, sk):
    """Versión SCD2 vigente a la fecha de compra; si la compra es anterior a la primera versión, la primera."""
    c = df.join(scd2.withColumnRenamed(clave_scd2, clave_df).alias("v"), clave_df, "left")
    ok = ((F.col("v.valid_from") <= F.col("order_purchase_timestamp"))
          & (F.col("v.valid_to").isNull() | (F.col("order_purchase_timestamp") < F.col("v.valid_to"))))
    w = Window.partitionBy("order_id").orderBy(ok.desc(), F.col("v.version").asc_nulls_last())
    return (c.withColumn("_ok", ok).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
             .withColumn(f"{sk}_estimada", F.col(sk).isNotNull() & ~F.coalesce(F.col("_ok"), F.lit(False)))
             .drop("_rn", "_ok", F.col("v.valid_from"), F.col("v.valid_to"), F.col("v.version")))


con_cli = version_vigente(base, cli_scd2, "customer_unique_id", "customer_unique_id", "sk_cliente")
con_ven = version_vigente(con_cli, ven_scd2, "seller_id_principal", "seller_id", "sk_vendedor")

# 3. MÉTRICAS DE ENTREGA
entregado = F.col("order_delivered_customer_date").isNotNull()
fact = (con_ven
    .withColumn("fecha_compra", F.col("order_purchase_timestamp"))
    .withColumn("fecha", F.to_date("order_purchase_timestamp"))
    .withColumn("fecha_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
    .withColumn("dias_aprobacion", dias("order_approved_at", "order_purchase_timestamp"))
    .withColumn("dias_hasta_transportista", dias("order_delivered_carrier_date", "order_purchase_timestamp"))
    .withColumn("dias_transporte", dias("order_delivered_customer_date", "order_delivered_carrier_date"))
    .withColumn("dias_entrega", dias("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("dias_estimados", dias("order_estimated_delivery_date", "order_purchase_timestamp"))
    .withColumn("entregado", entregado)
    .withColumn("dias_atraso", F.when(entregado, F.datediff(F.to_date("order_delivered_customer_date"),
                                                             F.to_date("order_estimated_delivery_date"))))
    .withColumn("llego_tarde", F.when(entregado, F.col("dias_atraso") > 0))
    .withColumn("mismo_estado", F.col("estado_cliente") == F.col("estado_vendedor"))
    .withColumn("region_cliente", mapa_region[F.col("estado_cliente")])
    .withColumn("region_vendedor", mapa_region[F.col("estado_vendedor")])
    .withColumn("ruta", F.concat_ws(" -> ", F.col("estado_vendedor"), F.col("estado_cliente")))
    .withColumn("pct_flete", F.when(F.col("valor_productos") > 0,
                                    F.round(F.col("valor_flete") / F.col("valor_productos") * 100, 2)))
    .withColumn("fecha_procesamiento_gold", F.current_timestamp())
    .withColumn("capa_datos", F.lit("gold"))
    .select("order_id", "customer_id", "customer_unique_id", "sk_cliente", "sk_cliente_estimada",
            F.col("seller_id_principal").alias("seller_id"), "sk_vendedor", "sk_vendedor_estimada",
            "order_status", "fecha_compra", "fecha", "fecha_key",
            F.col("order_approved_at").alias("fecha_aprobacion"),
            F.col("order_delivered_carrier_date").alias("fecha_entrega_transportista"),
            F.col("order_delivered_customer_date").alias("fecha_entrega"),
            F.col("order_estimated_delivery_date").alias("fecha_entrega_estimada"),
            "dias_aprobacion", "dias_hasta_transportista", "dias_transporte", "dias_entrega", "dias_estimados",
            "entregado", "dias_atraso", "llego_tarde",
            "ciudad_cliente", "estado_cliente", "region_cliente", "ciudad_vendedor", "estado_vendedor",
            "region_vendedor", "mismo_estado", "ruta",
            "cantidad_items", "cantidad_vendedores", "valor_productos", "valor_flete", "pct_flete",
            "fecha_procesamiento_gold", "capa_datos")
    .cache())

# 4. VALIDACIONES
total, pedidos = fact.count(), orders.count()
print(f"Pedidos silver: {pedidos} | filas fact_entregas: {total}")
if total != pedidos:
    raise ValueError("ERROR: fact_entregas no tiene una fila por pedido.")
if fact.groupBy("order_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: order_id duplicado en fact_entregas.")
fact.agg(F.sum(F.col("entregado").cast("int")).alias("entregados"),
         F.sum(F.col("llego_tarde").cast("int")).alias("tarde"),
         F.round(F.avg("dias_entrega"), 2).alias("dias_entrega_prom"),
         F.count("sk_cliente").alias("con_sk_cliente"), F.count("sk_vendedor").alias("con_sk_vendedor")).show(truncate=False)

# 5. ESCRIBIR (MERGE: inserta, actualiza y quita pedidos que ya no están en silver)
if DeltaTable.isDeltaTable(spark, DESTINO):
    (DeltaTable.forPath(spark, DESTINO).alias("t")
        .merge(fact.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
    print("MERGE aplicado en gold fact_entregas.")
else:
    fact.write.format("delta").mode("overwrite").save(DESTINO)
    print("Primera carga Delta de gold fact_entregas.")
fact.unpersist()
print("ETL GOLD FACT ENTREGAS FINALIZADO CORRECTAMENTE.")
job.commit()
