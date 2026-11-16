import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL GOLD - FACT RESEÑAS (una fila por reseña: review_id + order_id)    2026-11-16
# Une cada reseña con la entrega de su pedido (calculada desde silver orders, con la misma regla que
# gold fact_entregas: llego_tarde = día de entrega > día estimado) para medir cuánto cuesta un atraso en
# satisfacción. No lee fact_entregas: corre en paralelo con ese job. En Power BI se relacionan por order_id.
# Si todavía no hay silver reviews (el Colab no envió reseñas), el job termina bien sin escribir nada.
# Tabla nueva: no reemplaza nada de lo que usa Power BI. Salida Delta con MERGE.
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

RUTA_RESENAS = "s3://trabajo-integrador-g1/erp/silver/reviews/"
RUTA_ORDERS = "s3://trabajo-integrador-g1/erp/silver/orders/"
RUTA_CUSTOMERS = "s3://trabajo-integrador-g1/erp/silver/customers/"
DESTINO = "s3://trabajo-integrador-g1/erp/gold/fact_resenas/"

if not DeltaTable.isDeltaTable(spark, RUTA_RESENAS):
    print("Todavía no hay silver reviews: nada que procesar.")
else:
    # 1. LECTURA
    resenas = spark.read.format("delta").load(RUTA_RESENAS)
    clientes = spark.read.format("delta").load(RUTA_CUSTOMERS).select("customer_id", "customer_unique_id",
                                                                       F.col("customer_state").alias("estado_cliente"))
    entregado = F.col("fecha_entrega").isNotNull()
    entregas = (spark.read.format("delta").load(RUTA_ORDERS)
                .select("order_id", "customer_id", F.col("order_purchase_timestamp").alias("fecha_compra"),
                        F.col("order_delivered_customer_date").alias("fecha_entrega"),
                        F.col("order_estimated_delivery_date").alias("fecha_estimada"))
                .join(clientes, "customer_id", "left")
                .withColumn("entregado", entregado)
                .withColumn("dias_atraso", F.when(entregado, F.datediff(F.to_date("fecha_entrega"), F.to_date("fecha_estimada"))))
                .withColumn("llego_tarde", F.when(entregado, F.col("dias_atraso") > 0))
                .withColumn("dias_entrega", F.round((F.col("fecha_entrega").cast("long") - F.col("fecha_compra").cast("long")) / 86400.0, 2))
                .drop("customer_id", "fecha_estimada"))

    # 2. HECHO
    fact = (resenas.join(entregas, "order_id", "left")
        .withColumn("tiene_titulo", F.col("review_comment_title").isNotNull())
        .withColumn("tiene_comentario", F.col("review_comment_message").isNotNull())
        .withColumn("es_negativa", F.col("review_score") <= 2)
        .withColumn("es_positiva", F.col("review_score") >= 4)
        .withColumn("dias_respuesta", F.round((F.col("review_answer_timestamp").cast("long")
                                               - F.col("review_creation_date").cast("long")) / 86400.0, 2))
        .withColumn("dias_desde_entrega", F.datediff(F.to_date("review_creation_date"), F.to_date("fecha_entrega")))
        .withColumn("fecha", F.to_date("review_creation_date"))
        .withColumn("fecha_key", F.date_format("review_creation_date", "yyyyMMdd").cast("int"))
        .withColumn("fecha_procesamiento_gold", F.current_timestamp())
        .withColumn("capa_datos", F.lit("gold"))
        .select("review_id", "order_id", "customer_unique_id", "review_score",
                "es_negativa", "es_positiva", "tiene_titulo", "tiene_comentario", "review_creation_date",
                "review_answer_timestamp", "dias_respuesta", "fecha", "fecha_key", "fecha_compra", "fecha_entrega",
                "entregado", "llego_tarde", "dias_atraso", "dias_entrega", "dias_desde_entrega", "estado_cliente",
                "fecha_procesamiento_gold", "capa_datos")
        .cache())

    # 3. VALIDACIONES
    total = fact.count()
    print(f"Reseñas: {total} | sin entrega asociada: {fact.filter(F.col('fecha_compra').isNull()).count()}")
    if fact.groupBy("review_id", "order_id").count().filter("count > 1").limit(1).count():
        raise ValueError("ERROR: reseña duplicada (review_id + order_id).")
    fact.groupBy("llego_tarde").agg(F.count("*").alias("resenas"), F.round(F.avg("review_score"), 2).alias("puntaje"),
                                    F.round(F.avg(F.col("es_negativa").cast("int")) * 100, 1).alias("pct_negativas")).show()

    # 4. ESCRIBIR
    if DeltaTable.isDeltaTable(spark, DESTINO):
        (DeltaTable.forPath(spark, DESTINO).alias("t")
            .merge(fact.alias("s"), "t.review_id = s.review_id AND t.order_id = s.order_id")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
        print("MERGE aplicado en gold fact_resenas.")
    else:
        fact.write.format("delta").mode("overwrite").save(DESTINO)
        print("Primera carga Delta de gold fact_resenas.")
    fact.unpersist()

print("ETL GOLD FACT RESEÑAS FINALIZADO.")
job.commit()
