import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# PROYECTO INTEGRADOR OLIST - GRUPO 1
# ETL GOLD - DIMENSIONES                                                 2026-11-16
#
# A) Dimensiones que ya usa Power BI (creadas con CTAS de Athena): se REFRESCAN con la MISMA consulta,
#    las MISMAS columnas, los MISMOS tipos, el MISMO formato (Parquet) y la MISMA ruta. Su tabla del catálogo
#    no se toca. Así el modelo de Power BI sigue igual y solo ve datos al día.
#      gold_dim_customers  <- silver customers (MAX por customer_id)       erp/gold/dim_customers/
#      gold_dim_products   <- silver products  (MAX por product_id)        erp/gold/dim_products/
#    gold_dim_date (calendario 2026 fijo) NO se toca.
#
# B) Dimensiones NUEVAS (Delta, tablas aparte; Power BI las suma cuando quiera):
#      gold_dim_fecha          calendario 2016-01-01..2026-12-31 + fecha_presentacion (desplazamiento 418 semanas)
#      gold_dim_cliente_scd2   una fila por versión del cliente (sk_cliente, vigencia, región)
#      gold_dim_vendedor_scd2  una fila por versión del vendedor (sk_vendedor, vigencia, región)
# Geografía: se usa gold_geolocation (ya agregada por zip).
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

S = "s3://trabajo-integrador-g1/erp/silver/"
G = "s3://trabajo-integrador-g1/erp/gold/"
DESPLAZAMIENTO_DIAS = 418 * 7
FIN_HISTORICO = "2018-10-31"  # fechas de Olist; lo posterior ya es la línea de tiempo simulada (2026)
REGION = {"AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
          "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
          "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
          "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
          "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
          "PR": "Sul", "RS": "Sul", "SC": "Sul"}
mapa_region = F.create_map(*[x for k, v in REGION.items() for x in (F.lit(k), F.lit(v))])


def silver(entidad):
    return spark.read.format("delta").load(S + entidad + "/")


def validar_unica(df, claves, nombre):
    total = df.count()
    if df.groupBy(*claves).count().filter("count > 1").limit(1).count():
        raise ValueError(f"ERROR: {nombre} con claves duplicadas {claves}.")
    print(f"{nombre}: {total} filas")
    return total


# ---------- A) DIMENSIONES DE POWER BI (mismo esquema que el CTAS) ----------
# 1. gold_dim_customers: SELECT customer_id, MAX(...) ... FROM silver_customers GROUP BY customer_id
dim_customers = (silver("customers").filter(F.col("customer_id").isNotNull())
                 .groupBy("customer_id")
                 .agg(F.max("customer_unique_id").alias("customer_unique_id"),
                      F.max("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
                      F.max("customer_city").alias("customer_city"),
                      F.max("customer_state").alias("customer_state"))
                 .select(F.col("customer_id").cast("string"), F.col("customer_unique_id").cast("string"),
                         F.col("customer_zip_code_prefix").cast("string"), F.col("customer_city").cast("string"),
                         F.col("customer_state").cast("string")))
validar_unica(dim_customers, ["customer_id"], "gold_dim_customers")

# 2. gold_dim_products: SELECT product_id, MAX(...) ... FROM silver_products GROUP BY product_id
dim_products = (silver("products").filter(F.col("product_id").isNotNull())
                .groupBy("product_id")
                .agg(F.max("product_category_name").alias("product_category_name"),
                     F.max("product_name_lenght").alias("product_name_lenght"),
                     F.max("product_description_lenght").alias("product_description_lenght"),
                     F.max("product_photos_qty").alias("product_photos_qty"),
                     F.max("product_weight_g").alias("product_weight_g"),
                     F.max("product_length_cm").alias("product_length_cm"),
                     F.max("product_height_cm").alias("product_height_cm"),
                     F.max("product_width_cm").alias("product_width_cm"))
                .select(F.col("product_id").cast("string"), F.col("product_category_name").cast("string"),
                        F.col("product_name_lenght").cast("int"), F.col("product_description_lenght").cast("int"),
                        F.col("product_photos_qty").cast("int"), F.col("product_weight_g").cast("double"),
                        F.col("product_length_cm").cast("double"), F.col("product_height_cm").cast("double"),
                        F.col("product_width_cm").cast("double")))
validar_unica(dim_products, ["product_id"], "gold_dim_products")

# Parquet overwrite en la misma ruta del CTAS (la tabla del catálogo lee por nombre de columna)
dim_customers.coalesce(1).write.mode("overwrite").parquet(G + "dim_customers/")
dim_products.coalesce(1).write.mode("overwrite").parquet(G + "dim_products/")
print("Dimensiones de Power BI refrescadas (mismo esquema).")

# ---------- B) DIMENSIONES NUEVAS (Delta) ----------
# 3. gold_dim_fecha
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre",
         "Noviembre", "Diciembre"]
nombre_mes = F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("fecha"))
dia_semana = ((F.dayofweek("fecha") + 5) % 7) + 1  # ISO como Athena day_of_week: 1 = lunes ... 7 = domingo
fechas = (spark.sql("SELECT explode(sequence(to_date('2016-01-01'), to_date('2026-12-31'), interval 1 day)) AS fecha")
          .withColumn("anio", F.year("fecha").cast("bigint"))
          .withColumn("mes", F.month("fecha").cast("bigint"))
          .withColumn("trimestre", F.quarter("fecha").cast("bigint"))
          .withColumn("dia", F.dayofmonth("fecha").cast("bigint"))
          .withColumn("dia_semana", dia_semana.cast("bigint"))
          .withColumn("nombre_mes", nombre_mes)
          .withColumn("fecha_key", F.date_format("fecha", "yyyyMMdd").cast("int"))
          .withColumn("es_fin_de_semana", dia_semana >= 6)
          # Black Friday = viernes siguiente al 4.º jueves de noviembre (día 23 a 29)
          .withColumn("es_black_friday", (F.month("fecha") == 11) & (dia_semana == 5) & F.dayofmonth("fecha").between(23, 29))
          .withColumn("periodo", F.when(F.col("fecha") <= F.lit(FIN_HISTORICO).cast("date"), "historico_olist")
                      .otherwise("simulado"))
          .withColumn("fecha_presentacion", F.when(F.col("periodo") == "historico_olist",
                                                   F.date_add("fecha", DESPLAZAMIENTO_DIAS)).otherwise(F.col("fecha"))))
validar_unica(fechas, ["fecha"], "gold_dim_fecha")
fechas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(G + "dim_fecha/")

# 4. gold_dim_cliente_scd2 (de silver customers_scd2)
cli = (silver("customers_scd2")
       .select("sk_cliente", "customer_unique_id", "customer_id_origen", "customer_zip_code_prefix", "customer_city",
               "customer_state", "valid_from", "valid_to", "is_current", "version", "vigencia_estimada")
       .withColumn("region", F.coalesce(mapa_region[F.col("customer_state")], F.lit("desconocida")))
       .withColumn("fecha_procesamiento_gold", F.current_timestamp()))
validar_unica(cli, ["sk_cliente"], "gold_dim_cliente_scd2")
if cli.filter("is_current").groupBy("customer_unique_id").count().filter("count > 1").limit(1).count():
    raise ValueError("ERROR: dim_cliente_scd2 con más de una versión vigente por persona.")
cli.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(G + "dim_cliente_scd2/")

# 5. gold_dim_vendedor_scd2 (de silver sellers)
ven = (silver("sellers")
       .select("sk_vendedor", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
               "valid_from", "valid_to", "is_current", "version")
       .withColumn("region", F.coalesce(mapa_region[F.col("seller_state")], F.lit("desconocida")))
       .withColumn("fecha_procesamiento_gold", F.current_timestamp()))
validar_unica(ven, ["sk_vendedor"], "gold_dim_vendedor_scd2")
ven.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(G + "dim_vendedor_scd2/")

print("ETL GOLD DIMENSIONES FINALIZADO CORRECTAMENTE.")
job.commit()
