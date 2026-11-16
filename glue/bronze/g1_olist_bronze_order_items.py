import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

BUCKET = "trabajo-integrador-g1"
LANDING = f"s3://{BUCKET}/erp/landing/order_items/"
BRONZE = f"s3://{BUCKET}/erp/bronze/order_items/"

# Lee SOLO los archivos nuevos (el bookmark recuerda los ya leídos)
dyf = glue.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [LANDING], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="landing_order_items",
)
df = dyf.toDF()

if len(df.columns) == 0 or df.rdd.isEmpty():
    print("No hay archivos nuevos. Nada que procesar.")
else:
    print("Columnas leidas:", df.columns)
    # order_items no trae fecha de compra: year/month salen de la ruta
    # del archivo (.../year=2017/month=01/order_items_2017_01.csv)
    bronze = (df
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.regexp_extract("_source_file", r"year=(\d{4})", 1))
        .withColumn("month", F.regexp_extract("_source_file", r"month=(\d{2})", 1)))

    (bronze.write.mode("append")
        .partitionBy("year", "month")
        .parquet(BRONZE))

    print("Filas escritas en bronze:", bronze.count())

job.commit()