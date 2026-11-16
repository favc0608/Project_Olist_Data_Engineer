"""
Crea o actualiza los 28 jobs de AWS Glue del pipeline a partir de los scripts de glue/.

Los jobs del proyecto se crearon originalmente desde la consola; este script deja su definición como código
(mismo nombre, Glue 5.1, 2 workers G.1X, 0 reintentos: los reintentos los maneja el orquestador).

Uso:
    python infra/scripts/crear_jobs_glue.py --rol arn:aws:iam::<ACCOUNT_ID>:role/<rol-glue> \
        --bucket-scripts aws-glue-assets-<ACCOUNT_ID>-<region> [--region us-east-2] [--solo-mostrar]

Requisitos: boto3 y credenciales con glue:CreateJob/UpdateJob/GetJob y s3:PutObject en el bucket de scripts.
"""
import argparse
from pathlib import Path

import boto3

RAIZ = Path(__file__).resolve().parents[2]
COMPARTIDO = "glue/compartido/olist_pipeline_payments_geolocation.py"
GOLD_COMPARTIDO = "glue/compartido/gold_payments_geolocation.py"

# nombre del job -> (script en el repo, usa bookmark, usa Delta, timeout en minutos)
JOBS = {
    # BRONZE: CSV/JSONL -> Parquet append (bookmark = solo archivos nuevos)
    "g1_olist_bronze_orders": ("glue/bronze/g1_olist_bronze_orders.py", True, False, 10),
    "g1_olist_bronze_order_items": ("glue/bronze/g1_olist_bronze_order_items.py", True, False, 10),
    "g1_olist_bronze_customers": ("glue/bronze/g1_olist_bronze_customers.py", True, False, 10),
    "g1_olist_bronze_productos": ("glue/bronze/g1_olist_bronze_productos.py", True, False, 10),
    "g1_olist_bronze_sellers": ("glue/bronze/g1_olist_bronze_sellers.py", True, False, 10),
    "g1_olist_bronze_reviews": ("glue/bronze/g1_olist_bronze_reviews.py", True, False, 30),
    "g1_olist_bronze_category_translation": ("glue/bronze/g1_olist_bronze_category_translation.py", True, False, 30),
    "g1_olist_bronze_payments": (COMPARTIDO, True, False, 10),
    "g1_olist_bronze_geolocation": (COMPARTIDO, True, False, 10),
    # SILVER: Delta MERGE (SCD1 / SCD2)
    "g1_olist_silver_orders": ("glue/silver/g1_olist_silver_orders.py", False, True, 30),
    "g1_olist_silver_order_items": ("glue/silver/g1_olist_silver_order_items.py", False, True, 30),
    "g1_olist_silver_customers": ("glue/silver/g1_olist_silver_customers.py", False, True, 30),
    "g1_olist_silver_customers_scd2": ("glue/silver/g1_olist_silver_customers_scd2.py", False, True, 30),
    "g1_olist_silver_productos": ("glue/silver/g1_olist_silver_productos.py", False, True, 30),
    "g1_olist_silver_sellers": ("glue/silver/g1_olist_silver_sellers.py", False, True, 30),
    "g1_olist_silver_reviews": ("glue/silver/g1_olist_silver_reviews.py", False, True, 30),
    "g1_olist_silver_category_translation": ("glue/silver/g1_olist_silver_category_translation.py", False, True, 30),
    "g1_olist_silver_payments": (COMPARTIDO, False, True, 20),
    "g1_olist_silver_geolocation": (COMPARTIDO, False, True, 20),
    # GOLD
    "g1_olist_gold_ventas": ("glue/gold/g1_olist_gold_ventas.py", False, True, 30),
    "g1_olist_gold_dimensiones": ("glue/gold/g1_olist_gold_dimensiones.py", False, True, 30),
    "g1_olist_gold_entregas": ("glue/gold/g1_olist_gold_entregas.py", False, True, 30),
    "g1_olist_gold_resenas": ("glue/gold/g1_olist_gold_resenas.py", False, True, 30),
    "g1_olist_gold_payments": (GOLD_COMPARTIDO, False, True, 20),
    "g1_olist_gold_geolocation": (GOLD_COMPARTIDO, False, True, 20),
    # WEB (precios de la competencia)
    "g1_olist_web_bronze_precios": ("glue/web/g1_olist_web_bronze_precios.py", False, False, 30),
    "g1_olist_web_silver_precios_competencia": ("glue/web/g1_olist_web_silver_precios_competencia.py", False, True, 30),
    "g1_olist_web_silver_productos_competencia": ("glue/web/g1_olist_web_silver_productos_competencia.py", False, True, 30),
}

DELTA_CONF = ("spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
              "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")


def definicion(nombre, script, bookmark, delta, timeout, rol, bucket):
    args = {
        "--job-language": "python",
        "--enable-glue-datacatalog": "true",
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--job-bookmark-option": "job-bookmark-enable" if bookmark else "job-bookmark-disable",
        "--TempDir": f"s3://{bucket}/temporary/",
    }
    if delta:
        args["--datalake-formats"] = "delta"
        args["--conf"] = DELTA_CONF
    return {
        "Role": rol,
        "Command": {"Name": "glueetl", "ScriptLocation": f"s3://{bucket}/scripts/{Path(script).name}",
                    "PythonVersion": "3"},
        "DefaultArguments": args,
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": timeout,
        "MaxRetries": 0,  # los reintentos los decide el orquestador
        "ExecutionClass": "STANDARD",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rol", required=True)
    ap.add_argument("--bucket-scripts", required=True)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--solo-mostrar", action="store_true")
    a = ap.parse_args()

    s3 = boto3.client("s3", region_name=a.region)
    glue = boto3.client("glue", region_name=a.region)
    subidos = set()
    for nombre, (script, bookmark, delta, timeout) in JOBS.items():
        d = definicion(nombre, script, bookmark, delta, timeout, a.rol, a.bucket_scripts)
        if a.solo_mostrar:
            print(nombre, d["Command"]["ScriptLocation"], d["DefaultArguments"]["--job-bookmark-option"])
            continue
        if script not in subidos:
            s3.upload_file(str(RAIZ / script), a.bucket_scripts, f"scripts/{Path(script).name}")
            subidos.add(script)
        try:
            glue.get_job(JobName=nombre)
            glue.update_job(JobName=nombre, JobUpdate=d)
            print("actualizado", nombre)
        except glue.exceptions.EntityNotFoundException:
            glue.create_job(Name=nombre, **d)
            print("creado", nombre)


if __name__ == "__main__":
    main()
