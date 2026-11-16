import hashlib
import json
import math
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import reduce
from urllib.parse import unquote, urlparse


ACCOUNT_ID = "<ACCOUNT_ID>"
REGION = "us-east-2"
BUCKET = "trabajo-integrador-g1"
DATABASE = "g1_olist_lakehouse"
FIELDS = {
    "payments": [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ],
    "geolocation": [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
    ],
}
KEYS = {
    "payments": ["order_id", "payment_sequential"],
    "geolocation": ["geolocation_id"],
}
PAYMENT_TYPES = {"boleto", "credit_card", "debit_card", "voucher", "not_defined"}
STATES = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())
EXTENSIONS = {
    "csv": (".csv", ".csv.gz"),
    "json": (".json", ".json.gz"),
    "jsonl": (".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz"),
}


def text(value, field):
    if value is None or isinstance(value, (dict, list, bool)):
        raise ValueError(f"{field}: valor obligatorio o tipo incompatible")
    result = str(value).strip()
    if not result:
        raise ValueError(f"{field}: valor obligatorio")
    return result


def integer(value, field, minimum):
    raw = text(value, field)
    if not re.fullmatch(r"[0-9]+", raw):
        raise ValueError(f"{field}: se esperaba un entero")
    result = int(raw)
    if result < minimum or result > 2147483647:
        raise ValueError(f"{field}: fuera del rango permitido")
    return result


def fingerprint(values):
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_payments(raw):
    order_id = text(raw.get("order_id"), "order_id").lower()
    if not re.fullmatch(r"[a-f0-9]{32}", order_id):
        raise ValueError("order_id: se esperaba el identificador Olist de 32 caracteres hexadecimales")
    payment_type = text(raw.get("payment_type"), "payment_type").lower()
    if payment_type not in PAYMENT_TYPES:
        raise ValueError("payment_type: categoría desconocida")
    try:
        value = Decimal(text(raw.get("payment_value"), "payment_value"))
    except InvalidOperation as error:
        raise ValueError("payment_value: importe inválido") from error
    if not value.is_finite() or value < 0 or value >= Decimal("10000000000000000"):
        raise ValueError("payment_value: fuera del rango Decimal(18,2)")
    try:
        cents = value.quantize(Decimal("0.01"))
    except InvalidOperation as error:
        raise ValueError("payment_value: precisión inválida") from error
    if cents != value:
        raise ValueError("payment_value: más de dos decimales significativos")
    if cents == 0:
        cents = Decimal("0.00")
    result = {
        "order_id": order_id,
        "payment_sequential": integer(raw.get("payment_sequential"), "payment_sequential", 1),
        "payment_type": payment_type,
        "payment_installments": integer(raw.get("payment_installments"), "payment_installments", 0),
        "payment_value": cents,
    }
    result["row_hash"] = fingerprint([result[name] for name in FIELDS["payments"]])
    return result


def normalize_geolocation(raw):
    prefix = text(raw.get("geolocation_zip_code_prefix"), "geolocation_zip_code_prefix")
    if not re.fullmatch(r"\d{1,5}", prefix, flags=re.ASCII):
        raise ValueError("geolocation_zip_code_prefix: se esperaban de uno a cinco dígitos")
    coordinates = {}
    for field, limit in (("geolocation_lat", 90), ("geolocation_lng", 180)):
        try:
            value = float(text(raw.get(field), field))
        except (ValueError, OverflowError) as error:
            raise ValueError(f"{field}: coordenada inválida") from error
        if not math.isfinite(value) or not -limit <= value <= limit:
            raise ValueError(f"{field}: coordenada fuera de rango")
        coordinates[field] = 0.0 if value == 0 else value
    state = text(raw.get("geolocation_state"), "geolocation_state").upper()
    if state not in STATES:
        raise ValueError("geolocation_state: UF brasileña desconocida")
    result = {
        "geolocation_zip_code_prefix": prefix.zfill(5),
        **coordinates,
        "geolocation_city": text(raw.get("geolocation_city"), "geolocation_city").lower(),
        "geolocation_state": state,
    }
    result["geolocation_id"] = fingerprint([result[name] for name in FIELDS["geolocation"]])
    result["row_hash"] = result["geolocation_id"]
    return result


def file_kind(key):
    for kind, extensions in EXTENSIONS.items():
        if key.endswith(extensions):
            return kind
    raise ValueError(f"Formato no soportado: {key}")


def is_marker(key):
    return key.endswith("/") or any(part.startswith(("_", ".")) for part in key.split("/"))


def exclusions(kind):
    return ["**/_*", "**/.*"] + [
        f"**/*{extension}"
        for other, extensions in EXTENSIONS.items() if other != kind
        for extension in extensions
    ]


def metric(event, **values):
    print(json.dumps({"event": event, **values}, ensure_ascii=False, default=str), flush=True)


def assert_account(session):
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != ACCOUNT_ID:
        raise RuntimeError(f"Cuenta incorrecta: {identity['Account']}")
    return identity


def inventory(s3, entity):
    result = {kind: [] for kind in EXTENSIONS}
    prefix = f"erp/landing/{entity}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not is_marker(key):
                result[file_kind(key)].append(key)
    return result


def read_raw(spark, paths, entity, kind):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(name, StringType(), True) for name in FIELDS[entity]])
    reader = spark.read.schema(schema).option("mode", "FAILFAST")
    if kind == "csv":
        frame = (reader.option("header", True).option("enforceSchema", False)
                 .option("multiLine", True).option("escape", '"').csv(paths))
    else:
        frame = reader.option("multiLine", kind == "json").json(paths)
    return frame.withColumn("_source_file", F.input_file_name())


def run_bronze(glue, spark, s3, entity, now):
    from pyspark.sql import functions as F

    groups = inventory(s3, entity)
    root = f"s3://{BUCKET}/erp/landing/{entity}/"
    frames = []
    for kind, keys in groups.items():
        if not keys:
            continue
        options = {"attachFilename": "_bookmark_file"}
        options.update({"withHeader": True, "multiLine": True} if kind == "csv" else {"multiline": kind == "json"})
        selection = glue.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [root], "recurse": True, "exclusions": json.dumps(exclusions(kind)), "isFailFast": True},
            format="csv" if kind == "csv" else "json",
            format_options=options,
            transformation_ctx=f"landing_{entity}_{kind}",
        )
        if selection.count() == 0:
            continue
        paths = []
        for row in selection.toDF().select("_bookmark_file").distinct().collect():
            uri = urlparse(row["_bookmark_file"])
            key = unquote(uri.path.lstrip("/"))
            if uri.scheme not in ("s3", "s3a") or uri.netloc != BUCKET or key not in keys:
                raise RuntimeError("El bookmark devolvió un archivo fuera de la fuente esperada")
            paths.append(f"s3://{BUCKET}/{key}")
        metric("bronze_files", entity=entity, format=kind, files=len(paths))
        frames.append(read_raw(spark, paths, entity, kind))
    if not frames:
        metric("bronze_no_new_rows", entity=entity)
        return
    frame = (reduce(lambda left, right: left.unionByName(right), frames)
             .withColumn("_ingested_at", F.lit(now).cast("timestamp"))
             .withColumn("year", F.lit(now.year).cast("int"))
             .withColumn("month", F.lit(now.month).cast("int"))).cache()
    try:
        rows = frame.count()
        frame.write.mode("append").option("compression", "snappy").partitionBy("year", "month").parquet(
            f"s3://{BUCKET}/erp/bronze/{entity}/"
        )
        metric("bronze_written", entity=entity, rows=rows, partition_semantics="ingestion_utc")
    finally:
        frame.unpersist()


def silver_schema(entity):
    from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    types = {
        "payment_sequential": IntegerType(), "payment_installments": IntegerType(),
        "payment_value": DecimalType(18, 2),
        "geolocation_lat": DoubleType(), "geolocation_lng": DoubleType(),
        "_ingested_at": TimestampType(),
    }
    names = FIELDS[entity] + (["geolocation_id"] if entity == "geolocation" else [])
    names += ["row_hash", "_source_file", "_ingested_at", "_validation_error"]
    return StructType([StructField(name, types.get(name, StringType()), True) for name in names])


def normalize_partition(rows, entity):
    normalize = normalize_payments if entity == "payments" else normalize_geolocation
    for row in rows:
        raw = row.asDict()
        result = {"_source_file": raw.get("_source_file"), "_ingested_at": raw.get("_ingested_at")}
        try:
            if not result["_source_file"] or result["_ingested_at"] is None:
                raise ValueError("Falta trazabilidad de Bronze")
            result.update(normalize(raw))
            result["_validation_error"] = None
        except ValueError as error:
            result["_validation_error"] = str(error)
        yield result


def prepare_silver(bronze, entity):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    required = set(FIELDS[entity] + ["_source_file", "_ingested_at"])
    missing = required - set(bronze.columns)
    if missing:
        raise RuntimeError(f"Faltan columnas en Bronze: {sorted(missing)}")
    normalized = bronze.sparkSession.createDataFrame(
        bronze.rdd.mapPartitions(lambda rows: normalize_partition(rows, entity)), silver_schema(entity)
    ).cache()
    try:
        total = normalized.count()
        invalid = normalized.where(F.col("_validation_error").isNotNull())
        errors = invalid.count()
        metric("silver_quality", entity=entity, input_rows=total, invalid_rows=errors)
        if errors:
            samples = [row.asDict() for row in invalid.select("_source_file", "_validation_error").distinct().limit(10).collect()]
            raise ValueError(json.dumps({"invalid_rows": errors, "samples": samples}, ensure_ascii=False))
        keys = KEYS[entity]
        conflicts = (normalized.groupBy(*(keys + ["_ingested_at"]))
                     .agg(F.countDistinct("row_hash").alias("versions")).where("versions > 1"))
        if conflicts.limit(1).count():
            raise ValueError("Versiones distintas de una misma clave en el mismo lote; falta un orden de eventos fiable")
        window = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").asc())
        result = (normalized.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
                  .drop("_rn", "_validation_error").cache())
        kept = result.count()
        metric("silver_dedup", entity=entity, input_rows=total, output_rows=kept, removed_rows=total-kept)
        return result
    finally:
        normalized.unpersist()


def payment_references(frame, orders):
    from pyspark.sql import functions as F

    parents = orders.select("order_id").distinct().withColumn("_parent_found", F.lit(True))
    result = (frame.join(parents, "order_id", "left")
              .withColumn("order_reference_status", F.when(F.col("_parent_found"), "MATCHED").otherwise("PENDING"))
              .drop("_parent_found"))
    counts = {row["order_reference_status"]: row["count"] for row in result.groupBy("order_reference_status").count().collect()}
    metric("payment_order_coverage", **counts)
    return result


def merge_silver(spark, frame, entity, path):
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        frame.write.format("delta").mode("errorifexists").partitionBy("year", "month").save(path)
        return
    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in KEYS[entity])
    changes = "source.row_hash <> target.row_hash"
    if entity == "payments":
        changes += " OR NOT (source.order_reference_status <=> target.order_reference_status)"
    updates = {name: f"source.`{name}`" for name in frame.columns if name not in ("year", "month")}
    (target.alias("target").merge(frame.alias("source"), condition)
     .whenMatchedUpdate(condition=f"({changes}) AND source._ingested_at >= target._ingested_at", set=updates)
     .whenNotMatchedInsertAll().execute())


def run_silver(spark, entity, now):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    bronze_path = f"s3://{BUCKET}/erp/bronze/{entity}/"
    silver_path = f"s3://{BUCKET}/erp/silver/{entity}/"
    bronze = spark.read.parquet(bronze_path)
    prepared = prepare_silver(bronze, entity)
    frame = prepared
    try:
        if frame.limit(1).count() == 0:
            metric("silver_empty", entity=entity)
            return
        if entity == "payments":
            orders_path = f"s3://{BUCKET}/erp/silver/orders/"
            if not DeltaTable.isDeltaTable(spark, orders_path):
                raise RuntimeError("Silver Orders no está disponible como Delta para verificar la relación")
            frame = payment_references(frame, spark.read.format("delta").load(orders_path))
        output = (frame.withColumn("year", F.year("_ingested_at").cast("int"))
                  .withColumn("month", F.month("_ingested_at").cast("int"))
                  .withColumn("fecha_procesamiento_silver", F.lit(now).cast("timestamp"))
                  .withColumn("capa_datos", F.lit("silver")))
        merge_silver(spark, output, entity, silver_path)
        written = spark.read.format("delta").load(silver_path)
        duplicate_keys = written.groupBy(*KEYS[entity]).count().where("count > 1").limit(1).count()
        if duplicate_keys:
            raise RuntimeError("La salida Silver contiene claves duplicadas")
        metric("silver_committed", entity=entity, rows=written.count(), format="delta", semantics="SCD1" if entity == "payments" else "distinct_normalized_points")
    finally:
        prepared.unpersist()


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    match = re.fullmatch(r"g1_olist_(bronze|silver)_(payments|geolocation)", args["JOB_NAME"])
    if not match:
        raise ValueError("Solo se admiten los cuatro jobs de Payments/Geolocation hasta Silver")
    layer, entity = match.groups()
    for name, expected in (("ENTITY", entity), ("LAYER", layer)):
        if f"--{name}" in sys.argv:
            supplied = getResolvedOptions(sys.argv, [name])[name]
            if supplied != expected:
                raise ValueError(f"{name} no coincide con el nombre del job")
    session = boto3.Session(region_name=REGION)
    identity = assert_account(session)
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.caseSensitive", "true")
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    metric("job_started", account=identity["Account"], region=REGION, entity=entity, layer=layer)
    if layer == "bronze":
        run_bronze(glue, spark, session.client("s3"), entity, now)
    else:
        run_silver(spark, entity, now)
    job.commit()
    metric("job_succeeded", entity=entity, layer=layer)


if __name__ == "__main__":
    main()
