import json
import re
import sys
import uuid
from datetime import datetime, timezone
from functools import reduce
from urllib.parse import unquote, urlparse


ACCOUNT_ID = "<ACCOUNT_ID>"
REGION = "us-east-2"
BUCKET = "trabajo-integrador-g1"
DATABASE = "g1_olist_lakehouse"
WRITER_ARTEFACT = "olist_bryan_gold/glue/olist_gold.py"
PAYMENT_COLUMNS = [
    "order_id", "payment_sequential", "customer_key", "payment_date_key",
    "payment_type", "installments", "payment_value",
]
GEO_FIELDS = [
    "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
    "geolocation_city", "geolocation_state",
]
STATES = "AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split()
TARGETS = {"payments": "fact_payments", "geolocation": "gold_geolocation"}
KEYS = {"payments": ["order_id", "payment_sequential"], "geolocation": ["geolocation_zip_code_prefix"]}


def _required(frame, names, label):
    missing = sorted(set(names) - set(frame.columns))
    if missing:
        raise ValueError(f"{label}: faltan columnas {missing}")
    if len(frame.columns) != len(set(frame.columns)):
        raise ValueError(f"{label}: nombres de columnas duplicados")


def _count_when(condition):
    from pyspark.sql import functions as F

    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long")


def _unique(frame, keys, label):
    from pyspark.sql import functions as F

    invalid = reduce(lambda a, b: a | b, [F.col(k).isNull() | (F.trim(F.col(k).cast("string")) == "") for k in keys])
    row = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct(*[F.col(k) for k in keys])).alias("keys"),
        _count_when(invalid).alias("invalid"),
    ).first()
    if row["invalid"]:
        raise ValueError(f"{label}: claves nulas o vacías")
    if row["rows"] != row["keys"]:
        raise ValueError(f"{label}: claves duplicadas {keys}")
    return int(row["rows"])


def _strings(frame, names, label):
    from pyspark.sql.types import StringType

    for name in names:
        if not isinstance(frame.schema[name].dataType, StringType):
            raise ValueError(f"{label}.{name}: se requiere string")


def build_payments(payments, orders, customers):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, StringType

    payments.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    _required(payments, ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"], "payments")
    _required(orders, ["order_id", "customer_id", "order_purchase_timestamp"], "orders")
    _required(customers, ["customer_id", "customer_unique_id"], "customers")
    _strings(payments, ["order_id", "payment_type"], "payments")
    _strings(orders, ["order_id", "customer_id"], "orders")
    _strings(customers, ["customer_id", "customer_unique_id"], "customers")
    if not isinstance(payments.schema["payment_value"].dataType, (DecimalType, StringType)):
        raise ValueError("payment_value: se requiere decimal exacto o texto, nunca float/double")
    counts = {
        "input_rows": _unique(payments, KEYS["payments"], "payments"),
        "orders_rows": _unique(orders, ["order_id"], "orders"),
        "customers_rows": _unique(customers, ["customer_id"], "customers"),
    }
    raw_value = F.col("payment_value").cast("string")
    prepared = payments.select(
        "order_id", "payment_type",
        F.expr("try_cast(payment_sequential AS INT)").alias("payment_sequential"),
        F.expr("try_cast(payment_installments AS INT)").alias("installments"),
        F.expr("try_cast(payment_value AS DECIMAL(18,2))").alias("payment_value"),
        raw_value.alias("_raw_value"),
        F.col("payment_sequential").cast("string").alias("_raw_sequence"),
        F.col("payment_installments").cast("string").alias("_raw_installments"),
    )
    invalid = (
        F.col("payment_value").isNull()
        | ~F.col("_raw_value").rlike(r"^[+-]?[0-9]+([.][0-9]+)?$")
        | ~F.col("_raw_value").rlike(r"^[+-]?[0-9]+([.][0-9]{0,2}0*)?$")
        | (F.col("payment_value") < 0)
        | F.col("payment_sequential").isNull()
        | ~F.col("_raw_sequence").rlike(r"^[0-9]+$")
        | (F.col("payment_sequential") < 1)
        | F.col("installments").isNull()
        | ~F.col("_raw_installments").rlike(r"^[0-9]+$")
        | (F.col("installments") < 0)
        | F.col("payment_type").isNull()
        | ~F.col("payment_type").isin("boleto", "credit_card", "debit_card", "voucher", "not_defined")
    )
    validation = prepared.agg(
        _count_when(invalid).alias("invalid"),
        F.sum("payment_value").alias("value_sum"),
        F.countDistinct(F.struct("order_id", "payment_sequential")).alias("normalized_keys"),
    ).first()
    if validation["invalid"]:
        raise ValueError("payments: valor, precisión, secuencia, cuotas o tipo inválidos")
    if validation["normalized_keys"] != counts["input_rows"]:
        raise ValueError("payments: claves duplicadas después de normalizar payment_sequential")
    p = prepared.drop("_raw_value", "_raw_sequence", "_raw_installments")
    timestamp_type = orders.schema["order_purchase_timestamp"].dataType.typeName()
    if timestamp_type not in {"string", "timestamp", "timestamp_ntz", "date"}:
        raise ValueError("orders.order_purchase_timestamp: tipo incompatible")
    purchase = F.expr("try_cast(order_purchase_timestamp AS TIMESTAMP_NTZ)")
    if timestamp_type == "string":
        wall_clock = F.col("order_purchase_timestamp").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?)?$")
        purchase = F.when(wall_clock, purchase)
    o = orders.select(
        F.col("order_id").alias("_order_id"),
        F.col("customer_id").alias("_order_customer_id"),
        purchase.alias("_purchase_timestamp"),
        F.col("order_purchase_timestamp").isNotNull().alias("_purchase_supplied"),
        F.lit(True).alias("_order_found"),
    )
    c = customers.select(
        F.col("customer_id").alias("_customer_id"),
        F.when(F.trim(F.col("customer_unique_id")) != "", F.col("customer_unique_id")).alias("customer_key"),
        F.lit(True).alias("_customer_found"),
    )
    joined = (p.join(o, p.order_id == o._order_id, "left")
              .join(c, F.col("_order_customer_id") == F.col("_customer_id"), "left")
              .withColumn("payment_date_key", F.date_format("_purchase_timestamp", "yyyyMMdd").cast("int")))
    stats = joined.agg(
        F.count("*").alias("output_rows"),
        F.sum("payment_value").alias("payment_value_sum"),
        _count_when(F.col("payment_value") == 0).alias("zero_value_rows"),
        _count_when(F.col("_order_found").isNull()).alias("missing_order_rows"),
        _count_when(F.col("_order_found").isNotNull() & F.col("_customer_found").isNull()).alias("missing_customer_reference_rows"),
        _count_when(F.col("_customer_found").isNotNull() & F.col("customer_key").isNull()).alias("missing_customer_unique_id_rows"),
        _count_when(F.col("customer_key").isNull()).alias("null_customer_key_rows"),
        _count_when(F.col("payment_date_key").isNull()).alias("null_payment_date_key_rows"),
        _count_when(F.col("_order_found").isNotNull() & ~F.col("_purchase_supplied")).alias("missing_purchase_timestamp_rows"),
        _count_when(F.col("_purchase_supplied") & F.col("_purchase_timestamp").isNull()).alias("invalid_purchase_timestamp_rows"),
    ).first().asDict()
    if counts["input_rows"] != stats["output_rows"] or validation["value_sum"] != stats["payment_value_sum"]:
        raise ValueError("payments: el cruce modificó la cardinalidad o el importe")
    stats["payment_value_sum"] = str(stats["payment_value_sum"]) if stats["payment_value_sum"] is not None else None
    return joined.select(*PAYMENT_COLUMNS), {**counts, **stats}


def build_geolocation(geo):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _required(geo, GEO_FIELDS + ["geolocation_id"], "geolocation")
    _strings(geo, ["geolocation_id", "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"], "geolocation")
    invalid = (
        F.col("geolocation_id").isNull()
        | ~F.col("geolocation_id").rlike(r"^[a-f0-9]{64}$")
        | F.col("geolocation_zip_code_prefix").isNull()
        | ~F.col("geolocation_zip_code_prefix").rlike(r"^[0-9]{5}$")
        | F.col("geolocation_city").isNull()
        | (F.length(F.col("geolocation_city")) == 0)
        | (F.col("geolocation_city") != F.lower(F.trim(F.col("geolocation_city"))))
        | F.col("geolocation_state").isNull()
        | ~F.col("geolocation_state").isin(*STATES)
    )
    for name, bound in (("geolocation_lat", 90), ("geolocation_lng", 180)):
        if geo.schema[name].dataType.typeName() not in {"double", "float", "decimal"}:
            raise ValueError(f"geolocation.{name}: tipo numérico incompatible")
        invalid = invalid | F.col(name).isNull() | F.isnan(name) | ~F.col(name).between(-bound, bound)
    if "row_hash" in geo.columns:
        invalid = invalid | ~F.col("geolocation_id").eqNullSafe(F.col("row_hash"))
    stats = geo.agg(F.count("*").alias("input_rows"), _count_when(invalid).alias("invalid")).first().asDict()
    if stats["invalid"]:
        raise ValueError("geolocation: puntos no normalizados, coordenadas inválidas o hash incoherente")
    points = geo.select("geolocation_id", *GEO_FIELDS)
    if points.groupBy("geolocation_id").agg(F.countDistinct(F.struct(*GEO_FIELDS)).alias("versions")).where("versions > 1").limit(1).count():
        raise ValueError("geolocation: un geolocation_id identifica puntos distintos")
    if points.groupBy(*GEO_FIELDS).agg(F.countDistinct("geolocation_id").alias("identities")).where("identities > 1").limit(1).count():
        raise ValueError("geolocation: un punto normalizado tiene identificadores distintos")
    points = points.dropDuplicates(["geolocation_id"])
    prefix = "geolocation_zip_code_prefix"
    grouped = points.groupBy(prefix).agg(
        F.bround(F.avg(F.col("geolocation_lat").cast("decimal(24,16)")), 12).cast("double").alias("geolocation_lat"),
        F.bround(F.avg(F.col("geolocation_lng").cast("decimal(24,16)")), 12).cast("double").alias("geolocation_lng"),
        F.count("*").alias("point_count"),
    )
    pairs = points.groupBy(prefix, "geolocation_city", "geolocation_state").count()
    partition = Window.partitionBy(prefix)
    ranking = partition.orderBy(F.col("count").desc(), F.col("geolocation_city").asc(), F.col("geolocation_state").asc())
    modes = (pairs.withColumn("_max_count", F.max("count").over(partition))
             .withColumn("_tied_pairs", F.sum(F.when(F.col("count") == F.col("_max_count"), 1).otherwise(0)).over(partition))
             .withColumn("_rank", F.row_number().over(ranking)).where("_rank = 1"))
    result = grouped.join(modes.select(prefix, "geolocation_city", "geolocation_state", "_tied_pairs"), prefix, "inner")
    totals = result.agg(
        F.count("*").alias("output_rows"),
        F.sum("point_count").alias("distinct_points"),
        _count_when(F.col("_tied_pairs") > 1).alias("modal_tie_prefixes"),
    ).first().asDict()
    totals["distinct_points"] = totals["distinct_points"] or 0
    metrics = {"input_rows": stats["input_rows"], **totals, "duplicate_points_removed": stats["input_rows"] - totals["distinct_points"]}
    return result.select(*GEO_FIELDS, "point_count"), metrics


def _s3_location(location, prefix):
    parsed = urlparse(location)
    key = unquote(parsed.path).lstrip("/").rstrip("/")
    if (parsed.scheme != "s3" or parsed.netloc != BUCKET or parsed.query or parsed.fragment
            or any(part in {"", ".", ".."} for part in key.split("/"))
            or not (key == prefix.rstrip("/") or key.startswith(prefix.rstrip("/") + "/"))):
        raise ValueError(f"Ubicación fuera del prefijo autorizado: {location}")
    return f"s3://{BUCKET}/{key}/"


def _table(glue, name):
    try:
        return glue.get_table(CatalogId=ACCOUNT_ID, DatabaseName=DATABASE, Name=name)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None


def _read_snapshot(spark, glue, entity):
    from delta.tables import DeltaTable

    name = f"silver_{entity}"
    table = _table(glue, name)
    if table is None:
        raise ValueError(f"Falta la tabla {name}")
    location = _s3_location(table.get("StorageDescriptor", {}).get("Location", ""), f"erp/silver/{entity}/")
    if not DeltaTable.isDeltaTable(spark, location):
        raise ValueError(f"{name}: la ubicación actual no es Delta")
    version = int(DeltaTable.forPath(spark, location).history(1).select("version").first()["version"])
    frame = spark.read.format("delta").option("versionAsOf", version).load(location)
    return frame, {"table": name, "location": location, "version": version, "schema": frame.schema.jsonValue()}


def _guard_target(spark, glue, s3, entity, location):
    from delta.tables import DeltaTable

    table = _table(glue, TARGETS[entity])
    catalog_owned = False
    if table is not None:
        registered = _s3_location(table.get("StorageDescriptor", {}).get("Location", ""), f"erp/gold/{TARGETS[entity]}/")
        if registered != location:
            raise ValueError("La tabla Gold apunta a otra ubicación")
        catalog_writer = table.get("Parameters", {}).get("writer_artefact")
        if catalog_writer is not None and catalog_writer != WRITER_ARTEFACT:
            raise ValueError("La tabla Gold pertenece a otro escritor")
        catalog_owned = catalog_writer == WRITER_ARTEFACT
    exists = DeltaTable.isDeltaTable(spark, location)
    if exists:
        properties = DeltaTable.forPath(spark, location).detail().select("properties").first()["properties"] or {}
        delta_writer = properties.get("writer_artefact")
        if delta_writer is not None and delta_writer != WRITER_ARTEFACT:
            raise ValueError("El destino Delta pertenece a otro escritor")
        if not catalog_owned and delta_writer != WRITER_ARTEFACT:
            raise ValueError("El destino Delta existente no identifica este escritor")
    else:
        if table is not None:
            raise ValueError("Existe una tabla Gold sin destino Delta válido")
        prefix = urlparse(location).path.lstrip("/")
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1, ExpectedBucketOwner=ACCOUNT_ID)
        if objects.get("KeyCount", len(objects.get("Contents", []))):
            raise ValueError("El prefijo Gold contiene datos ajenos o no Delta")
    return exists, table


def _same_schema(left, right):
    return [(f.name, f.dataType) for f in left.schema] == [(f.name, f.dataType) for f in right.schema]


def merge_gold(spark, output, entity, location, exists, run_id):
    from delta.tables import DeltaTable

    keys = KEYS[entity]
    if exists:
        target = DeltaTable.forPath(spark, location)
        current = target.toDF()
        if not _same_schema(current, output):
            raise ValueError("El esquema Delta existente no coincide con el contrato Gold")
        _unique(current, keys, TARGETS[entity])
        if current.select(*keys).join(output.select(*keys), keys, "left_anti").limit(1).count():
            raise ValueError("El destino contiene claves ausentes del snapshot; no se infieren borrados")
        condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)
        equality = " AND ".join(f"target.`{name}` <=> source.`{name}`" for name in output.columns)
        (target.alias("target").merge(output.alias("source"), condition)
         .whenMatchedUpdateAll(condition=f"NOT ({equality})")
         .whenNotMatchedInsertAll().execute())
    else:
        (output.write.format("delta").mode("errorifexists")
         .option("userMetadata", json.dumps({"run_id": run_id, "writer_artefact": WRITER_ARTEFACT}))
         .save(location))
        spark.sql(f"ALTER TABLE delta.`{location}` SET TBLPROPERTIES ('writer_artefact' = '{WRITER_ARTEFACT}')")
    written = spark.read.format("delta").load(location)
    count = _unique(written, keys, TARGETS[entity])
    if count != output.count():
        raise ValueError("La validación posterior detectó un conteo distinto")
    if entity == "payments":
        from pyspark.sql import functions as F

        expected = output.agg(F.sum("payment_value").alias("total")).first()["total"]
        actual = written.agg(F.sum("payment_value").alias("total")).first()["total"]
        if expected != actual:
            raise ValueError("La validación posterior detectó una suma monetaria distinta")
    if written.exceptAll(output).limit(1).count() or output.exceptAll(written).limit(1).count():
        raise ValueError("La validación posterior detectó valores distintos")
    return {"rows": count, "delta_version": int(DeltaTable.forPath(spark, location).history(1).select("version").first()["version"])}


def _register(glue, output, entity, location, previous):
    parameters = dict((previous or {}).get("Parameters", {}))
    parameters.update({"EXTERNAL": "TRUE", "classification": "delta", "table_type": "DELTA", "spark.sql.sources.provider": "delta", "writer_artefact": WRITER_ARTEFACT})
    definition = {
        "Name": TARGETS[entity], "TableType": "EXTERNAL_TABLE", "Parameters": parameters,
        "StorageDescriptor": {
            "Columns": [{"Name": field.name, "Type": field.dataType.simpleString()} for field in output.schema],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "Parameters": {"serialization.format": "1", "path": location}},
        },
    }
    if previous is None:
        glue.create_table(CatalogId=ACCOUNT_ID, DatabaseName=DATABASE, TableInput=definition)
    else:
        glue.update_table(CatalogId=ACCOUNT_ID, DatabaseName=DATABASE, TableInput=definition, SkipArchive=False)
    published = _table(glue, TARGETS[entity])
    if (published is None or published.get("Parameters", {}).get("writer_artefact") != WRITER_ARTEFACT
            or published.get("StorageDescriptor", {}).get("Location") != location
            or published.get("StorageDescriptor", {}).get("SerdeInfo", {}).get("Parameters", {}).get("path") != location):
        raise ValueError("No se pudo verificar la publicación en Glue")


def _metric(event, **values):
    print(json.dumps({"event": event, **values}, ensure_ascii=False, default=str), flush=True)


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    match = re.fullmatch(r"g1_olist_gold_(payments|geolocation)", args["JOB_NAME"])
    if not match:
        raise ValueError("Nombre de job Gold no autorizado")
    entity = match.group(1)
    if "--ENTITY" in sys.argv and getResolvedOptions(sys.argv, ["ENTITY"])["ENTITY"] != entity:
        raise ValueError("ENTITY no coincide con el job")
    run_id = args.get("JOB_RUN_ID") or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
        raise ValueError("Identificador de ejecución inválido")
    session = boto3.Session(region_name=REGION)
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != ACCOUNT_ID or session.region_name != REGION:
        raise ValueError("Cuenta o región fuera del alcance autorizado")
    s3 = session.client("s3")
    bucket_region = s3.get_bucket_location(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT_ID).get("LocationConstraint") or "us-east-1"
    if bucket_region != REGION:
        raise ValueError("Región de bucket no autorizada")
    glue_client = session.client("glue")
    job_definition = glue_client.get_job(JobName=args["JOB_NAME"])["Job"]
    if job_definition.get("GlueVersion") != "5.1":
        raise ValueError("Este artefacto requiere Glue 5.1")
    if job_definition.get("ExecutionProperty", {}).get("MaxConcurrentRuns", 1) != 1:
        raise ValueError("El job debe limitar MaxConcurrentRuns a 1")
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.caseSensitive", "true")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    audit = {
        "run_id": run_id, "job_name": args["JOB_NAME"], "entity": entity,
        "account": ACCOUNT_ID, "region": REGION, "writer_artefact": WRITER_ARTEFACT,
        "started_at": datetime.now(timezone.utc).isoformat(), "status": "RUNNING", "snapshots": {},
    }
    cached = []
    try:
        _metric("gold_started", **audit)
        location = f"s3://{BUCKET}/erp/gold/{TARGETS[entity]}/"
        audit["output_location"] = location
        exists, previous = _guard_target(spark, glue_client, s3, entity, location)
        entities = ["payments", "orders", "customers"] if entity == "payments" else ["geolocation"]
        sources = {}
        for source_entity in entities:
            frame, snapshot = _read_snapshot(spark, glue_client, source_entity)
            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(frame)
            if not frame.limit(1).count():
                raise ValueError(f"Entrada Silver vacía: {source_entity}")
            sources[source_entity] = frame
            audit["snapshots"][source_entity] = snapshot
        if entity == "payments":
            output, metrics = build_payments(sources["payments"], sources["orders"], sources["customers"])
        else:
            output, metrics = build_geolocation(sources["geolocation"])
        output = output.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(output)
        if not output.limit(1).count():
            raise ValueError("La salida Gold está vacía")
        audit["metrics"] = metrics
        _metric("gold_quality", run_id=run_id, entity=entity, **metrics)
        exists, previous = _guard_target(spark, glue_client, s3, entity, location)
        audit["postwrite"] = merge_gold(spark, output, entity, location, exists, run_id)
        _register(glue_client, output, entity, location, previous)
        job.commit()
        audit["status"] = "SUCCEEDED"
    except Exception as error:
        audit["status"] = "FAILED"
        audit["error"] = {"type": type(error).__name__, "message": str(error)[:4000]}
        _metric("gold_failed", **audit)
        raise
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        audit["finished_at"] = datetime.now(timezone.utc).isoformat()
        key = f"artefactos/bryan/gold/runs/{run_id}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(audit, ensure_ascii=False, default=str).encode("utf-8"), ContentType="application/json", ExpectedBucketOwner=ACCOUNT_ID)
        _metric("gold_finished", **audit, audit_location=f"s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
