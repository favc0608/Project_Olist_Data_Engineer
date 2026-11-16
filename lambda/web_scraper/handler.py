"""Lambda g1-olist-web-scraper: scrapea los sitios de la competencia por CloudFront y deja el resultado en landing.

Disparadores (docs/PLAN_WEB.md, sección 4.1):
- Demo: S3 ObjectCreated del bucket del sitio -> SQS -> esta Lambda. Scrapea solo las tiendas que cambiaron.
- Producción: EventBridge Scheduler {"modo": "programado"} cada 24 h. Scrapea las 4 tiendas.
- Manual: {"tiendas": ["selva"]}.

Salida en s3://<BUCKET>/web/landing/ (partición por fecha de captura, UTC):
  html/year=/month=/day=/<tienda>-<run>.jsonl.gz     HTML crudo de cada página (url, estado, html)
  precios/year=/month=/day=/<tienda>-<run>.jsonl.gz  un registro por producto (listado + ficha + cruce con Olist)
  control/year=/month=/day=/<run>.json               resultado de las reglas R1-R13 por tienda
"""
import gzip
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

import extractor
import reglas

BUCKET = os.environ["BUCKET"]
PREFIJO = os.environ.get("PREFIJO", "web/")
URL_SITIO = os.environ["URL_SITIO"]
DISTRIBUCION = os.environ["DISTRIBUCION_ID"]
BUCKET_SITIO = os.environ.get("BUCKET_SITIO", "")
PAUSA = float(os.environ.get("PAUSA_SEGUNDOS", "0.05"))

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")
cloudwatch = boto3.client("cloudwatch")


CAMPOS_PUBLICOS = ("tienda", "sku_loja", "sku_olist", "titulo", "precio_brl", "precio_lista_brl", "precio_usd",
                   "descuento_pct", "en_stock", "precio_olist", "diferencia_pct")


def leer_json(clave):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read())


def escribir(clave, cuerpo: bytes, tipo="application/json"):
    s3.put_object(Bucket=BUCKET, Key=clave, Body=cuerpo, ContentType=tipo)


def jsonl_gz(filas) -> bytes:
    return gzip.compress("".join(json.dumps(f, ensure_ascii=False) + "\n" for f in filas).encode("utf-8"))


def modo_y_tiendas(event):
    """Devuelve (modo, tiendas) según el disparador."""
    if "Records" in event:  # SQS con notificaciones de S3
        tiendas = set()
        for r in event["Records"]:
            cuerpo = json.loads(r["body"])
            for n in cuerpo.get("Records", []):  # s3:TestEvent no trae Records
                clave = n["s3"]["object"]["key"]
                if clave.split("/", 1)[0] in extractor.TIENDAS:
                    tiendas.add(clave.split("/", 1)[0])
        return "evento", sorted(tiendas)
    return event.get("modo", "manual"), event.get("tiendas") or list(extractor.TIENDAS)


def invalidar_cache(tiendas):
    """Limpia CloudFront de las tiendas cambiadas y espera a que termine (máx. ~90 s)."""
    rutas = [f"/{t}/*" for t in tiendas] + ["/index.html"]
    inv = cloudfront.create_invalidation(DistributionId=DISTRIBUCION, InvalidationBatch={
        "Paths": {"Quantity": len(rutas), "Items": rutas}, "CallerReference": str(uuid.uuid4())})
    for _ in range(30):
        estado = cloudfront.get_invalidation(DistributionId=DISTRIBUCION, Id=inv["Invalidation"]["Id"])
        if estado["Invalidation"]["Status"] == "Completed":
            return True
        time.sleep(3)
    return False


def scrapear_tienda(tienda, actual, catalogo, verdades, run, modo, ahora, particion):
    """Scrapea una tienda, la evalúa contra la verdad de la versión leída y guarda landing. Devuelve el informe."""
    for intento in range(3):  # R10: si CloudFront aún sirve la versión anterior, reintentar
        fuente = extractor.Fuente(URL_SITIO, pausa=PAUSA, guardar=True)
        resultado = extractor.scrapear_tienda(fuente, tienda)
        if resultado["version"] == actual["version"] or intento == 2:
            break
        time.sleep(15)
    version = resultado["version"] or actual["version"]
    if version not in verdades:
        verdades[version] = leer_json(f"{PREFIJO}referencia/verdad/{version}.json")
    verdad = verdades[version]
    if tienda not in verdad["tiendas"]:
        return None
    informe = reglas.evaluar(resultado, verdad, catalogo)
    if resultado["version"] != actual["version"]:
        informe["reglas"]["R10"].update(ok=False, fallas=1,
                                        ejemplos=[f"leída {resultado['version']} != publicada {actual['version']}"])
        informe["ok"] = False

    comunes = {"run_id": run, "modo": modo, "capturado_en": ahora.isoformat(), "version_sitio": resultado["version"]}
    cruce = {c["sku_loja"]: c for c in informe["comparativa"]}
    filas = []
    for r in resultado["listados"]:
        if r["patrocinado"]:
            continue
        ficha = resultado["fichas"].get(r["sku_loja"], {})
        c = cruce.get(r["sku_loja"], {})
        filas.append({**comunes, **{k: v for k, v in r.items() if k not in ("agotado_clase", "patrocinado")},
                      "ficha_ok": bool(ficha), "precio_ficha_brl": ficha.get("precio_brl"),
                      "fretes_json": json.dumps(ficha.get("fretes", {})),
                      "otros_vendedores_json": json.dumps(ficha.get("otros_vendedores", [])),
                      "sku_olist": c.get("sku_olist"), "product_id": c.get("product_id"),
                      "precio_olist": c.get("precio_olist"), "diferencia_pct": c.get("diferencia_pct")})
    escribir(f"{PREFIJO}landing/precios/{particion}{tienda}-{run}.jsonl.gz", jsonl_gz(filas), "application/gzip")
    escribir(f"{PREFIJO}landing/html/{particion}{tienda}-{run}.jsonl.gz",
             jsonl_gz({"run_id": run, "url": u, "estado": e, "html": h} for u, e, h in fuente.paginas),
             "application/gzip")
    informe.pop("comparativa")
    # Resumen público por producto para el Colab (tabla "precios actuales"): sin HTML ni fletes
    informe["_precios"] = [{k: f.get(k) for k in CAMPOS_PUBLICOS} for f in filas]
    cloudwatch.put_metric_data(Namespace="g1-olist/web", MetricData=[
        {"MetricName": "ReglasFallidas", "Dimensions": [{"Name": "Tienda", "Value": tienda}],
         "Value": sum(not x["ok"] for x in informe["reglas"].values())},
        {"MetricName": "ProductosScrapeados", "Dimensions": [{"Name": "Tienda", "Value": tienda}],
         "Value": informe["productos"]}])
    return informe


def lambda_handler(event, context):
    modo, tiendas = modo_y_tiendas(event)
    if not tiendas:
        return {"modo": modo, "mensaje": "sin tiendas que scrapear"}
    ahora = datetime.now(timezone.utc)
    run = f"{ahora:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    particion = f"year={ahora:%Y}/month={ahora:%m}/day={ahora:%d}/"
    actual = leer_json(f"{PREFIJO}referencia/verdad/actual.json")

    if modo == "evento":
        # Una publicación genera cientos de avisos repartidos en varios lotes: se scrapea una vez por versión,
        # todas las tiendas publicadas. Las demás invocaciones de la misma versión terminan sin hacer nada.
        tiendas = actual["tiendas"]
        ultimo = _leer_opcional(f"{PREFIJO}landing/control/ultimo.json")
        if ultimo and ultimo["version"] == actual["version"]:
            return {"modo": modo, "mensaje": f"versión {actual['version']} ya scrapeada"}
        marca = _leer_opcional(f"{PREFIJO}landing/control/en_curso.json")
        if marca and marca["version"] == actual["version"] and time.time() - marca["desde"] < 300:
            return {"modo": modo, "mensaje": f"versión {actual['version']} en curso ({marca['run_id']})"}
        escribir(f"{PREFIJO}landing/control/en_curso.json",
                 json.dumps({"version": actual["version"], "run_id": run, "desde": time.time()}).encode())
        invalidar_cache(tiendas)

    catalogo = leer_json(f"{PREFIJO}referencia/catalogo/catalogo.json")
    verdades = {}
    with ThreadPoolExecutor(max_workers=4) as pool:  # las 4 tiendas en paralelo
        informes = [i for i in pool.map(lambda t: scrapear_tienda(t, actual, catalogo, verdades, run, modo,
                                                                  ahora, particion), tiendas) if i]
    precios = [f for i in informes for f in i.pop("_precios")]

    control = {"run_id": run, "modo": modo, "capturado_en": ahora.isoformat(), "version": actual["version"],
               "tiendas": [i["tienda"] for i in informes], "ok": all(i["ok"] for i in informes), "informes": informes}
    escribir(f"{PREFIJO}landing/control/{particion}{run}.json", json.dumps(control, ensure_ascii=False).encode())
    ultimo = {k: control[k] for k in ("run_id", "modo", "capturado_en", "version", "tiendas", "ok")}
    ultimo["terminado_en"] = datetime.now(timezone.utc).isoformat()
    ultimo["reglas_fallidas"] = {i["tienda"]: [k for k, v in i["reglas"].items() if not v["ok"]] for i in informes}
    escribir(f"{PREFIJO}landing/control/ultimo.json", json.dumps(ultimo).encode())
    if BUCKET_SITIO:  # estado público para el Colab (no es .html: no genera avisos)
        publico = {"run_id": run, "version": actual["version"], "capturado_en": ahora.isoformat(), "filas": precios}
        for clave, cuerpo in (("_estado/precios.json", publico), ("_estado/scraping.json", ultimo)):
            s3.put_object(Bucket=BUCKET_SITIO, Key=clave, Body=json.dumps(cuerpo, ensure_ascii=False).encode(),
                          ContentType="application/json", CacheControl="max-age=5")
    resumen = {i["tienda"]: {"productos": i["productos"], "ok": i["ok"],
                             "fallas": [k for k, v in i["reglas"].items() if not v["ok"]]} for i in informes}
    print(json.dumps({"run_id": run, "modo": modo, "version": actual["version"], "resumen": resumen}))
    return {"run_id": run, "modo": modo, "ok": control["ok"], "resumen": resumen}


def _leer_opcional(clave):
    try:
        return leer_json(clave)
    except s3.exceptions.NoSuchKey:
        return None
