"""Lambda g1-olist-web-publicador: genera el sitio con los precios del día y lo sube al bucket del sitio.

Disparadores:
- EventBridge Scheduler {"modo": "diario"} a las 05:00 (Lima): precios nuevos del día, sin cambios manuales.
- SQS g1-olist-web-cambios (Colab): un cambio manual {"cambio_id", "tienda", "sku", "accion", "valor", "autor"}
  con accion = precio | descuento | agotar | reponer | restaurar.
- Manual: {"modo": "diario", "fecha": "AAAA-MM-DD"}.

Solo sube los archivos que cambiaron. La subida genera eventos S3 que disparan el scraper (modo demo).
Antes de publicar corre la prueba de contrato (nivel 1): si falla, no publica.
"""
import hashlib
import json
import mimetypes
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

RAIZ = Path(os.environ.get("RAIZ_COMPETENCIA", Path(__file__).resolve().parent / "competencia"))
PLANTILLAS = Path(os.environ["DIR_PLANTILLAS"]) if os.environ.get("DIR_PLANTILLAS") else None
sys.path.insert(0, str(RAIZ / "generador"))
sys.path.insert(0, str(RAIZ / "tests"))
import construir  # noqa: E402
import validar_sitio  # noqa: E402

BUCKET = os.environ["BUCKET"]
BUCKET_SITIO = os.environ["BUCKET_SITIO"]
PREFIJO = os.environ.get("PREFIJO", "web/")
URL_SITIO = os.environ["URL_SITIO"]
LIMA = timezone(timedelta(hours=-5))
ACCIONES = {"precio", "descuento", "agotar", "reponer", "restaurar"}
TIENDAS = ["mercadoabierto", "selva", "compree", "ridley"]

s3 = boto3.client("s3")


def leer_json(clave, defecto=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=clave)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return defecto


def escribir_json(clave, datos):
    s3.put_object(Bucket=BUCKET, Key=clave, Body=json.dumps(datos, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")


def validar_cambio(c: dict, catalogo: dict) -> str:
    if c.get("accion") not in ACCIONES:
        return f"acción desconocida {c.get('accion')!r}"
    if c["accion"] == "restaurar":
        return ""
    if c.get("tienda") not in TIENDAS:
        return f"tienda desconocida {c.get('tienda')!r}"
    if c.get("sku") not in {p["sku"] for p in catalogo["productos"]}:
        return f"producto desconocido {c.get('sku')!r}"
    if c["accion"] == "precio" and not (0 < float(c.get("valor", 0)) < 100000):
        return "precio fuera de rango"
    if c["accion"] == "descuento" and not (1 <= float(c.get("valor", 0)) <= 90):
        return "descuento fuera de 1-90%"
    return ""


def subir_sitio(salida: Path, tiendas: list) -> dict:
    """Sube solo lo que cambió (compara MD5 con el ETag) y borra lo obsoleto de las tiendas generadas."""
    existentes = {}
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET_SITIO):
        for o in pagina.get("Contents", []):
            existentes[o["Key"]] = o["ETag"].strip('"')
    nuevos, subidos = set(), 0
    for f in salida.rglob("*"):
        if not f.is_file():
            continue
        clave = f.relative_to(salida).as_posix()
        nuevos.add(clave)
        cuerpo = f.read_bytes()
        if existentes.get(clave) == hashlib.md5(cuerpo).hexdigest():
            continue
        tipo = mimetypes.guess_type(clave)[0] or "application/octet-stream"
        if tipo.startswith("text/") or tipo in ("application/xml",):
            tipo += "; charset=utf-8"
        s3.put_object(Bucket=BUCKET_SITIO, Key=clave, Body=cuerpo, ContentType=tipo, CacheControl="max-age=60")
        subidos += 1
    obsoletos = [k for k in existentes if k not in nuevos and k.split("/", 1)[0] in tiendas]
    for i in range(0, len(obsoletos), 1000):
        s3.delete_objects(Bucket=BUCKET_SITIO, Delete={"Objects": [{"Key": k} for k in obsoletos[i:i + 1000]]})
    return {"subidos": subidos, "sin_cambios": len(nuevos) - subidos, "borrados": len(obsoletos)}


def lambda_handler(event, context):
    catalogo = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))
    hoy = datetime.now(LIMA).date()
    fecha = datetime.strptime(event["fecha"], "%Y-%m-%d").date() if event.get("fecha") else hoy
    clave_cambios = f"{PREFIJO}referencia/cambios_dia/{fecha.isoformat()}.json"
    cambios = leer_json(clave_cambios, [])
    rechazados, procesados = [], []

    if "Records" in event:  # cambios desde el Colab
        ahora = datetime.now(timezone.utc)
        for r in event["Records"]:
            c = json.loads(r["body"])
            c["recibido_en"] = ahora.isoformat()
            motivo = validar_cambio(c, catalogo)
            c["estado"] = "rechazado" if motivo else "aplicado"
            c["motivo"] = motivo
            procesados.append({"cambio_id": c.get("cambio_id"), "estado": c["estado"], "motivo": motivo})
            s3.put_object(Bucket=BUCKET, Key=f"{PREFIJO}landing/cambios/year={ahora:%Y}/month={ahora:%m}/day={ahora:%d}/"
                                             f"{c.get('cambio_id', r['messageId'])}.json",
                          Body=json.dumps(c, ensure_ascii=False).encode("utf-8"), ContentType="application/json")
            if motivo:
                rechazados.append(c)
            elif c["accion"] == "restaurar":
                cambios = []
            else:
                cambios.append({k: c.get(k) for k in ("cambio_id", "tienda", "sku", "accion", "valor")})
        escribir_json(clave_cambios, cambios)

    with tempfile.TemporaryDirectory() as tmp:
        salida = Path(tmp) / "sitio"
        verdad, listas, pendientes, errores = construir.construir(fecha, TIENDAS, cambios, URL_SITIO, salida,
                                                                  PLANTILLAS, Path(tmp))
        if not errores and listas:
            errores = validar_sitio.validar(salida, verdad, listas)
        if errores:
            print(json.dumps({"publicado": False, "errores": errores[:20]}, ensure_ascii=False))
            raise RuntimeError(f"No se publica: {len(errores)} errores de contrato")
        # La verdad va antes que el sitio: los avisos de S3 disparan el scraper, que la necesita
        escribir_json(f"{PREFIJO}referencia/verdad/{verdad['version']}.json", verdad)
        escribir_json(f"{PREFIJO}referencia/catalogo/catalogo.json", catalogo)
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIJO}referencia/catalogo_tabla/catalogo.jsonl",
                      Body="".join(json.dumps(p, ensure_ascii=False) + "\n" for p in catalogo["productos"]).encode("utf-8"),
                      ContentType="application/x-ndjson")
        actual = {"version": verdad["version"], "fecha": fecha.isoformat(), "tiendas": listas,
                  "cambios": len(cambios), "publicado_en": datetime.now(timezone.utc).isoformat()}
        escribir_json(f"{PREFIJO}referencia/verdad/actual.json", actual)
        subida = subir_sitio(salida, listas)

    # Estado público para el Colab (no es .html: no dispara el scraper)
    s3.put_object(Bucket=BUCKET_SITIO, Key="_estado/publicacion.json", ContentType="application/json",
                  CacheControl="max-age=5", Body=json.dumps({**actual, "procesados": procesados}).encode("utf-8"))
    resumen = {"publicado": True, "version": verdad["version"], "fecha": fecha.isoformat(), "tiendas": listas,
               "pendientes": pendientes, "cambios_del_dia": len(cambios), "rechazados": len(rechazados), **subida}
    print(json.dumps(resumen, ensure_ascii=False))
    return resumen
