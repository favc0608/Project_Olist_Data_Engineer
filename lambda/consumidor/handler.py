"""
PROYECTO INTEGRADOR OLIST - GRUPO 1
LAMBDA CONSUMIDOR: SQS -> S3 erp/landing (ventas sintéticas del simulador en formato ERP).

Por cada lote (hasta 1,000 mensajes o lo acumulado en la ventana):
- erp/landing/eventos/year=/month=/day=/hour=/*.jsonl.gz   copia cruda de los eventos válidos
  (auditoría, reproceso y monitoreo con Athena; partición = llegada en UTC)
- erp/landing/<entidad>/year=/month=/<entidad>_AAAA_MM_<llegada>_<id>.csv   filas en formato Olist
  que leen los jobs de bronze del ERP (ver erp_csv.py)
- erp/landing/rechazados/...   mensajes inválidos con el motivo, sin bloquear el lote.
  La DLQ queda solo para fallos de infraestructura.

Entrega "al menos una vez": si S3 falla se reintenta el lote completo y pueden repetirse filas;
son idénticas y silver deduplica por la clave de cada entidad.

Contrato del mensaje (v2):
    {"evento_id": uuid, "tipo": str, "ts_evento": ISO-8601, "version": 2, "payload": {...}}
"""
import gzip
import json
import os
import uuid
from datetime import datetime, timezone

import erp_csv

# Campos obligatorios del payload por tipo de evento ("a.b" = campo anidado)
PAYLOAD_OBLIGATORIO = {
    "pedido_creado": ["pedido.order_id", "pedido.customer_id", "pedido.order_purchase_timestamp",
                      "pedido.order_estimated_delivery_date",
                      "cliente.customer_id", "cliente.customer_unique_id", "items"],
    # v2: los cambios de estado traen la fila completa del pedido (el ERP la guarda como fila CSV)
    "pedido_aprobado": ["order_id", "fecha", "pedido.order_id", "pedido.order_purchase_timestamp"],
    "pedido_enviado": ["order_id", "fecha", "pedido.order_id", "pedido.order_purchase_timestamp"],
    "pedido_entregado": ["order_id", "fecha", "pedido.order_id", "pedido.order_purchase_timestamp"],
    "pedido_cancelado": ["order_id", "pedido.order_id", "pedido.order_purchase_timestamp"],
    "resena_creada": ["review_id", "order_id", "review_score", "review_creation_date"],
    "cliente_actualizado": ["customer_id", "customer_unique_id"],
}

_s3 = None


def cliente_s3():
    global _s3
    if _s3 is None:
        import boto3
        _s3 = boto3.client("s3")
    return _s3


# ==========================================
# 1. VALIDACION
# ==========================================
def obtener(dic, ruta):
    for parte in ruta.split("."):
        if not isinstance(dic, dict) or dic.get(parte) in (None, ""):
            return None
        dic = dic[parte]
    return dic


def validar(mensaje):
    """Devuelve el motivo de rechazo, o None si el mensaje es valido."""
    if not isinstance(mensaje, dict):
        return "El mensaje no es un objeto JSON"
    faltantes = {"evento_id", "tipo", "ts_evento", "payload"} - mensaje.keys()
    if faltantes:
        return f"Faltan campos: {sorted(faltantes)}"
    if mensaje["tipo"] not in PAYLOAD_OBLIGATORIO:
        return f"Tipo desconocido: {mensaje['tipo']}"
    try:
        uuid.UUID(str(mensaje["evento_id"]))
    except ValueError:
        return "evento_id no es un UUID"
    try:
        datetime.fromisoformat(str(mensaje["ts_evento"]).replace("Z", "+00:00"))
    except ValueError:
        return "ts_evento no es una fecha ISO-8601"
    if not isinstance(mensaje["payload"], dict):
        return "payload no es un objeto"
    faltantes = [c for c in PAYLOAD_OBLIGATORIO[mensaje["tipo"]]
                 if obtener(mensaje["payload"], c) is None]
    if faltantes:
        return f"Faltan campos en payload: {faltantes}"
    return None


# ==========================================
# 2. ESCRITURA EN S3
# ==========================================
def escribir(prefijo, filas, ahora):
    clave = (f"{prefijo}year={ahora:%Y}/month={ahora:%m}/day={ahora:%d}/hour={ahora:%H}/"
             f"{ahora:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:12]}.jsonl.gz")
    cuerpo = gzip.compress(
        "\n".join(json.dumps(f, ensure_ascii=False) for f in filas).encode("utf-8"))
    cliente_s3().put_object(Bucket=os.environ["BUCKET"], Key=clave, Body=cuerpo,
                            ContentType="application/x-ndjson")
    return clave


# ==========================================
# 3. HANDLER
# ==========================================
def lambda_handler(event, context):
    ahora = datetime.now(timezone.utc)
    validos, rechazados = [], []

    for registro in event.get("Records", []):
        enviado_ms = int(registro.get("attributes", {}).get("SentTimestamp", 0))
        meta = {
            "_message_id": registro["messageId"],
            "_enviado_en": datetime.fromtimestamp(enviado_ms / 1000, timezone.utc).isoformat()
            if enviado_ms else None,
            "_recibido_en": ahora.isoformat(),
        }
        try:
            mensaje = json.loads(registro["body"])
            motivo = validar(mensaje)
        except json.JSONDecodeError as error:
            mensaje, motivo = None, f"JSON invalido: {error}"

        if motivo:
            rechazados.append({**meta, "_motivo": motivo, "_cuerpo": registro["body"][:100_000]})
        else:
            validos.append({**mensaje, **meta})

    # Si S3 falla se lanza la excepcion y SQS reintenta el lote completo
    archivos, filas_csv = [], {}
    if validos:
        archivos.append(escribir(os.environ["PREFIJO_EVENTOS"], validos, ahora))
        # Archivos CSV para el ERP (después de la copia cruda: si fallan, SQS reintenta el lote)
        prefijo_erp = os.environ.get("PREFIJO_ERP")
        if prefijo_erp:
            lote = uuid.uuid4().hex[:8]
            for clave, cuerpo, entidad, filas in erp_csv.archivos(validos, prefijo_erp,
                                                                  f"{ahora:%Y%m%dT%H%M%S}", lote):
                cliente_s3().put_object(Bucket=os.environ["BUCKET"], Key=clave, Body=cuerpo,
                                        ContentType="text/csv")
                archivos.append(clave)
                filas_csv[entidad] = filas_csv.get(entidad, 0) + filas
    if rechazados:
        archivos.append(escribir(os.environ["PREFIJO_RECHAZADOS"], rechazados, ahora))

    resumen = {"recibidos": len(validos) + len(rechazados), "validos": len(validos),
               "rechazados": len(rechazados), "filas_csv": filas_csv, "archivos": archivos}
    print(json.dumps(resumen))
    return resumen
