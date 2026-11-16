"""
PROYECTO INTEGRADOR OLIST - GRUPO 1
Conversión de eventos del simulador a archivos CSV con el formato del ERP.

Los jobs de bronze del ERP leen erp/landing/<entidad>/ como CSV con encabezado (bookmark):
cada evento válido se convierte en filas con las columnas exactas de los CSV de Olist.

  pedido_creado        -> orders, order_items, payments, customers
  pedido_aprobado/...  -> orders (fila completa con el estado nuevo)
  resena_creada        -> reviews
  cliente_actualizado  -> (solo copia cruda: trae el mismo cliente que su pedido_creado)

Reglas del formato (para que el lector CSV de los jobs no se rompa):
- encabezado con los nombres de Olist, coma, UTF-8, comillas solo cuando hace falta
- sin saltos de línea dentro de los textos (los comentarios de reseñas se limpian)
- fechas "yyyy-MM-dd HH:mm:ss"; zips como texto de 5 dígitos; nulos = campo vacío
- carpeta year=/month= = periodo de negocio (compra del pedido; creación de la reseña)
- un archivo por entidad, periodo y lote, con la hora de llegada en el nombre:
  orders_2026_11_20261111T101500_<id>.csv  (el orden alfabético = el orden de llegada)
"""
import csv
import io
import re

ORDERS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
          "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
ORDER_ITEMS = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
               "price", "freight_value"]
PAYMENTS = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
REVIEWS = ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
           "review_creation_date", "review_answer_timestamp"]
# Mismas 5 columnas que exige g1_olist_bronze_customers (erp/landing/customers/)
CUSTOMERS = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]

# entidad -> (columnas, plantilla de carpeta). {anio}/{mes} = periodo de negocio
ENTIDADES = {
    "orders": (ORDERS, "orders/year={anio}/month={mes}/"),
    "order_items": (ORDER_ITEMS, "order_items/year={anio}/month={mes}/"),
    "payments": (PAYMENTS, "payments/year={anio}/month={mes}/"),
    "reviews": (REVIEWS, "reviews/year={anio}/month={mes}/"),
    "customers": (CUSTOMERS, "customers/year={anio}/month={mes}/"),
}

# Si en un mismo lote llegan dos estados del mismo pedido con la misma hora, gana el más avanzado
RANGO_ESTADO = {"created": 0, "approved": 1, "invoiced": 2, "processing": 3, "shipped": 4,
                "delivered": 5, "unavailable": 6, "canceled": 6}
_ESPACIOS = re.compile(r"\s+")


# ==========================================
# 1. FORMATO DE CAMPOS
# ==========================================
def texto(valor):
    """Valor de un campo CSV: vacío si es nulo, sin saltos de línea, números sin notación científica."""
    if valor is None:
        return ""
    if isinstance(valor, bool):
        return str(valor).lower()
    if isinstance(valor, float):
        return f"{valor:.2f}".rstrip("0").rstrip(".") if valor != int(valor) else str(int(valor))
    return _ESPACIOS.sub(" ", str(valor)).strip()


def zip5(valor):
    return str(valor).strip().zfill(5) if valor not in (None, "") else ""


def periodo(fecha):
    """('2026', '11') desde 'yyyy-MM-dd ...' o ISO-8601."""
    return fecha[:4], fecha[5:7]


# ==========================================
# 2. EVENTOS -> FILAS POR ENTIDAD
# ==========================================
def fila_cliente(cliente):
    fila = {c: cliente.get(c) for c in CUSTOMERS}
    fila["customer_zip_code_prefix"] = zip5(cliente.get("customer_zip_code_prefix"))
    return fila


def filas_por_entidad(eventos):
    """{(entidad, anio, mes): [filas]} a partir de eventos válidos (ya pasaron el contrato)."""
    pedidos = {}      # order_id -> (orden, fila): un pedido queda una sola vez por lote, en su estado más reciente
    clientes = {}     # customer_id -> (periodo de compra, fila): un cliente una sola vez por lote
    salida = {}

    def agregar(entidad, fecha, fila):
        salida.setdefault((entidad, *periodo(fecha)), []).append(fila)

    for e in eventos:
        p, tipo = e["payload"], e["tipo"]
        if tipo == "pedido_creado" or tipo in ("pedido_aprobado", "pedido_enviado",
                                               "pedido_entregado", "pedido_cancelado"):
            pedido = {c: p["pedido"].get(c) for c in ORDERS}
            orden = (e["ts_evento"], RANGO_ESTADO.get(pedido["order_status"], 0))
            previo = pedidos.get(pedido["order_id"])
            if previo is None or orden >= previo[0]:
                pedidos[pedido["order_id"]] = (orden, pedido)

        if tipo == "pedido_creado":
            compra = p["pedido"]["order_purchase_timestamp"]
            oid = p["pedido"]["order_id"]
            for item in p["items"]:
                agregar("order_items", compra, {
                    "order_id": oid, "order_item_id": item.get("order_item_id"),
                    "product_id": (item.get("producto") or {}).get("product_id"),
                    "seller_id": (item.get("vendedor") or {}).get("seller_id"),
                    "shipping_limit_date": item.get("shipping_limit_date"),
                    "price": item.get("price"), "freight_value": item.get("freight_value")})
            for pago in p.get("pagos") or []:
                agregar("payments", compra, {"order_id": oid, **{c: pago.get(c) for c in PAYMENTS[1:]}})
            # En Olist customer_id es único por pedido: el cliente se escribe junto con su pedido.
            # cliente_actualizado no genera fila (repetiría el mismo customer_id y el bronze lo rechaza).
            clientes[p["cliente"]["customer_id"]] = (compra, fila_cliente(p["cliente"]))

        elif tipo == "resena_creada":
            agregar("reviews", p["review_creation_date"], {c: p.get(c) for c in REVIEWS})

    for _, pedido in pedidos.values():
        agregar("orders", pedido["order_purchase_timestamp"], pedido)
    for compra, fila in clientes.values():
        agregar("customers", compra, fila)
    return salida


# ==========================================
# 3. ARCHIVOS CSV
# ==========================================
def csv_bytes(columnas, filas):
    buf = io.StringIO()
    escritor = csv.writer(buf, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
    escritor.writerow(columnas)
    for fila in filas:
        escritor.writerow([texto(fila.get(c)) for c in columnas])
    return buf.getvalue().encode("utf-8")


def archivos(eventos, prefijo, sello, lote):
    """[(clave_s3, cuerpo, entidad, n_filas)] de un lote.
    sello = 'AAAAMMDDTHHMMSS' de llegada; lote = id corto único del lote."""
    resultado = []
    for (entidad, anio, mes), filas in sorted(filas_por_entidad(eventos).items()):
        columnas, carpeta = ENTIDADES[entidad]
        nombre = f"{entidad}_{anio}_{mes}_{sello}_{lote}.csv"
        resultado.append((prefijo + carpeta.format(anio=anio, mes=mes) + nombre,
                          csv_bytes(columnas, filas), entidad, len(filas)))
    return resultado
