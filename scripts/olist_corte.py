"""Division del dataset Olist en historico (antes del corte) y futuro (simulacion).

El historico es una "foto" de la empresa en la fecha de corte: las fechas
posteriores al corte se vacian y el estado del pedido retrocede al que tenia
en ese momento. Esos cambios pendientes los emitira luego el generador como
eventos (pedido_aprobado, pedido_enviado, pedido_entregado, resena_creada).
"""
import csv
from datetime import datetime
from pathlib import Path

ARCHIVOS = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

FECHAS_PEDIDO = ["order_approved_at", "order_delivered_carrier_date",
                 "order_delivered_customer_date"]


def fecha(valor):
    return datetime.strptime(valor, "%Y-%m-%d %H:%M:%S") if valor else None


def pedido_al_corte(fila, corte):
    """Devuelve una copia del pedido tal como se veia en la fecha de corte."""
    fila = dict(fila)
    for c in FECHAS_PEDIDO:
        if fila[c] and fecha(fila[c]) >= corte:
            fila[c] = ""

    estado = fila["order_status"]
    aprobado = bool(fila["order_approved_at"])
    enviado = bool(fila["order_delivered_carrier_date"])
    entregado = bool(fila["order_delivered_customer_date"])

    if estado == "delivered" and not entregado:
        estado = "shipped" if enviado else ("approved" if aprobado else "created")
    elif estado == "shipped" and not enviado:
        estado = "approved" if aprobado else "created"
    elif estado in ("invoiced", "processing") and not aprobado:
        estado = "created"
    fila["order_status"] = estado
    return fila


def leer(directorio, tabla):
    with open(Path(directorio) / ARCHIVOS[tabla], encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f)
        return lector.fieldnames, list(lector)


def historico_al_corte(directorio, corte):
    """Devuelve {tabla: (columnas, filas)} con la foto de la empresa al corte.

    Los maestros (clientes, productos, vendedores) incluyen solo los que
    participaron en pedidos anteriores al corte; los demas llegaran como
    eventos. geolocation y category_translation son referencias estaticas.
    """
    col_ped, pedidos = leer(directorio, "orders")
    pedidos = [pedido_al_corte(p, corte) for p in pedidos
               if fecha(p["order_purchase_timestamp"]) < corte]
    ids_pedido = {p["order_id"] for p in pedidos}
    ids_cliente = {p["customer_id"] for p in pedidos}

    col_items, items = leer(directorio, "order_items")
    items = [f for f in items if f["order_id"] in ids_pedido]
    ids_producto = {f["product_id"] for f in items}
    ids_vendedor = {f["seller_id"] for f in items}

    col_pagos, pagos = leer(directorio, "order_payments")
    pagos = [f for f in pagos if f["order_id"] in ids_pedido]

    col_res, resenas = leer(directorio, "order_reviews")
    resenas_corte = []
    for f in resenas:
        if f["order_id"] in ids_pedido and fecha(f["review_creation_date"]) < corte:
            if f["review_answer_timestamp"] and fecha(f["review_answer_timestamp"]) >= corte:
                f = dict(f, review_answer_timestamp="")
            resenas_corte.append(f)

    col_cli, clientes = leer(directorio, "customers")
    col_prod, productos = leer(directorio, "products")
    col_vend, vendedores = leer(directorio, "sellers")

    return {
        "orders": (col_ped, pedidos),
        "order_items": (col_items, items),
        "order_payments": (col_pagos, pagos),
        "order_reviews": (col_res, resenas_corte),
        "customers": (col_cli, [f for f in clientes if f["customer_id"] in ids_cliente]),
        "products": (col_prod, [f for f in productos if f["product_id"] in ids_producto]),
        "sellers": (col_vend, [f for f in vendedores if f["seller_id"] in ids_vendedor]),
        "geolocation": leer(directorio, "geolocation"),
        "category_translation": leer(directorio, "category_translation"),
    }
