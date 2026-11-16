"""
PROYECTO INTEGRADOR OLIST - GRUPO 1
Genera los lotes mensuales del ERP (formato CSV de Olist) a partir del dataset público de Kaggle.

Simula que el ERP entrega un archivo por entidad y mes de negocio:
    erp/landing/<entidad>/year=AAAA/month=MM/<entidad>_AAAA_MM.csv

Reglas (las mismas que usa la Lambda consumidora para las ventas en streaming):
- orders, order_items y customers van al mes de la COMPRA del pedido.
- reviews va al mes de CREACIÓN de la reseña (puede caer meses después de la compra).
- products y sellers van al mes en que aparecen por PRIMERA VEZ en una venta (maestros incrementales).
- payments no se genera: en este proyecto se cargó como un CSV completo aparte.
- zips como texto de 5 dígitos; textos de reseña sin saltos de línea; valores tal cual (sin castear).

Uso:
    python scripts/generar_lotes_historicos.py --desde 2017-01 --hasta 2017-12 --salida build/lotes
    python scripts/generar_lotes_historicos.py --desde 2018-01 --hasta 2018-01 --salida build/demo_2018_01

Luego se suben a S3 respetando la ruta relativa (consola de S3, AWS CLI o URLs prefirmadas)
y el pipeline los detecta como archivos nuevos.
"""
import argparse
import os
import re

import pandas as pd

ORIGEN = "data/raw/olist/"


def leer(nombre):
    return pd.read_csv(os.path.join(ORIGEN, nombre), dtype=str, keep_default_na=False)


def escribir(df, entidad, salida, sufijo=""):
    """Un CSV por mes (columna auxiliar 'p' = AAAA-MM). Devuelve [(ruta, filas)]."""
    escritos = []
    for periodo, grupo in df.groupby("p"):
        anio, mes = periodo.split("-")
        ruta = os.path.join(salida, "erp", "landing", entidad, f"year={anio}", f"month={mes}",
                            f"{entidad}_{anio}_{mes}{sufijo}.csv")
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        grupo.drop(columns="p").to_csv(ruta, index=False, encoding="utf-8")
        escritos.append((ruta, len(grupo)))
    return escritos


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde", required=True, help="primer mes de compra, AAAA-MM")
    ap.add_argument("--hasta", required=True, help="último mes de compra, AAAA-MM")
    ap.add_argument("--salida", default="build/lotes")
    ap.add_argument("--sufijo", default="", help="sufijo del nombre de archivo (evita pisar lotes ya subidos)")
    a = ap.parse_args()

    # 1. PEDIDOS DEL RANGO (por mes de compra)
    todos = leer("olist_orders_dataset.csv")
    todos["p"] = todos.order_purchase_timestamp.str[:7]
    pedidos = todos[(todos.p >= a.desde) & (todos.p <= a.hasta)]

    # 2. DEPENDIENTES DEL PEDIDO
    items_todos = leer("olist_order_items_dataset.csv").merge(todos[["order_id", "p"]], on="order_id")
    items = items_todos[items_todos.order_id.isin(pedidos.order_id)]
    clientes = leer("olist_customers_dataset.csv").merge(pedidos[["customer_id", "p"]], on="customer_id")
    clientes["customer_zip_code_prefix"] = clientes.customer_zip_code_prefix.str.zfill(5)
    resenas = leer("olist_order_reviews_dataset.csv")
    resenas = resenas[resenas.order_id.isin(pedidos.order_id)].copy()
    for col in ("review_comment_title", "review_comment_message"):
        resenas[col] = resenas[col].map(lambda v: re.sub(r"\s+", " ", v).strip())
    resenas["p"] = resenas.review_creation_date.str[:7]

    # 3. MAESTROS: mes de primera aparición en todo el histórico, solo si cae dentro del rango
    primera_prod = items_todos.groupby("product_id").p.min().reset_index()
    primera_vend = items_todos.groupby("seller_id").p.min().reset_index()
    productos = leer("olist_products_dataset.csv").merge(primera_prod, on="product_id")
    vendedores = leer("olist_sellers_dataset.csv").merge(primera_vend, on="seller_id")
    vendedores["seller_zip_code_prefix"] = vendedores.seller_zip_code_prefix.str.zfill(5)
    productos = productos[(productos.p >= a.desde) & (productos.p <= a.hasta)]
    vendedores = vendedores[(vendedores.p >= a.desde) & (vendedores.p <= a.hasta)]

    # 4. ESCRIBIR
    for entidad, df in [("orders", pedidos), ("order_items", items), ("customers", clientes),
                        ("reviews", resenas), ("products", productos), ("sellers", vendedores)]:
        archivos = escribir(df, entidad, a.salida, a.sufijo)
        print(f"{entidad:12s} {len(archivos):3d} archivos {sum(n for _, n in archivos):8,d} filas")


if __name__ == "__main__":
    main()
