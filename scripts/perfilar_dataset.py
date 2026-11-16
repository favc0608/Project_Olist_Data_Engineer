"""Perfilado del dataset Olist: volumen, nulos, claves, integridad y rangos."""
from pathlib import Path

import pandas as pd

RUTA = Path(__file__).resolve().parent.parent / "data" / "raw" / "olist"

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

CLAVES = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category_translation": ["product_category_name"],
}

# (tabla hija, columna, tabla padre, columna)
RELACIONES = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
    ("customers", "customer_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
    ("sellers", "seller_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
    ("products", "product_category_name", "category_translation", "product_category_name"),
]

# 1. CARGA (todo como texto para no perder ceros a la izquierda)
dfs = {n: pd.read_csv(RUTA / f, dtype=str, keep_default_na=False, na_values=[""]) for n, f in ARCHIVOS.items()}

# 2. VOLUMEN, COLUMNAS Y NULOS
print("=" * 70, "\n1. VOLUMEN Y NULOS\n" + "=" * 70)
for n, df in dfs.items():
    print(f"\n## {n}: {len(df):,} filas x {df.shape[1]} columnas")
    for c in df.columns:
        nulos = df[c].isna().sum()
        print(f"   {c:<32} distintos={df[c].nunique():>9,}  nulos={nulos:>7,} ({nulos / len(df):.1%})")

# 3. UNICIDAD DE CLAVES
print("\n" + "=" * 70, "\n2. UNICIDAD DE CLAVES\n" + "=" * 70)
for n, k in CLAVES.items():
    dup = dfs[n].duplicated(subset=k).sum()
    print(f"   {n:<22} {'+'.join(k):<40} duplicados={dup:,}")
print(f"   filas 100% duplicadas en geolocation: {dfs['geolocation'].duplicated().sum():,}")

# 4. INTEGRIDAD REFERENCIAL
print("\n" + "=" * 70, "\n3. INTEGRIDAD REFERENCIAL (huérfanos)\n" + "=" * 70)
for hija, ch, padre, cp in RELACIONES:
    valores = dfs[hija][ch].dropna()
    huerf = (~valores.isin(set(dfs[padre][cp]))).sum()
    print(f"   {hija}.{ch} -> {padre}.{cp}: {huerf:,} huérfanos de {len(valores):,}")

# 5. RANGOS DE FECHAS
print("\n" + "=" * 70, "\n4. FECHAS\n" + "=" * 70)
o = dfs["orders"]
for c in [c for c in o.columns if "date" in c or "timestamp" in c or "approved" in c]:
    f = pd.to_datetime(o[c], errors="coerce")
    print(f"   {c:<32} min={f.min()}  max={f.max()}  nulos={f.isna().sum():,}")
compra = pd.to_datetime(o["order_purchase_timestamp"])
print("\n   Pedidos por mes:")
print(compra.dt.to_period("M").value_counts().sort_index().to_string())

# 6. DISTRIBUCIONES CLAVE
print("\n" + "=" * 70, "\n5. DISTRIBUCIONES\n" + "=" * 70)
print("\n   order_status:\n" + o["order_status"].value_counts().to_string())
print("\n   payment_type:\n" + dfs["order_payments"]["payment_type"].value_counts().to_string())
print("\n   review_score:\n" + dfs["order_reviews"]["review_score"].value_counts().sort_index().to_string())
print("\n   customer_state (top 10):\n" + dfs["customers"]["customer_state"].value_counts().head(10).to_string())

# 7. HALLAZGOS RELEVANTES PARA EL MODELO
print("\n" + "=" * 70, "\n6. HALLAZGOS PARA MODELADO\n" + "=" * 70)
c = dfs["customers"]
pedidos_por_cliente = c.groupby("customer_unique_id")["customer_id"].nunique()
print(f"   customer_unique_id con >1 customer_id: {(pedidos_por_cliente > 1).sum():,} de {len(pedidos_por_cliente):,}")
cambios = c.groupby("customer_unique_id")[["customer_zip_code_prefix", "customer_city", "customer_state"]].nunique()
print(f"   customer_unique_id con cambio de zip/ciudad/estado: {(cambios > 1).any(axis=1).sum():,}")
items_por_pedido = dfs["order_items"].groupby("order_id").size()
print(f"   items por pedido: media={items_por_pedido.mean():.2f} max={items_por_pedido.max()}")
pagos_por_pedido = dfs["order_payments"].groupby("order_id").size()
print(f"   pagos por pedido: media={pagos_por_pedido.mean():.2f} max={pagos_por_pedido.max()}")
print(f"   pedidos con >1 reseña: {(dfs['order_reviews'].groupby('order_id').size() > 1).sum():,}")
print(f"   pedidos sin items: {(~o['order_id'].isin(dfs['order_items']['order_id'])).sum():,}")
print(f"   pedidos sin pagos: {(~o['order_id'].isin(dfs['order_payments']['order_id'])).sum():,}")
p = dfs["products"]
print(f"   productos sin categoría: {p['product_category_name'].isna().sum():,}")
print(f"   categorías sin traducción: {sorted(set(p['product_category_name'].dropna()) - set(dfs['category_translation']['product_category_name']))}")
oi = dfs["order_items"]
pago_total = dfs["order_payments"].assign(v=lambda d: d["payment_value"].astype(float)).groupby("order_id")["v"].sum()
item_total = oi.assign(v=oi["price"].astype(float) + oi["freight_value"].astype(float)).groupby("order_id")["v"].sum()
dif = (pago_total - item_total).dropna().abs()
print(f"   pedidos donde pagos != items+flete (>1 BRL): {(dif > 1).sum():,}")
ent = pd.to_datetime(o["order_delivered_customer_date"], errors="coerce")
est = pd.to_datetime(o["order_estimated_delivery_date"], errors="coerce")
print(f"   entregas tardías (entrega > estimada): {(ent > est).sum():,} de {ent.notna().sum():,}")
print(f"   entregados sin fecha de entrega: {((o['order_status'] == 'delivered') & ent.isna()).sum():,}")
