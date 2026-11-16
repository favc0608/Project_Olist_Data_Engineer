#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orquestador Maestro de los 4 Scrapers Individuales:
  1. scrape_mercadoabierto.py
  2. scrape_selva.py
  3. scrape_compree.py
  4. scrape_ridley.py

Ejecuta la extracción en cada tienda, guarda los archivos individuales en `scrapers/output/`
y genera la Matriz Consolidada de Inteligencia Competitiva cruzada por `SKU` y `EAN-13`.
"""

import argparse
import csv
import json
from common import OUTPUT_DIR, ROOT_DIR, save_store_output
from scrape_selva import scrape_selva_catalog
from scrape_mercadoabierto import scrape_mercadoabierto_catalog
from scrape_ridley import scrape_ridley_catalog
from scrape_compree import scrape_compree_catalog


def main():
    parser = argparse.ArgumentParser(description="Orquestador ETL Multi-Tienda (Mercado Abierto, Selva, Compree, Ridley)")
    parser.add_argument(
        "--source",
        default=str(ROOT_DIR),
        help="Ruta local a la carpeta COMPETENCIA o URL base del bucket S3 (ej. http://mi-bucket.s3-website-us-east-1.selvaaws.com)",
    )
    args = parser.parse_args()

    print("=" * 84)
    print(" EJECUTANDO SCRAPERS POR TIENDA (MERCADO ABIERTO, SELVA, COMPREE, RIDLEY)")
    print("=" * 84)

    meli_data = scrape_mercadoabierto_catalog(args.source)
    save_store_output("mercadoabierto", meli_data)
    print(f"  [1/4] Mercado Abierto : {len(meli_data)} productos únicos extraídos.")

    selva_data = scrape_selva_catalog(args.source)
    save_store_output("selva", selva_data)
    print(f"  [2/4] Selva        : {len(selva_data)} productos únicos extraídos (convertidos USD -> PEN).")

    compree_data = scrape_compree_catalog(args.source)
    save_store_output("compree", compree_data)
    print(f"  [3/4] Compree        : {len(compree_data)} productos únicos extraídos (DOM + JSON-LD).")

    ridley_data = scrape_ridley_catalog(args.source)
    save_store_output("ridley", ridley_data)
    print(f"  [4/4] Ridley        : {len(ridley_data)} productos únicos extraídos (Precio Internet + Tarjeta).")

    all_records = meli_data + selva_data + compree_data + ridley_data

    # Guardar dataset unificado plano (48 filas)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    unified_csv = OUTPUT_DIR / "all_stores_unified.csv"
    unified_fields = [
        "sku",
        "ean",
        "store",
        "page",
        "title",
        "currency",
        "price_original",
        "price_pen",
        "list_price_pen",
        "in_stock",
        "stock_status",
    ]
    with unified_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=unified_fields, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(all_records, key=lambda r: (r["sku"], r["store"])):
            writer.writerow(row)

    # Construir matriz comparativa por SKU
    matrix = {}
    for r in all_records:
        sku = r["sku"]
        if sku not in matrix:
            matrix[sku] = {
                "sku": sku,
                "ean": r["ean"],
                "in_stock": r["in_stock"],
                "prices_pen": {},
                "ridley_card_price_pen": None,
            }
        matrix[sku]["prices_pen"][r["store"]] = r["price_pen"]
        if r["store"] == "ridley":
            matrix[sku]["ridley_card_price_pen"] = r.get("card_price_pen")

    print("\n" + "=" * 84)
    print(f"{'SKU':<14} | {'MABI (S/)':>10} | {'SELVA (S/)':>11} | {'COMPREE (S/)':>11} | {'RIDLEY (S/)':>11} | {'MEJOR OFERTA EN MERCADO'}")
    print("-" * 84)

    for sku in sorted(matrix.keys()):
        item = matrix[sku]
        p = item["prices_pen"]
        best_store = min(p, key=p.get)
        item["best_store"] = best_store
        item["min_price_pen"] = p[best_store]
        item["max_price_pen"] = max(p.values())
        stock_flag = "" if item["in_stock"] else " [AGOTADO]"
        print(
            f"{sku:<14} | {p.get('mercadoabierto', 0):>10.2f} | {p.get('selva', 0):>11.2f} | "
            f"{p.get('compree', 0):>11.2f} | {p.get('ridley', 0):>11.2f} | "
            f"{best_store.upper()} (S/ {item['min_price_pen']:.2f}){stock_flag}"
        )

    matrix_json = OUTPUT_DIR / "competitive_price_matrix.json"
    matrix_json.write_text(json.dumps(list(matrix.values()), indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 84)
    print(f"📁 Archivos individuales y consolidados guardados en: {OUTPUT_DIR}")
    print("=" * 84)


if __name__ == "__main__":
    main()

