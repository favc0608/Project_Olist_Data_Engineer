#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Especializado: RIDLEY (ridley/)
Extrae:
  - Listado Página 1 (index.html) y Página 2 (pagina-2.html)
  - Ficha de Especificaciones Técnicas (producto-detalle.html)
Maneja los retos de Ingeniería de Datos de Ridley:
  1. Desglose de Triple Precio por tarjeta:
     - Precio Normal (.catalog-prices__list-price)
     - Precio Internet (.catalog-prices__offer-price) -> usado como precio estándar abierto
     - Precio Tarjeta Ridley (.catalog-prices__card-price) -> precio exclusivo con medio de pago
  2. Extracción de data-partnumber además de data-sku y data-ean.
  3. Detección y filtrado de anuncio patrocinado duplicado (data-sponsored="true").
  4. Identificación de producto sin stock (SKU-TECH-007).
"""

import argparse
from common import (
    ROOT_DIR,
    clean_whitespace,
    fetch_html,
    parse_html,
    parse_price,
    save_store_output,
)


def scrape_ridley_catalog(source_base: str, include_sponsored: bool = False) -> list[dict]:
    records = []

    for page_num, page_file in [(1, "index.html"), (2, "pagina-2.html")]:
        html = fetch_html(source_base, f"ridley/{page_file}")
        dom = parse_html(html)

        items = dom.find_all(tag="article", class_name="catalog-product-item")
        for item in items:
            is_sponsored = item.get("data-sponsored") == "true"
            if is_sponsored and not include_sponsored:
                continue

            sku = item.get("data-sku")
            ean = item.get("data-ean")
            partnumber = item.get("data-partnumber")

            brand_node = item.find_one(class_name="brand-logo")
            title_node = item.find_one(tag="h3", class_name="catalog-product-details__name")
            list_price_node = item.find_one(class_name="catalog-prices__list-price")
            offer_price_node = item.find_one(class_name="catalog-prices__offer-price")
            card_price_node = item.find_one(class_name="catalog-prices__card-price")
            discount_node = item.find_one(class_name="catalog-product-details__discount-tag")
            avail_node = item.find_one(class_name="catalog-product-details__availability")

            def extract_val(price_container):
                if not price_container:
                    return 0.0
                val_span = price_container.find_one(class_name="price-value")
                return parse_price(val_span.text() if val_span else price_container.text())

            normal_pen = extract_val(list_price_node)
            internet_pen = extract_val(offer_price_node)
            card_pen = extract_val(card_price_node)

            stock_text = avail_node.text() if avail_node else "Disponible"
            in_stock = ("agotado" not in stock_text.lower()) and ("out-of-stock" not in item.classes)

            records.append({
                "store": "ridley",
                "page": page_num,
                "sku": sku,
                "ean": ean,
                "partnumber": partnumber,
                "brand": brand_node.text() if brand_node else "",
                "title": clean_whitespace(title_node.text(strip=False) if title_node else ""),
                "currency": "PEN",
                "price_original": round(internet_pen, 2),
                "price_pen": round(internet_pen, 2),
                "card_price_pen": round(card_pen, 2),
                "list_price_pen": round(normal_pen, 2),
                "discount_text": discount_node.text() if discount_node else "",
                "in_stock": in_stock,
                "stock_status": stock_text,
                "is_sponsored": is_sponsored,
            })

    return records


def scrape_ridley_detail(source_base: str) -> dict:
    """Extrae la tabla de especificaciones técnicas de producto-detalle.html."""
    html = fetch_html(source_base, "ridley/producto-detalle.html")
    dom = parse_html(html)
    specs = {}
    table = dom.find_one(tag="table", class_name="ripley-specs-table")
    if not table:
        table = dom.find_one(tag="table")
    if table:
        for row in table.find_all(tag="tr"):
            th = row.find_one(tag="th")
            td = row.find_one(tag="td")
            if th and td:
                specs[th.text()] = td.text()
    return specs


def main():
    parser = argparse.ArgumentParser(description="Scraper Ridley")
    parser.add_argument("--source", default=str(ROOT_DIR), help="Ruta local o URL del bucket S3")
    parser.add_argument("--include-sponsored", action="store_true", help="Incluir anuncios duplicados")
    args = parser.parse_args()

    records = scrape_ridley_catalog(args.source, include_sponsored=args.include_sponsored)
    specs = scrape_ridley_detail(args.source)
    json_path, csv_path = save_store_output("ridley", records)

    print(f"✅ [Ridley Scraper] {len(records)} productos extraídos (Triple Precio) | {len(specs)} specs técnicas en detalle.")
    print(f"   -> JSON: {json_path}")
    print(f"   -> CSV : {csv_path}")


if __name__ == "__main__":
    main()

