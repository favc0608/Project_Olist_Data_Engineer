#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Especializado: MERCADO ABIERTO (mercadoabierto/)
Extrae:
  - Listado Página 1 (index.html) y Página 2 (pagina-2.html)
  - Ficha de Especificaciones Técnicas (producto-detalle.html)
Maneja los retos de Ingeniería de Datos de Mercado Abierto:
  1. Separador de miles con punto ('.') y centavos en nodo separado.
  2. Detección y filtrado de anuncio patrocinado duplicado (data-sponsored="true").
  3. Limpieza de espacios extra en títulos (SKU-TECH-004).
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


def scrape_mercadoabierto_catalog(source_base: str, include_sponsored: bool = False) -> list[dict]:
    records = []

    for page_num, page_file in [(1, "index.html"), (2, "pagina-2.html")]:
        html = fetch_html(source_base, f"mercadoabierto/{page_file}")
        dom = parse_html(html)

        items = dom.find_all(tag="li", class_name="ui-search-layout__item")
        for item in items:
            is_sponsored = item.get("data-sponsored") == "true"
            if is_sponsored and not include_sponsored:
                continue

            sku = item.get("data-sku")
            ean = item.get("data-ean")

            brand_node = item.find_one(class_name="ui-search-item__brand-discoverability")
            title_node = item.find_one(tag="h2", class_name="ui-search-item__title")
            prev_price_node = item.find_one(tag="s", class_name="andes-money-amount--previous")
            second_line = item.find_one(class_name="ui-search-price__second-line")
            discount_node = item.find_one(class_name="ui-search-price__discount")
            stock_node = item.find_one(class_name="ui-search-item__stock-status")
            rating_node = item.find_one(class_name="ui-search-reviews__rating-number")
            reviews_node = item.find_one(class_name="ui-search-reviews__amount")

            # Precio actual (entero + centavos)
            price_pen = 0.0
            if second_line:
                frac_node = second_line.find_one(class_name="andes-money-amount__fraction")
                cents_node = second_line.find_one(class_name="andes-money-amount__cents")
                price_pen = parse_price(frac_node.text() if frac_node else "0")
                if cents_node and cents_node.text().isdigit():
                    price_pen += int(cents_node.text()) / 100.0

            # Precio anterior tachado
            list_price_pen = 0.0
            if prev_price_node:
                prev_frac = prev_price_node.find_one(class_name="andes-money-amount__fraction")
                list_price_pen = parse_price(prev_frac.text() if prev_frac else "0")

            stock_text = stock_node.text() if stock_node else "Disponible"
            in_stock = ("agotado" not in stock_text.lower()) and ("out-of-stock" not in item.classes)

            records.append({
                "store": "mercadoabierto",
                "page": page_num,
                "sku": sku,
                "ean": ean,
                "brand": brand_node.text() if brand_node else "",
                "title": clean_whitespace(title_node.text(strip=False) if title_node else ""),
                "currency": "PEN",
                "price_original": round(price_pen, 2),
                "price_pen": round(price_pen, 2),
                "list_price_pen": round(list_price_pen, 2),
                "discount_text": discount_node.text() if discount_node else "",
                "rating": float(rating_node.text()) if rating_node and rating_node.text() else 0.0,
                "reviews_count": int(parse_price(reviews_node.text())) if reviews_node else 0,
                "in_stock": in_stock,
                "stock_status": stock_text,
                "is_sponsored": is_sponsored,
            })

    return records


def scrape_mercadoabierto_detail(source_base: str) -> dict:
    """Extrae la tabla de especificaciones técnicas de producto-detalle.html."""
    html = fetch_html(source_base, "mercadoabierto/producto-detalle.html")
    dom = parse_html(html)
    specs = {}
    table = dom.find_one(tag="table", class_name="andes-table")
    if table:
        for row in table.find_all(tag="tr"):
            th = row.find_one(tag="th")
            td = row.find_one(tag="td")
            if th and td:
                specs[th.text()] = td.text()
    return specs


def main():
    parser = argparse.ArgumentParser(description="Scraper Mercado Abierto")
    parser.add_argument("--source", default=str(ROOT_DIR), help="Ruta local o URL del bucket S3")
    parser.add_argument("--include-sponsored", action="store_true", help="Incluir anuncios duplicados")
    args = parser.parse_args()

    records = scrape_mercadoabierto_catalog(args.source, include_sponsored=args.include_sponsored)
    specs = scrape_mercadoabierto_detail(args.source)
    json_path, csv_path = save_store_output("mercadoabierto", records)

    print(f"✅ [Mercado Abierto Scraper] {len(records)} productos extraídos | {len(specs)} specs técnicas en detalle.")
    print(f"   -> JSON: {json_path}")
    print(f"   -> CSV : {csv_path}")


if __name__ == "__main__":
    main()

