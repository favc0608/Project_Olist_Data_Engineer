#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Especializado: COMPREE (compree/)
Extrae:
  - Listado Página 1 (index.html) y Página 2 (pagina-2.html) vía DOM HTML y vía JSON-LD
  - Ficha de Especificaciones Técnicas (producto-detalle.html)
Maneja los retos de Ingeniería de Datos de Compree:
  1. Conversión de contadores abreviados de ventas ("1.4k vendidos" -> 1400).
  2. Extracción dual/híbrida: validación cruzada entre el DOM visual y el bloque
     estructurado <script type="application/ld+json" id="shopee-structured-data">.
  3. Detección y filtrado de anuncio patrocinado duplicado (data-sponsored="true").
  4. Identificación de producto sin stock (SKU-TECH-007).
"""

import argparse
import json
from common import (
    ROOT_DIR,
    clean_whitespace,
    fetch_html,
    parse_html,
    parse_price,
    parse_sold_k,
    save_store_output,
)


def scrape_compree_catalog(source_base: str, include_sponsored: bool = False) -> list[dict]:
    records = []

    for page_num, page_file in [(1, "index.html"), (2, "pagina-2.html")]:
        html = fetch_html(source_base, f"compree/{page_file}")
        dom = parse_html(html)

        # 1. Extraer JSON-LD del <head> para enriquecer/validar
        json_ld_map = {}
        script_node = dom.find_one(tag="script", attr_filter={"id": "shopee-structured-data"})
        if script_node:
            try:
                ld_data = json.loads(script_node.text(strip=False))
                for elem in ld_data.get("itemListElement", []):
                    prod = elem.get("item", elem)
                    if isinstance(prod, dict) and prod.get("sku"):
                        json_ld_map[prod["sku"]] = prod
            except Exception:
                pass

        # 2. Extraer tarjetas del DOM
        items = dom.find_all(tag="div", class_name="shopee-search-item-result__item")
        for item in items:
            is_sponsored = item.get("data-sponsored") == "true"
            if is_sponsored and not include_sponsored:
                continue

            sku = item.get("data-sku")
            ean = item.get("data-ean")

            title_node = item.find_one(class_name="shopee-item-card__text-name")
            curr_price_node = item.find_one(class_name="shopee-item-card__current-price")
            orig_price_node = item.find_one(class_name="shopee-item-card__original-price")
            discount_node = item.find_one(class_name="shopee-item-card__discount-badge")
            sold_node = item.find_one(class_name="shopee-item-card__sold-count")
            rating_node = item.find_one(class_name="shopee-item-card__rating")
            location_node = item.find_one(class_name="shopee-item-card__location")
            stock_node = item.find_one(class_name="shopee-item-card__stock-badge")

            price_pen = parse_price(curr_price_node.text() if curr_price_node else "0")
            list_price_pen = parse_price(orig_price_node.text() if orig_price_node else "0")
            sold_text = sold_node.text() if sold_node else "0 vendidos"
            sold_units = parse_sold_k(sold_text)

            stock_text = stock_node.text() if stock_node else "En Stock"
            in_stock = ("agotado" not in stock_text.lower()) and ("out-of-stock" not in item.classes)

            rating_val = parse_price(rating_node.text()) if rating_node else 0.0

            records.append({
                "store": "compree",
                "page": page_num,
                "sku": sku,
                "ean": ean,
                "title": clean_whitespace(title_node.text(strip=False) if title_node else ""),
                "currency": "PEN",
                "price_original": round(price_pen, 2),
                "price_pen": round(price_pen, 2),
                "list_price_pen": round(list_price_pen, 2),
                "discount_text": discount_node.text() if discount_node else "",
                "sold_text_raw": sold_text,
                "sold_units_parsed": sold_units,
                "rating": rating_val,
                "location": location_node.text() if location_node else "",
                "in_stock": in_stock,
                "stock_status": stock_text,
                "in_json_ld": sku in json_ld_map,
                "is_sponsored": is_sponsored,
            })

    return records


def scrape_compree_detail(source_base: str) -> dict:
    """Extrae la tabla de especificaciones técnicas de producto-detalle.html."""
    html = fetch_html(source_base, "compree/producto-detalle.html")
    dom = parse_html(html)
    specs = {}
    table = dom.find_one(tag="table", class_name="shopee-product-specs-table")
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
    parser = argparse.ArgumentParser(description="Scraper Compree")
    parser.add_argument("--source", default=str(ROOT_DIR), help="Ruta local o URL del bucket S3")
    parser.add_argument("--include-sponsored", action="store_true", help="Incluir anuncios duplicados")
    args = parser.parse_args()

    records = scrape_compree_catalog(args.source, include_sponsored=args.include_sponsored)
    specs = scrape_compree_detail(args.source)
    json_path, csv_path = save_store_output("compree", records)

    print(f"✅ [Compree Scraper] {len(records)} productos extraídos (DOM + JSON-LD) | {len(specs)} specs técnicas en detalle.")
    print(f"   -> JSON: {json_path}")
    print(f"   -> CSV : {csv_path}")


if __name__ == "__main__":
    main()

