#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Especializado: SELVA (selva/)
Extrae:
  - Listado Página 1 (index.html) y Página 2 (pagina-2.html)
  - Ficha de Especificaciones Técnicas (producto-detalle.html)
Maneja los retos de Ingeniería de Datos de Selva:
  1. Precios en Dólares (USD) divididos en nodos .a-price-whole + .a-price-fraction
     y conversión a Soles (PEN) con tipo de cambio o lectura de .fx-estimate.
  2. Extracción del identificador propietario data-asin además de data-sku y data-ean.
  3. Detección y filtrado de anuncio patrocinado duplicado (data-sponsored="true").
  4. Identificación de producto sin stock (SKU-TECH-007).
"""

import argparse
from common import (
    ROOT_DIR,
    clean_whitespace,
    fetch_html,
    load_seed,
    parse_html,
    parse_price,
    save_store_output,
)


def scrape_selva_catalog(source_base: str, include_sponsored: bool = False) -> list[dict]:
    seed = load_seed()
    fx_rate = seed.get("configuracion_global", {}).get("tipo_cambio_usd_pen", 3.75)
    records = []

    for page_num, page_file in [(1, "index.html"), (2, "pagina-2.html")]:
        html = fetch_html(source_base, f"selva/{page_file}")
        dom = parse_html(html)

        items = dom.find_all(tag="div", class_name="s-result-item")
        for item in items:
            sku = item.get("data-sku")
            if not sku:
                continue

            is_sponsored = item.get("data-sponsored") == "true"
            if is_sponsored and not include_sponsored:
                continue

            asin = item.get("data-asin")
            ean = item.get("data-ean")

            title_node = item.find_one(tag="h2")
            rating_node = item.find_one(class_name="a-icon-alt")
            fx_node = item.find_one(class_name="fx-estimate")
            stock_node = item.find_one(class_name="s-stock-status")

            # Precio actual en USD y precio anterior tachado
            price_usd = 0.0
            list_price_usd = 0.0
            for price_span in item.find_all(tag="span", class_name="a-price"):
                offscreen = price_span.find_one(class_name="a-offscreen")
                val = parse_price(offscreen.text() if offscreen else "0")
                if "a-text-price" in price_span.classes or price_span.get("data-a-strike") == "true":
                    list_price_usd = val
                elif price_usd == 0.0:
                    price_usd = val

            approx_pen = parse_price(fx_node.text()) if fx_node else round(price_usd * fx_rate, 2)
            stock_text = stock_node.text() if stock_node else "En Stock"
            in_stock = ("agotado" not in stock_text.lower()) and ("sin stock" not in stock_text.lower()) and (
                "out-of-stock" not in item.classes
            )

            rating_val = 0.0
            if rating_node:
                parts = rating_node.text().split()
                if parts:
                    try:
                        rating_val = float(parts[0])
                    except ValueError:
                        rating_val = 0.0

            records.append({
                "store": "selva",
                "page": page_num,
                "sku": sku,
                "ean": ean,
                "asin": asin,
                "title": clean_whitespace(title_node.text(strip=False) if title_node else ""),
                "currency": "USD",
                "price_original": round(price_usd, 2),
                "price_pen": round(price_usd * fx_rate, 2),
                "fx_estimate_pen_dom": round(approx_pen, 2),
                "list_price_pen": round(list_price_usd * fx_rate, 2),
                "rating": rating_val,
                "in_stock": in_stock,
                "stock_status": stock_text,
                "is_sponsored": is_sponsored,
            })

    return records


def scrape_selva_detail(source_base: str) -> dict:
    """Extrae la tabla técnica #productDetails_techSpec_section_1 de producto-detalle.html."""
    html = fetch_html(source_base, "selva/producto-detalle.html")
    dom = parse_html(html)
    specs = {}
    table = dom.find_one(tag="table", attr_filter={"id": "productDetails_techSpec_section_1"})
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
    parser = argparse.ArgumentParser(description="Scraper Selva")
    parser.add_argument("--source", default=str(ROOT_DIR), help="Ruta local o URL del bucket S3")
    parser.add_argument("--include-sponsored", action="store_true", help="Incluir anuncios duplicados")
    args = parser.parse_args()

    records = scrape_selva_catalog(args.source, include_sponsored=args.include_sponsored)
    specs = scrape_selva_detail(args.source)
    json_path, csv_path = save_store_output("selva", records)

    print(f"✅ [Selva Scraper] {len(records)} productos extraídos (USD->PEN) | {len(specs)} specs técnicas en detalle.")
    print(f"   -> JSON: {json_path}")
    print(f"   -> CSV : {csv_path}")


if __name__ == "__main__":
    main()

