#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincronizador Bidireccional / Propagador desde `catalog_seed.json` hacia las 4 tiendas HTML:
  - mercadoabierto/ (index.html, pagina-2.html, producto-detalle.html)
  - selva/       (index.html, pagina-2.html, producto-detalle.html)
  - compree/       (index.html, pagina-2.html, producto-detalle.html)
  - ridley/       (index.html, pagina-2.html, producto-detalle.html)

¿Cómo funciona?
Cada vez que modifiques un precio, título, descuento o estado de stock en `catalog_seed.json`,
ejecuta:
    python scrapers/sync_seed_to_html.py

El script localiza cada producto por su atributo `data-sku="SKU-..."` en los archivos HTML
de las 4 tiendas y actualiza automáticamente sus precios, descuentos, estados de stock
y bloques JSON-LD manteniendo intactos los diseños CSS y los SVGs.
"""

import json
import re
from pathlib import Path

from common import ROOT_DIR, SEED_PATH


def fmt_meli_int(val: float) -> str:
    """4299.0 -> '4.299'"""
    return f"{int(val):,}".replace(",", ".")


def fmt_meli_cents(val: float) -> str:
    """4299.00 -> '00', 4299.50 -> '50'"""
    cents = int(round((val - int(val)) * 100))
    return f"{cents:02d}"


def fmt_comma(val: float, decimals: int = 2) -> str:
    """4189.0 -> '4,189.00'"""
    if decimals == 0:
        return f"{int(round(val)):,}"
    return f"{val:,.{decimals}f}"


def update_block_by_sku(html: str, tag: str, sku: str, replacer_fn) -> str:
    """Encuentra cada bloque <tag ... data-sku="SKU"...>...</tag> y le aplica replacer_fn."""
    pattern = rf"(<{tag}\b[^>]*\bdata-sku=\"{re.escape(sku)}\"[^>]*>)(.*?)(</{tag}>)"

    def _wrapper(match):
        open_tag, body, close_tag = match.group(1), match.group(2), match.group(3)
        new_open, new_body = replacer_fn(open_tag, body)
        return f"{new_open}{new_body}{close_tag}"

    return re.sub(pattern, _wrapper, html, flags=re.DOTALL)


def sync_mercadoabierto_html(html: str, products: list[dict]) -> str:
    for prod in products:
        sku = prod["sku"]
        m = prod["tiendas"]["mercadoabierto"]
        in_stock = prod["en_stock"]

        def _update(open_tag: str, body: str, m=m, in_stock=in_stock):
            # Actualizar clase out-of-stock en open_tag
            if not in_stock and "out-of-stock" not in open_tag:
                open_tag = open_tag.replace('class="', 'class="out-of-stock ')
            elif in_stock and "out-of-stock" in open_tag:
                open_tag = re.sub(r"\bout-of-stock\s*", "", open_tag)

            # Precio anterior
            body = re.sub(
                r'(<s\s+class="andes-money-amount\s+andes-money-amount--previous">.*?<span\s+class="andes-money-amount__fraction">)(.*?)(</span>)',
                rf"\g<1>{fmt_meli_int(m['precio_lista_pen'])}\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Precio actual (fracción + centavos)
            body = re.sub(
                r'(<div\s+class="ui-search-price__second-line">.*?<span\s+class="andes-money-amount__fraction"[^>]*>)(.*?)(</span>)',
                rf"\g<1>{fmt_meli_int(m['precio_oferta_pen'])}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<span\s+class="andes-money-amount__cents"[^>]*>)(.*?)(</span>)',
                rf"\g<1>{fmt_meli_cents(m['precio_oferta_pen'])}\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Descuento
            body = re.sub(
                r'(<span\s+class="ui-search-price__discount"[^>]*>)(.*?)(</span>)',
                rf"\g<1>{m['descuento_pct']}% OFF\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Estado de stock
            body = re.sub(
                r'(<span\s+class="ui-search-item__stock-status[^"]*"[^>]*>)(.*?)(</span>)',
                rf"\g<1>{m['estado_stock']}\g<3>",
                body,
                flags=re.DOTALL,
            )
            return open_tag, body

        html = update_block_by_sku(html, "li", sku, _update)
    return html


def sync_selva_html(html: str, products: list[dict], fx_rate: float) -> str:
    for prod in products:
        sku = prod["sku"]
        a = prod["tiendas"]["selva"]
        in_stock = prod["en_stock"]
        approx_pen = a.get("precio_aprox_pen", round(a["precio_oferta_usd"] * fx_rate, 2))
        whole_usd = f"{int(a['precio_oferta_usd']):,}"
        frac_usd = fmt_meli_cents(a["precio_oferta_usd"])

        def _update(open_tag: str, body: str, a=a, in_stock=in_stock, approx_pen=approx_pen, whole_usd=whole_usd, frac_usd=frac_usd):
            if not in_stock and "out-of-stock" not in open_tag:
                open_tag = open_tag.replace('class="', 'class="out-of-stock ')
            elif in_stock and "out-of-stock" in open_tag:
                open_tag = re.sub(r"\bout-of-stock\s*", "", open_tag)

            # Precio principal offscreen + whole + fraction
            body = re.sub(
                r'(<span\s+class="a-price"\s+data-a-size="xl"[^>]*>\s*<span\s+class="a-offscreen">)(.*?)(</span>)',
                rf"\g<1>US${fmt_comma(a['precio_oferta_usd'], 2)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<span\s+class="a-price-whole">)(\d[\d,]*)(<span\s+class="a-price-decimal">)',
                rf"\g<1>{whole_usd}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<span\s+class="a-price-fraction">)(\d+)(</span>)',
                rf"\g<1>{frac_usd}\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Precio lista tachado
            body = re.sub(
                r'(<span\s+class="a-price\s+a-text-price"[^>]*>\s*<span\s+class="a-offscreen">)(.*?)(</span>)',
                rf"\g<1>US${fmt_comma(a['precio_lista_usd'], 2)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Estimado PEN
            body = re.sub(
                r'(<span\s+class="fx-estimate">)(.*?)(</span>)',
                rf"\g<1>aprox. S/ {fmt_comma(approx_pen, 2)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            # Stock
            body = re.sub(
                r'(<span\s+class="[^"]*s-stock-status[^"]*"[^>]*>)(.*?)(</span>)',
                rf"\g<1>{a['estado_stock']}\g<3>",
                body,
                flags=re.DOTALL,
            )
            return open_tag, body

        html = update_block_by_sku(html, "div", sku, _update)
    return html


def sync_compree_html(html: str, products_page: list[dict]) -> str:
    # 1. Actualizar JSON-LD en el <head>
    item_list = []
    for idx, prod in enumerate(products_page, start=1):
        s_data = prod["tiendas"]["compree"]
        item_list.append({
            "@type": "ListItem",
            "position": idx,
            "item": {
                "@type": "Product",
                "sku": prod["sku"],
                "gtin13": prod["ean"],
                "name": s_data["titulo"],
                "brand": {"@type": "Brand", "name": prod["marca"]},
                "offers": {
                    "@type": "Offer",
                    "price": f"{s_data['precio_oferta_pen']:.2f}",
                    "priceCurrency": "PEN",
                    "availability": "https://schema.org/InStock" if prod["en_stock"] else "https://schema.org/OutOfStock",
                },
            },
        })

    new_ld = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "Catálogo Compree Flash Sale Perú",
        "numberOfItems": len(item_list),
        "itemListElement": item_list,
    }
    ld_str = json.dumps(new_ld, indent=2, ensure_ascii=False)
    pattern = r'(<script\s+type="application/ld\+json"\s+id="shopee-structured-data">)(.*?)(</script>)'
    html = re.sub(pattern, lambda m: f"{m.group(1)}\n{ld_str}\n  {m.group(3)}", html, flags=re.DOTALL)

    # 2. Actualizar tarjetas en el DOM
    for prod in products_page:
        sku = prod["sku"]
        s = prod["tiendas"]["compree"]
        in_stock = prod["en_stock"]

        def _update(open_tag: str, body: str, s=s, in_stock=in_stock):
            if not in_stock and "out-of-stock" not in open_tag:
                open_tag = open_tag.replace('class="', 'class="out-of-stock ')
            elif in_stock and "out-of-stock" in open_tag:
                open_tag = re.sub(r"\bout-of-stock\s*", "", open_tag)

            body = re.sub(
                r'(<span\s+class="shopee-item-card__original-price">)(.*?)(</span>)',
                rf"\g<1>S/ {fmt_comma(s['precio_lista_pen'], 2)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="shopee-item-card__current-price">.*?<span\s+class="price-value">)(.*?)(</span>)',
                rf"\g<1>{fmt_comma(s['precio_oferta_pen'], 2)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="shopee-item-card__discount-badge">)(.*?)(</div>)',
                rf"\g<1>-{s['descuento_pct']}%\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="shopee-item-card__sold-count">)(.*?)(</div>)',
                rf"\g<1>{s['vendidos_texto']}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<span\s+class="[^"]*shopee-item-card__stock-badge[^"]*">)(.*?)(</span>)',
                rf"\g<1>{s['estado_stock']}\g<3>",
                body,
                flags=re.DOTALL,
            )
            return open_tag, body

        html = update_block_by_sku(html, "div", sku, _update)
    return html


def sync_ridley_html(html: str, products: list[dict]) -> str:
    for prod in products:
        sku = prod["sku"]
        r = prod["tiendas"]["ridley"]
        in_stock = prod["en_stock"]

        def _update(open_tag: str, body: str, r=r, in_stock=in_stock):
            if not in_stock and "out-of-stock" not in open_tag:
                open_tag = open_tag.replace('class="', 'class="out-of-stock ')
            elif in_stock and "out-of-stock" in open_tag:
                open_tag = re.sub(r"\bout-of-stock\s*", "", open_tag)

            body = re.sub(
                r'(<div\s+class="catalog-prices__list-price">.*?<span\s+class="price-value">)(.*?)(</span>)',
                rf"\g<1>S/ {fmt_comma(r['precio_normal_pen'], 0)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="catalog-prices__offer-price">.*?<span\s+class="price-value">)(.*?)(</span>)',
                rf"\g<1>S/ {fmt_comma(r['precio_internet_pen'], 0)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="catalog-prices__card-price">.*?<span\s+class="price-value">)(.*?)(</span>)',
                rf"\g<1>S/ {fmt_comma(r['precio_tarjeta_pen'], 0)}\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<span\s+class="catalog-product-details__discount-tag">)(.*?)(</span>)',
                rf"\g<1>-{r['descuento_pct']}%\g<3>",
                body,
                flags=re.DOTALL,
            )
            body = re.sub(
                r'(<div\s+class="[^"]*catalog-product-details__availability[^"]*">)(.*?)(</div>)',
                rf"\g<1>{r['estado_stock']}\g<3>",
                body,
                flags=re.DOTALL,
            )
            return open_tag, body

        html = update_block_by_sku(html, "article", sku, _update)
    return html


def main():
    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    fx_rate = seed.get("configuracion_global", {}).get("tipo_cambio_usd_pen", 3.75)
    products = seed.get("catalogo_productos", [])

    p1_products = [p for p in products if p.get("pagina") == 1]
    p2_products = [p for p in products if p.get("pagina") == 2]
    ofertas_skus = {"SKU-HOME-008", "SKU-TECH-005", "SKU-TECH-012", "SKU-TECH-003", "SKU-TECH-011", "SKU-TECH-009"}
    tech_skus = {"SKU-TECH-001", "SKU-TECH-002", "SKU-TECH-004", "SKU-TECH-005", "SKU-TECH-006", "SKU-TECH-011", "SKU-TECH-012"}
    gaming_skus = {"SKU-TECH-003", "SKU-TECH-007", "SKU-HOME-008", "SKU-TECH-009", "SKU-HOME-010"}

    views_map = [
        ("index.html", p1_products),
        ("pagina-2.html", p2_products),
        ("ofertas.html", [p for p in products if p["sku"] in ofertas_skus]),
        ("categoria-tecnologia.html", [p for p in products if p["sku"] in tech_skus]),
        ("categoria-gaming.html", [p for p in products if p["sku"] in gaming_skus]),
    ]

    for filename, view_prods in views_map:
        # 1. Mercado Abierto
        meli_path = ROOT_DIR / "mercadoabierto" / filename
        if meli_path.exists():
            meli_path.write_text(sync_mercadoabierto_html(meli_path.read_text(encoding="utf-8"), view_prods), encoding="utf-8")

        # 2. Selva
        amz_path = ROOT_DIR / "selva" / filename
        if amz_path.exists():
            amz_path.write_text(sync_selva_html(amz_path.read_text(encoding="utf-8"), view_prods, fx_rate), encoding="utf-8")

        # 3. Compree
        shp_path = ROOT_DIR / "compree" / filename
        if shp_path.exists():
            shp_path.write_text(sync_compree_html(shp_path.read_text(encoding="utf-8"), view_prods), encoding="utf-8")

        # 4. Ridley
        rip_path = ROOT_DIR / "ridley" / filename
        if rip_path.exists():
            rip_path.write_text(sync_ridley_html(rip_path.read_text(encoding="utf-8"), view_prods), encoding="utf-8")

    print("=" * 76)
    print(f"✅ SINCRONIZACIÓN COMPLETA DESDE '{SEED_PATH.name}' HACIA LAS 4 TIENDAS HTML")
    print("=" * 76)
    print(f"   - Vistas sincronizadas por tienda  : 5 vistas de listado (index, pagina-2, ofertas, tech, gaming)")
    print(f"   - Total productos base en semilla  : {len(products)} SKUs x 4 tiendas")
    print(f"   - Tipo de cambio aplicado (Selva) : 1 USD = S/ {fx_rate:.2f}")
    print("=" * 76)


if __name__ == "__main__":
    main()
