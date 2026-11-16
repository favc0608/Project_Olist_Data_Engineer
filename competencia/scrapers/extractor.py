"""Scraper v3 de las 4 tiendas (contratos DOM de AGENTS.md, sección 6).

No conoce la verdad: solo lee HTML. Recorre la búsqueda siguiendo rel="next", entra a cada ficha y devuelve
registros normalizados (BRL decimal). Solo librería estándar: lo usan la Lambda scraper y las pruebas locales.
"""
import json
import re
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

try:
    from .common import clean_whitespace, parse_html
except ImportError:
    from common import clean_whitespace, parse_html

USER_AGENT = "g1-olist-scraper/3.0 (DMC Grupo 1; fines academicos)"
TIENDAS = ["mercadoabierto", "selva", "compree", "ridley"]
CONTENEDOR = {"mercadoabierto": "ui-search-layout__item", "selva": "s-result-item",
              "compree": "shopee-search-item-result__item", "ridley": "catalog-product-item"}
REGIONES = {"Norte": "N", "Nordeste": "NE", "Centro-Oeste": "CO", "Sudeste": "SE", "Sul": "S"}


# ---------- 1. DESCARGA ----------
class Fuente:
    """Lee páginas desde una carpeta local o desde una URL (CloudFront). Devuelve (estado HTTP, html)."""

    def __init__(self, base: str, pausa: float = 0.0, reintentos: int = 2, guardar: bool = False):
        self.base, self.pausa, self.reintentos = base.rstrip("/") + "/", pausa, reintentos
        self.remota = base.startswith(("http://", "https://"))
        self.descargas = 0
        self.guardar, self.paginas = guardar, []  # (ruta, estado, html) para guardar el HTML crudo en landing

    def leer(self, ruta: str):
        estado, html = self._leer(ruta)
        if self.guardar:
            self.paginas.append((ruta, estado, html))
        return estado, html

    def _leer(self, ruta: str):
        self.descargas += 1
        if not self.remota:
            f = Path(self.base) / ruta
            return (200, f.read_text(encoding="utf-8")) if f.exists() else (404, "")
        for intento in range(self.reintentos + 1):
            try:
                req = Request(self.base + ruta, headers={"User-Agent": USER_AGENT})
                with urlopen(req, timeout=15) as r:
                    html = r.read().decode("utf-8")
                if self.pausa:
                    time.sleep(self.pausa)
                return r.status, html
            except HTTPError as e:
                if e.code in (403, 404):  # S3 privado responde 403 a lo que no existe
                    return 404, ""
                if intento == self.reintentos:
                    return e.code, ""
            except OSError:
                if intento == self.reintentos:
                    return 0, ""
            time.sleep(1 + intento)
        return 0, ""


# ---------- 2. NÚMEROS ----------
def num_br(texto: str):
    """'R$ 1.299,90' -> 1299.9 | 'R$ 149' -> 149.0 | '4,6' -> 4.6"""
    m = re.search(r"\d[\d.]*(,\d+)?", texto or "")
    return float(m.group(0).replace(".", "").replace(",", ".")) if m else None


def num_us(texto: str):
    """'US$1,129.99' -> 1129.99"""
    m = re.search(r"\d[\d,]*(\.\d+)?", texto or "")
    return float(m.group(0).replace(",", "")) if m else None


def entero(texto: str):
    m = re.search(r"\d[\d.]*", texto or "")
    return int(m.group(0).replace(".", "")) if m else None


def vendidos(texto: str):
    """'1,4 mil vendidos' -> 1400 | '850 vendidos' -> 850"""
    t = (texto or "").lower()
    n = num_br(t)
    if n is None:
        return None
    return int(round(n * 1000)) if "mil" in t else int(n)


def cuotas(texto: str):
    """'em 10x R$ 12,99 sem juros' / 'ou 6x de R$ 24,83' -> (10, 12.99)"""
    m = re.search(r"(\d+)\s*x\D*?([\d.]+,\d{2})", texto or "")
    return (int(m.group(1)), num_br(m.group(2))) if m else (1, None)


def txt(nodo, clase=None, tag=None):
    n = nodo.find_one(tag=tag, class_name=clase) if (clase or tag) else nodo
    return n.text() if n else ""


# ---------- 3. TARJETA DE CADA TIENDA ----------
def leer_tarjeta(tienda: str, nodo) -> dict:
    r = {"tienda": tienda, "sku_loja": nodo.get("data-sku"), "ean": nodo.get("data-ean"),
         "patrocinado": nodo.get("data-sponsored") == "true",
         "agotado_clase": "out-of-stock" in nodo.classes}
    enlace = nodo.find_one(tag="a")
    r["url"] = enlace.get("href") if enlace else ""
    if tienda == "mercadoabierto":
        actual = nodo.find_one(class_name="ui-search-price__second-line")
        anterior = nodo.find_one(class_name="andes-money-amount--previous")
        precio = lambda n: (entero(txt(n, "andes-money-amount__fraction")) or 0) + \
            (entero(txt(n, "andes-money-amount__cents")) or 0) / 100 if n else None
        n_c, v_c = cuotas(txt(nodo, "ui-search-installments"))
        r.update(titulo=txt(nodo, "ui-search-item__title"), precio_brl=precio(actual), precio_lista_brl=precio(anterior),
                 descuento_pct=entero(txt(nodo, "ui-search-price__discount")), cuotas=n_c, valor_cuota=v_c,
                 texto_estoque=txt(nodo, "ui-search-item__stock-status"),
                 rating=num_br(txt(nodo, "ui-search-reviews__rating-number")),
                 avaliacoes=entero(txt(nodo, "ui-search-reviews__amount")))
    elif tienda == "selva":
        usd = usd_lista = None
        for p in nodo.find_all(tag="span", class_name="a-price"):
            valor = num_us(txt(p, "a-offscreen"))
            if p.get("data-a-strike") == "true" or "a-text-price" in p.classes:
                usd_lista = valor
            elif usd is None:
                usd = valor
        r.update(titulo=txt(nodo, tag="h2"), precio_usd=usd, precio_lista_usd=usd_lista,
                 precio_brl=num_br(txt(nodo, "fx-estimate")), texto_estoque=txt(nodo, "s-stock-status"),
                 rating=num_br(txt(nodo, "a-icon-alt")), avaliacoes=entero(txt(nodo, "s-underline-text")),
                 plus="plus" in txt(nodo, "s-prime-badge").lower())
    elif tienda == "compree":
        valores = [num_br(n.text()) for n in nodo.find_all(class_name="price-value")
                   if n.parent and "shopee-item-card__current-price" in _ancestros_clases(n)]
        r.update(titulo=txt(nodo, "shopee-item-card__text-name"),
                 precio_brl=min(valores) if valores else None, precio_max_brl=max(valores) if valores else None,
                 precio_lista_brl=num_br(txt(nodo, "shopee-item-card__original-price")),
                 descuento_pct=entero(txt(nodo, "shopee-item-card__discount-badge")),
                 vendidos=vendidos(txt(nodo, "shopee-item-card__sold-count")),
                 localizacao=txt(nodo, "shopee-item-card__location"),
                 rating=num_br(txt(nodo, "shopee-item-card__rating")),
                 texto_estoque=txt(nodo, "shopee-item-card__stock-badge"))
    elif tienda == "ridley":
        n_c, v_c = cuotas(txt(nodo, "catalog-prices__installments"))
        r.update(titulo=txt(nodo, "catalog-product-details__name"),
                 precio_lista_brl=num_br(txt(nodo.find_one(class_name="catalog-prices__list-price") or nodo, "price-value")),
                 precio_brl=num_br(txt(nodo.find_one(class_name="catalog-prices__offer-price") or nodo, "price-value")),
                 precio_cartao_brl=num_br(txt(nodo.find_one(class_name="catalog-prices__card-price") or nodo, "price-value")),
                 descuento_pct=entero(txt(nodo, "catalog-product-details__discount-tag")), cuotas=n_c, valor_cuota=v_c,
                 texto_estoque=txt(nodo, "catalog-product-details__availability"), partnumber=nodo.get("data-partnumber"))
    if tienda == "selva" and r.get("precio_usd") and r.get("precio_lista_usd"):
        r["descuento_pct"] = round(100 * (1 - r["precio_usd"] / r["precio_lista_usd"]))
    texto = (r.get("texto_estoque") or "").lower()
    r["en_stock"] = not (r["agotado_clase"] or any(p in texto for p in ("esgotado", "indisponível", "sem estoque")))
    r["titulo"] = clean_whitespace(r.get("titulo", ""))
    return r


def _ancestros_clases(nodo) -> set:
    clases, n = set(), nodo.parent
    while n is not None:
        clases |= n.classes
        n = n.parent
    return clases


def jsonld(dom) -> dict:
    """Precio por sku desde el bloque JSON-LD (solo Compree)."""
    salida = {}
    for s in dom.find_all(tag="script", attr_filter={"type": "application/ld+json"}):
        try:
            datos = json.loads(s.text(strip=False))
        except ValueError:
            continue
        for el in datos.get("itemListElement", []):
            item = el.get("item", {})
            salida[item.get("sku")] = item.get("offers", {}).get("price")
    return salida


# ---------- 4. RECORRIDO DE UNA TIENDA ----------
def scrapear_tienda(fuente: Fuente, tienda: str, con_fichas: bool = True) -> dict:
    listados, fichas, errores, paginas, version = [], {}, [], 0, None
    ruta, vistas = f"{tienda}/busca/pagina-1.html", set()
    while ruta and ruta not in vistas:  # trampa T10: seguir rel=next sin bucles
        vistas.add(ruta)
        estado, html = fuente.leer(ruta)
        if estado != 200:
            errores.append({"url": ruta, "estado": estado})
            break
        paginas += 1
        dom = parse_html(html)
        version = version or _meta(dom, "g1-version")
        ld = jsonld(dom)
        for nodo in dom.find_all(class_name=CONTENEDOR[tienda]):
            if not nodo.get("data-sku"):
                continue
            r = leer_tarjeta(tienda, nodo)
            r["pagina"] = paginas
            if ld:
                r["precio_jsonld"] = ld.get(r["sku_loja"])
            listados.append(r)
        siguiente = next((n.get("href") for n in dom.find_all(tag="link", attr_filter={"rel": "next"})), None)
        ruta = f"{tienda}/busca/{siguiente}" if siguiente else None

    if con_fichas:
        for r in {x["sku_loja"]: x for x in listados if not x["patrocinado"]}.values():
            ruta_ficha = f"{tienda}/produto/{r['url'].rsplit('/', 1)[-1]}"
            estado, html = fuente.leer(ruta_ficha)
            if estado != 200:  # trampa T7: producto descontinuado
                errores.append({"url": ruta_ficha, "estado": estado, "sku_loja": r["sku_loja"]})
                continue
            fichas[r["sku_loja"]] = leer_ficha(tienda, parse_html(html))
    return {"tienda": tienda, "version": version, "paginas_busca": paginas, "listados": listados,
            "fichas": fichas, "errores": errores, "descargas": fuente.descargas}


def _meta(dom, nombre):
    m = dom.find_one(tag="meta", attr_filter={"name": nombre})
    return m.get("content") if m else None


def leer_ficha(tienda: str, dom) -> dict:
    principal = next((n for n in _todos(dom) if n.get("data-sku")), None)
    ficha = leer_tarjeta(tienda, principal) if principal else {}
    ficha["version"] = _meta(dom, "g1-version")
    ficha["specs"], ficha["fretes"] = {}, {}
    tabla_fretes = dom.find_one(tag="table", class_name="g1-fretes") or dom
    for fila in dom.find_all(tag="tr"):
        celdas = [c for c in fila.children if c.tag in ("th", "td")]
        if len(celdas) == 2 and celdas[0].tag == "th":
            ficha["specs"][celdas[0].text()] = celdas[1].text()
    for fila in tabla_fretes.find_all(tag="tr"):
        celdas = [c for c in fila.children if c.tag in ("th", "td")]
        region = next((r for r in REGIONES if re.fullmatch(rf"(Região\s+)?{r}", celdas[0].text())), None) if celdas else None
        if len(celdas) >= 3 and region:  # "Sudeste", "Região Sudeste"...
            valor, prazo = celdas[1].text(), celdas[2].text()
            dias = [int(x) for x in re.findall(r"\d+", prazo)]
            ficha["fretes"][REGIONES[region]] = {
                "valor": 0.0 if "grát" in valor.lower() else num_br(valor),
                "dias_min": dias[0] if dias else None, "dias_max": dias[-1] if dias else None}
    tabla = dom.find_one(tag="table", class_name="ui-pdp-other-sellers")
    if tabla:
        ficha["otros_vendedores"] = [num_br(f.text()) for f in tabla.find_all(tag="tr") if num_br(f.text())]
    return ficha


def _todos(nodo):
    for c in nodo.children:
        yield c
        yield from _todos(c)


def scrapear(base: str, tiendas=None, con_fichas=True, pausa=0.0) -> list:
    return [scrapear_tienda(Fuente(base, pausa), t, con_fichas) for t in (tiendas or TIENDAS)]
