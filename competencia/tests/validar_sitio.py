"""Nivel 1 - Contrato del sitio generado (v3). Revisa sitio/ contra la verdad del día, sin scrapear precios.

Uso: .venv\\Scripts\\python.exe competencia\\tests\\validar_sitio.py [--fecha AAAA-MM-DD]
(construir.py lo ejecuta automáticamente al final).
"""
import argparse
import json
import re
import sys
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from posixpath import normpath

RAIZ = Path(__file__).resolve().parents[1]
MARCAS_REALES = re.compile(r"mercado\s*libre|mercadolibre|mercado\s*livre|amazon(?!as|ia|ía|\s*(s3|cloudfront|web services))"
                           r"|\bprime\b|shopee|ripley", re.I)
VARIABLE_SIN_RELLENAR = re.compile(r"\$\{?[a-z_]{3,}")
SELECTORES = {  # clases que deben existir en las páginas de búsqueda (sección 6 de AGENTS.md)
    "mercadoabierto": ["ui-search-layout__item", "ui-search-item__title", "andes-money-amount__fraction",
                       "andes-money-amount__cents", "ui-search-installments"],
    "selva": ["s-result-item", "a-price", "a-offscreen", "a-price-whole", "a-price-fraction", "fx-estimate", "a-icon-alt"],
    "compree": ["shopee-search-item-result__item", "shopee-item-card__current-price", "price-value",
                "shopee-item-card__sold-count", "shopee-item-card__location"],
    "ridley": ["catalog-product-item", "catalog-prices__list-price", "catalog-prices__offer-price",
               "catalog-prices__card-price", "price-value"],
}


class Pagina(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags, self.texto, self.clases, self.scripts = [], [], set(), []
        self._omitir = 0
        self._script = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        self.tags.append((tag, a))
        self.clases.update((a.get("class") or "").split())
        if tag in ("style", "script"):
            self._omitir += 1
            if tag == "script":
                self._script = {"attrs": a, "texto": ""}

    def handle_endtag(self, tag):
        if tag in ("style", "script") and self._omitir:
            self._omitir -= 1
            if tag == "script" and self._script is not None:
                self.scripts.append(self._script)
                self._script = None

    def handle_data(self, data):
        if self._script is not None:
            self._script["texto"] += data
        elif not self._omitir:
            self.texto.append(data)

    def meta(self, nombre):
        return next((a.get("content") for t, a in self.tags if t == "meta" and a.get("name") == nombre), None)


def leer(ruta: Path) -> Pagina:
    p = Pagina()
    p.feed(ruta.read_text(encoding="utf-8"))
    return p


def validar(dir_sitio: Path, verdad: dict, tiendas: list) -> list:
    errores = []
    for tienda in tiendas:
        filas = verdad["tiendas"][tienda]
        skus_loja = {f["sku_loja"] for f in filas.values()}
        descontinuado = next(f["sku_loja"] for f in filas.values() if f["descontinuado"])
        faltantes_permitidos = {f"{tienda}/produto/{descontinuado}.html"}
        paginas = verdad["paginas"][tienda]
        rotos = set()
        for rel in paginas:
            ruta = dir_sitio / tienda / rel
            if not ruta.exists():
                errores.append(f"[{tienda}] falta {rel}")
                continue
            if not rel.endswith(".html"):
                continue
            crudo = ruta.read_text(encoding="utf-8")
            p = leer(ruta)
            donde = f"[{tienda}/{rel}]"
            if p.meta("g1-version") != verdad["version"]:
                errores.append(f"{donde} g1-version = {p.meta('g1-version')!r}; esperado {verdad['version']!r}")
            if "noindex" not in (p.meta("robots") or ""):
                errores.append(f"{donde} falta <meta name=robots content=noindex>")
            if 'class="g1-aviso"' not in crudo:
                errores.append(f"{donde} falta el aviso académico ($aviso)")
            if not re.search(r'<html[^>]+lang="pt-BR"', crudo):
                errores.append(f"{donde} falta lang=\"pt-BR\"")
            if m := VARIABLE_SIN_RELLENAR.search(crudo.replace("R$", "")):
                errores.append(f"{donde} texto de variable sin rellenar: {m.group(0)}")
            for s in p.scripts:
                if s["attrs"].get("type") != "application/ld+json":
                    errores.append(f"{donde} JavaScript no permitido")
            texto = " ".join(p.texto) + " " + " ".join(a.get(k, "") for _, a in p.tags for k in ("title", "alt", "aria-label", "placeholder"))
            if m := MARCAS_REALES.search(texto):
                errores.append(f"{donde} marca real en texto visible: {m.group(0)!r}")
            for tag, a in p.tags:
                for attr in ("href", "src"):
                    url = a.get(attr)
                    if not url or url.startswith(("#", "mailto:", "tel:")):
                        continue
                    if url.startswith(("http://", "https://", "//", "/")):
                        errores.append(f"{donde} ruta no relativa o externa: {url}")
                        continue
                    destino = normpath(f"{tienda}/{Path(rel).parent.as_posix()}/{url.split('#')[0]}")
                    if not (dir_sitio / destino).exists():
                        if destino in faltantes_permitidos:
                            rotos.add(destino)
                        else:
                            errores.append(f"{donde} enlace roto: {url}")
            # tarjetas y fichas
            tarjetas = [a for t, a in p.tags if "data-sku" in a]
            for a in tarjetas:
                if a["data-sku"] not in skus_loja:
                    errores.append(f"{donde} data-sku desconocido {a['data-sku']}")
                if not a.get("data-ean"):
                    errores.append(f"{donde} tarjeta sin data-ean ({a['data-sku']})")
            if rel.startswith("busca/"):
                patro = sum(a.get("data-sponsored") == "true" for a in tarjetas)
                if patro != 1:
                    errores.append(f"{donde} debe tener 1 patrocinado; tiene {patro}")
                n = int(re.search(r"pagina-(\d+)", rel).group(1))
                esperados = sum(f["pagina_busca"] == n for f in filas.values())
                if len(tarjetas) - patro != esperados:
                    errores.append(f"{donde} {len(tarjetas) - patro} tarjetas; esperado {esperados}")
                faltan = [c for c in SELECTORES[tienda] if c not in p.clases]
                if tienda == "mercadoabierto" and all(filas[s]["cuotas"] == 1 for s in filas if filas[s]["pagina_busca"] == n):
                    faltan = [c for c in faltan if c != "ui-search-installments"]
                if faltan:
                    errores.append(f"{donde} faltan selectores: {', '.join(faltan)}")
                if tienda == "compree" and not any(s["attrs"].get("id") == "shopee-structured-data" for s in p.scripts):
                    errores.append(f"{donde} falta JSON-LD #shopee-structured-data")
            if rel.startswith("produto/"):
                if not any(t == "table" for t, _ in p.tags):
                    errores.append(f"{donde} la ficha no tiene tabla técnica <table>")
                if "g1-fretes" not in p.clases:
                    errores.append(f"{donde} falta <table class=\"g1-fretes\"> alrededor de $fretes (CONTRATO 4.1)")
                if not tarjetas or tarjetas[0]["data-sku"] != rel.split("/")[1][:-5]:
                    errores.append(f"{donde} el primer elemento con data-sku debe ser el producto de la ficha")
        if not rotos:
            errores.append(f"[{tienda}] ninguna tarjeta enlaza al producto descontinuado (trampa T7)")
    return errores


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fecha", default=date.today().isoformat())
    ap.add_argument("--sitio", default=str(RAIZ / "sitio"))
    args = ap.parse_args()
    verdad = json.loads((RAIZ / "datos" / "precios" / f"{args.fecha}.json").read_text(encoding="utf-8"))
    tiendas = [t for t in verdad.get("paginas", {})]
    errores = validar(Path(args.sitio), verdad, tiendas)
    for e in errores:
        print("ERROR", e)
    print(f"Contrato: {len(errores)} errores en {len(tiendas)} tiendas")
    sys.exit(1 if errores else 0)


if __name__ == "__main__":
    main()
