"""Genera el sitio de las tiendas a partir de las plantillas y de la verdad del día.

Solo librería estándar: lo usan construir.py (local) y la Lambda publicadora.
Contrato de variables: plantillas/CONTRATO.md.
"""
import hashlib
import json
import math
from pathlib import Path
from string import Template

try:
    from . import precios as P
except ImportError:
    import precios as P

TIENDAS = {"mercadoabierto": "Mercado Abierto", "selva": "Selva", "compree": "Compree", "ridley": "Ridley"}
POR_PAGINA = 12
REGIONES = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste", "SE": "Sudeste", "S": "Sul"}
PAGINAS = ["layout", "portada", "busca", "categoria", "produto", "ofertas", "frete", "vendedores", "404"]
FRAGMENTOS = ["tarjeta", "categoria_item", "pagina_item", "fila_frete", "fila_regiao", "fila_spec", "resena",
              "fila_vendedor"]
FRAGMENTOS_EXTRA = {"mercadoabierto": ["otro_vendedor"], "compree": ["variante"]}
TEXTO_STOCK = {  # (disponible, pocas unidades, agotado)
    "mercadoabierto": ("Disponível", "Últimas {n} unidades", "Esgotado"),
    "selva": ("Em estoque", "Restam apenas {n}", "Indisponível"),
    "compree": ("Em estoque", "Últimas {n} unidades", "Esgotado"),
    "ridley": ("Disponível para entrega", "Últimas {n} unidades", "Esgotado temporariamente"),
}
AVISO = ('<div class="g1-aviso" style="background:#111;color:#fff;font:12px/1.6 Arial,sans-serif;'
         'text-align:center;padding:4px 8px;">Site simulado para fins acadêmicos - DMC Grupo 1. '
         'Loja fictícia: marca, produtos e preços não são reais.</div>')
SVG_PROVISIONAL = ('<svg viewBox="0 0 120 120" width="120" height="120" role="img" aria-label="$nome">'
                   '<rect x="10" y="10" width="100" height="100" rx="14" fill="none" stroke="currentColor" '
                   'stroke-width="6"/><text x="60" y="68" text-anchor="middle" font-size="16" '
                   'fill="currentColor">$nome</text></svg>')


ESTADOS = {uf: nombre for uf, nombre, _ in P.UF}


def h(*partes) -> int:
    return int(hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest(), 16)


# ---------- 1. FORMATOS ----------
def miles(entero: int, sep: str = ".") -> str:
    return f"{entero:,}".replace(",", sep)


def brl(v: float) -> str:
    """1299.9 -> '1.299,90'"""
    entero, cent = divmod(round(v * 100), 100)
    return f"{miles(entero)},{cent:02d}"


def rs(v: float) -> str:
    return f"R$ {brl(v)}"


def rs_entero(v: float) -> str:
    return f"R$ {miles(int(round(v)))}"


def vendidos_texto(n: int) -> str:
    if n >= 1000:
        return f"{n / 1000:.1f}".replace(".", ",").replace(",0", "") + " mil vendidos"
    return f"{n} vendidos"


def prazo_texto(dmin: int, dmax: int) -> str:
    return f"{dmin} a {dmax} dias úteis"


def titulo_tienda(tienda: str, prod: dict) -> str:
    nombre, marca = prod["nombre"], prod["marca"]
    if tienda == "mercadoabierto":
        return f"{nombre} {marca}"
    if tienda == "selva":
        return f"{marca} {nombre} - Envio Internacional"
    if tienda == "compree":
        return f"[Envio Imediato] {nombre} {marca} Promoção"
    return f"{nombre} {marca}".upper()


# ---------- 2. CARGA DE PLANTILLAS ----------
class FaltaPlantilla(Exception):
    pass


def cargar_plantillas(dir_plantillas: Path, tienda: str) -> dict:
    base = dir_plantillas / tienda
    nombres = [f"{p}.html" for p in PAGINAS] + [f"fragmentos/{f}.html" for f in
                                                 FRAGMENTOS + FRAGMENTOS_EXTRA.get(tienda, [])]
    faltan = [n for n in nombres if not (base / n).exists()]
    if not (base / "css" / "styles.css").exists():
        faltan.append("css/styles.css")
    if faltan:
        raise FaltaPlantilla(f"{tienda}: faltan {', '.join(faltan)}")
    return {Path(n).stem: Template((base / n).read_text(encoding="utf-8")) for n in nombres}


def cargar_svgs(dir_plantillas: Path, categorias: list) -> dict:
    svgs = {}
    for c in categorias:
        f = dir_plantillas / "_comun" / "svg" / f"{c['slug']}.svg"
        svgs[c["slug"]] = (f.read_text(encoding="utf-8").strip() if f.exists()
                           else Template(SVG_PROVISIONAL).substitute(nome=c["nombre"].split(",")[0]))
    return svgs


def rellenar(plantilla: Template, nombre: str, variables: dict, errores: list) -> str:
    try:
        return plantilla.substitute(variables)
    except KeyError as e:
        errores.append(f"variable inexistente ${e.args[0]} en {nombre}")
    except ValueError as e:
        errores.append(f"sintaxis inválida en {nombre}: {e} (un $ literal se escribe $$)")
    return ""


# ---------- 3. VARIABLES DE PRODUCTO ----------
def vars_producto(tienda, prod, fila, cat_nombre, raiz, svg, patrocinado=False, titulo_sucio=False):
    titulo = titulo_tienda(tienda, prod)
    if titulo_sucio:  # trampa T4: espacios y saltos de línea en el título
        partes = titulo.split(" ", 2)
        titulo = f"\n        {partes[0]}   {partes[1]}\n   {partes[2] if len(partes) > 2 else ''}  "
    disp, pocas, agot = TEXTO_STOCK[tienda]
    stock_txt = agot if not fila["en_stock"] else (pocas.format(n=fila["stock"]) if fila["stock"] <= 5 else disp)
    p, lista = fila["precio_brl"], fila["precio_lista_brl"]
    v = {
        "url_produto": f"{raiz}produto/{fila['sku_loja']}.html", "sku_loja": fila["sku_loja"], "ean": fila["ean"],
        "data_extra": ' data-sponsored="true"' if patrocinado else "",
        "badge_patrocinado": '<span class="badge-patrocinado">Patrocinado</span>' if patrocinado else "",
        "titulo_produto": titulo, "marca": prod["marca"], "categoria_nome": cat_nombre, "svg": svg,
        "classe_estoque": "" if fila["en_stock"] else " out-of-stock", "texto_estoque": stock_txt,
        "rating": f"{fila['rating']:.1f}".replace(".", ","), "avaliacoes": miles(fila["avaliacoes"]),
        "desconto": str(fila["descuento_pct"]), "frete_gratis": "Frete grátis" if fila["envio_gratis"] else "",
        # mercadoabierto
        "preco_inteiro": miles(int(p)), "preco_centavos": f"{round(p * 100) % 100:02d}",
        "preco_lista_inteiro": miles(int(lista)), "preco_lista_centavos": f"{round(lista * 100) % 100:02d}",
        "parcelas": "", "full": "", "vendedor": fila.get("vendedor", TIENDAS[tienda]),
        # selva
        "asin": fila["sku_loja"], "usd_texto": "", "usd_inteiro": "", "usd_fracao": "", "usd_lista_texto": "",
        "brl_estimado": "", "plus": "",
        # compree
        "preco_html": "", "preco_original": rs(lista), "vendidos": "", "localizacao": "",
        # ridley
        "partnumber": fila["sku_loja"], "preco_de": rs_entero(lista), "preco_por": rs_entero(p),
        "preco_cartao": rs_entero(fila.get("precio_cartao_brl", p)),
    }
    if tienda == "mercadoabierto":
        if fila["cuotas"] > 1:
            v["parcelas"] = f"em {fila['cuotas']}x R$ {brl(fila['valor_cuota'])} sem juros"
        v["full"] = "⚡ FULL" if h("full", fila["sku"]) % 2 == 0 else ""
    elif tienda == "selva":
        usd, usd_l = fila["precio_usd"], fila["precio_lista_usd"]
        entero, cent = divmod(round(usd * 100), 100)
        v.update(usd_texto=f"US${miles(entero, ',')}.{cent:02d}", usd_inteiro=miles(entero, ","),
                 usd_fracao=f"{cent:02d}", usd_lista_texto=f"US${usd_l:,.2f}",
                 brl_estimado=f"aprox. {rs(p)}", plus="✓plus" if fila.get("envio_plus") else "")
    elif tienda == "compree":
        precios = [x["precio_brl"] for x in fila.get("variantes", [])] or [p]
        lo, hi = min(precios), max(precios)
        v["preco_html"] = (f'<span class="currency">R$</span><span class="price-value">{brl(lo)}</span>' +
                           (f' - <span class="currency">R$</span><span class="price-value">{brl(hi)}</span>'
                            if hi > lo else ""))
        v["vendidos"] = vendidos_texto(fila["vendidos"])
        uf = fila.get("uf_vendedor", "SP")
        v["localizacao"] = f"{ESTADOS.get(uf, uf)} - {uf}"
    elif tienda == "ridley":
        v["parcelas"] = f"ou {fila['cuotas']}x de R$ {brl(fila['valor_cuota'])}"
    return v


# ---------- 4. GENERACIÓN DE UNA TIENDA ----------
def tabla_frete(tienda: str) -> list:
    FACTOR_FLETE, DIAS_EXTRA, R, UF, a_90 = P.FACTOR_FLETE, P.DIAS_EXTRA, P.REGIONES, P.UF, P.a_90
    filas = []
    for uf, estado, reg in UF:
        mult, dmin, dmax = R[reg]
        valor = round(a_90(15 * mult * FACTOR_FLETE[tienda] * (1 + (h("uf", tienda, uf) % 9) / 100)), 2)
        extra = DIAS_EXTRA[tienda]
        filas.append({"uf": uf, "estado": estado, "regiao": REGIONES[reg], "valor": valor,
                      "dias_min": max(1, dmin + extra), "dias_max": max(2, dmax + extra)})
    return filas


def generar_tienda(tienda, plantillas, svgs, catalogo, surtido, verdad, base_url, errores) -> dict:
    """Devuelve {ruta_relativa: contenido} de la tienda y completa la verdad con títulos y páginas."""
    productos = {p["sku"]: p for p in catalogo["productos"]}
    cats = {c["slug"]: c["nombre"] for c in catalogo["categorias"]}
    filas = verdad["tiendas"][tienda]
    uf_vendedor = {v["vendedor"]: v["uf"] for v in surtido[tienda]["vendedores"]}
    for f in filas.values():
        if f.get("vendedor") in uf_vendedor:
            f["uf_vendedor"] = uf_vendedor[f["vendedor"]]
    orden = sorted(filas, key=lambda s: h("orden", tienda, s))
    activos = [s for s in orden if not filas[s]["descontinuado"]]
    sucios = set(sorted(orden, key=lambda s: h("sucio", tienda, s))[:4])
    gratis = verdad["frete_gratis_acima"][tienda]
    archivos, urls = {}, []
    comunes = {"rs": "R$", "tienda": TIENDAS[tienda], "version": verdad["version"], "aviso": AVISO,
               "frete_gratis_acima": brl(gratis)}

    def tarjeta(sku, raiz, patrocinado=False):
        prod = productos[sku]
        return rellenar(plantillas["tarjeta"], f"{tienda}/fragmentos/tarjeta.html",
                        {**comunes, "raiz": raiz, **vars_producto(tienda, prod, filas[sku], cats[prod["categoria"]], raiz,
                                                                  svgs[prod["categoria"]], patrocinado, sku in sucios)},
                        errores)

    def menu(raiz):
        return "\n".join(rellenar(plantillas["categoria_item"], f"{tienda}/fragmentos/categoria_item.html",
                                  {**comunes, "raiz": raiz, "url": f"{raiz}categoria/{slug}.html", "nome": nombre,
                                   "svg": svgs[slug], "total": str(sum(productos[s]["categoria"] == slug for s in orden))},
                                  errores) for slug, nombre in cats.items())

    def pagina(ruta, contenido_tpl, variables, titulo, head_extra=""):
        raiz = "../" * ruta.count("/")
        v = {**comunes, "raiz": raiz, **variables}
        conteudo = rellenar(plantillas[contenido_tpl], f"{tienda}/{contenido_tpl}.html", v, errores)
        archivos[f"{tienda}/{ruta}"] = rellenar(plantillas["layout"], f"{tienda}/layout.html", {
            **v, "titulo": f"{titulo} - {TIENDAS[tienda]}", "descricao": f"{titulo} na {TIENDAS[tienda]}.",
            "head_extra": head_extra, "menu_categorias": menu(raiz), "conteudo": conteudo}, errores)
        urls.append(f"{base_url}{tienda}/{ruta}")

    def jsonld(skus):
        if tienda != "compree":
            return ""
        lista = {"@context": "https://schema.org", "@type": "ItemList", "itemListElement": [
            {"@type": "ListItem", "position": i + 1, "item": {
                "@type": "Product", "sku": filas[s]["sku_loja"], "gtin13": filas[s]["ean"],
                "name": titulo_tienda(tienda, productos[s]),
                "offers": {"@type": "Offer", "priceCurrency": "BRL",
                           "price": min([x["precio_brl"] for x in filas[s].get("variantes", [])] or [filas[s]["precio_brl"]]),
                           "availability": "https://schema.org/" + ("InStock" if filas[s]["en_stock"] else "OutOfStock")}}}
            for i, s in enumerate(skus)]}
        return ('<script type="application/ld+json" id="shopee-structured-data">'
                + json.dumps(lista, ensure_ascii=False) + "</script>")

    # Portada
    destaques = activos[:8]
    ofertas = sorted([s for s in activos if filas[s]["descuento_pct"] >= 20], key=lambda s: -filas[s]["descuento_pct"])
    pagina("index.html", "portada", {
        "destaques": "\n".join(tarjeta(s, "") for s in destaques),
        "categorias": menu(""), "ofertas_destaque": "\n".join(tarjeta(s, "") for s in ofertas[:4])}, "Início")

    # Búsqueda: 12 por página + 1 patrocinado (producto de otra página)
    n_pag = math.ceil(len(orden) / POR_PAGINA)
    for i in range(n_pag):
        skus = orden[i * POR_PAGINA:(i + 1) * POR_PAGINA]
        otros = [s for s in activos if s not in skus]
        patrocinado = otros[h("patro", tienda, i) % len(otros)]
        cards = [tarjeta(s, "../") for s in skus]
        cards.insert(h("pos", tienda, i) % 4, tarjeta(patrocinado, "../", patrocinado=True))
        num = i + 1
        rel = "".join([f'<link rel="prev" href="pagina-{num - 1}.html">' if num > 1 else "",
                       f'<link rel="next" href="pagina-{num + 1}.html">' if num < n_pag else ""])
        paginacao = "\n".join(rellenar(plantillas["pagina_item"], f"{tienda}/fragmentos/pagina_item.html",
                                       {**comunes, "raiz": "../", "url": f"pagina-{k}.html", "numero": str(k),
                                        "classe_atual": " is-current" if k == num else ""}, errores)
                              for k in range(1, n_pag + 1))
        pagina(f"busca/pagina-{num}.html", "busca", {
            "tarjetas": "\n".join(cards), "pagina": str(num), "paginas": str(n_pag),
            "desde": str(i * POR_PAGINA + 1), "hasta": str(i * POR_PAGINA + len(skus)), "total": str(len(orden)),
            "paginacao": paginacao, "url_anterior": f"pagina-{num - 1}.html" if num > 1 else "",
            "url_seguinte": f"pagina-{num + 1}.html" if num < n_pag else ""},
            f"Resultados da busca - página {num}", rel + jsonld(skus))

    # Categorías
    for slug, nombre in cats.items():
        skus = [s for s in orden if productos[s]["categoria"] == slug]
        pagina(f"categoria/{slug}.html", "categoria", {
            "categoria_nome": nombre, "categoria_slug": slug, "tarjetas": "\n".join(tarjeta(s, "../") for s in skus),
            "total": str(len(skus)), "svg": svgs[slug]}, nombre, jsonld(skus))

    # Fichas (el descontinuado no se publica: su enlace responde 404, trampa T7)
    for s in activos:
        prod, f = productos[s], filas[s]
        frag = lambda nombre, v: rellenar(plantillas[nombre], f"{tienda}/fragmentos/{nombre}.html", {**comunes, "raiz": "../", **v}, errores)
        medidas = " x ".join(str(m) for m in prod["medidas_cm"] if m) + " cm"
        specs = [("SKU", f["sku_loja"]), ("EAN", f["ean"]), ("Marca", prod["marca"]), ("Categoria", cats[prod["categoria"]]),
                 ("Peso", f"{prod['peso_g']} g" if prod["peso_g"] else "-"), ("Dimensões (C x A x L)", medidas),
                 ("Garantia", "3 meses")]
        relacionados = [x for x in activos if productos[x]["categoria"] == prod["categoria"] and x != s][:4]
        pagina(f"produto/{f['sku_loja']}.html", "produto", {
            **vars_producto(tienda, prod, f, cats[prod["categoria"]], "../", svgs[prod["categoria"]]),
            "specs": "\n".join(frag("fila_spec", {"nome": n, "valor": v}) for n, v in specs),
            "fretes": "\n".join(frag("fila_regiao", {"regiao": REGIONES[r], "valor": "Grátis" if d["valor"] == 0 else rs(d["valor"]),
                                                     "prazo": prazo_texto(d["dias_min"], d["dias_max"])})
                                for r, d in f["fretes"].items()),
            "resenas": "\n".join(frag("resena", {"autor": r["autor"], "nota": str(r["nota"]), "texto": r["texto"],
                                                 "data": "/".join(reversed(r["fecha"].split("-")))}) for r in f["resenas"]),
            "relacionados": "\n".join(tarjeta(x, "../") for x in relacionados),
            "outros_vendedores": "\n".join(frag("otro_vendedor", {"vendedor": o["vendedor"], "uf": "-", "reputacao": "Ouro",
                                                                  "vendas": f"+{100 + h('ov', o['vendedor']) % 900} vendas",
                                                                  "preco": rs(o["precio_brl"])})
                                           for o in f.get("otros_vendedores", [])) if tienda == "mercadoabierto" else "",
            "variantes": "\n".join(frag("variante", {"nome": x["nombre"], "preco": rs(x["precio_brl"])})
                                   for x in f.get("variantes", [])) if tienda == "compree" else "",
            "url_categoria": f"../categoria/{prod['categoria']}.html"}, titulo_tienda(tienda, prod))

    # Ofertas, frete, vendedores, 404
    pagina("ofertas.html", "ofertas", {"tarjetas": "\n".join(tarjeta(s, "") for s in ofertas),
                                       "termina_em": f"{h('reloj', verdad['fecha']) % 6:02d}:{h('min', tienda) % 60:02d}:{h('s', tienda) % 60:02d}"},
           "Ofertas do dia")
    frete = tabla_frete(tienda)
    pagina("frete.html", "frete", {
        "filas_frete": "\n".join(rellenar(plantillas["fila_frete"], f"{tienda}/fragmentos/fila_frete.html",
                                          {**comunes, "raiz": "", "uf": x["uf"], "estado": x["estado"], "regiao": x["regiao"],
                                           "valor": rs(x["valor"]), "prazo": prazo_texto(x["dias_min"], x["dias_max"])}, errores)
                                 for x in frete),
        "cambio_usd_brl": f"{verdad['cambio_usd_brl']:.2f}".replace(".", ",")}, "Frete e prazos")
    vendedores = surtido[tienda]["vendedores"] or [{"vendedor": f"Loja oficial {m}", "uf": "SP"} for m in
                                                    sorted({productos[s]["marca"] for s in orden})]
    pagina("vendedores.html", "vendedores", {"filas_vendedores": "\n".join(
        rellenar(plantillas["fila_vendedor"], f"{tienda}/fragmentos/fila_vendedor.html",
                 {**comunes, "raiz": "", "vendedor": x["vendedor"], "uf": x["uf"],
                  "reputacao": ["Platina", "Ouro", "Prata"][h("rep", x["vendedor"]) % 3],
                  "vendas": f"+{miles(500 + h('vt', x['vendedor']) % 20000)} vendas", "preco": ""}, errores)
        for x in vendedores)}, "Vendedores")
    pagina("404.html", "404", {}, "Página não encontrada")

    archivos[f"{tienda}/css/styles.css"] = None  # se copia tal cual desde plantillas
    archivos[f"{tienda}/sitemap.xml"] = sitemap(urls)

    # Datos para las pruebas: título publicado, página de búsqueda y frete de cada producto
    for i, s in enumerate(orden):
        filas[s]["titulo"] = titulo_tienda(tienda, productos[s])
        filas[s]["titulo_sucio"] = s in sucios
        filas[s]["pagina_busca"] = i // POR_PAGINA + 1
    verdad.setdefault("tabla_frete", {})[tienda] = frete
    verdad.setdefault("paginas", {})[tienda] = sorted(k.split("/", 1)[1] for k in archivos)
    return archivos


def sitemap(urls: list) -> str:
    cuerpo = "\n".join(f"  <url><loc>{u}</loc></url>" for u in urls)
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n{cuerpo}\n</urlset>\n'


ROBOTS = "User-agent: g1-olist-scraper\nAllow: /\n\nUser-agent: *\nDisallow: /\n"
