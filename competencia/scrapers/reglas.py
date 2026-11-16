"""Reglas de prueba del scraping (docs/PLAN_WEB.md, sección 5): niveles 2 (exactitud) y 3 (emparejamiento).

evaluar() compara lo scrapeado de una tienda con la verdad del día y el catálogo de Olist.
Devuelve un resumen por regla y la comparativa de precios por producto. Solo librería estándar.
"""
from collections import Counter

TOLERANCIA = 0.011
REGLAS = {
    "R1": "Cobertura: están todos los productos del surtido",
    "R2": "Sin duplicados; 1 patrocinado por página detectado y excluido",
    "R3": "Precio actual = verdad (±0,01)",
    "R4": "Precio de lista y descuento = verdad",
    "R5": "Precios especiales (USD, cartão, cuotas, vendidos) = verdad",
    "R6": "Stock = verdad",
    "R7": "Título limpio = título publicado",
    "R8": "Emparejado con Olist por EAN (100%)",
    "R9": "Precio plausible: 50%-150% del precio Olist",
    "R10": "Versión leída = versión publicada",
    "R12": "Consistencia: listado = ficha; DOM = JSON-LD; 404 solo en el descontinuado",
    "R13": "Flete por región en la ficha = verdad",
}


def _igual(a, b, tol=TOLERANCIA):
    return a is not None and b is not None and abs(float(a) - float(b)) <= tol


def evaluar(resultado: dict, verdad: dict, catalogo: dict) -> dict:
    tienda = resultado["tienda"]
    filas = {f["sku_loja"]: f for f in verdad["tiendas"][tienda].values()}
    olist = {p["ean"]: p for p in catalogo["productos"]}
    listados = resultado["listados"]
    reales = [r for r in listados if not r["patrocinado"]]
    fallas = {k: [] for k in REGLAS}

    # R1 / R2
    vistos = {r["sku_loja"] for r in reales}
    for sku in set(filas) - vistos:
        fallas["R1"].append(f"falta {sku}")
    for sku in vistos - set(filas):
        fallas["R1"].append(f"sobra {sku}")
    for sku, n in Counter(r["sku_loja"] for r in reales).items():
        if n > 1:
            fallas["R2"].append(f"{sku} aparece {n} veces")
    patro = Counter(r["pagina"] for r in listados if r["patrocinado"])
    for p in range(1, resultado["paginas_busca"] + 1):
        if patro.get(p, 0) != 1:
            fallas["R2"].append(f"página {p}: {patro.get(p, 0)} patrocinados")

    comparativa = []
    for r in reales:
        v = filas.get(r["sku_loja"])
        if not v:
            continue
        sku = r["sku_loja"]
        if not _igual(r.get("precio_brl"), v["precio_brl"]):
            fallas["R3"].append(f"{sku}: {r.get('precio_brl')} != {v['precio_brl']}")
        if tienda == "selva":
            if not _igual(r.get("precio_usd"), v["precio_usd"]) or not _igual(r.get("precio_lista_usd"), v["precio_lista_usd"]):
                fallas["R5"].append(f"{sku}: USD {r.get('precio_usd')}/{r.get('precio_lista_usd')} != {v['precio_usd']}/{v['precio_lista_usd']}")
        else:
            if not _igual(r.get("precio_lista_brl"), v["precio_lista_brl"]):
                fallas["R4"].append(f"{sku}: lista {r.get('precio_lista_brl')} != {v['precio_lista_brl']}")
        # Selva no muestra el % de descuento: el scraper lo deriva de los precios en USD que sí muestra,
        # así que se compara contra el descuento calculado con los USD de la verdad (no con el de BRL,
        # que por el redondeo a ,99 puede diferir en 1 punto).
        esperado = v["descuento_pct"]
        if tienda == "selva" and v.get("precio_usd") and v.get("precio_lista_usd"):
            esperado = round(100 * (1 - v["precio_usd"] / v["precio_lista_usd"]))
        if r.get("descuento_pct") is not None and r["descuento_pct"] != esperado:
            fallas["R4"].append(f"{sku}: descuento {r['descuento_pct']} != {esperado}")
        if tienda == "ridley" and not _igual(r.get("precio_cartao_brl"), v["precio_cartao_brl"]):
            fallas["R5"].append(f"{sku}: cartão {r.get('precio_cartao_brl')} != {v['precio_cartao_brl']}")
        if tienda in ("mercadoabierto", "ridley") and v["cuotas"] > 1 and r.get("cuotas") != v["cuotas"]:
            fallas["R5"].append(f"{sku}: cuotas {r.get('cuotas')} != {v['cuotas']}")
        if tienda == "compree":
            tol = 50 if v["vendidos"] >= 1000 else 0
            if r.get("vendidos") is None or abs(r["vendidos"] - v["vendidos"]) > tol:
                fallas["R5"].append(f"{sku}: vendidos {r.get('vendidos')} != {v['vendidos']}")
            precios_var = [x["precio_brl"] for x in v.get("variantes", [])]
            if precios_var and not _igual(r.get("precio_max_brl"), max(precios_var)):
                fallas["R5"].append(f"{sku}: máximo de variantes {r.get('precio_max_brl')} != {max(precios_var)}")
            if r.get("precio_jsonld") is not None and not _igual(r["precio_jsonld"], r.get("precio_brl")):
                fallas["R12"].append(f"{sku}: JSON-LD {r['precio_jsonld']} != DOM {r.get('precio_brl')}")
        if r["en_stock"] != v["en_stock"]:
            fallas["R6"].append(f"{sku}: en_stock {r['en_stock']} != {v['en_stock']}")
        if r["titulo"] != " ".join(v["titulo"].split()):
            fallas["R7"].append(f"{sku}: {r['titulo']!r}")
        prod = olist.get(r.get("ean"))
        if not prod:
            fallas["R8"].append(f"{sku}: EAN {r.get('ean')} no está en el catálogo")
            continue
        ratio = (r.get("precio_brl") or 0) / prod["precio_olist"]
        if not 0.5 <= ratio <= 1.5:
            fallas["R9"].append(f"{sku}: {r.get('precio_brl')} vs Olist {prod['precio_olist']} ({ratio:.0%})")
        ficha = resultado["fichas"].get(sku)
        if ficha:
            if not _igual(ficha.get("precio_brl"), r.get("precio_brl")):
                fallas["R12"].append(f"{sku}: ficha {ficha.get('precio_brl')} != listado {r.get('precio_brl')}")
            campo_lista = "precio_lista_usd" if tienda == "selva" else "precio_lista_brl"
            if r.get(campo_lista) is not None and not _igual(ficha.get(campo_lista), r.get(campo_lista)):
                fallas["R12"].append(f"{sku}: lista en ficha {ficha.get(campo_lista)} != listado {r.get(campo_lista)}")
            for reg, d in v["fretes"].items():
                f = ficha.get("fretes", {}).get(reg)
                if not f or not _igual(f["valor"], d["valor"]) or (f["dias_min"], f["dias_max"]) != (d["dias_min"], d["dias_max"]):
                    fallas["R13"].append(f"{sku} {reg}: {f} != {d}")
                    break
        comparativa.append({"tienda": tienda, "sku_loja": sku, "ean": r["ean"], "product_id": prod["product_id"],
                            "sku_olist": prod["sku"], "precio_olist": prod["precio_olist"],
                            "precio_competencia": r.get("precio_brl"), "en_stock": r["en_stock"],
                            "diferencia_pct": round(100 * (r.get("precio_brl", 0) / prod["precio_olist"] - 1), 1)})

    # R10 / R12 (404)
    if resultado.get("version") != verdad["version"]:
        fallas["R10"].append(f"versión {resultado.get('version')} != {verdad['version']}")
    for sku, ficha in resultado["fichas"].items():
        if ficha.get("version") != verdad["version"]:
            fallas["R10"].append(f"ficha {sku}: versión {ficha.get('version')}")
            break
    desc = {s for s, f in filas.items() if f["descontinuado"]}
    no_encontradas = {e.get("sku_loja") for e in resultado["errores"] if e.get("sku_loja")}
    if resultado["fichas"] or no_encontradas:
        if no_encontradas != desc:
            fallas["R12"].append(f"404 en {sorted(no_encontradas)}; esperado {sorted(desc)}")
    otros = [e for e in resultado["errores"] if not e.get("sku_loja")]
    if otros:
        fallas["R1"].append(f"errores de descarga: {otros}")

    resumen = {k: {"descripcion": d, "ok": not fallas[k], "fallas": len(fallas[k]), "ejemplos": fallas[k][:5]}
               for k, d in REGLAS.items()}
    return {"tienda": tienda, "version": resultado.get("version"), "productos": len(reales),
            "patrocinados": sum(r["patrocinado"] for r in listados), "fichas": len(resultado["fichas"]),
            "descargas": resultado["descargas"], "ok": all(x["ok"] for x in resumen.values()),
            "reglas": resumen, "comparativa": comparativa}
