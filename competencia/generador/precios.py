"""Precios del día de cada tienda: la "verdad" que publica el sitio y contra la que se valida el scraping.

Determinista: la misma fecha produce siempre los mismos precios. Solo librería estándar (corre en Lambda).
"""
import hashlib
import json
import math
from datetime import date, timedelta

FACTOR_PRECIO = {"mercadoabierto": 0.98, "selva": 1.04, "compree": 0.91, "ridley": 1.07}
FACTOR_FLETE = {"mercadoabierto": 0.90, "selva": 1.30, "compree": 1.00, "ridley": 1.10}
FRETE_GRATIS_ACIMA = {"mercadoabierto": 79.0, "selva": 99.0, "compree": 59.0, "ridley": 199.0}
DIAS_EXTRA = {"mercadoabierto": -1, "selva": 5, "compree": 2, "ridley": 0}
DESCUENTOS = [0.0, 0.10, 0.15, 0.20, 0.30]
BLACK_FRIDAY = date(2026, 11, 27)

REGIONES = {  # multiplicador del flete y plazo base (días hábiles)
    "SE": (0.80, 2, 4), "S": (1.00, 3, 6), "CO": (1.15, 4, 7), "NE": (1.35, 6, 10), "N": (1.60, 8, 14),
}
UF = [("AC", "Acre", "N"), ("AL", "Alagoas", "NE"), ("AP", "Amapá", "N"), ("AM", "Amazonas", "N"),
      ("BA", "Bahia", "NE"), ("CE", "Ceará", "NE"), ("DF", "Distrito Federal", "CO"),
      ("ES", "Espírito Santo", "SE"), ("GO", "Goiás", "CO"), ("MA", "Maranhão", "NE"),
      ("MT", "Mato Grosso", "CO"), ("MS", "Mato Grosso do Sul", "CO"), ("MG", "Minas Gerais", "SE"),
      ("PA", "Pará", "N"), ("PB", "Paraíba", "NE"), ("PR", "Paraná", "S"), ("PE", "Pernambuco", "NE"),
      ("PI", "Piauí", "NE"), ("RJ", "Rio de Janeiro", "SE"), ("RN", "Rio Grande do Norte", "NE"),
      ("RS", "Rio Grande do Sul", "S"), ("RO", "Rondônia", "N"), ("RR", "Roraima", "N"),
      ("SC", "Santa Catarina", "S"), ("SP", "São Paulo", "SE"), ("SE", "Sergipe", "NE"), ("TO", "Tocantins", "N")]

RESENAS = ["Produto chegou antes do prazo, muito bom.", "Qualidade excelente pelo preço.",
           "Veio bem embalado, recomendo.", "Atendeu minhas expectativas.", "Entrega demorou um pouco, mas o produto é bom.",
           "Material melhor do que eu esperava.", "Comprei de novo, ótimo custo-benefício.",
           "O tamanho é menor do que parece na foto.", "Funciona perfeitamente.", "Chegou com a caixa amassada, mas intacto."]
AUTORES = ["Ana P.", "Carlos M.", "Juliana S.", "Rafael T.", "Fernanda L.", "Bruno C.", "Patrícia R.", "Diego A."]


def h(*partes) -> int:
    return int(hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest(), 16)


def u(*partes) -> float:
    """Número determinista entre -1 y 1."""
    return (h(*partes) % 20001) / 10000 - 1


def a_90(valor: float) -> float:
    """Redondeo comercial a ,90 (39,90; 129,90)."""
    return max(math.floor(valor) + 0.90, 0.90)


def cambio_usd_brl(fecha: date) -> float:
    return round(5.42 + u("cambio", fecha) * 0.05, 4)


def precio_base(tienda, prod, fecha: date) -> float:
    semana = fecha.isocalendar()[1]
    p = prod["precio_olist"] * FACTOR_PRECIO[tienda]
    p *= 1 + 0.05 * u("sem", tienda, prod["sku"], fecha.year, semana) + 0.01 * u("dia", tienda, prod["sku"], fecha)
    if BLACK_FRIDAY - timedelta(days=3) <= fecha <= BLACK_FRIDAY + timedelta(days=3):
        p *= 0.80
    return p


def redondear(tienda, p, cambio):
    """Devuelve (precio_brl, precio_usd) con el redondeo propio de cada tienda."""
    if tienda == "selva":
        usd = math.floor(p / cambio) + 0.99
        return round(usd * cambio, 2), usd
    if tienda == "ridley":
        return float(round(p)), None
    return round(a_90(p), 2), None


def fretes(tienda, prod, precio, envio_plus):
    gratis = precio >= FRETE_GRATIS_ACIMA[tienda] and (tienda != "selva" or envio_plus)
    salida = {}
    for reg, (mult, dmin, dmax) in REGIONES.items():
        valor = 0.0 if gratis else round(a_90(prod["flete_olist"] * mult * FACTOR_FLETE[tienda]), 2)
        extra = DIAS_EXTRA[tienda]
        salida[reg] = {"valor": valor, "dias_min": max(1, dmin + extra), "dias_max": max(2, dmax + extra)}
    return salida, gratis


def calcular(fecha: date, catalogo: dict, surtido: dict) -> dict:
    cambio = cambio_usd_brl(fecha)
    productos = {p["sku"]: p for p in catalogo["productos"]}
    tiendas = {}
    for tienda, datos in surtido.items():
        filas = {}
        semana = fecha.isocalendar()[1]
        for s in datos["productos"]:
            prod = productos[s["sku"]]
            precio, usd = redondear(tienda, precio_base(tienda, prod, fecha), cambio)
            desc = DESCUENTOS[h("desc", tienda, s["sku"], fecha.year, semana) % len(DESCUENTOS)]
            lista_bruta = precio / (1 - desc) if desc else precio
            lista, lista_usd = redondear(tienda, lista_bruta, cambio) if desc else (precio, usd)
            if tienda == "ridley" and desc:
                lista = float(math.ceil(lista_bruta))
            descuento = round(100 * (1 - precio / lista)) if lista > precio else 0
            envio_plus = tienda == "selva" and h("plus", s["sku"]) % 3 != 0
            frete, gratis = fretes(tienda, prod, precio, envio_plus)
            stock = 0 if s["agotado"] else 1 + h("stock", tienda, s["sku"], fecha) % 180
            if not s["agotado"] and h("pocos", tienda, s["sku"], fecha) % 9 == 0:
                stock = 1 + h("n", tienda, s["sku"], fecha) % 5
            n_cuotas = (10 if precio >= 100 else 3 if precio >= 30 else 1) if tienda == "mercadoabierto" else \
                       (6 if precio >= 120 else 3) if tienda == "ridley" else 1
            fila = {
                "sku": s["sku"], "sku_loja": s["sku_loja"], "ean": prod["ean"],
                "precio_brl": precio, "precio_lista_brl": lista, "descuento_pct": descuento,
                "cuotas": n_cuotas, "valor_cuota": round(precio / n_cuotas, 2),
                "en_stock": stock > 0, "stock": stock, "envio_gratis": gratis, "fretes": frete,
                "rating": round(3.8 + (h("rating", tienda, s["sku"]) % 12) / 10, 1),
                "avaliacoes": 20 + h("aval", tienda, s["sku"]) % 3000,
                "descontinuado": s["descontinuado"],
            }
            if tienda == "selva":
                fila.update(precio_usd=usd, precio_lista_usd=lista_usd if desc else usd, envio_plus=envio_plus)
            if tienda == "ridley":
                fila["precio_cartao_brl"] = float(round(precio * 0.93))
            if tienda == "compree":
                fila["vendidos"] = prod["ventas_olist"] * (3 + h("vend", s["sku"]) % 12) + h("vd", s["sku"], fecha) % 40
            if "vendedor" in s:
                fila["vendedor"] = s["vendedor"]
            if s.get("otros_vendedores"):
                fila["otros_vendedores"] = [{"vendedor": o["vendedor"], "precio_brl": round(a_90(precio * (1 + o["recargo"])), 2)}
                                            for o in s["otros_vendedores"]]
            if s.get("variantes"):
                fila["variantes"] = [{"nombre": v["nombre"], "precio_brl": round(a_90(precio * (1 + v["recargo"])), 2)}
                                     if v["recargo"] else {"nombre": v["nombre"], "precio_brl": precio}
                                     for v in s["variantes"]]
            fila["resenas"] = [{"autor": AUTORES[h("au", tienda, s["sku"], i) % len(AUTORES)],
                                "nota": 3 + h("nota", tienda, s["sku"], i) % 3,
                                "fecha": (fecha - timedelta(days=1 + h("fd", tienda, s["sku"], i) % 60)).isoformat(),
                                "texto": RESENAS[h("tx", tienda, s["sku"], i) % len(RESENAS)]} for i in range(3)]
            filas[s["sku"]] = fila
        tiendas[tienda] = filas
    cuerpo = {"fecha": fecha.isoformat(), "cambio_usd_brl": cambio, "frete_gratis_acima": FRETE_GRATIS_ACIMA,
              "tiendas": tiendas}
    huella = hashlib.sha256(json.dumps(cuerpo, sort_keys=True).encode()).hexdigest()[:6]
    return {"version": f"{fecha.isoformat()}T05:00Z-{huella}", **cuerpo}


def aplicar_cambios(verdad: dict, cambios: list) -> dict:
    """Aplica cambios manuales (Colab) sobre la verdad del día: precio, descuento, agotar o reponer."""
    for c in cambios:
        f = verdad["tiendas"][c["tienda"]][c["sku"]]
        if c["accion"] == "precio":
            f["precio_brl"] = float(c["valor"])
        elif c["accion"] == "descuento":
            f["precio_brl"] = round(a_90(f["precio_lista_brl"] * (1 - float(c["valor"]) / 100)), 2)
        elif c["accion"] == "agotar":
            f["stock"], f["en_stock"] = 0, False
        elif c["accion"] == "reponer":
            f["stock"], f["en_stock"] = 50, True
        if c["tienda"] == "selva":  # el sitio muestra USD: el precio en BRL sale del USD redondeado
            f["precio_usd"] = round(f["precio_brl"] / verdad["cambio_usd_brl"], 2)
            f["precio_brl"] = round(f["precio_usd"] * verdad["cambio_usd_brl"], 2)
            f["precio_lista_usd"] = max(f["precio_lista_usd"], f["precio_usd"])
            f["precio_lista_brl"] = round(f["precio_lista_usd"] * verdad["cambio_usd_brl"], 2)
        if c["tienda"] == "ridley":
            f["precio_brl"] = float(round(f["precio_brl"]))
            f["precio_cartao_brl"] = float(round(f["precio_brl"] * 0.93))
        f["precio_lista_brl"] = max(f["precio_lista_brl"], f["precio_brl"])
        f["descuento_pct"] = round(100 * (1 - f["precio_brl"] / f["precio_lista_brl"])) if f["precio_lista_brl"] > f["precio_brl"] else 0
        f["valor_cuota"] = round(f["precio_brl"] / f["cuotas"], 2)
        f["cambio_manual"] = c.get("cambio_id", True)
    huella = hashlib.sha256(json.dumps(verdad["tiendas"], sort_keys=True).encode()).hexdigest()[:6]
    verdad["version"] = verdad["version"].rsplit("-", 1)[0] + "-" + huella
    return verdad
