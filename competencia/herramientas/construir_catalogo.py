"""Construye el catálogo de la competencia a partir de Olist.

Salida:
  datos/catalogo.json  48 productos (los 8 más vendidos de 6 categorías) con EAN sintético y precio Olist.
  datos/surtido.json   qué vende cada tienda, SKU propio, vendedores, variantes, agotados y descontinuado.

Uso (local, necesita pandas):
  .venv\\Scripts\\python.exe competencia\\herramientas\\construir_catalogo.py
"""
import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
OLIST = RAIZ.parent / "data" / "raw" / "olist"
DATOS = RAIZ / "datos"

POR_CATEGORIA = 8
TIENDAS = ["mercadoabierto", "selva", "compree", "ridley"]
COBERTURA = {"mercadoabierto": 48, "selva": 42, "compree": 46, "ridley": 40}
AGOTADOS = {"mercadoabierto": 2, "selva": 3, "compree": 2, "ridley": 3}

# Nombres ordenados de menor a mayor precio típico: se asignan según el precio Olist de cada producto
CATEGORIAS = {
    "cama_mesa_banho": ("cama-mesa-banho", "Cama, Mesa e Banho",
        ["Fronha Avulsa Percal 200 Fios", "Toalha de Rosto Fio Penteado", "Jogo de Toalhas de Banho 2 Peças",
         "Travesseiro Fibra Siliconada", "Jogo de Lençol Solteiro 3 Peças", "Jogo de Lençol Casal 4 Peças 200 Fios",
         "Edredom Casal Dupla Face", "Kit Colcha Matelassê Queen 3 Peças"],
        ["Casa Serena", "Fios do Sul", "Ninho Bom"]),
    "beleza_saude": ("beleza-saude", "Beleza e Saúde",
        ["Sabonete Líquido Facial 150ml", "Máscara Capilar Hidratação 500g", "Kit Shampoo e Condicionador 300ml",
         "Protetor Solar FPS 50 200ml", "Sérum Facial Vitamina C 30ml", "Escova Secadora Alisadora",
         "Balança Digital Corporal", "Prancha Alisadora Titanium 230°C"],
        ["Bella Flor", "Vita Pura", "Derma Sol"]),
    "esporte_lazer": ("esporte-lazer", "Esporte e Lazer",
        ["Squeeze Térmico 750ml", "Corda de Pular com Rolamento", "Tapete de Yoga Antiderrapante",
         "Kit Elásticos de Resistência 5 Níveis", "Luvas de Musculação Acolchoadas", "Halteres Emborrachados 2kg (Par)",
         "Bola de Futebol Campo Oficial", "Mochila Esportiva Impermeável"],
        ["Ritmo Fit", "Arena Pro", "Trilha Viva"]),
    "moveis_decoracao": ("moveis-decoracao", "Móveis e Decoração",
        ["Quadro Decorativo 40x60", "Relógio de Parede Minimalista", "Nicho Organizador MDF",
         "Prateleira Flutuante 60cm", "Espelho Redondo Adnet 50cm", "Luminária de Mesa Articulada",
         "Banqueta Alta Estofada", "Cabideiro de Chão em Madeira"],
        ["Lar Moderno", "Madeira Nobre", "Estilo Casa"]),
    "informatica_acessorios": ("informatica-acessorios", "Informática e Acessórios",
        ["Mousepad Gamer Extra Grande", "Mouse Óptico sem Fio 1600 DPI", "Hub USB 3.0 com 4 Portas",
         "Teclado sem Fio ABNT2", "Suporte Ajustável para Notebook", "Webcam Full HD 1080p",
         "Headset Gamer com Microfone", "Kit Teclado e Mouse Gamer RGB"],
        ["Byte Max", "Conecta+", "Pixel Tech"]),
    "utilidades_domesticas": ("utilidades-domesticas", "Utilidades Domésticas",
        ["Porta-Temperos Giratório 12 Potes", "Kit Potes Herméticos 10 Peças", "Garrafa Térmica 1L",
         "Jogo de Facas Inox 6 Peças", "Escorredor de Louças Inox", "Organizador de Geladeira 6 Peças",
         "Lixeira Inox com Pedal 12L", "Jogo de Panelas Antiaderente 5 Peças"],
        ["Cozinha Feliz", "Inox Lar", "Prática Casa"]),
}

VENDEDORES = {
    "mercadoabierto": [("Loja Oficial Casa Serena", "SP"), ("MegaStore Brasil", "SP"), ("Bazar do Sul", "RS"),
                       ("Ponto Fit", "PR"), ("Tech Mais", "SC"), ("Nordeste Shop", "PE"),
                       ("Minas Utilidades", "MG"), ("Rio Decor", "RJ")],
    "compree": [("compree_oficial", "SP"), ("achadinhos.br", "SP"), ("casa_e_cia", "MG"), ("fit_store", "RJ"),
                ("gamer_point", "PR"), ("bela_vida", "BA"), ("lar_doce", "GO"), ("ofertas_ja", "CE")],
}


def h(*partes) -> int:
    """Entero determinista a partir de un texto (la misma entrada da siempre el mismo valor)."""
    return int(hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest(), 16)


def ean13(semilla: str) -> str:
    base = "789" + str(h("ean", semilla) % 10**9).zfill(9)
    suma = sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(base))
    return base + str((10 - suma % 10) % 10)


def sku_tienda(tienda: str, product_id: str) -> str:
    n = h(tienda, product_id)
    if tienda == "mercadoabierto":
        return f"MAB-{n % 10**10:010d}"
    if tienda == "selva":
        alfabeto = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        return "B0" + "".join(alfabeto[(n >> (5 * i)) % len(alfabeto)] for i in range(8))
    if tienda == "compree":
        return f"CP-{n % 10**11:011d}"
    return f"RDL-2004{n % 10**4:04d}"


def construir_catalogo():
    items = pd.read_csv(OLIST / "olist_order_items_dataset.csv")
    productos = pd.read_csv(OLIST / "olist_products_dataset.csv")
    m = items.merge(productos[["product_id", "product_category_name"]], on="product_id")
    m = m[m.product_category_name.isin(CATEGORIAS)]
    resumen = (m.groupby(["product_category_name", "product_id"])
                 .agg(ventas=("price", "size"), precio=("price", "median"), flete=("freight_value", "median"))
                 .reset_index())
    dims = productos.set_index("product_id")

    catalogo, orden = [], 0
    for cat, (slug, nombre_cat, nombres, marcas) in CATEGORIAS.items():
        top = resumen[resumen.product_category_name == cat].nlargest(POR_CATEGORIA, "ventas")
        top = top.sort_values("precio")  # el nombre más barato al producto más barato
        for nombre, fila in zip(nombres, top.itertuples()):
            orden += 1
            d = dims.loc[fila.product_id]
            catalogo.append({
                "sku": f"OL-{orden:04d}",
                "product_id": fila.product_id,
                "ean": ean13(fila.product_id),
                "categoria_olist": cat,
                "categoria": slug,
                "nombre": nombre,
                "marca": marcas[h("marca", fila.product_id) % len(marcas)],
                "precio_olist": round(float(fila.precio), 2),
                "flete_olist": round(float(fila.flete), 2),
                "ventas_olist": int(fila.ventas),
                "peso_g": None if pd.isna(d.product_weight_g) else int(d.product_weight_g),
                "medidas_cm": [None if pd.isna(v) else int(v) for v in
                               (d.product_length_cm, d.product_height_cm, d.product_width_cm)],
            })
    return catalogo


def construir_surtido(catalogo):
    surtido = {}
    for tienda in TIENDAS:
        orden = sorted(catalogo, key=lambda p: h("cobertura", tienda, p["sku"]))
        vendidos = sorted(orden[:COBERTURA[tienda]], key=lambda p: p["sku"])
        por_agotar = sorted(vendidos, key=lambda p: h("agotado", tienda, p["sku"]))
        agotados = {p["sku"] for p in por_agotar[:AGOTADOS[tienda]]}
        descontinuado = por_agotar[AGOTADOS[tienda]]["sku"]  # su ficha no se publica: responde 404
        productos = []
        for p in vendidos:
            fila = {"sku": p["sku"], "sku_loja": sku_tienda(tienda, p["product_id"]),
                    "agotado": p["sku"] in agotados, "descontinuado": p["sku"] == descontinuado}
            if tienda in VENDEDORES:
                vend = VENDEDORES[tienda]
                fila["vendedor"] = vend[h("vend", tienda, p["sku"]) % len(vend)][0]
            if tienda == "mercadoabierto":
                otros = h("otros", p["sku"]) % 4
                fila["otros_vendedores"] = [
                    {"vendedor": VENDEDORES[tienda][(h("ov", p["sku"], i)) % 8][0],
                     "recargo": round(0.02 + (h("rec", p["sku"], i) % 15) / 100, 2)}
                    for i in range(otros)]
            if tienda == "compree" and h("var", p["sku"]) % 10 < 3:
                n = 2 + h("nvar", p["sku"]) % 2
                fila["variantes"] = [{"nombre": v, "recargo": round(i * (0.15 + (h("vr", p["sku"]) % 20) / 100), 2)}
                                     for i, v in enumerate(["Padrão", "Grande", "Premium"][:n])]
            productos.append(fila)
        surtido[tienda] = {"productos": productos,
                           "vendedores": [{"vendedor": v, "uf": uf} for v, uf in VENDEDORES.get(tienda, [])]}
    return surtido


def main():
    DATOS.mkdir(exist_ok=True)
    catalogo = construir_catalogo()
    surtido = construir_surtido(catalogo)
    meta = {"version": 3, "generado": date.today().isoformat(), "fuente": "Olist (Kaggle)",
            "categorias": [{"slug": s, "nombre": n, "olist": c} for c, (s, n, _, _) in CATEGORIAS.items()]}
    (DATOS / "catalogo.json").write_text(json.dumps({**meta, "productos": catalogo}, ensure_ascii=False, indent=1),
                                         encoding="utf-8")
    (DATOS / "surtido.json").write_text(json.dumps(surtido, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Catálogo: {len(catalogo)} productos en {len(CATEGORIAS)} categorías")
    for t, s in surtido.items():
        prods = s["productos"]
        print(f"  {t:<15} vende {len(prods)} | agotados {sum(p['agotado'] for p in prods)} | "
              f"descontinuado {sum(p['descontinuado'] for p in prods)}")


if __name__ == "__main__":
    main()
