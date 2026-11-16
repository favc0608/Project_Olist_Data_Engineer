"""Exporta al repositorio público del Colab (Scraping_Competencia) la lista de productos por tienda.

Solo datos del catálogo sintético (nada de la cuenta AWS). Uso:
  .venv\\Scripts\\python.exe competencia\\herramientas\\exportar_colab.py scraping_competencia\\olist_competencia\\datos
"""
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
NOMBRES = {"mercadoabierto": "Mercado Abierto", "selva": "Selva", "compree": "Compree", "ridley": "Ridley"}


def main():
    destino = Path(sys.argv[1])
    destino.mkdir(parents=True, exist_ok=True)
    catalogo = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))
    surtido = json.loads((RAIZ / "datos" / "surtido.json").read_text(encoding="utf-8"))
    cats = {c["slug"]: c["nombre"] for c in catalogo["categorias"]}
    datos = {
        "version_catalogo": catalogo["generado"],
        "productos": [{"sku": p["sku"], "nombre": p["nombre"], "marca": p["marca"], "categoria": cats[p["categoria"]],
                       "precio_olist": p["precio_olist"]} for p in catalogo["productos"]],
        "tiendas": {t: {"nombre": NOMBRES[t],
                        "productos": {s["sku"]: s["sku_loja"] for s in d["productos"] if not s["descontinuado"]}}
                    for t, d in surtido.items()},
    }
    (destino / "catalogo.json").write_text(json.dumps(datos, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{len(datos['productos'])} productos y {len(datos['tiendas'])} tiendas -> {destino / 'catalogo.json'}")


if __name__ == "__main__":
    main()
