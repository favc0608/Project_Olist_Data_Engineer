"""Niveles 2 y 3: scrapea el sitio (carpeta local o URL de CloudFront) y lo compara con la verdad del día.

Uso:
  .venv\\Scripts\\python.exe competencia\\tests\\validar_scraping.py --fecha 2026-11-10
  .venv\\Scripts\\python.exe competencia\\tests\\validar_scraping.py --fuente https://<distribucion>.cloudfront.net
Salida: tabla de reglas por tienda, comparativa de precios y código 1 si alguna regla falla.
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scrapers"))
import extractor  # noqa: E402
import reglas  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fecha", default=date.today().isoformat())
    ap.add_argument("--fuente", default=str(RAIZ / "sitio"), help="Carpeta local o URL base")
    ap.add_argument("--tiendas", help="Lista separada por comas (por defecto, las de la verdad)")
    ap.add_argument("--salida", help="Guardar el informe JSON en este archivo")
    args = ap.parse_args()

    verdad = json.loads((RAIZ / "datos" / "precios" / f"{args.fecha}.json").read_text(encoding="utf-8"))
    catalogo = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))
    tiendas = args.tiendas.split(",") if args.tiendas else list(verdad["tiendas"])
    informes = [reglas.evaluar(r, verdad, catalogo) for r in extractor.scrapear(args.fuente, tiendas)]

    print(f"Fuente: {args.fuente} | verdad {verdad['version']}")
    print(f"{'Regla':<6}" + "".join(f"{i['tienda']:>16}" for i in informes) + "  Descripción")
    for k, desc in reglas.REGLAS.items():
        celdas = "".join(f"{'OK' if i['reglas'][k]['ok'] else 'FALLA (' + str(i['reglas'][k]['fallas']) + ')':>16}"
                         for i in informes)
        print(f"{k:<6}{celdas}  {desc}")
    print(f"{'':<6}" + "".join(f"{str(i['productos']) + ' prod / ' + str(i['descargas']) + ' pág':>16}" for i in informes))
    for i in informes:
        for k, r in i["reglas"].items():
            for e in r["ejemplos"]:
                print(f"  [{i['tienda']}] {k}: {e}")

    # Comparativa: la tienda más barata por producto
    por_producto = {}
    for i in informes:
        for c in i["comparativa"]:
            por_producto.setdefault(c["sku_olist"], []).append(c)
    mas_barata = {}
    for filas in por_producto.values():
        mejor = min(filas, key=lambda c: c["precio_competencia"])
        mas_barata[mejor["tienda"]] = mas_barata.get(mejor["tienda"], 0) + 1
    print("Tienda más barata por producto:", ", ".join(f"{t} {n}" for t, n in sorted(mas_barata.items(), key=lambda x: -x[1])))

    if args.salida:
        Path(args.salida).write_text(json.dumps(informes, ensure_ascii=False, indent=1), encoding="utf-8")
    ok = all(i["ok"] for i in informes)
    print("RESULTADO:", "100% OK" if ok else "HAY FALLAS")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
