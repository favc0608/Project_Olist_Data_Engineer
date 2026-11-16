"""Construye el sitio de la competencia para una fecha y valida el contrato.

Uso:
  .venv\\Scripts\\python.exe competencia\\generador\\construir.py --fecha 2026-11-10
  [--tiendas mercadoabierto,selva] [--cambios cambios.json] [--base-url https://.../]

Salida: competencia/sitio/ (lo que se publica) y competencia/datos/precios/<fecha>.json (la verdad, no se publica).
Las tiendas cuyas plantillas aún no están completas se informan y se omiten.
"""
import argparse
import json
import shutil
import sys
from datetime import date
from pathlib import Path
from string import Template

sys.path.insert(0, str(Path(__file__).resolve().parent))
import precios  # noqa: E402
import sitio  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
URL_SITIO = "https://d1wz0ixjw02i7c.cloudfront.net/"
INSPIRACION = {"mercadoabierto": "Mercado Libre", "selva": "Amazon", "compree": "Shopee", "ridley": "Ripley"}


def construir(fecha: date, tiendas: list, cambios: list, base_url: str, salida: Path, dir_plantillas: Path = None,
              dir_verdad: Path = None):
    catalogo = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))
    surtido = json.loads((RAIZ / "datos" / "surtido.json").read_text(encoding="utf-8"))
    verdad = precios.calcular(fecha, catalogo, surtido)
    if cambios:
        verdad = precios.aplicar_cambios(verdad, cambios)
    dir_plantillas = dir_plantillas or RAIZ / "plantillas"
    svgs = sitio.cargar_svgs(dir_plantillas, catalogo["categorias"])

    archivos, errores, listas, pendientes = {}, [], [], []
    for tienda in tiendas:
        try:
            plantillas = sitio.cargar_plantillas(dir_plantillas, tienda)
        except sitio.FaltaPlantilla as e:
            pendientes.append(str(e))
            continue
        errores_tienda = []
        archivos.update(sitio.generar_tienda(tienda, plantillas, svgs, catalogo, surtido, verdad, base_url, errores_tienda))
        errores += [f"[{tienda}] {e}" for e in dict.fromkeys(errores_tienda)]
        listas.append(tienda)

    if salida.exists():
        shutil.rmtree(salida)
    for rel, contenido in archivos.items():
        destino = salida / rel
        destino.parent.mkdir(parents=True, exist_ok=True)
        if contenido is None:
            shutil.copyfile(dir_plantillas / rel.split("/")[0] / "css" / "styles.css", destino)
        else:
            destino.write_text(contenido, encoding="utf-8")

    hub = Template((Path(__file__).parent / "hub.html").read_text(encoding="utf-8"))
    tarjetas = "\n".join(
        f'<a class="tienda" href="{t}/index.html"><strong>{sitio.TIENDAS[t]}</strong>'
        f'<span>inspirada en {INSPIRACION[t]}</span><em>{len(verdad["tiendas"][t])} productos</em></a>'
        for t in listas) or "<p>Aún no hay tiendas publicadas.</p>"
    (salida / "index.html").write_text(hub.substitute(version=verdad["version"], tiendas=tarjetas,
                                                      fecha=fecha.isoformat()), encoding="utf-8")
    (salida / "404.html").write_text(hub.substitute(version=verdad["version"], fecha=fecha.isoformat(),
                                                    tiendas="<p>Página não encontrada.</p>"), encoding="utf-8")
    (salida / "robots.txt").write_text(sitio.ROBOTS, encoding="utf-8")
    (salida / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "".join(f"  <sitemap><loc>{base_url}{t}/sitemap.xml</loc></sitemap>\n" for t in listas)
        + "</sitemapindex>\n", encoding="utf-8")

    verdad["tiendas"] = {t: verdad["tiendas"][t] for t in listas}
    dir_precios = dir_verdad or RAIZ / "datos" / "precios"
    dir_precios.mkdir(parents=True, exist_ok=True)
    (dir_precios / f"{fecha.isoformat()}.json").write_text(json.dumps(verdad, ensure_ascii=False, indent=1),
                                                          encoding="utf-8")
    return verdad, listas, pendientes, errores


def main():
    ap = argparse.ArgumentParser(description="Genera competencia/sitio/ para una fecha")
    ap.add_argument("--fecha", default=date.today().isoformat())
    ap.add_argument("--tiendas", default=",".join(sitio.TIENDAS))
    ap.add_argument("--cambios", help="JSON con una lista de cambios manuales")
    ap.add_argument("--base-url", default=URL_SITIO)
    ap.add_argument("--salida", default=str(RAIZ / "sitio"))
    ap.add_argument("--plantillas", default=str(RAIZ / "plantillas"))
    args = ap.parse_args()

    cambios = json.loads(Path(args.cambios).read_text(encoding="utf-8")) if args.cambios else []
    salida = Path(args.salida)
    verdad, listas, pendientes, errores = construir(date.fromisoformat(args.fecha), args.tiendas.split(","),
                                                    cambios, args.base_url, salida, Path(args.plantillas))
    print(f"Versión {verdad['version']} | tiendas generadas: {', '.join(listas) or 'ninguna'}")
    for p in pendientes:
        print("PENDIENTE", p)
    if listas and not errores:
        sys.path.insert(0, str(RAIZ / "tests"))
        import validar_sitio
        errores = validar_sitio.validar(salida, verdad, listas)
    for e in errores[:60]:
        print("ERROR", e)
    if len(errores) > 60:
        print(f"... y {len(errores) - 60} errores más")
    paginas = sum(1 for _ in salida.rglob("*.html"))
    print(f"Resultado: {paginas} páginas en {salida} | {len(errores)} errores")
    sys.exit(1 if errores else 0)


if __name__ == "__main__":
    main()
