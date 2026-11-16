"""Empaqueta las Lambdas (fuente WEB y consumidora del ERP) en zips con nombre por contenido (cambia el código -> cambia la clave S3).

Uso: .venv\\Scripts\\python.exe infra/scripts/empaquetar_lambda.py <carpeta_salida>
Imprime las claves para subir a s3://trabajo-integrador-g1/artefactos/lambda/ y pasar a los stacks g1-olist-web
(CodigoScraperKey, CodigoPublicadorKey) g1-olist-streaming (CodigoConsumidorKey) y g1-olist-orquestacion (CodigoOrquestadorKey).
"""
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
COMP = RAIZ / "competencia"


def archivos_scraper():
    yield RAIZ / "lambda" / "web_scraper" / "handler.py", "handler.py"
    for n in ("common.py", "extractor.py", "reglas.py"):
        yield COMP / "scrapers" / n, n


def archivos_consumidor():
    for n in ("handler.py", "erp_csv.py"):
        yield RAIZ / "lambda" / "consumidor" / n, n


def archivos_orquestador():
    yield RAIZ / "lambda" / "orquestador" / "handler.py", "handler.py"


def archivos_publicador():
    yield RAIZ / "lambda" / "web_publicador" / "handler.py", "handler.py"
    for n in ("precios.py", "sitio.py", "construir.py", "hub.html"):
        yield COMP / "generador" / n, f"competencia/generador/{n}"
    yield COMP / "tests" / "validar_sitio.py", "competencia/tests/validar_sitio.py"
    for n in ("catalogo.json", "surtido.json"):
        yield COMP / "datos" / n, f"competencia/datos/{n}"
    for f in sorted((COMP / "plantillas").rglob("*")):
        if f.is_file() and f.suffix in (".html", ".css", ".svg"):
            yield f, "competencia/" + f.relative_to(COMP).as_posix()


def zip_determinista(archivos) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for origen, destino in archivos:
            info = zipfile.ZipInfo(destino, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            z.writestr(info, origen.read_bytes())
    return buf.getvalue()


def main():
    salida = Path(sys.argv[1])
    salida.mkdir(parents=True, exist_ok=True)
    claves = {}
    for nombre, archivos in (("web-scraper", archivos_scraper()), ("web-publicador", archivos_publicador()),
                             ("consumidor", archivos_consumidor()),
                             ("orquestador", archivos_orquestador())):
        datos = zip_determinista(list(archivos))
        archivo = f"{nombre}-{hashlib.sha256(datos).hexdigest()[:10]}.zip"
        (salida / archivo).write_bytes(datos)
        claves[nombre] = {"archivo": str(salida / archivo), "clave": f"artefactos/lambda/{archivo}", "bytes": len(datos)}
    print(json.dumps(claves, indent=1))


if __name__ == "__main__":
    main()
