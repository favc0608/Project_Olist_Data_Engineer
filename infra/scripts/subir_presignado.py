"""Sube archivos locales con las URLs prefirmadas que devuelve get_presigned_url del AWS MCP.

Uso: python infra/subir_presignado.py <respuesta.json> clave=ruta_local [clave=ruta_local ...]
La respuesta puede traer claves repetidas: se usa la primera URL de cada clave.
"""
import json
import sys
import urllib.request


def main():
    respuesta = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
    urls = {}
    for r in respuesta:
        urls.setdefault(r["key"], r["url"])
    for par in sys.argv[2:]:
        clave, ruta = par.rsplit("=", 1)  # las claves de landing llevan "=" (year=AAAA/month=MM); la ruta local no
        req = urllib.request.Request(urls[clave], data=open(ruta, "rb").read(), method="PUT")
        with urllib.request.urlopen(req, timeout=60) as r:
            print(r.status, clave)


if __name__ == "__main__":
    main()
