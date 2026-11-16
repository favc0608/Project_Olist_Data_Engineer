"""Prueba local de punta a punta de las Lambdas web con un S3 falso:
publicador (diario) -> sitio -> scraper (evento) -> cambio desde Colab -> publicador -> scraper ve el cambio.
Usa las plantillas mínimas de competencia/tests/fixtures (no depende del diseño final).
"""
import importlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
COMP = RAIZ / "competencia"


class Cuerpo(io.BytesIO):
    pass


class NoSuchKey(Exception):
    pass


class S3Falso:
    class exceptions:
        NoSuchKey = NoSuchKey

    def __init__(self):
        self.buckets = {}

    def put_object(self, Bucket, Key, Body, **kw):
        import hashlib
        self.buckets.setdefault(Bucket, {})[Key] = (Body if isinstance(Body, bytes) else Body.encode(), kw)

    def get_object(self, Bucket, Key):
        try:
            return {"Body": Cuerpo(self.buckets[Bucket][Key][0])}
        except KeyError:
            raise NoSuchKey(Key)

    def get_paginator(self, _):
        s3 = self

        class P:
            def paginate(self, Bucket):
                import hashlib
                return [{"Contents": [{"Key": k, "ETag": f'"{hashlib.md5(v[0]).hexdigest()}"'}
                                      for k, v in s3.buckets.get(Bucket, {}).items()]}]
        return P()

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.buckets[Bucket].pop(o["Key"], None)

    def volcar(self, bucket, destino: Path):
        for k, (v, _) in self.buckets[bucket].items():
            (destino / k).parent.mkdir(parents=True, exist_ok=True)
            (destino / k).write_bytes(v)


class Nada:
    def __getattr__(self, _):
        return lambda **kw: {"Invalidation": {"Id": "x", "Status": "Completed"}}


class FlujoWeb(unittest.TestCase):
    def setUp(self):
        sys.path[:0] = [str(COMP / "tests"), str(COMP / "scrapers")]
        import crear_plantillas_minimas
        crear_plantillas_minimas.main()
        self.tmp = tempfile.TemporaryDirectory()
        self.sitio_local = Path(self.tmp.name) / "sitio"
        os.environ.update(AWS_DEFAULT_REGION="us-east-2", BUCKET="lake", BUCKET_SITIO="sitio", URL_SITIO=str(self.sitio_local),
                          DISTRIBUCION_ID="E1", RAIZ_COMPETENCIA=str(COMP), PAUSA_SEGUNDOS="0",
                          DIR_PLANTILLAS=str(crear_plantillas_minimas.DESTINO))
        self.s3 = S3Falso()
        sys.path.insert(0, str(RAIZ / "lambda" / "web_publicador"))
        self.pub = importlib.import_module("handler")
        sys.modules.pop("handler")
        sys.path.pop(0)
        sys.path.insert(0, str(RAIZ / "lambda" / "web_scraper"))
        self.scr = importlib.import_module("handler")
        sys.modules.pop("handler")
        self.pub.s3 = self.scr.s3 = self.s3
        self.scr.cloudfront = self.scr.cloudwatch = Nada()

    def tearDown(self):
        self.tmp.cleanup()

    def evento_s3(self, claves):
        cuerpo = {"Records": [{"s3": {"object": {"key": k}}} for k in claves]}
        return {"Records": [{"body": json.dumps(cuerpo), "messageId": "m1"}]}

    def test_flujo_completo(self):
        r = self.pub.lambda_handler({"modo": "diario", "fecha": "2026-11-10"}, None)
        self.assertTrue(r["publicado"])
        self.assertEqual(len(r["tiendas"]), 4)
        self.s3.volcar("sitio", self.sitio_local)

        r = self.scr.lambda_handler(self.evento_s3(["selva/index.html", "ridley/busca/pagina-1.html"]), None)
        self.assertEqual(sorted(r["resumen"]), ["compree", "mercadoabierto", "ridley", "selva"])
        self.assertTrue(r["ok"], r["resumen"])

        # El mismo evento otra vez: la versión ya fue scrapeada, no repite
        r = self.scr.lambda_handler(self.evento_s3(["selva/index.html"]), None)
        self.assertIn("ya scrapeada", r["mensaje"])

        # Cambio desde el Colab: Selva baja OL-0037 a R$ 120
        cambio = {"cambio_id": "c1", "tienda": "selva", "sku": "OL-0037", "accion": "precio", "valor": 120}
        r = self.pub.lambda_handler({"fecha": "2026-11-10", "Records": [{"body": json.dumps(cambio), "messageId": "m2"}]}, None)
        self.assertEqual(r["cambios_del_dia"], 1)
        self.assertGreater(r["subidos"], 0)
        self.s3.volcar("sitio", self.sitio_local)
        r = self.scr.lambda_handler(self.evento_s3(["selva/index.html"]), None)
        self.assertTrue(r["ok"], r["resumen"])
        clave = next(k for k in self.s3.buckets["lake"] if k.startswith("web/landing/precios/") and "selva" in k
                     and r["run_id"] in k)
        import gzip
        filas = [json.loads(l) for l in gzip.decompress(self.s3.buckets["lake"][clave][0]).decode().splitlines()]
        precio = next(f["precio_brl"] for f in filas if f["sku_olist"] == "OL-0037")
        self.assertAlmostEqual(precio, 120, delta=0.1)

        estado = json.loads(self.s3.buckets["sitio"]["_estado/scraping.json"][0])
        self.assertEqual(estado["run_id"], r["run_id"])
        self.assertEqual(estado["reglas_fallidas"]["selva"], [])
        precios = json.loads(self.s3.buckets["sitio"]["_estado/precios.json"][0])
        self.assertEqual(precios["run_id"], r["run_id"])
        self.assertEqual(len(precios["filas"]), 48 + 42 + 46 + 40)
        fila = next(f for f in precios["filas"] if f["tienda"] == "selva" and f["sku_olist"] == "OL-0037")
        self.assertAlmostEqual(fila["precio_brl"], 120, delta=0.1)
        self.assertNotIn("fretes_json", fila)
        pub = json.loads(self.s3.buckets["sitio"]["_estado/publicacion.json"][0])
        self.assertEqual(pub["procesados"][0]["cambio_id"], "c1")

        # Cambio inválido: queda registrado como rechazado y no se aplica
        malo = {"cambio_id": "c2", "tienda": "selva", "sku": "OL-9999", "accion": "precio", "valor": 1}
        r = self.pub.lambda_handler({"fecha": "2026-11-10", "Records": [{"body": json.dumps(malo), "messageId": "m3"}]}, None)
        self.assertEqual((r["rechazados"], r["cambios_del_dia"]), (1, 1))

        # Restaurar todo
        r = self.pub.lambda_handler({"fecha": "2026-11-10", "Records": [{"body": json.dumps({"cambio_id": "c3", "accion": "restaurar"}), "messageId": "m4"}]}, None)
        self.assertEqual(r["cambios_del_dia"], 0)


if __name__ == "__main__":
    unittest.main()
