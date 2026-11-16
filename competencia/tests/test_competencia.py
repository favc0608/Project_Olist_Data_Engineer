"""Pruebas del generador, el scraper y las reglas con las plantillas mínimas.

Uso: .venv\\Scripts\\python.exe -m unittest discover -s competencia\\tests -p "test_*.py" -v
"""
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
for carpeta in ("generador", "scrapers", "tests"):
    sys.path.insert(0, str(RAIZ / carpeta))

import construir  # noqa: E402
import crear_plantillas_minimas  # noqa: E402
import extractor  # noqa: E402
import precios  # noqa: E402
import reglas  # noqa: E402
import sitio  # noqa: E402
import validar_sitio  # noqa: E402

FECHA = date(2026, 11, 10)


class Formatos(unittest.TestCase):
    def test_numeros(self):
        self.assertEqual(sitio.brl(1299.9), "1.299,90")
        self.assertEqual(extractor.num_br("R$ 1.299,90"), 1299.9)
        self.assertEqual(extractor.num_br("R$ 149"), 149.0)
        self.assertEqual(extractor.num_us("US$1,129.99"), 1129.99)
        self.assertEqual(extractor.vendidos(sitio.vendidos_texto(1437)), 1400)
        self.assertEqual(extractor.cuotas("em 10x R$ 12,99 sem juros"), (10, 12.99))

    def test_precios_deterministas(self):
        cat = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))
        sur = json.loads((RAIZ / "datos" / "surtido.json").read_text(encoding="utf-8"))
        self.assertEqual(precios.calcular(FECHA, cat, sur), precios.calcular(FECHA, cat, sur))
        self.assertNotEqual(precios.calcular(FECHA, cat, sur)["version"],
                            precios.calcular(date(2026, 11, 11), cat, sur)["version"])


class SitioCompleto(unittest.TestCase):
    """Genera con plantillas mínimas, valida el contrato y scrapea: todo debe quedar al 100%."""

    @classmethod
    def setUpClass(cls):
        crear_plantillas_minimas.main()
        cls.tmp = tempfile.TemporaryDirectory()
        cls.salida = Path(cls.tmp.name) / "sitio"
        cls.verdad, cls.tiendas, _, errores = construir.construir(
            FECHA, list(sitio.TIENDAS), [], construir.URL_SITIO, cls.salida, crear_plantillas_minimas.DESTINO,
            Path(cls.tmp.name))
        assert not errores, errores
        cls.catalogo = json.loads((RAIZ / "datos" / "catalogo.json").read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_contrato(self):
        self.assertEqual(validar_sitio.validar(self.salida, self.verdad, self.tiendas), [])

    def test_scraping_100(self):
        for r in extractor.scrapear(str(self.salida), self.tiendas):
            informe = reglas.evaluar(r, self.verdad, self.catalogo)
            fallas = {k: v["ejemplos"] for k, v in informe["reglas"].items() if not v["ok"]}
            self.assertEqual(fallas, {}, r["tienda"])

    def test_detecta_precio_alterado(self):
        pagina = self.salida / "ridley" / "busca" / "pagina-1.html"
        original = pagina.read_text(encoding="utf-8")
        try:
            pagina.write_text(original.replace('Por:</span> <span class="price-value">R$ ', 'Por:</span> <span class="price-value">R$ 9', 1),
                              encoding="utf-8")
            r = extractor.scrapear(str(self.salida), ["ridley"], con_fichas=False)[0]
            self.assertFalse(reglas.evaluar(r, self.verdad, self.catalogo)["reglas"]["R3"]["ok"])
        finally:
            pagina.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
