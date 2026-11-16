"""Pruebas locales (sin AWS): Lambda consumidor (contrato v2 + CSV del ERP) y division historico/corte.

Ejecutar desde la raiz:  .venv\\Scripts\\python.exe -m unittest discover -s tests -v
"""
import csv
import gzip
import importlib.util
import io
import json
import os
import sys
import unittest
import uuid
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "scripts"))
import olist_corte  # noqa: E402

sys.path.insert(0, str(RAIZ / "lambda" / "consumidor"))
spec = importlib.util.spec_from_file_location("consumidor", RAIZ / "lambda" / "consumidor" / "handler.py")
consumidor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumidor)

PEDIDO = {"order_id": "o1", "customer_id": "c1", "order_status": "created",
          "order_purchase_timestamp": "2026-11-02 10:00:00", "order_approved_at": None,
          "order_delivered_carrier_date": None, "order_delivered_customer_date": None,
          "order_estimated_delivery_date": "2026-11-20 00:00:00"}
CLIENTE = {"customer_id": "c1", "customer_unique_id": "u1", "customer_zip_code_prefix": "1310",
           "customer_city": "sao paulo", "customer_state": "SP"}


class S3Falso:
    def __init__(self, fallar=False):
        self.objetos, self.fallar = {}, fallar

    def put_object(self, Bucket, Key, Body, ContentType):
        if self.fallar:
            raise ConnectionError("S3 no disponible")
        if Key.endswith(".csv"):
            self.objetos[Key] = list(csv.DictReader(io.StringIO(Body.decode("utf-8"))))
        else:
            self.objetos[Key] = [json.loads(l) for l in gzip.decompress(Body).decode("utf-8").splitlines()]

    def csv(self, carpeta):
        return {k: v for k, v in self.objetos.items() if k.endswith(".csv") and f"/{carpeta}/" in k}


def evento(tipo="pedido_aprobado", ts="2026-11-02T11:00:00Z", **payload):
    if not payload:
        payload = {"order_id": "o1", "order_status": "approved", "fecha": "2026-11-02 11:00:00",
                   "pedido": dict(PEDIDO, order_status="approved", order_approved_at="2026-11-02 11:00:00")}
    return {"evento_id": str(uuid.uuid4()), "tipo": tipo, "ts_evento": ts, "version": 2, "payload": payload}


def creado():
    return evento("pedido_creado", ts="2026-11-02T10:00:00Z", pedido=dict(PEDIDO), cliente=dict(CLIENTE),
                  items=[{"order_item_id": 1, "shipping_limit_date": "2026-11-05 10:00:00", "price": 58.9,
                          "freight_value": 12.0, "producto": {"product_id": "p1"}, "vendedor": {"seller_id": "s1"}}],
                  pagos=[{"payment_sequential": 1, "payment_type": "credit_card", "payment_installments": 3,
                          "payment_value": 70.9}])


def registros(*cuerpos):
    return {"Records": [
        {"messageId": f"m{i}", "attributes": {"SentTimestamp": "1700000000000"},
         "body": c if isinstance(c, str) else json.dumps(c)}
        for i, c in enumerate(cuerpos)]}


class PruebasConsumidor(unittest.TestCase):

    def setUp(self):
        os.environ.update(BUCKET="b", PREFIJO_EVENTOS="erp/landing/eventos/",
                          PREFIJO_RECHAZADOS="erp/landing/rechazados/", PREFIJO_ERP="erp/landing/")
        self.s3 = S3Falso()
        consumidor._s3 = self.s3

    def test_lote_valido_deja_copia_cruda_y_csv(self):
        resumen = consumidor.lambda_handler(registros(evento(), evento(), evento()), None)
        self.assertEqual(resumen["validos"], 3)
        crudos = [k for k in self.s3.objetos if k.endswith(".jsonl.gz")]
        self.assertEqual(len(crudos), 1)
        self.assertRegex(crudos[0], r"^erp/landing/eventos/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}/.+\.jsonl\.gz$")
        filas = self.s3.objetos[crudos[0]]
        self.assertEqual(filas[0]["_message_id"], "m0")
        self.assertTrue(filas[0]["_enviado_en"].startswith("2023-11-14"))
        self.assertEqual(resumen["filas_csv"], {"orders": 1})  # 3 eventos del mismo pedido = 1 fila

    def test_csv_con_formato_olist(self):
        consumidor.lambda_handler(registros(creado(), evento()), None)
        (clave, filas), = self.s3.csv("orders").items()
        self.assertRegex(clave, r"^erp/landing/orders/year=2026/month=11/orders_2026_11_\d{8}T\d{6}_[0-9a-f]{8}\.csv$")
        self.assertEqual(list(filas[0]), consumidor.erp_csv.ORDERS)
        self.assertEqual((filas[0]["order_status"], filas[0]["order_approved_at"]),
                         ("approved", "2026-11-02 11:00:00"))  # gana el estado más reciente del lote
        self.assertEqual(filas[0]["order_delivered_customer_date"], "")
        (_, items), = self.s3.csv("order_items").items()
        self.assertEqual((items[0]["product_id"], items[0]["seller_id"], items[0]["price"]), ("p1", "s1", "58.9"))
        self.assertEqual(items[0]["freight_value"], "12")
        (_, pagos), = self.s3.csv("payments").items()
        self.assertEqual(pagos[0]["payment_value"], "70.9")
        (clave_c, clientes), = self.s3.csv("customers").items()
        self.assertRegex(clave_c, r"^erp/landing/customers/year=2026/month=11/customers_2026_11_\d{8}T\d{6}_[0-9a-f]{8}\.csv$")
        self.assertEqual(list(clientes[0]), consumidor.erp_csv.CUSTOMERS)  # las 5 columnas de bronze_customers
        self.assertEqual(clientes[0]["customer_zip_code_prefix"], "01310")

    def test_cliente_actualizado_no_duplica_customer_id(self):
        actualizado = evento("cliente_actualizado", ts="2026-11-02T09:50:00Z", **dict(CLIENTE, motivo="cambio_direccion"))
        consumidor.lambda_handler(registros(actualizado, creado()), None)
        filas = [f for v in self.s3.csv("customers").values() for f in v]
        self.assertEqual([f["customer_id"] for f in filas], ["c1"])
        consumidor.lambda_handler(registros(actualizado), None)  # solo el evento de cambio: no genera CSV
        self.assertEqual(len([f for v in self.s3.csv("customers").values() for f in v]), 1)

    def test_resena_sin_saltos_de_linea(self):
        r = evento("resena_creada", review_id="r1", order_id="o1", review_score=2,
                   review_comment_title=None, review_comment_message="chegou\r\nquebrado,  ruim",
                   review_creation_date="2026-12-01 00:00:00", review_answer_timestamp="2026-12-02 10:00:00")
        consumidor.lambda_handler(registros(r), None)
        (clave, filas), = self.s3.csv("reviews").items()
        self.assertIn("reviews/year=2026/month=12/", clave)
        self.assertEqual(filas[0]["review_comment_message"], "chegou quebrado, ruim")
        self.assertEqual(filas[0]["review_comment_title"], "")

    def test_invalidos_van_a_rechazados_sin_bloquear_el_lote(self):
        v1 = evento("pedido_entregado", order_id="o1", fecha="2026-11-03 10:00:00")  # contrato v1: sin pedido
        resumen = consumidor.lambda_handler(registros(evento(), "{no es json", {"tipo": "x"}, v1), None)
        self.assertEqual((resumen["validos"], resumen["rechazados"]), (1, 3))
        rechazados = next(v for k, v in self.s3.objetos.items() if "rechazados" in k)
        self.assertTrue(rechazados[0]["_motivo"].startswith("JSON invalido"))
        self.assertIn("pedido.order_id", rechazados[2]["_motivo"])

    def test_sin_prefijo_erp_solo_copia_cruda(self):
        os.environ.pop("PREFIJO_ERP")
        consumidor.lambda_handler(registros(creado()), None)
        self.assertEqual([k for k in self.s3.objetos if k.endswith(".csv")], [])

    def test_fallo_de_s3_propaga_error_para_reintentar(self):
        consumidor._s3 = S3Falso(fallar=True)
        with self.assertRaises(ConnectionError):
            consumidor.lambda_handler(registros(evento()), None)

    def test_validaciones(self):
        self.assertIsNone(consumidor.validar(evento()))
        self.assertIn("UUID", consumidor.validar(dict(evento(), evento_id="abc")))
        self.assertIn("ISO", consumidor.validar(dict(evento(), ts_evento="ayer")))
        self.assertIn("desconocido", consumidor.validar(evento("pedido_teletransportado")))
        c = creado()
        self.assertIsNone(consumidor.validar(c))
        c["payload"]["cliente"].pop("customer_unique_id")
        self.assertIn("cliente.customer_unique_id", consumidor.validar(c))


class PruebasCorte(unittest.TestCase):

    PEDIDO = {
        "order_id": "o1", "customer_id": "c1", "order_status": "delivered",
        "order_purchase_timestamp": "2017-12-20 10:00:00",
        "order_approved_at": "2017-12-20 11:00:00",
        "order_delivered_carrier_date": "2017-12-28 09:00:00",
        "order_delivered_customer_date": "2018-01-05 15:00:00",
        "order_estimated_delivery_date": "2018-01-10 00:00:00",
    }

    def test_pedido_entregado_despues_del_corte_queda_enviado(self):
        fila = olist_corte.pedido_al_corte(self.PEDIDO, datetime(2018, 1, 1))
        self.assertEqual(fila["order_status"], "shipped")
        self.assertEqual(fila["order_delivered_customer_date"], "")
        self.assertEqual(self.PEDIDO["order_status"], "delivered")  # no muta el original

    def test_pedido_aprobado_despues_del_corte_queda_creado(self):
        pedido = dict(self.PEDIDO, order_approved_at="2018-01-01 00:10:00",
                      order_delivered_carrier_date="2018-01-02 00:00:00")
        self.assertEqual(olist_corte.pedido_al_corte(pedido, datetime(2018, 1, 1))["order_status"], "created")

    def test_pedido_entregado_antes_del_corte_no_cambia(self):
        self.assertEqual(olist_corte.pedido_al_corte(self.PEDIDO, datetime(2018, 2, 1))["order_status"], "delivered")


if __name__ == "__main__":
    unittest.main()
