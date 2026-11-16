"""Pruebas locales (sin AWS) de la Lambda orquestadora PROPUESTA (auditoría 2026-11-15).

Mientras sea propuesta:   .venv\\Scripts\\python.exe infra\\propuestas\\orquestador\\test_orquestador.py -v
Al aplicarla, este archivo reemplaza a tests/test_orquestador.py (cambiar RUTA_HANDLER como indica la ficha E1-02).
"""
import importlib.util
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

RUTA_HANDLER = Path(__file__).resolve().parent.parent / "lambda" / "orquestador" / "handler.py"
spec = importlib.util.spec_from_file_location("orquestador", RUTA_HANDLER)
orq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orq)


class Cuerpo:
    def __init__(self, datos):
        self.datos = datos

    def read(self):
        return self.datos


class S3Falso:
    def __init__(self):
        self.objetos = {}  # clave -> (bytes, LastModified)

    def put_object(self, Bucket, Key, Body, ContentType=None, IfNoneMatch=None):
        if IfNoneMatch == "*" and Key in self.objetos:
            raise Exception("An error occurred (PreconditionFailed) when calling the PutObject operation")
        self.objetos[Key] = (Body, datetime.now(timezone.utc))

    def get_object(self, Bucket, Key):
        if Key not in self.objetos:
            raise Exception("An error occurred (NoSuchKey) when calling the GetObject operation")
        return {"Body": Cuerpo(self.objetos[Key][0])}

    def delete_object(self, Bucket, Key):
        self.objetos.pop(Key, None)

    def get_paginator(self, _):
        s3 = self

        class Paginador:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k, "LastModified": m} for k, (_, m) in s3.objetos.items()
                                    if k.startswith(Prefix)]}
        return Paginador()

    def archivo(self, clave, hace_min=0):
        self.objetos[clave] = (b"x", datetime.now(timezone.utc) - timedelta(minutes=hace_min))


class SfnFalso:
    def __init__(self):
        self.iniciadas, self.estado = [], "RUNNING"

    def start_execution(self, stateMachineArn, name, input):
        self.iniciadas.append((name, json.loads(input)))
        return {"executionArn": f"{stateMachineArn}:{name}"}

    def describe_execution(self, executionArn):
        return {"status": self.estado}


class GlueFalso:
    def __init__(self):
        self.corriendo, self.sin_tabla = set(), set()

    def get_table(self, DatabaseName, Name):
        if Name in self.sin_tabla:
            raise Exception("An error occurred (EntityNotFoundException) when calling the GetTable operation")
        return {"Table": {"Name": Name}}

    def get_job_runs(self, JobName, MaxResults=5):
        return {"JobRuns": [{"JobRunState": "RUNNING"}] if JobName in self.corriendo else []}


class AthenaFalso:
    """Devuelve el conteo configurado por fragmento de SQL (0 por defecto)."""
    def __init__(self):
        self.conteos, self.consultas = {}, {}

    def start_query_execution(self, QueryString, WorkGroup):
        qid = f"q{len(self.consultas)}"
        self.consultas[qid] = QueryString
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, QueryExecutionId):
        sql = self.consultas[QueryExecutionId]
        n = next((v for k, v in self.conteos.items() if k in sql), 0)
        return {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "_col0"}]}, {"Data": [{"VarCharValue": str(n)}]}]}}


class StsFalso:
    def get_caller_identity(self):
        return {"Account": "000000000000"}


def hace(minutos):
    return (datetime.now(timezone.utc) - timedelta(minutes=minutos)).isoformat()


def aviso_s3(clave):
    return {"source": "aws.s3", "detail": {"object": {"key": clave}}}


def fin(nombre, estado):
    return {"source": "aws.states", "detail": {"name": nombre, "status": estado}}


class PruebasOrquestador(unittest.TestCase):

    def setUp(self):
        self.s3, self.sfn, self.glue, self.athena = S3Falso(), SfnFalso(), GlueFalso(), AthenaFalso()
        orq._clientes.update(s3=self.s3, stepfunctions=self.sfn, sts=StsFalso(), glue=self.glue, athena=self.athena)

    def control(self, nombre):
        return json.loads(self.s3.objetos[orq.PREFIJO_CONTROL + nombre][0])

    def existe(self, nombre):
        return orq.PREFIJO_CONTROL + nombre in self.s3.objetos

    def retroceder_inicio(self, minutos=60):
        """Simula que el último inicio fue hace `minutos` (para superar el intervalo mínimo)."""
        ultima = self.control("ultima.json")
        ultima["iniciada_en"] = hace(minutos)
        orq.escribir("ultima.json", ultima)

    # ---------- comportamiento que ya existía ----------
    def test_un_solo_pipeline_a_la_vez_y_pendiente(self):
        r1 = orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)
        r2 = orq.lambda_handler(aviso_s3("erp/landing/orders/b.csv"), None)
        self.assertTrue(r1["iniciada"])
        self.assertFalse(r2["iniciada"])
        self.assertEqual(len(self.sfn.iniciadas), 1)
        self.assertTrue(self.existe("pendiente.json"))

    def test_al_terminar_libera_candado_y_respeta_intervalo(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        orq.lambda_handler({"origen": "horario"}, None)  # deja pendiente
        final = orq.lambda_handler(fin(r["ejecucion"], "SUCCEEDED"), None)
        self.assertFalse(self.existe("candado.json"))
        self.assertFalse(final["iniciada"])  # pasaron menos de 30 min
        self.assertIn("intervalo", final["motivo"])
        self.assertEqual(self.control("ejecuciones/" + r["ejecucion"] + ".json")["estado"], "SUCCEEDED")

    def test_pendiente_se_atiende_si_paso_el_intervalo(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        orq.lambda_handler({"origen": "horario"}, None)
        self.retroceder_inicio()
        final = orq.lambda_handler(fin(r["ejecucion"], "FAILED"), None)  # 1 fallo: todavía no se pausa
        self.assertTrue(final["iniciada"])
        self.assertEqual(len(self.sfn.iniciadas), 2)

    def test_candado_de_ejecucion_muerta_se_libera(self):
        orq.escribir("candado.json", {"nombre": "vieja", "desde": datetime.now(timezone.utc).isoformat()})
        self.sfn.estado = "FAILED"
        self.assertTrue(orq.lambda_handler({"accion": "iniciar"}, None)["iniciada"])

    def test_plan_segun_archivos_nuevos(self):
        orq.escribir("ultima.json", {"corte": hace(10)})  # formato antiguo (corte global): debe seguir funcionando
        self.s3.archivo("erp/landing/orders/year=2026/month=09/orders_x.csv", hace_min=1)
        self.s3.archivo("erp/landing/payments/year=2026/month=09/payments_x.csv", hace_min=1)
        self.s3.archivo("erp/landing/products/year=2017/month=01/viejo.csv", hace_min=60)  # anterior al corte
        self.s3.archivo("erp/landing/eventos/year=2026/a.jsonl.gz", hace_min=1)  # no es una entidad
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {}}, None)
        self.assertEqual(set(plan["entidades"]), {"orders", "payments"})
        self.assertEqual(plan["bronze"], ["g1_olist_bronze_orders", "g1_olist_bronze_payments"])
        self.assertEqual(plan["silver_1"], ["g1_olist_silver_orders"])
        self.assertEqual(plan["silver_2"], ["g1_olist_silver_payments"])
        self.assertEqual(plan["gold"], ["g1_olist_gold_payments", "g1_olist_gold_entregas", "g1_olist_gold_resenas"])
        self.assertTrue(plan["hay_trabajo"])

    def test_plan_forzado_web_y_cierre(self):
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["web"]}}, None)
        self.assertEqual(plan["bronze"], ["g1_olist_web_bronze_precios"])
        self.assertEqual(len(plan["silver_1"]), 2)
        self.assertEqual(plan["gold"], [])
        orq.lambda_handler({"accion": "cerrar", "plan": plan, "nombre": "p1"}, None)
        self.assertEqual(self.control("ultima.json")["cortes"]["web"], plan["corte"])
        with self.assertRaises(ValueError):
            orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["inventada"]}}, None)

    def test_sin_archivos_nuevos_no_hay_trabajo(self):
        orq.escribir("ultima.json", {"corte": datetime.now(timezone.utc).isoformat()})
        self.assertFalse(orq.lambda_handler({"accion": "preparar", "entrada": {}}, None)["hay_trabajo"])

    # ---------- E1-03: marca de agua por entidad ----------
    def test_inicio_manual_de_una_entidad_no_esconde_archivos_de_otra(self):
        orq.escribir("ultima.json", {"corte": hace(120)})
        self.s3.archivo("erp/landing/payments/year=2026/month=09/payments_x.csv", hace_min=30)
        # Ejecución manual solo de orders, que termina bien
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["orders"]}}, None)
        orq.lambda_handler({"accion": "cerrar", "plan": plan, "nombre": "manual-1"}, None)
        ultima = self.control("ultima.json")
        self.assertEqual(ultima["cortes"]["orders"], plan["corte"])
        self.assertLess(ultima["cortes"]["payments"], plan["corte"])  # payments conserva su marca anterior
        self.assertNotIn("corte", ultima)  # el corte global antiguo ya no se usa
        # La siguiente ejecución automática todavía ve el archivo de payments
        siguiente = orq.lambda_handler({"accion": "preparar", "entrada": {}}, None)
        self.assertEqual(set(siguiente["entidades"]), {"payments"})

    def test_cierre_sin_trabajo_no_mueve_marcas(self):
        orq.escribir("ultima.json", {"cortes": {"orders": hace(50)}, "corte": hace(90)})
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {}}, None)
        self.assertFalse(plan["hay_trabajo"])
        antes = orq.cortes_por_entidad(self.control("ultima.json"), datetime.now(timezone.utc))
        orq.lambda_handler({"accion": "cerrar", "plan": plan, "nombre": "vacia"}, None)
        self.assertEqual(self.control("ultima.json")["cortes"], antes)

    # ---------- E1-02: freno por fallos seguidos ----------
    def test_dos_fallos_seguidos_pausan_los_inicios_automaticos(self):
        r1 = orq.lambda_handler({"origen": "horario"}, None)
        orq.lambda_handler(fin(r1["ejecucion"], "FAILED"), None)
        self.retroceder_inicio()
        r2 = orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)
        self.assertTrue(r2["iniciada"])  # con 1 fallo todavía se reintenta
        orq.lambda_handler(fin(r2["ejecucion"], "FAILED"), None)
        self.retroceder_inicio()
        self.assertEqual(self.control("ultima.json")["fallos_seguidos"], 2)
        r3 = orq.lambda_handler(aviso_s3("erp/landing/orders/b.csv"), None)
        self.assertFalse(r3["iniciada"])
        self.assertIn("pausado", r3["motivo"])
        self.assertTrue(self.existe("pendiente.json"))
        self.assertEqual(len(self.sfn.iniciadas), 2)
        # El horario de pendientes tampoco lo reactiva
        self.assertFalse(orq.lambda_handler({"origen": "revisar_pendiente"}, None)["iniciada"])

    def test_inicio_manual_exitoso_quita_la_pausa(self):
        orq.escribir("ultima.json", {"fallos_seguidos": 2, "iniciada_en": hace(60)})
        manual = orq.lambda_handler({"accion": "iniciar", "entidades": ["orders"]}, None)
        self.assertTrue(manual["iniciada"])  # el inicio manual ignora la pausa
        orq.lambda_handler(fin(manual["ejecucion"], "SUCCEEDED"), None)
        self.assertEqual(self.control("ultima.json")["fallos_seguidos"], 0)
        self.retroceder_inicio()
        self.assertTrue(orq.lambda_handler({"origen": "horario"}, None)["iniciada"])

    # ---------- E1-04: pendiente atendido por el horario de 10 min ----------
    def test_revisar_pendiente_sin_pendiente_no_hace_nada(self):
        r = orq.lambda_handler({"origen": "revisar_pendiente"}, None)
        self.assertFalse(r["iniciada"])
        self.assertEqual(r["motivo"], "sin pendiente")
        self.assertEqual(self.sfn.iniciadas, [])

    def test_revisar_pendiente_inicia_cuando_pasa_el_intervalo(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        orq.lambda_handler(fin(r["ejecucion"], "SUCCEEDED"), None)
        orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)  # intervalo no cumplido -> pendiente
        primer_aviso = self.control("pendiente.json")["desde"]
        self.assertFalse(orq.lambda_handler({"origen": "revisar_pendiente"}, None)["iniciada"])
        self.assertEqual(self.control("pendiente.json")["desde"], primer_aviso)  # conserva el primer aviso
        self.retroceder_inicio()
        self.assertTrue(orq.lambda_handler({"origen": "revisar_pendiente"}, None)["iniciada"])
        self.assertFalse(self.existe("pendiente.json"))
        self.assertEqual(len(self.sfn.iniciadas), 2)

    # ---------- E1-05: avisos de S3 que no son del pipeline ----------
    def test_aviso_de_archivo_ajeno_se_descarta(self):
        for clave in ("web/landing/precios/_prueba_eventbridge.txt",
                      "erp/landing/Incrementales/clientes/periodo=2026-09/clientes.csv",
                      "erp/landing/eventos/year=2026/a.jsonl.gz"):
            r = orq.lambda_handler(aviso_s3(clave), None)
            self.assertFalse(r["iniciada"])
            self.assertIn("fuera de las carpetas", r["motivo"])
        self.assertEqual(self.sfn.iniciadas, [])
        self.assertFalse(self.existe("pendiente.json"))
        self.assertTrue(orq.lambda_handler(
            aviso_s3("web/landing/precios/year=2026/month=11/day=15/selva-x.jsonl.gz"), None)["iniciada"])

    def test_customers_incluye_scd2_en_la_etapa_dependiente(self):
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["customers", "orders"]}}, None)
        self.assertEqual(plan["silver_1"], ["g1_olist_silver_customers", "g1_olist_silver_orders"])
        self.assertEqual(plan["silver_2"], ["g1_olist_silver_customers_scd2"])
        self.assertEqual(plan["gold"], ["g1_olist_gold_payments", "g1_olist_gold_dimensiones", "g1_olist_gold_entregas", "g1_olist_gold_resenas"])

    # ---------- 2026-11-16: sellers SCD2, category_translation, gold ventas y controles ----------
    def test_sellers_y_category_translation_en_el_plan(self):
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["sellers", "category_translation"]}}, None)
        self.assertEqual(plan["bronze"], ["g1_olist_bronze_sellers", "g1_olist_bronze_category_translation"])
        self.assertEqual(plan["silver_1"], ["g1_olist_silver_sellers", "g1_olist_silver_category_translation"])

    def test_controles_sin_problemas_registran_resumen(self):
        plan = {"entidades": {"customers": 1, "orders": 1}}
        r = orq.lambda_handler({"accion": "controlar", "plan": plan, "nombre": "p1"}, None)
        self.assertGreaterEqual(r["controles"], 4)
        self.assertEqual(r["problemas"], [])
        self.assertTrue(self.existe("controles/p1.json"))
        self.assertTrue(all("web_silver" not in q for q in self.athena.consultas.values()))  # solo los del plan

    def test_control_con_problema_hace_fallar(self):
        self.athena.conteos["silver_customers_scd2 WHERE is_current"] = 3
        with self.assertRaises(ValueError) as ctx:
            orq.lambda_handler({"accion": "controlar", "plan": {"entidades": {"customers": 1}}, "nombre": "p2"}, None)
        self.assertIn("más de una versión vigente", str(ctx.exception))

    def test_control_de_gold_solo_si_corre_gold_ventas_y_omite_tabla_no_registrada(self):
        r = orq.lambda_handler({"accion": "controlar", "plan": {"entidades": {"orders": 1}, "gold": []}, "nombre": "a"}, None)
        self.assertTrue(all("gold_fact_sales" not in q for q in self.athena.consultas.values()))
        self.glue.sin_tabla.add("gold_fact_sales")
        r = orq.lambda_handler({"accion": "controlar", "plan": {"entidades": {"orders": 1},
                                                               "gold": ["g1_olist_gold_ventas"]}, "nombre": "b"}, None)
        self.assertEqual(r["problemas"], [])
        self.assertIn("no registrada", r["omitidos"][0])

    def test_gold_nuevos_en_el_plan(self):
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["orders", "customers"]}}, None)
        for job in ("g1_olist_gold_dimensiones", "g1_olist_gold_entregas", "g1_olist_gold_resenas", "g1_olist_gold_payments"):
            self.assertIn(job, plan["gold"])
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["reviews"]}}, None)
        self.assertEqual(plan["gold"], ["g1_olist_gold_resenas"])

    def test_controles_sin_entidades_no_consultan(self):
        r = orq.lambda_handler({"accion": "controlar", "plan": {"entidades": {}}, "nombre": "p3"}, None)
        self.assertEqual(r["controles"], 0)
        self.assertEqual(self.athena.consultas, {})

    # ---------- v2: aviso de fin repetido ----------
    def test_aviso_de_fin_repetido_cuenta_un_solo_fallo(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        orq.lambda_handler(fin(r["ejecucion"], "FAILED"), None)
        orq.lambda_handler(fin(r["ejecucion"], "FAILED"), None)  # EventBridge entrega el aviso otra vez
        self.assertEqual(self.control("ultima.json")["fallos_seguidos"], 1)

    # ---------- v2: pausa manual ----------
    def test_pausa_manual_frena_inicios_automaticos_y_vence(self):
        p = orq.lambda_handler({"accion": "pausar", "minutos": 30, "por": "ana", "motivo": "pruebo silver"}, None)
        self.assertTrue(p["pausado"])
        r = orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)
        self.assertFalse(r["iniciada"])
        self.assertIn("pausa manual de ana", r["motivo"])
        self.assertTrue(self.existe("pendiente.json"))
        # el inicio manual explícito sí pasa
        self.assertTrue(orq.lambda_handler({"accion": "iniciar", "entidades": ["web"]}, None)["iniciada"])
        # una pausa vencida se borra sola
        pausa = self.control("pausa.json")
        pausa["hasta"] = hace(1)
        orq.escribir("pausa.json", pausa)
        self.assertIsNone(orq.pausa_vigente())
        self.assertFalse(self.existe("pausa.json"))

    def test_reanudar_atiende_lo_pendiente(self):
        orq.lambda_handler({"accion": "pausar", "minutos": 30, "por": "ana"}, None)
        orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)
        r = orq.lambda_handler({"accion": "reanudar"}, None)
        self.assertFalse(r["pausado"])
        self.assertTrue(r["iniciada"])
        self.assertFalse(self.existe("pausa.json"))

    def test_pausa_no_supera_el_maximo(self):
        p = orq.lambda_handler({"accion": "pausar", "minutos": 100000}, None)
        hasta = datetime.fromisoformat(p["hasta"])
        self.assertLessEqual(hasta - datetime.now(timezone.utc), timedelta(minutes=orq.PAUSA_MAXIMA_MIN))

    # ---------- v2: jobs ocupados por una corrida manual ----------
    def test_jobs_ocupados_aplazan_sin_correr_nada(self):
        self.glue.corriendo.add("g1_olist_silver_orders")
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["orders"], "nombre": "p1"},
                                   "ejecucion": "p1"}, None)
        self.assertFalse(plan["hay_trabajo"])
        self.assertEqual(plan["ocupados"], ["g1_olist_silver_orders"])
        self.assertTrue(self.control("pendiente.json")["sin_intervalo"])
        orq.lambda_handler({"accion": "cerrar", "plan": plan, "nombre": "p1"}, None)
        self.assertNotIn("orders", self.control("ultima.json").get("entidades") or {})

    def test_pendiente_por_ocupados_no_relanza_en_bucle_y_lo_toma_el_horario(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        self.glue.corriendo.add("g1_olist_bronze_orders")
        self.s3.archivo("erp/landing/orders/year=2026/month=09/o.csv")
        orq.escribir("ultima.json", {**self.control("ultima.json"), "corte": hace(5)})
        orq.lambda_handler({"accion": "preparar", "entrada": {"nombre": r["ejecucion"]}, "ejecucion": r["ejecucion"]}, None)
        final = orq.lambda_handler(fin(r["ejecucion"], "SUCCEEDED"), None)
        self.assertFalse(final["iniciada"])  # no relanza al instante
        self.assertEqual(len(self.sfn.iniciadas), 1)
        # el horario de 10 min lo reintenta aunque no pasó el intervalo mínimo
        self.glue.corriendo.clear()
        self.assertTrue(orq.lambda_handler({"origen": "revisar_pendiente"}, None)["iniciada"])

    # ---------- v2: inicio desde la consola de Step Functions ----------
    def test_inicio_desde_consola_toma_el_candado(self):
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["web"]},
                                   "ejecucion": "demo-consola"}, None)
        self.assertTrue(plan["hay_trabajo"])
        self.assertEqual(self.control("candado.json")["nombre"], "demo-consola")
        # mientras corre, un aviso automático queda pendiente
        r = orq.lambda_handler(aviso_s3("erp/landing/orders/a.csv"), None)
        self.assertFalse(r["iniciada"])
        orq.lambda_handler(fin("demo-consola", "SUCCEEDED"), None)
        self.assertFalse(self.existe("candado.json"))

    def test_inicio_desde_consola_con_otra_en_curso_no_corre(self):
        r = orq.lambda_handler({"origen": "horario"}, None)
        plan = orq.lambda_handler({"accion": "preparar", "entrada": {"entidades": ["web"]},
                                   "ejecucion": "demo-consola"}, None)
        self.assertFalse(plan["hay_trabajo"])
        self.assertIn(r["ejecucion"], plan["motivo"])
        self.assertEqual(self.control("candado.json")["nombre"], r["ejecucion"])  # no le quita el candado


if __name__ == "__main__":
    unittest.main()
