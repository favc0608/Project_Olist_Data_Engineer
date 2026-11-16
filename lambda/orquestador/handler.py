"""
PROYECTO INTEGRADOR OLIST - GRUPO 1
LAMBDA ORQUESTADORA: decide cuándo arranca el pipeline y qué jobs corre.

Versión 2 (2026-11-16): auditoría E1-02 a E1-05 + convivencia con corridas manuales.
  - Marca de agua POR ENTIDAD (ultima.json -> "cortes"): un inicio manual de una entidad no adelanta
    la marca de las demás.
  - Freno por fallos seguidos: tras MAX_FALLOS_SEGUIDOS ejecuciones fallidas, los inicios automáticos se
    pausan hasta un inicio manual que termine bien. Cada ejecución se cuenta una sola vez (EventBridge
    puede repetir el aviso de fin).
  - Horario cada 10 min {"origen": "revisar_pendiente"}: atiende pendiente.json sin esperar al de 3 h.
  - Avisos de S3 de archivos fuera de las carpetas del pipeline se descartan.
  - Pausa manual: {"accion": "pausar", "minutos": 60, "por": "nombre", "motivo": "..."} y
    {"accion": "reanudar"}. Mientras dure, no hay inicios automáticos (vence sola).
  - Jobs ocupados: si un job del plan ya está corriendo (alguien lo lanzó a mano), Preparar no corre
    nada, deja pendiente y se reintenta en el siguiente horario de 10 min (sin esperar el intervalo).
  - Inicio desde la consola de Step Functions: Preparar toma el candado si está libre.

Avisos que recibe:
  - S3 "Object Created" en erp/landing/*.csv o web/landing/precios/  -> intentar iniciar
  - EventBridge Scheduler cada 3 h (respaldo) y cada 10 min (pendientes)
  - Step Functions "Execution Status Change" (fin de ejecución)       -> liberar candado, contar fallos,
    atender lo pendiente
  - {"accion": "preparar" | "cerrar" | "registrar_fallo"}            -> pasos de la máquina de estados
  - {"accion": "iniciar" | "pausar" | "reanudar"}                     -> operación manual

Control en s3://<BUCKET>/<PREFIJO_CONTROL>: candado.json, pendiente.json, pausa.json, ultima.json (marcas
de agua y contador de fallos), ejecuciones/<nombre>.json, fallos/<nombre>.json.
"""
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

BUCKET = os.environ.get("BUCKET", "trabajo-integrador-g1")
PREFIJO_CONTROL = os.environ.get("PREFIJO_CONTROL", "erp/_control/orquestacion/")
NOMBRE_MAQUINA = os.environ.get("NOMBRE_MAQUINA", "g1-olist-pipeline")
INTERVALO_MINIMO_MIN = int(os.environ.get("INTERVALO_MINIMO_MIN", "30"))
MAX_FALLOS_SEGUIDOS = int(os.environ.get("MAX_FALLOS_SEGUIDOS", "2"))
CANDADO_VENCIDO_MIN = 180
PAUSA_MAXIMA_MIN = 480
HORAS_INICIALES = 24  # sin marca de agua previa, se miran los archivos de las últimas 24 h
INCLUIR_GOLD_VENTAS = os.environ.get("INCLUIR_GOLD_VENTAS", "false") == "true"
ESTADOS_OCUPADO = {"STARTING", "RUNNING", "STOPPING", "WAITING"}

# ==========================================
# 1. MAPA ENTIDAD -> CARPETA DE LANDING Y JOBS
# ==========================================
# etapa "silver_1": no dependen de otra entidad; "silver_2": necesitan silver orders (filtran por pedido).
ENTIDADES = {
    "orders": {"landing": "erp/landing/orders/", "bronze": ["g1_olist_bronze_orders"],
               "silver_1": ["g1_olist_silver_orders"]},
    "order_items": {"landing": "erp/landing/order_items/", "bronze": ["g1_olist_bronze_order_items"],
                    "silver_2": ["g1_olist_silver_order_items"]},
    "payments": {"landing": "erp/landing/payments/", "bronze": ["g1_olist_bronze_payments"],
                 "silver_2": ["g1_olist_silver_payments"]},
    "reviews": {"landing": "erp/landing/reviews/", "bronze": ["g1_olist_bronze_reviews"],
                "silver_2": ["g1_olist_silver_reviews"]},
    # SCD2 de clientes (customer_unique_id): necesita silver customers y silver orders -> etapa 2
    "customers": {"landing": "erp/landing/customers/", "bronze": ["g1_olist_bronze_customers"],
                  "silver_1": ["g1_olist_silver_customers"], "silver_2": ["g1_olist_silver_customers_scd2"]},
    "products": {"landing": "erp/landing/products/", "bronze": ["g1_olist_bronze_productos"],
                 "silver_1": ["g1_olist_silver_productos"]},
    # SCD2 de vendedores (job nuevo sin tabulador, 2026-11-16)
    "sellers": {"landing": "erp/landing/sellers/", "bronze": ["g1_olist_bronze_sellers"],
                "silver_1": ["g1_olist_silver_sellers"]},
    "category_translation": {"landing": "erp/landing/category_translation/",
                             "bronze": ["g1_olist_bronze_category_translation"],
                             "silver_1": ["g1_olist_silver_category_translation"]},
    "geolocation": {"landing": "erp/landing/geolocation/", "bronze": ["g1_olist_bronze_geolocation"],
                    "silver_1": ["g1_olist_silver_geolocation"]},
    "web": {"landing": "web/landing/precios/", "sufijo": ".jsonl.gz", "bronze": ["g1_olist_web_bronze_precios"],
            "silver_1": ["g1_olist_web_silver_precios_competencia", "g1_olist_web_silver_productos_competencia"]},
}
# gold: se corre si cambió alguna de las entidades que lee
GOLD = [
    ("g1_olist_gold_payments", {"payments", "orders", "customers"}),
    ("g1_olist_gold_geolocation", {"geolocation"}),
    ("g1_olist_gold_ventas", {"orders", "order_items", "customers", "products", "sellers", "category_translation"}),
    # 2026-11-16: refresca las dimensiones de Power BI (mismo esquema) + dimensiones SCD2 y calendario nuevos
    ("g1_olist_gold_dimensiones", {"customers", "products", "sellers"}),
    ("g1_olist_gold_entregas", {"orders", "order_items", "customers", "sellers"}),
    ("g1_olist_gold_resenas", {"reviews", "orders"}),
]

# Controles de calidad al final del pipeline (Athena, workgroup g1-olist). Cada consulta devuelve la cantidad de
# filas con problema: debe ser 0. Solo se corren los de las entidades del plan ("siempre" = en toda ejecución con trabajo).
BASE = "g1_olist_lakehouse"
CONTROLES = [  # (nombre, entidades que lo activan, tabla del catálogo, consulta)
    ("silver_orders: order_id duplicado", {"orders"}, "silver_orders",
     f"SELECT count(*) FROM (SELECT order_id FROM {BASE}.silver_orders GROUP BY 1 HAVING count(*) > 1)"),
    ("silver_customers: customer_id duplicado", {"customers"}, "silver_customers",
     f"SELECT count(*) FROM (SELECT customer_id FROM {BASE}.silver_customers GROUP BY 1 HAVING count(*) > 1)"),
    ("customers_scd2: más de una versión vigente", {"customers"}, "silver_customers_scd2",
     f"SELECT count(*) FROM (SELECT customer_unique_id FROM {BASE}.silver_customers_scd2 WHERE is_current "
     f"GROUP BY 1 HAVING count(*) > 1)"),
    ("customers_scd2: vigencias que no encadenan", {"customers"}, "silver_customers_scd2",
     f"SELECT count(*) FROM (SELECT valid_to, lead(valid_from) OVER (PARTITION BY customer_unique_id ORDER BY version) "
     f"AS sig FROM {BASE}.silver_customers_scd2) WHERE sig IS NOT NULL AND valid_to <> sig"),
    ("silver_sellers: más de una versión vigente", {"sellers"}, "silver_sellers",
     f"SELECT count(*) FROM (SELECT seller_id FROM {BASE}.silver_sellers WHERE is_current GROUP BY 1 HAVING count(*) > 1)"),
    ("gold_fact_sales: ítem duplicado", {"gold:g1_olist_gold_ventas"}, "gold_fact_sales",
     f"SELECT count(*) FROM (SELECT order_id, order_item_id FROM {BASE}.gold_fact_sales GROUP BY 1, 2 HAVING count(*) > 1)"),
    ("gold_fact_entregas: pedido duplicado", {"gold:g1_olist_gold_entregas"}, "gold_fact_entregas",
     f"SELECT count(*) FROM (SELECT order_id FROM {BASE}.gold_fact_entregas GROUP BY 1 HAVING count(*) > 1)"),
    ("gold_fact_entregas: sk_cliente sin dimensión (huérfano)", {"gold:g1_olist_gold_entregas"}, "gold_dim_cliente_scd2",
     f"SELECT count(*) FROM {BASE}.gold_fact_entregas f LEFT JOIN {BASE}.gold_dim_cliente_scd2 d "
     f"ON f.sk_cliente = d.sk_cliente WHERE f.sk_cliente IS NOT NULL AND d.sk_cliente IS NULL"),
    ("gold_fact_entregas: entregado sin fecha de entrega", {"gold:g1_olist_gold_entregas"}, "gold_fact_entregas",
     f"SELECT count(*) FROM {BASE}.gold_fact_entregas WHERE order_status = 'delivered' AND fecha_entrega IS NULL "
     f"AND fecha_compra >= TIMESTAMP '2019-01-01 00:00:00'"),
    ("gold_fact_resenas: reseña duplicada", {"gold:g1_olist_gold_resenas"}, "gold_fact_resenas",
     f"SELECT count(*) FROM (SELECT review_id, order_id FROM {BASE}.gold_fact_resenas GROUP BY 1, 2 HAVING count(*) > 1)"),
    ("gold_dim_customers: customer_id duplicado", {"gold:g1_olist_gold_dimensiones"}, "gold_dim_customers",
     f"SELECT count(*) FROM (SELECT customer_id FROM {BASE}.gold_dim_customers GROUP BY 1 HAVING count(*) > 1)"),
    ("web precios: más de una versión vigente", {"web"}, "web_silver_precios_competencia",
     f"SELECT count(*) FROM (SELECT tienda, sku_loja FROM {BASE}.web_silver_precios_competencia WHERE is_current "
     f"GROUP BY 1, 2 HAVING count(*) > 1)"),
]
WORKGROUP = os.environ.get("WORKGROUP_ATHENA", "g1-olist")

_clientes = {}


def cliente(servicio):
    if servicio not in _clientes:
        import boto3
        _clientes[servicio] = boto3.client(servicio)
    return _clientes[servicio]


def ahora():
    return datetime.now(timezone.utc)


def arn_maquina():
    sts = cliente("sts").get_caller_identity()
    region = os.environ.get("AWS_REGION", "us-east-2")
    return f"arn:aws:states:{region}:{sts['Account']}:stateMachine:{NOMBRE_MAQUINA}"


def entidad_de(clave):
    """Entidad del pipeline a la que pertenece un archivo de landing (None si no pertenece a ninguna)."""
    for entidad, conf in ENTIDADES.items():
        if clave.startswith(conf["landing"]) and clave.endswith(conf.get("sufijo", ".csv")):
            return entidad
    return None


# ==========================================
# 2. CONTROL EN S3
# ==========================================
def leer(nombre):
    try:
        cuerpo = cliente("s3").get_object(Bucket=BUCKET, Key=PREFIJO_CONTROL + nombre)["Body"].read()
        return json.loads(cuerpo)
    except Exception as error:  # NoSuchKey o JSON inválido: se trata como ausente
        if "NoSuchKey" in str(error) or "Not Found" in str(error) or isinstance(error, ValueError):
            return None
        raise


def escribir(nombre, datos, solo_si_no_existe=False):
    """True si se escribió. Con solo_si_no_existe usa escritura condicional (candado atómico)."""
    parametros = {"Bucket": BUCKET, "Key": PREFIJO_CONTROL + nombre, "ContentType": "application/json",
                  "Body": json.dumps(datos, ensure_ascii=False, default=str).encode("utf-8")}
    if solo_si_no_existe:
        parametros["IfNoneMatch"] = "*"
    try:
        cliente("s3").put_object(**parametros)
        return True
    except Exception as error:
        if "PreconditionFailed" in str(error) or "ConditionalRequestConflict" in str(error):
            return False
        raise


def borrar(nombre):
    cliente("s3").delete_object(Bucket=BUCKET, Key=PREFIJO_CONTROL + nombre)


def cortes_por_entidad(ultima, momento):
    """Marca de agua de cada entidad (ISO-8601).

    Compatibilidad: un ultima.json antiguo solo trae "corte" (global); se usa para las entidades que
    todavía no tienen marca propia. Sin ningún corte, se miran las últimas HORAS_INICIALES horas.
    """
    propios = ultima.get("cortes") or {}
    por_defecto = ultima.get("corte") or (momento - timedelta(hours=HORAS_INICIALES)).isoformat()
    return {entidad: propios.get(entidad, por_defecto) for entidad in ENTIDADES}


# ==========================================
# 3. PAUSA MANUAL
# ==========================================
def pausa_vigente():
    """pausa.json si sigue vigente; si venció, se borra y devuelve None."""
    pausa = leer("pausa.json")
    if not pausa:
        return None
    if ahora() >= datetime.fromisoformat(pausa["hasta"]):
        borrar("pausa.json")
        return None
    return pausa


def pausar(evento):
    minutos = min(int(evento.get("minutos", 60)), PAUSA_MAXIMA_MIN)
    pausa = {"por": evento.get("por", "sin nombre"), "motivo": evento.get("motivo", ""),
             "desde": ahora().isoformat(), "hasta": (ahora() + timedelta(minutes=minutos)).isoformat()}
    escribir("pausa.json", pausa)
    return {"pausado": True, **pausa}


def reanudar():
    borrar("pausa.json")
    resultado = revisar_pendiente()
    return {"pausado": False, **resultado}


# ==========================================
# 4. INICIAR (candado, pendiente, intervalo mínimo, pausa y freno por fallos)
# ==========================================
def dejar_pendiente(origen, momento, sin_intervalo=False):
    """Marca que hay archivos por atender. Conserva la fecha del primer aviso sin atender.

    sin_intervalo: el aviso se aplazó por jobs ocupados (no por el intervalo): el horario de 10 min
    lo reintenta sin esperar el intervalo mínimo.
    """
    previo = leer("pendiente.json") or {}
    escribir("pendiente.json", {"origen": previo.get("origen", origen), "desde": previo.get("desde", momento.isoformat()),
                                "ultimo_aviso": momento.isoformat(),
                                "sin_intervalo": sin_intervalo or previo.get("sin_intervalo", False)})


def intentar_iniciar(origen, entidades=None, forzar=False, sin_intervalo=False):
    momento = ahora()
    ultima = leer("ultima.json") or {}
    if not forzar:
        pausa = pausa_vigente()
        if pausa:
            dejar_pendiente(origen, momento)
            return {"iniciada": False, "motivo": f"pausa manual de {pausa['por']} hasta {pausa['hasta']}"}
        fallos = int(ultima.get("fallos_seguidos", 0))
        if fallos >= MAX_FALLOS_SEGUIDOS:
            dejar_pendiente(origen, momento)
            return {"iniciada": False,
                    "motivo": f"pausado: {fallos} ejecuciones fallidas seguidas; revisar fallos/ e iniciar a mano"}
        inicio_previo = ultima.get("iniciada_en")
        if (not sin_intervalo and inicio_previo
                and momento - datetime.fromisoformat(inicio_previo) < timedelta(minutes=INTERVALO_MINIMO_MIN)):
            dejar_pendiente(origen, momento)
            return {"iniciada": False, "motivo": f"intervalo mínimo ({INTERVALO_MINIMO_MIN} min) no cumplido"}

    nombre = f"pipeline-{momento:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:4]}"
    if not escribir("candado.json", {"nombre": nombre, "desde": momento.isoformat(), "origen": origen},
                    solo_si_no_existe=True):
        candado = leer("candado.json") or {}
        if not candado_vencido(candado):
            dejar_pendiente(origen, momento)
            return {"iniciada": False, "motivo": f"ya hay una ejecución en curso ({candado.get('nombre')})"}
        borrar("candado.json")
        return intentar_iniciar(origen, entidades, forzar, sin_intervalo)

    try:
        borrar("pendiente.json")
        entrada = {"origen": origen, "entidades": entidades, "nombre": nombre}
        respuesta = cliente("stepfunctions").start_execution(
            stateMachineArn=arn_maquina(), name=nombre, input=json.dumps(entrada))
    except Exception:
        borrar("candado.json")  # no dejar el candado tomado si no arrancó
        raise
    escribir("ultima.json", {**ultima, "iniciada_en": momento.isoformat(), "ejecucion": nombre})
    return {"iniciada": True, "ejecucion": nombre, "arn": respuesta["executionArn"]}


def candado_vencido(candado):
    """Un candado sin ejecución viva (o muy antiguo) se puede liberar."""
    desde = candado.get("desde")
    if not desde:
        return True
    if ahora() - datetime.fromisoformat(desde) < timedelta(minutes=CANDADO_VENCIDO_MIN):
        try:
            arn = arn_maquina().replace(":stateMachine:", ":execution:") + f":{candado['nombre']}"
            estado = cliente("stepfunctions").describe_execution(executionArn=arn)["status"]
            return estado != "RUNNING"
        except Exception as error:
            return "ExecutionDoesNotExist" in str(error)
    return True


def revisar_pendiente():
    """Horario cada 10 min: si quedó un aviso sin atender, intenta iniciar (respeta pausa y freno)."""
    pendiente = leer("pendiente.json")
    if not pendiente:
        return {"iniciada": False, "motivo": "sin pendiente"}
    return intentar_iniciar("pendiente", sin_intervalo=bool(pendiente.get("sin_intervalo")))


def al_terminar(detalle):
    """Fin de una ejecución: resumen, contador de fallos (una vez por ejecución), candado y pendiente."""
    nombre, estado = detalle.get("name"), detalle.get("status")
    escribir(f"ejecuciones/{nombre}.json", {"nombre": nombre, "estado": estado,
                                             "inicio": detalle.get("startDate"), "fin": detalle.get("stopDate")})
    ultima = leer("ultima.json") or {}
    if ultima.get("ultima_contada") != nombre:  # EventBridge puede entregar el mismo aviso dos veces
        fallos = 0 if estado == "SUCCEEDED" else int(ultima.get("fallos_seguidos", 0)) + 1
        escribir("ultima.json", {**ultima, "fallos_seguidos": fallos, "ultimo_estado": estado,
                                 "ultima_contada": nombre})
    candado = leer("candado.json") or {}
    if candado.get("nombre") == nombre:
        borrar("candado.json")
    pendiente = leer("pendiente.json")
    if pendiente and pendiente.get("sin_intervalo"):
        # aplazado por jobs ocupados: reintentar ya volvería a encontrarlos ocupados (bucle); lo toma el horario de 10 min
        return {"terminada": nombre, "estado": estado, "iniciada": False,
                "motivo": "pendiente por jobs ocupados: lo atiende el horario de 10 min"}
    if pendiente:
        return {"terminada": nombre, "estado": estado, **intentar_iniciar("pendiente")}
    return {"terminada": nombre, "estado": estado, "iniciada": False}


# ==========================================
# 5. PASOS DE LA MÁQUINA DE ESTADOS
# ==========================================
def archivos_nuevos(prefijo, sufijo, desde):
    paginador = cliente("s3").get_paginator("list_objects_v2")
    total = 0
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=prefijo):
        for objeto in pagina.get("Contents", []):
            if objeto["Key"].endswith(sufijo) and objeto["LastModified"] > desde:
                total += 1
    return total


def jobs_ocupados(jobs):
    """Jobs del plan que ya tienen una corrida en curso (p. ej. lanzada a mano desde la consola)."""
    ocupados = []
    for job in jobs:
        corridas = cliente("glue").get_job_runs(JobName=job, MaxResults=5).get("JobRuns", [])
        if any(c.get("JobRunState") in ESTADOS_OCUPADO for c in corridas):
            ocupados.append(job)
    return ocupados


def sin_trabajo(momento, motivo, **extra):
    return {"bronze": [], "silver_1": [], "silver_2": [], "gold": [], "entidades": {}, "hay_trabajo": False,
            "desde": {}, "corte": momento.isoformat(), "motivo": motivo, **extra}


def tomar_candado_consola(ejecucion, momento):
    """Ejecución iniciada desde la consola de Step Functions (sin pasar por la orquestadora).

    Devuelve None si tomó (o ya tenía) el candado, o el nombre de la ejecución que lo tiene.
    """
    if escribir("candado.json", {"nombre": ejecucion, "desde": momento.isoformat(), "origen": "consola"},
                solo_si_no_existe=True):
        escribir("ultima.json", {**(leer("ultima.json") or {}), "iniciada_en": momento.isoformat(),
                                 "ejecucion": ejecucion})
        return None
    candado = leer("candado.json") or {}
    if candado.get("nombre") == ejecucion:
        return None
    if candado_vencido(candado):
        borrar("candado.json")
        return tomar_candado_consola(ejecucion, momento)
    return candado.get("nombre")


def preparar(evento):
    """Plan de la ejecución: qué entidades tienen archivos nuevos desde SU última corrida exitosa."""
    momento = ahora()
    entrada = evento.get("entrada") or {}
    ejecucion = evento.get("ejecucion")
    if ejecucion and not entrada.get("nombre"):  # iniciada desde la consola de Step Functions
        otra = tomar_candado_consola(ejecucion, momento)
        if otra:
            dejar_pendiente("consola", momento)
            return sin_trabajo(momento, f"ya hay una ejecución en curso ({otra}); quedó pendiente")

    ultima = leer("ultima.json") or {}
    cortes = cortes_por_entidad(ultima, momento)
    forzadas = entrada.get("entidades")
    if forzadas:
        desconocidas = set(forzadas) - set(ENTIDADES)
        if desconocidas:
            raise ValueError(f"Entidades desconocidas: {sorted(desconocidas)}")
        detectadas = {e: None for e in forzadas}
    else:
        detectadas = {}
        for entidad, conf in ENTIDADES.items():
            n = archivos_nuevos(conf["landing"], conf.get("sufijo", ".csv"), datetime.fromisoformat(cortes[entidad]))
            if n:
                detectadas[entidad] = n
    plan = {"bronze": [], "silver_1": [], "silver_2": [], "gold": []}
    for entidad in detectadas:
        for etapa in ("bronze", "silver_1", "silver_2"):
            plan[etapa] += [j for j in ENTIDADES[entidad].get(etapa, []) if j not in plan[etapa]]
    for job, fuentes in GOLD:
        if fuentes & set(detectadas) and (job != "g1_olist_gold_ventas" or INCLUIR_GOLD_VENTAS):
            plan["gold"].append(job)

    ocupados = jobs_ocupados([j for etapa in plan.values() for j in etapa]) if detectadas else []
    if ocupados:  # alguien los está corriendo a mano: no se pisa, se reintenta en 10 min
        dejar_pendiente(entrada.get("origen", "consola"), momento, sin_intervalo=True)
        return sin_trabajo(momento, f"jobs ocupados: {ocupados}; se reintenta en el horario de 10 min",
                           ocupados=ocupados)
    return {**plan, "entidades": detectadas, "hay_trabajo": bool(detectadas),
            "desde": {e: cortes[e] for e in detectadas}, "corte": momento.isoformat()}


def cerrar(evento):
    """Ejecución exitosa: avanza la marca de agua SOLO de las entidades del plan (nunca en un fallo)."""
    plan = evento.get("plan") or {}
    momento = ahora()
    ultima = leer("ultima.json") or {}
    cortes = cortes_por_entidad(ultima, momento)  # materializa todas las entidades (migra el corte global antiguo)
    corte = plan.get("corte")
    if corte:
        for entidad in plan.get("entidades") or {}:
            cortes[entidad] = corte
    resto = {k: v for k, v in ultima.items() if k != "corte"}  # el corte global antiguo deja de usarse
    escribir("ultima.json", {**resto, "cortes": cortes, "ultimo_corte": corte,
                             "ultima_exitosa": evento.get("nombre"), "entidades": plan.get("entidades")})
    return {"corte": corte, "entidades": sorted(plan.get("entidades") or {})}


def controlar(evento, espera_max_s=240, pausa_s=3):
    """Controles de calidad en Athena para las entidades del plan. Falla (ValueError) si alguno no da 0."""
    import time
    plan = evento.get("plan") or {}
    claves = set(plan.get("entidades") or {}) | {f"gold:{j}" for j in plan.get("gold") or []}
    aplicables, omitidos = [], []
    for nombre, fuentes, tabla, sql in CONTROLES:
        if not fuentes & claves:
            continue
        try:  # una tabla aún no registrada en el catálogo (p. ej. primera carga) no hace fallar al pipeline
            cliente("glue").get_table(DatabaseName=BASE, Name=tabla)
            aplicables.append((nombre, sql))
        except Exception as error:
            if "EntityNotFound" not in str(error):
                raise
            omitidos.append(f"{nombre} (tabla {tabla} no registrada)")
    if not aplicables:
        return {"controles": 0, "problemas": [], "omitidos": omitidos}
    athena = cliente("athena")
    ids = {nombre: athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
           for nombre, sql in aplicables}
    resultados, limite = {}, time.time() + espera_max_s
    while len(resultados) < len(ids):
        for nombre, qid in ids.items():
            if nombre in resultados:
                continue
            estado = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if estado["State"] == "SUCCEEDED":
                filas = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
                resultados[nombre] = int(filas[1]["Data"][0]["VarCharValue"])
            elif estado["State"] in ("FAILED", "CANCELLED"):
                resultados[nombre] = f"consulta fallida: {estado.get('StateChangeReason', '')[:300]}"
        if len(resultados) < len(ids):
            if time.time() > limite:
                raise TimeoutError(f"Controles sin terminar: {sorted(set(ids) - set(resultados))}")
            time.sleep(pausa_s)
    problemas = [f"{n}: {v}" for n, v in resultados.items() if v != 0]
    resumen = {"controles": len(resultados), "resultados": resultados, "problemas": problemas, "omitidos": omitidos}
    escribir(f"controles/{evento.get('nombre')}.json", {**resumen, "registrado_en": ahora().isoformat()})
    if problemas:
        raise ValueError("Controles de calidad fallidos: " + " | ".join(problemas))
    return resumen


def registrar_fallo(evento):
    error = evento.get("error") or {}
    escribir(f"fallos/{evento.get('nombre')}.json", {"nombre": evento.get("nombre"), "error": error.get("Error"),
                                                     "causa": str(error.get("Cause", ""))[:4000],
                                                     "registrado_en": ahora().isoformat()})
    return {"registrado": True}


# ==========================================
# 6. HANDLER
# ==========================================
def lambda_handler(event, context):
    accion = event.get("accion")
    if accion == "preparar":
        resultado = preparar(event)
    elif accion == "cerrar":
        resultado = cerrar(event)
    elif accion == "registrar_fallo":
        resultado = registrar_fallo(event)
    elif accion == "controlar":
        resultado = controlar(event)
    elif accion == "pausar":  # {"accion": "pausar", "minutos": 60, "por": "nombre", "motivo": "..."}
        resultado = pausar(event)
    elif accion == "reanudar":
        resultado = reanudar()
    elif event.get("source") == "aws.states":
        resultado = al_terminar(event.get("detail", {}))
    elif event.get("source") == "aws.s3":
        clave = event.get("detail", {}).get("object", {}).get("key", "")
        if entidad_de(clave) is None:
            resultado = {"iniciada": False, "motivo": f"archivo fuera de las carpetas del pipeline: {clave}"}
        else:
            resultado = intentar_iniciar("s3:" + clave)
    elif accion == "iniciar":  # manual (consola o prueba): {"accion": "iniciar", "entidades": ["web"]}
        resultado = intentar_iniciar("manual", event.get("entidades"), forzar=True)
    elif event.get("origen") == "revisar_pendiente":  # EventBridge Scheduler cada 10 min
        resultado = revisar_pendiente()
    else:  # EventBridge Scheduler cada 3 h: {"origen": "horario"}
        resultado = intentar_iniciar(event.get("origen", "horario"))
    print(json.dumps({"evento": accion or event.get("source") or event.get("origen"), **resultado}, default=str))
    return resultado
