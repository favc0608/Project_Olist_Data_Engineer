# 5. Orquestación

Stack `g1-olist-orquestacion` ([`infra/cloudformation/g1-olist-orquestacion.yaml`](../infra/cloudformation/g1-olist-orquestacion.yaml)):
una **Lambda orquestadora** decide *qué* correr y una **máquina de Step Functions** lo ejecuta por capas.

## 5.1 Disparadores

| Disparador | Para qué |
|---|---|
| EventBridge: `Object Created` en `erp/landing/` y `web/landing/precios/` | reaccionar a archivos nuevos (ERP o streaming) |
| EventBridge Scheduler cada 10 min | retomar trabajo pendiente (archivos que llegaron mientras otra corrida estaba activa) |
| EventBridge Scheduler cada 3 h | respaldo por si se perdió algún evento |
| Inicio manual | consola de Step Functions (`{}` = detectar archivos nuevos, o `{"entidades": ["web"]}`) o invocando la Lambda |
| Regla de fin de ejecución | liberar el candado y registrar el resultado |

## 5.2 La Lambda orquestadora

[`lambda/orquestador/handler.py`](../lambda/orquestador/handler.py). Todo su estado vive en S3
(`erp/_control/orquestacion/`), así que no necesita base de datos:

| Archivo | Función |
|---|---|
| `candado.json` | exclusión mutua entre ejecuciones; se crea con `PutObject` + `If-None-Match: *` (escritura condicional atómica) |
| `pendiente.json` | marca que llegaron archivos mientras había una corrida activa |
| `ultima.json` | **marca de agua por entidad** (`cortes`) y conteo de fallos seguidos |
| `pausa.json` | pausa manual con autor, motivo y vencimiento (máx. 8 h) |
| `ejecuciones/`, `fallos/`, `controles/` | historial auditable de cada corrida |

**Preparar** arma el plan:
1. Por cada entidad, lista los archivos de su carpeta de landing con fecha posterior a su marca de agua.
2. Agrega los jobs bronze y silver de esas entidades y los gold que dependen de ellas:

   ```python
   GOLD = [("g1_olist_gold_payments",    {"payments", "orders", "customers"}),
           ("g1_olist_gold_geolocation", {"geolocation"}),
           ("g1_olist_gold_ventas",      {"orders", "order_items", "customers", "products", "sellers", "category_translation"}),
           ("g1_olist_gold_dimensiones", {"customers", "products", "sellers"}),
           ("g1_olist_gold_entregas",    {"orders", "order_items", "customers", "sellers"}),
           ("g1_olist_gold_resenas",     {"reviews", "orders"})]
   ```
3. Si un job del plan **ya está corriendo** (alguien lo lanzó a mano), aplaza la corrida en vez de chocar.

**Reglas de seguridad operativa**
- Intervalo mínimo entre corridas automáticas (parámetro `IntervaloMinimoMin`); los inicios manuales lo ignoran.
- **Freno tras 2 fallos seguidos** (`MaxFallosSeguidos`): deja de lanzar corridas automáticas hasta que alguien
  revise, para no gastar en bucle sobre un archivo malo.
- Pausa manual para mantenimiento: `{"accion": "pausar", "minutos": 60, "por": "nombre"}` / `{"accion": "reanudar"}`.
- Archivos fuera de las carpetas del pipeline se descartan.

## 5.3 La máquina de estados

```
Preparar → ¿HayTrabajo? → Bronze → Silver → SilverDependiente → Gold → Controles → Cerrar → Fin
                └─ no ──────────────────────────────────────────────────────────→ Cerrar
          cualquier error (Catch States.ALL) → RegistrarFallo → Fallo (→ EventBridge → SNS)
```

- Cada capa es un **Map** que lanza jobs en paralelo con `arn:aws:states:::glue:startJobRun.sync`
  (Step Functions espera a que el job termine). Concurrencia: Bronze ≤4, Silver ≤4, SilverDependiente ≤3, Gold ≤4.
- **Silver vs. SilverDependiente:** silver 1 no depende de otras entidades; silver 2 necesita que silver orders
  ya esté actualizado (ítems, pagos, reseñas, SCD2 de clientes).
- Reintentos ante `Glue.ConcurrentRunsExceededException`: 8 intentos, backoff 1.5, máximo 300 s, jitter.
- Cada job se lanza con `ExecutionClass: STANDARD` (ver [lecciones](08_lecciones_aprendidas.md)).
- **Controles**: consultas de calidad en Athena sobre las tablas que tocó la corrida (ver
  [calidad](06_calidad_seguridad_monitoreo.md)).
- **Cerrar** avanza las marcas de agua solo si todo salió bien: una corrida fallida se reintenta completa.
- Timeout de la ejecución: 2 h.

## 5.4 ¿Por qué no Glue Workflows?

| Necesidad | Glue Workflows | Lambda + Step Functions |
|---|---|---|
| Correr solo lo que cambió | difícil (triggers fijos) | plan por entidad y marca de agua |
| Varias fuentes (S3, horario, manual) | limitado | EventBridge + una sola puerta de entrada |
| Control de concurrencia y pausa | no | candado, pausa y freno en S3 |
| Visualizar y auditar cada corrida | básico | gráfico de ejecución + historial en S3 |
| Probar la lógica sin AWS | no | 29 pruebas unitarias con dobles de S3/Glue/Athena |

`JobRunQueuingEnabled` (cola nativa de Glue) no lo acepta la integración de Step Functions; por eso los
reintentos están en la máquina de estados.

## 5.5 Operación

```bash
# Iniciar a mano solo ciertas entidades
aws lambda invoke --function-name g1-olist-orquestador \
  --payload '{"accion":"iniciar","entidades":["orders","reviews"]}' --cli-binary-format raw-in-base64-out out.json

# Pausar 60 minutos para correr jobs a mano sin chocar con el pipeline
aws lambda invoke --function-name g1-olist-orquestador \
  --payload '{"accion":"pausar","minutos":60,"por":"equipo","motivo":"mantenimiento"}' --cli-binary-format raw-in-base64-out out.json
```
