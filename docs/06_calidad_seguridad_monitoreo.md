# 6. Calidad, seguridad y monitoreo

## 6.1 Calidad de datos en tres niveles

| Nivel | Dónde | Qué valida |
|---|---|---|
| Al ingerir | Lambda consumidora | contrato del evento (tipo, campos obligatorios, fechas); inválidos → `rechazados/` |
| En cada job | Glue (antes de escribir) | encabezados esperados, nulos en claves, duplicados, conteos antes/después, rangos (zip, estado) |
| Al final de cada corrida | estado `Controles` (Athena) | reglas entre tablas sobre el resultado ya publicado |

**Controles del estado `Controles`** (todos deben devolver 0 filas):

| Control | Tabla |
|---|---|
| `order_id` duplicado | silver_orders |
| `customer_id` duplicado | silver_customers |
| más de una versión vigente por persona | silver_customers_scd2 |
| vigencias que no encadenan (`valid_to` ≠ siguiente `valid_from`) | silver_customers_scd2 |
| más de una versión vigente por vendedor | silver_sellers |
| ítem duplicado | gold_fact_sales |
| pedido duplicado | gold_fact_entregas |
| `sk_cliente` sin fila en la dimensión (huérfano) | gold_fact_entregas |
| entregado sin fecha de entrega | gold_fact_entregas |
| `customer_id` duplicado | gold_dim_customers |
| reseña duplicada | gold_fact_resenas |

Los resultados se guardan en `erp/_control/orquestacion/controles/<ejecucion>.json`; si alguno falla, la ejecución
termina en error y se dispara la alerta.

## 6.2 Seguridad

- **Bucket privado** y cifrado del lado del servidor por defecto de S3 (SSE-S3).
- Sitio de la competencia en un bucket aparte, accesible solo por **CloudFront con OAC**; S3 directo = 403.
- **Roles IAM por servicio**: Lambda consumidora (escribe solo en `erp/landing/`), orquestador (Step Functions,
  Glue `StartJobRun`/`GetJobRuns`, Athena en su workgroup, lectura de las capas para los controles),
  rol de Glue para los jobs.
- **Usuario del simulador**: solo `sqs:SendMessage` sobre la cola de eventos; la access key se sube en memoria
  en Colab y nunca se guarda en el notebook.
- Workgroup de Athena con resultados en un prefijo propio y límite de 1 GB escaneado por consulta.
- Sin credenciales en el código ni en el repositorio (`.gitignore` + revisión automática al exportar).

**Pendientes reconocidos** (cuenta compartida de un curso): algunos jobs antiguos usan un rol heredado más amplio
que el necesario; migrarlos al rol del proyecto y agregar KMS propio serían los siguientes pasos.

## 6.3 Monitoreo y alertas

- **SNS**: regla de EventBridge ante ejecución `FAILED`/`TIMED_OUT`/`ABORTED` → tema de alertas → correo con el
  nombre de la ejecución y el error.
- **CloudWatch Logs** de las Lambdas y de cada job de Glue (salida y errores).
- **Alarmas** del streaming: mensajes en la DLQ y errores de la Lambda consumidora.
- **Historial propio**: cada ejecución, fallo y control queda en `erp/_control/orquestacion/`, consultable sin
  entrar a la consola.
