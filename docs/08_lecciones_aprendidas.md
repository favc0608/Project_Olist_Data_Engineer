# 8. Lecciones aprendidas

Problemas reales que aparecieron al construir y operar el pipeline, y cómo se resolvieron.

| # | Síntoma | Causa | Solución |
|---|---|---|---|
| 1 | Jobs que nunca arrancan y fallan por timeout | Glue **FLEX** sin capacidad disponible ("Initial job has not accepted any resources") | El orquestador fuerza `ExecutionClass: STANDARD` en cada `StartJobRun` |
| 2 | Un job que "termina bien" aparece como FAILED | Glue 5.1 trata `sys.exit(0)` como error (`SYSTEM_EXIT_ERROR`) | Usar `if/else`; nunca `sys.exit` en un job |
| 3 | `DELTA_MERGE_UNRESOLVED_EXPRESSION` al agregar `row_hash` | Un `MERGE` con condición sobre una columna nueva no se resuelve con `autoMerge` | `ALTER TABLE delta.\`ruta\` ADD COLUMNS (...)` antes del `MERGE` |
| 4 | Athena: `INVALID_GLUE_SCHEMA` tras agregar una columna | El catálogo seguía con el esquema viejo | Actualizar la tabla del catálogo junto con el cambio de esquema |
| 5 | Athena: `No path property defined` en tablas Delta | Tabla registrada al estilo Spark sin `SerdeInfo.Parameters.path` | Registrar como Delta nativa (`table_type='DELTA'`) o agregar `path` |
| 6 | Step Functions rechaza `JobRunQueuingEnabled` | La integración `glue:startJobRun` no admite ese parámetro | Reintentos con backoff en la máquina de estados |
| 7 | `Fn::Sub` no reemplaza una variable dentro de otra | Limitación de CloudFormation | Escribir los bloques repetidos en línea |
| 8 | Nombres de estado duplicados en la máquina | En ASL los nombres deben ser únicos en toda la máquina, también dentro de los Map | Nombres distintos por capa (`JobBronze`, `JobSilver`, …) |
| 9 | La Lambda no podía consultar Athena | Faltaba `s3:GetBucketLocation` sin condición de prefijo | Sentencia IAM separada para `GetBucketLocation` |
| 10 | Clientes duplicados al reenviar lotes | El mismo `customer_id` llegaba en varios archivos | Bronze deduplica quedándose con el archivo más reciente |
| 11 | El simulador "no generaba reseñas" | La reseña llega días después de la entrega; con periodos de pocos días no alcanza | Documentarlo: simular ≥ 7 días |
| 12 | Un job lanzado a mano chocaba con el pipeline | Dos corridas del mismo job a la vez | El orquestador detecta jobs ocupados y aplaza; pausa manual para mantenimiento |
| 13 | Un archivo malo dispararía corridas fallidas en bucle | Cada evento de S3 relanzaba el pipeline | Freno tras 2 fallos seguidos + alerta por correo |

**Sobre trabajar en equipo en una cuenta compartida**
- Prefijo obligatorio en todos los recursos y nunca tocar recursos ajenos.
- Antes de cambiar un job, bajar su script vigente desde S3 (la copia local puede estar desactualizada).
- Cambios de infraestructura siempre con *change set* revisado antes de aplicarse.
