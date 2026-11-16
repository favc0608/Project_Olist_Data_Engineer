# 7. Decisiones de diseño

Formato corto de ADR (*Architecture Decision Record*): contexto, decisión, alternativas y consecuencias.

### D1. Arquitectura medallion con landing inmutable
- **Decisión:** landing (crudo) → bronze (Parquet append) → silver (Delta) → gold (Delta).
- **Por qué:** landing permite reprocesar todo desde el original; bronze conserva cada versión recibida;
  silver y gold son la verdad limpia y el modelo de negocio.
- **Alternativa descartada:** escribir directo a tablas limpias (se pierde la capacidad de reprocesar y auditar).

### D2. Delta Lake en silver y gold
- **Decisión:** Delta Lake con `MERGE` en silver/gold; Parquet append en bronze.
- **Por qué:** `MERGE` idempotente (reprocesar no duplica), SCD1/SCD2 nativos, transacciones ACID, Athena lee
  Delta de forma nativa.
- **Alternativas:** Parquet + overwrite (no incremental), Iceberg (también válido; Delta tenía mejor soporte
  en Glue 5.1 para el equipo).

### D3. Ingesta híbrida: lotes + streaming hacia el mismo formato
- **Decisión:** el streaming no tiene su propio pipeline: la Lambda convierte los eventos a **los mismos CSV del ERP**.
- **Por qué:** un solo camino de procesamiento (menos código, menos errores), y simula un ERP real que recibe
  ventas en línea y las expone en su formato.
- **Consecuencia:** la latencia es la del pipeline por lotes (minutos), suficiente para analítica.

### D4. SQS Standard en vez de FIFO
- **Por qué:** FIFO limita los lotes a 10 mensajes → miles de archivos diminutos en S3 y más costo de Glue.
- **Consecuencia aceptada:** entrega al menos una vez y sin orden → deduplicación y desempate por estado en silver.
- **Descartados:** Kinesis y Firehose (no habilitados en la cuenta del curso y más caros para este volumen).

### D5. Lambda + Step Functions en vez de Glue Workflows
- Ver [orquestación §5.4](05_orquestacion.md#54-por-qué-no-glue-workflows).

### D6. Estado de la orquestación en S3, sin base de datos
- **Decisión:** candado con escritura condicional (`If-None-Match`), marcas de agua y pendientes como JSON en S3.
- **Por qué:** cero infraestructura extra y costo nulo; S3 ofrece escritura condicional atómica.
- **Descartado:** DynamoDB (válido, pero un servicio más que administrar para un caso de una sola máquina).

### D7. SCD2 recalculado completo + `MERGE`
- **Por qué:** con datos tardíos y desordenados, comparar solo contra la versión vigente produce historias
  incorrectas. Recalcular es determinista e idempotente; a este volumen cuesta segundos.
- **Cuándo cambiarlo:** con cientos de millones de filas, recalcular solo las claves afectadas por el lote.

### D8. `valid_from` de negocio, nunca la hora del job
- **Por qué:** con la hora del job, reprocesar cambia la historia. Clientes usan la fecha de compra; vendedores, el
  periodo del lote; precios, la hora de captura.

### D9. Sin crawlers en cada corrida
- **Por qué:** cada corrida de crawler cobra mínimo 10 minutos. Las tablas Delta se registran una vez como
  tablas nativas de Athena (leen el esquema del log) y las tablas de eventos usan partition projection.

### D10. Convivir con el tablero existente
- **Decisión:** las tablas que ya consumía Power BI conservan nombre, columnas, tipos y ruta; lo nuevo va en
  tablas nuevas.
- **Por qué:** en un equipo, romper el trabajo de otro integrante cuesta más que duplicar una dimensión.

### D11. Glue STANDARD en el pipeline
- **Contexto:** FLEX usa capacidad sobrante (más barata) pero sin garantía; en pruebas los jobs esperaron
  10-30 min sin executors y fallaron por timeout.
- **Decisión:** el orquestador lanza cada job con `ExecutionClass: STANDARD` (+~USD 0.10 por corrida).

### D12. Fuente web simulada en vez de scrapear sitios reales
- **Por qué:** legal y reproducible; permite cambiar precios a voluntad y comparar lo extraído contra la
  "verdad" de cada versión para medir la exactitud del scraping.
