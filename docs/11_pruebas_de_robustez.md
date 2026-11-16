# 11. Pruebas de robustez

Cómo demostrar que el pipeline soporta datos transaccionales (streaming) e incrementales (lotes), datos
repetidos, datos malos y fallos. Cada prueba indica qué hacer y qué evidencia mostrar.

## 11.1 Datos transaccionales en tiempo real
1. En el Colab del simulador: periodo ≥ 7 días (para que haya entregas y reseñas), 1-2 minutos de duración.
2. En ~60 s aparecen CSV nuevos en `erp/landing/{orders,order_items,payments,customers,reviews}/year=2026/...`
   y la copia cruda en `erp/landing/eventos/`.
3. Step Functions → *Start execution* con `{}` (o esperar al disparador automático).
4. Evidencia en Athena:
   ```sql
   SELECT date_format(fecha_compra, '%Y-%m') mes, count(*) pedidos,
          sum(CASE WHEN entregado THEN 1 ELSE 0 END) entregados
   FROM g1_olist_lakehouse.gold_fact_entregas WHERE year(fecha_compra) = 2026 GROUP BY 1 ORDER BY 1;
   ```

## 11.2 Lote incremental del ERP
1. Generar un mes que todavía no se cargó:
   `python scripts/generar_lotes_historicos.py --desde 2018-01 --hasta 2018-01 --salida build/demo`
2. Subir los archivos a `erp/landing/<entidad>/year=2018/month=01/` (consola de S3 o `aws s3 sync`).
3. Ejecutar el pipeline y comprobar que solo corrieron los jobs de las entidades con archivos nuevos
   (gráfico de la ejecución) y que aparece 2018-01 en el conteo por mes.

## 11.3 Idempotencia (el mismo archivo dos veces)
Volver a subir un archivo ya procesado y ejecutar el pipeline. Evidencia: los controles de duplicados siguen en 0
y el conteo de `silver_orders` no cambia.
```sql
SELECT count(*), count(DISTINCT order_id) FROM g1_olist_lakehouse.silver_orders;
```

## 11.4 SCD Tipo 1 (actualización en el lugar)
Subir un CSV de orders con pedidos existentes que pasan de `shipped` a `delivered` (con su fecha de entrega).
Evidencia: el pedido sigue siendo una sola fila, con el estado nuevo y `fecha_procesamiento_silver` actualizada.

## 11.5 SCD Tipo 2 (historia)
Subir un CSV de customers con un `customer_unique_id` existente, un `customer_id` nuevo y otra ciudad, más su pedido.
Evidencia:
```sql
SELECT customer_unique_id, version, customer_city, valid_from, valid_to, is_current
FROM g1_olist_lakehouse.silver_customers_scd2
WHERE customer_unique_id = '<id>' ORDER BY version;
```
La versión anterior queda cerrada (`valid_to` = fecha de la compra nueva) y la nueva vigente.

## 11.6 Datos malos
- **Evento inválido** (streaming): aparece en `erp/landing/rechazados/` con `_motivo`; el resto del lote se procesa.
- **Archivo con columnas incorrectas** (lote): el job de bronze falla con un mensaje claro, la ejecución termina en
  `RegistrarFallo` y llega el correo de alerta. **Después:** retirar el archivo de landing y volver a ejecutar
  (el freno se activa tras 2 fallos seguidos; un inicio manual exitoso lo libera).

## 11.7 Concurrencia
Lanzar dos inicios seguidos: el segundo encuentra el candado y queda como pendiente; la revisión de pendientes lo
retoma cuando termina el primero. Evidencia: `erp/_control/orquestacion/pendiente.json` y una sola ejecución activa.

## 11.8 Pruebas automáticas (sin AWS)
```bash
python -m unittest discover -s tests -v
```
Cubren el plan incremental por entidad, el candado, la pausa, el freno por fallos, la detección de jobs ocupados,
los controles de calidad y la conversión de eventos a CSV de Olist.
