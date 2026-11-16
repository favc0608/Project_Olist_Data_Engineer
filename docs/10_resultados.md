# 10. Resultados

Cifras obtenidas de la capa gold tras cargar el histórico de **2016-09 a 2017-12** (lotes mensuales) más las ventas
sintéticas del simulador. Todas las consultas son sobre Athena.

## 10.1 Volumen procesado

| Métrica | Valor |
|---|---|
| Pedidos en silver | 45,444 |
| Ítems vendidos (gold_fact_sales) | 51,252 |
| Reseñas (gold_fact_resenas) | 45,370 |
| Vendedores con ventas | 1,814 |
| GMV (precio de productos) | R$ 6.21 M |
| Versiones SCD2 de clientes | 44,140 para 44,048 personas (**89 personas cambiaron de dirección**) |
| Archivos de una carga mensual completa (2017) | 70 lotes CSV |

## 10.2 Rendimiento y costo del pipeline

| Corrida | Jobs | Duración | Resultado |
|---|---|---|---|
| Carga de 2017 completo (70 archivos) | 18 | **15 min** de punta a punta | 11/11 controles en 0 |
| Carga de 2016-S2 (19 archivos) | 18 | 14 min | 10/10 controles en 0 |
| Corrida incremental típica (una entidad) | 5-9 | 6-10 min | — |

- Bronze en paralelo: 6 jobs en ~4 min. Silver: ~2 min por job. Gold: ~2.5 min por job.
- Costo aproximado: **USD 0.30-0.50 por corrida completa** (Glue STANDARD, 2 DPU por job); el streaming
  (SQS + Lambda) cuesta centavos al mes.

## 10.3 Respuestas a las preguntas de negocio

**01. Categorías más vendidas (por GMV)**

| Categoría | Ítems | GMV (R$) |
|---|---|---|
| watches_gifts | 2,279 | 492,758 |
| bed_bath_table | 5,143 | 492,452 |
| health_beauty | 3,576 | 448,700 |
| sports_leisure | 4,028 | 444,414 |
| computers_accessories | 3,091 | 401,701 |

`bed_bath_table` vende más del doble de unidades que `watches_gifts` con el mismo GMV: ticket bajo vs. ticket alto.

**02. Tiempos de entrega y logística** (pedidos entregados)

| Indicador | Valor |
|---|---|
| Entregas tardías (entrega > fecha estimada) | **5.6%** |
| Días promedio de compra a entrega | 13.1 |
| % tarde si vendedor y cliente están en el mismo estado | 3.9% |
| % tarde si están en estados distintos | 6.5% |

Rutas con más atraso (≥ 500 pedidos): SP → RJ 12.4%, SP → BA 10.6%, SP → PE 9.8%, SP → SC 8.8%, SP → ES 8.6%.

**Impacto en la satisfacción** (`gold_fact_resenas`)

| Entrega | Reseñas | Puntaje promedio |
|---|---|---|
| A tiempo | 41,278 | **4.28** |
| Tarde | 2,399 | **2.32** |
| No entregado | 1,693 | 1.78 |

Un atraso baja la reseña casi 2 puntos: la logística interestatal desde SP es la palanca principal.

**03. Vendedores con mejor rendimiento**: se combina GMV (`gold_fact_sales`), % de entregas a tiempo
(`gold_fact_entregas`) y puntaje (`gold_fact_resenas`) por `seller_id`; el tablero de Power BI muestra el ranking.
El top 3 por GMV concentra R$ 469 mil (7.6% del total) y dos de ellos están en SP.

## 10.4 Calidad

Todas las corridas terminan con los controles automáticos en 0: sin duplicados por clave en silver ni gold, una sola
versión vigente por cliente y vendedor, vigencias que encadenan y ningún hecho huérfano de su dimensión.
