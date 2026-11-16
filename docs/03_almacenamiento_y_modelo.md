# 3. Almacenamiento y modelo de datos

## 3.1 Un bucket, prefijos por sistema y capa

Se usa un solo bucket con prefijos `erp/` y `web/`, cada uno con sus capas. Ventajas: permisos IAM por prefijo,
un único lugar para el ciclo de vida y rutas predecibles para Athena.

| Capa | Formato | Partición | Escritura | Propósito |
|---|---|---|---|---|
| landing | CSV / JSONL.gz | `year=/month=` (negocio) o `year/month/day/hour` (llegada) | inmutable | fuente de verdad para reprocesar |
| bronze | Parquet (snappy) | `year/month` de negocio | append + job bookmark | copia fiel tipada + trazabilidad |
| silver | Delta Lake | — | `MERGE` por clave | entidades limpias, sin duplicados, con historia SCD |
| gold | Delta Lake | — | `MERGE` / overwrite | modelo estrella para negocio |

Trazabilidad en todas las capas: `_source_file`, `_ingested_at` (bronze), `fecha_procesamiento_silver`,
`fecha_procesamiento_gold`, `capa_datos`.

## 3.2 Entidades de origen (Olist)

| Entidad | Filas (dataset) | Clave | Notas del perfilado |
|---|---|---|---|
| orders | 99,441 | `order_id` | 97% delivered; 8 entregados sin fecha |
| order_items | 112,650 | `order_id + order_item_id` | 775 pedidos sin ítems |
| payments | 103,886 | `order_id + payment_sequential` | pagos combinados con voucher |
| reviews | 99,224 | `review_id + order_id` | `review_id` se repite 814 veces |
| customers | 99,441 | `customer_id` | la persona es `customer_unique_id` (96,096); 252 cambiaron de dirección |
| products | 32,951 | `product_id` | columnas con erratas de origen (`*_lenght`) |
| sellers | 3,095 | `seller_id` | 60% en SP |
| geolocation | 1,000,163 | — | 19,015 zips; agregar por zip antes de unir |
| category_translation | 71 | `product_category_name` | 2 categorías sin traducción (se completan en silver) |

**Reglas que se respetan en todo el pipeline:** zips siempre como texto de 5 dígitos (tienen ceros a la
izquierda), fechas `yyyy-MM-dd HH:mm:ss` en UTC, textos en portugués.

## 3.3 Catálogo

- Base de datos `g1_olist_lakehouse` en el Glue Data Catalog.
- **Tablas Delta nativas de Athena** (`table_type='DELTA'` + `LOCATION`): Athena lee el esquema y los datos del
  log de Delta, sin crawler. DDL en [`infra/athena/`](../infra/athena/).
- Tablas de eventos del streaming con **partition projection** (sin crawler ni `MSCK REPAIR`).
- Lección: una tabla Delta registrada "al estilo Spark" (`spark.sql.sources.provider=delta`) necesita
  `SerdeInfo.Parameters.path`; si no, Athena falla con `No path property defined`.

## 3.4 Modelo gold

Modelo estrella con hechos a distinto grano y dimensiones conformadas:

- `gold_fact_sales` (ítem): precio, flete, categoría en inglés, ciudad/estado del vendedor, `sk_cliente` y
  `sk_vendedor` de la versión SCD2 vigente a la compra (con bandera `*_estimada` si la compra es anterior a la
  primera versión conocida).
- `gold_fact_entregas` (pedido): días de aprobación, hasta el transportista, de transporte y totales; días
  estimados; `dias_atraso` (fecha de entrega − fecha estimada, por día); `llego_tarde`; `mismo_estado`; ruta
  `estado_vendedor -> estado_cliente`; regiones; valor de productos, flete y `% flete`.
- `gold_fact_resenas` (reseña): puntaje, negativa/positiva, días de respuesta, días desde la entrega y la
  misma regla de atraso que entregas, para medir el efecto del atraso en la satisfacción.
- `gold_dim_fecha`: 2016-01-01 a 2026-12-31, día de la semana ISO, mes en español, fin de semana, Black Friday y
  `fecha_presentacion` (el histórico desplazado 418 semanas para que conserve el día de la semana).
- `gold_dim_cliente_scd2`, `gold_dim_vendedor_scd2`: versiones con vigencia y región de Brasil.

Convivencia con el tablero existente: dos dimensiones creadas antes por CTAS (`gold_dim_products`,
`gold_dim_customers`) se refrescan con **la misma consulta, columnas, tipos y ruta**, para no romper Power BI;
todo lo nuevo se agregó como tablas nuevas.
