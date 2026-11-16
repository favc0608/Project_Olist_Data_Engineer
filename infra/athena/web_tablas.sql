-- ============================================================
-- PROYECTO INTEGRADOR OLIST - GRUPO 1
-- Tablas de Athena (workgroup g1-olist) para la fuente web y la landing del ERP.
-- Sin crawlers: Parquet con partition projection y Delta nativo de Athena
-- (Athena lee los datos nuevos del log Delta; solo hay que recrear si cambian columnas).
-- ============================================================

-- 1. LANDING DEL ERP (copia cruda de los eventos del simulador y rechazados)
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.erp_landing_eventos (
  evento_id string, tipo string, ts_evento string, version int, payload string,
  message_id string, enviado_en string, recibido_en string
)
PARTITIONED BY (year string, month string, day string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true', 'mapping.message_id'='_message_id',
  'mapping.enviado_en'='_enviado_en', 'mapping.recibido_en'='_recibido_en')
LOCATION 's3://trabajo-integrador-g1/erp/landing/eventos/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.year.type'='integer', 'projection.year.range'='2026,2035',
  'projection.month.type'='integer', 'projection.month.range'='1,12', 'projection.month.digits'='2',
  'projection.day.type'='integer', 'projection.day.range'='1,31', 'projection.day.digits'='2',
  'projection.hour.type'='integer', 'projection.hour.range'='0,23', 'projection.hour.digits'='2',
  'storage.location.template'='s3://trabajo-integrador-g1/erp/landing/eventos/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.erp_landing_rechazados (
  message_id string, enviado_en string, recibido_en string, motivo string, cuerpo string
)
PARTITIONED BY (year string, month string, day string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true', 'mapping.message_id'='_message_id',
  'mapping.enviado_en'='_enviado_en', 'mapping.recibido_en'='_recibido_en',
  'mapping.motivo'='_motivo', 'mapping.cuerpo'='_cuerpo')
LOCATION 's3://trabajo-integrador-g1/erp/landing/rechazados/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.year.type'='integer', 'projection.year.range'='2026,2035',
  'projection.month.type'='integer', 'projection.month.range'='1,12', 'projection.month.digits'='2',
  'projection.day.type'='integer', 'projection.day.range'='1,31', 'projection.day.digits'='2',
  'projection.hour.type'='integer', 'projection.hour.range'='0,23', 'projection.hour.digits'='2',
  'storage.location.template'='s3://trabajo-integrador-g1/erp/landing/rechazados/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);

-- 2. BRONZE DE PRECIOS DE LA COMPETENCIA (Parquet, partición por fecha de captura)
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.web_bronze_precios (
  run_id string, modo string, capturado_en timestamp, version_sitio string, tienda string, sku_loja string,
  ean string, url string, titulo string, pagina int, precio_brl double, precio_lista_brl double,
  precio_usd double, precio_lista_usd double, precio_max_brl double, precio_cartao_brl double,
  precio_jsonld double, descuento_pct int, cuotas int, valor_cuota double, texto_estoque string,
  en_stock boolean, rating double, avaliacoes int, vendidos int, localizacao string, plus boolean,
  partnumber string, ficha_ok boolean, precio_ficha_brl double, fretes_json string,
  otros_vendedores_json string, sku_olist string, product_id string, precio_olist double,
  diferencia_pct double, `_source_file` string, reglas_ok boolean, `_ingested_at` timestamp
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://trabajo-integrador-g1/web/bronze/precios/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.year.type'='integer', 'projection.year.range'='2026,2035',
  'projection.month.type'='integer', 'projection.month.range'='1,12', 'projection.month.digits'='2',
  'storage.location.template'='s3://trabajo-integrador-g1/web/bronze/precios/year=${year}/month=${month}/'
);

-- 3. SILVER (Delta nativo: Athena toma el esquema del log Delta)
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.web_silver_precios_competencia
LOCATION 's3://trabajo-integrador-g1/web/silver/precios_competencia/'
TBLPROPERTIES ('table_type'='DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.web_silver_productos_competencia
LOCATION 's3://trabajo-integrador-g1/web/silver/productos_competencia/'
TBLPROPERTIES ('table_type'='DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.silver_reviews
LOCATION 's3://trabajo-integrador-g1/erp/silver/reviews/'
TBLPROPERTIES ('table_type'='DELTA');

-- 4. CONTROLES
-- W1. Una sola versión vigente por producto y tienda (debe devolver 0 filas)
-- SELECT tienda, sku_loja, count(*) FROM g1_olist_lakehouse.web_silver_precios_competencia
-- WHERE is_current GROUP BY 1, 2 HAVING count(*) > 1;
-- W2. Historia de un producto (cambios de precio con su vigencia)
-- SELECT tienda, sku_loja, precio_brl, valid_from, valid_to, is_current
-- FROM g1_olist_lakehouse.web_silver_precios_competencia WHERE sku_olist = 'OL-0037' ORDER BY tienda, valid_from;
-- W3. Precio vigente de la competencia frente al de Olist
-- SELECT sku_olist, tienda, precio_brl, precio_olist, round(100 * (precio_brl / precio_olist - 1), 1) AS dif_pct
-- FROM g1_olist_lakehouse.web_silver_precios_competencia WHERE is_current ORDER BY sku_olist, precio_brl;

-- ============================================================
-- 5. GOLD DE LA COMPETENCIA (vistas para Power BI, DirectQuery)
-- Se calculan sobre el silver SCD2: siempre reflejan la última corrida de los jobs.
-- ============================================================

-- 5.1 Precio vigente por producto y tienda, con ranking (1 = la más barata con stock)
CREATE OR REPLACE VIEW g1_olist_lakehouse.gold_v_competencia_precio_actual AS
SELECT p.sku_olist, p.product_id, pr.titulo, p.tienda,
       CASE p.tienda WHEN 'mercadoabierto' THEN 'Mercado Abierto' WHEN 'selva' THEN 'Selva'
                     WHEN 'compree' THEN 'Compree' WHEN 'ridley' THEN 'Ridley' END AS tienda_nombre,
       p.sku_loja, p.precio_brl, p.precio_lista_brl, p.descuento_pct, p.precio_usd, p.precio_cartao_brl, p.en_stock,
       p.precio_olist, round(100 * (p.precio_brl / p.precio_olist - 1), 2) AS diferencia_vs_olist_pct,
       p.precio_brl < p.precio_olist AS mas_barata_que_olist,
       rank() OVER (PARTITION BY p.sku_olist ORDER BY CASE WHEN p.en_stock THEN 0 ELSE 1 END, p.precio_brl) AS ranking_precio,
       p.valid_from AS vigente_desde, p.version_sitio
FROM g1_olist_lakehouse.web_silver_precios_competencia p
LEFT JOIN g1_olist_lakehouse.web_silver_productos_competencia pr ON pr.tienda = p.tienda AND pr.sku_loja = p.sku_loja
WHERE p.is_current;

-- 5.2 Serie diaria (hora de Lima): el precio vigente al cierre de cada día, expandiendo las versiones SCD2
CREATE OR REPLACE VIEW g1_olist_lakehouse.gold_v_competencia_precio_diario AS
WITH base AS (
  SELECT p.*, CAST(p.valid_from AT TIME ZONE 'America/Lima' AS date) AS desde,
         CAST(coalesce(p.valid_to, current_timestamp) AT TIME ZONE 'America/Lima' AS date) AS hasta
  FROM g1_olist_lakehouse.web_silver_precios_competencia p
), dias AS (
  SELECT b.*, date_add('day', n, b.desde) AS fecha
  FROM base b CROSS JOIN UNNEST(sequence(0, date_diff('day', b.desde, b.hasta))) AS t(n)
), ultimo AS (
  SELECT *, row_number() OVER (PARTITION BY tienda, sku_loja, fecha ORDER BY valid_from DESC) AS rn FROM dias
)
SELECT p.fecha, p.sku_olist, p.product_id, p.tienda,
       CASE p.tienda WHEN 'mercadoabierto' THEN 'Mercado Abierto' WHEN 'selva' THEN 'Selva'
                     WHEN 'compree' THEN 'Compree' WHEN 'ridley' THEN 'Ridley' END AS tienda_nombre,
       p.sku_loja, p.precio_brl, p.precio_lista_brl, p.descuento_pct, p.en_stock, p.precio_olist,
       round(100 * (p.precio_brl / p.precio_olist - 1), 2) AS diferencia_vs_olist_pct
FROM ultimo p WHERE p.rn = 1;

-- 5.3 Resumen por tienda (posicionamiento frente a Olist)
CREATE OR REPLACE VIEW g1_olist_lakehouse.gold_v_competencia_resumen_tienda AS
SELECT p.tienda,
       CASE p.tienda WHEN 'mercadoabierto' THEN 'Mercado Abierto' WHEN 'selva' THEN 'Selva'
                     WHEN 'compree' THEN 'Compree' WHEN 'ridley' THEN 'Ridley' END AS tienda_nombre,
       count(*) AS productos, sum(CASE WHEN p.en_stock THEN 0 ELSE 1 END) AS agotados,
       round(avg(100 * (p.precio_brl / p.precio_olist - 1)), 2) AS diferencia_media_vs_olist_pct,
       round(100.0 * avg(CASE WHEN p.precio_brl < p.precio_olist THEN 1 ELSE 0 END), 1) AS pct_productos_mas_baratos_que_olist,
       round(avg(p.descuento_pct), 1) AS descuento_medio_pct, max(p.valid_from) AS ultimo_cambio
FROM g1_olist_lakehouse.web_silver_precios_competencia p WHERE p.is_current GROUP BY p.tienda;
