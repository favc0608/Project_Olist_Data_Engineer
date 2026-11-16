-- ============================================================
-- PROYECTO INTEGRADOR OLIST - GRUPO 1
-- Athena: silver de clientes SCD Tipo 2 (job g1_olist_silver_customers_scd2)
-- Workgroup g1-olist. Tabla Delta nativa: Athena lee el esquema y los datos del log Delta (sin crawler).
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.silver_customers_scd2
LOCATION 's3://trabajo-integrador-g1/erp/silver/customers_scd2/'
TBLPROPERTIES ('table_type'='DELTA');

-- C1. Una sola versión vigente por persona (debe devolver 0 filas)
-- SELECT customer_unique_id, count(*) FROM g1_olist_lakehouse.silver_customers_scd2
-- WHERE is_current GROUP BY 1 HAVING count(*) > 1;

-- C2. Vigencias sin solaparse: valid_to de una versión = valid_from de la siguiente (debe devolver 0 filas)
-- SELECT * FROM (
--   SELECT customer_unique_id, version, valid_to,
--          lead(valid_from) OVER (PARTITION BY customer_unique_id ORDER BY version) AS siguiente
--   FROM g1_olist_lakehouse.silver_customers_scd2)
-- WHERE siguiente IS NOT NULL AND valid_to <> siguiente;

-- C3. Personas con cambio de dirección (historia SCD2)
-- SELECT customer_unique_id, version, customer_zip_code_prefix, customer_city, customer_state,
--        valid_from, valid_to, is_current
-- FROM g1_olist_lakehouse.silver_customers_scd2
-- WHERE customer_unique_id IN (SELECT customer_unique_id FROM g1_olist_lakehouse.silver_customers_scd2
--                              GROUP BY 1 HAVING count(*) > 1)
-- ORDER BY customer_unique_id, version;

-- C4. Uso en gold: versión del cliente vigente a la fecha de compra
-- SELECT o.order_id, o.order_purchase_timestamp, c.sk_cliente, c.customer_city, c.customer_state
-- FROM g1_olist_lakehouse.silver_orders o
-- JOIN g1_olist_lakehouse.silver_customers s ON s.customer_id = o.customer_id
-- JOIN g1_olist_lakehouse.silver_customers_scd2 c
--   ON c.customer_unique_id = s.customer_unique_id
--  AND o.order_purchase_timestamp >= c.valid_from
--  AND (c.valid_to IS NULL OR o.order_purchase_timestamp < c.valid_to);

-- ============================================================
-- GOLD nuevo (2026-11-16): tablas Delta nativas (registradas tras la primera escritura)
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.gold_fact_entregas
LOCATION 's3://trabajo-integrador-g1/erp/gold/fact_entregas/' TBLPROPERTIES ('table_type'='DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.gold_dim_fecha
LOCATION 's3://trabajo-integrador-g1/erp/gold/dim_fecha/' TBLPROPERTIES ('table_type'='DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.gold_dim_cliente_scd2
LOCATION 's3://trabajo-integrador-g1/erp/gold/dim_cliente_scd2/' TBLPROPERTIES ('table_type'='DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.gold_dim_vendedor_scd2
LOCATION 's3://trabajo-integrador-g1/erp/gold/dim_vendedor_scd2/' TBLPROPERTIES ('table_type'='DELTA');
-- Reseñas (registradas 2026-11-16 tras la carga 2016-S2)
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.gold_fact_resenas
LOCATION 's3://trabajo-integrador-g1/erp/gold/fact_resenas/' TBLPROPERTIES ('table_type'='DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS g1_olist_lakehouse.silver_reviews
LOCATION 's3://trabajo-integrador-g1/erp/silver/reviews/' TBLPROPERTIES ('table_type'='DELTA');

-- Pregunta de negocio: tardanza por ruta y su efecto
-- SELECT ruta, count(*) pedidos, round(avg(case when llego_tarde then 1.0 else 0 end)*100,1) pct_tarde,
--        round(avg(dias_entrega),1) dias_prom
-- FROM g1_olist_lakehouse.gold_fact_entregas WHERE entregado GROUP BY 1 ORDER BY pedidos DESC;
