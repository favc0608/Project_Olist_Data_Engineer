# 4. Procesamiento y SCD

## 4.1 Jobs por capa

| Capa | Jobs | Patrón |
|---|---|---|
| Bronze (9) | orders, order_items, customers, productos, sellers, reviews, category_translation, payments, geolocation | lee landing con **job bookmark** (solo archivos nuevos), valida encabezados y claves, agrega trazabilidad, append Parquet particionado |
| Silver (10) | orders, order_items, customers, customers_scd2, productos, sellers, reviews, category_translation, payments, geolocation | deduplica, tipa, estandariza, `row_hash`, Delta `MERGE` |
| Gold (6) | ventas, dimensiones, entregas, reseñas, payments, geolocation | joins a versiones SCD2, métricas, Delta `MERGE` |
| Web (3) | bronze_precios, silver_precios_competencia (SCD2), silver_productos_competencia | igual que el ERP, sobre JSONL del scraper |

Convenciones (todas en el código):
- Glue 5.1, 2 workers G.1X, sesión en UTC, secciones numeradas y mensajes en español.
- Cada job **valida antes de escribir** (encabezados, nulos en claves, duplicados, conteos antes/después)
  y lanza `ValueError` si algo falla: el orquestador lo captura y alerta.
- Nunca `sys.exit()`: Glue 5.1 marca como FAILED un job que termina con `sys.exit(0)`. Se usa `if/else`.
- Cargas idempotentes: bookmark + `MERGE` en vez de `overwrite`/`errorifexists`.

## 4.2 SCD Tipo 1: `MERGE` con `row_hash`

Se usa cuando solo importa el valor actual (estado del pedido, atributos de producto).

```python
# Huella de las columnas de negocio: si no cambió, no se reescribe la fila
df = df.withColumn("row_hash", F.sha2(F.concat_ws("||", *columnas_negocio), 256))

(DeltaTable.forPath(spark, DESTINO).alias("t")
    .merge(df.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")   # solo si cambió
    .whenNotMatchedInsertAll()
    .execute())
```

Detalles que lo hacen robusto:
- **Desempate por avance del estado** en pedidos: si en un mismo lote llegan `shipped` y `delivered` del mismo
  pedido, gana el más avanzado (los eventos de SQS pueden llegar desordenados).
- **Evolución de esquema**: al agregar `row_hash` a una tabla existente, un `MERGE` con condición sobre la
  columna nueva necesita `ALTER TABLE ... ADD COLUMNS` antes (`autoMerge` no alcanza) y actualizar el catálogo
  para que Athena no falle con `INVALID_GLUE_SCHEMA`.

## 4.3 SCD Tipo 2: historia recalculada + `MERGE`

Se usa cuando importa **cómo era el dato en el momento del hecho**: la dirección del cliente cuando compró, la
ciudad del vendedor cuando vendió, el precio de la competencia en cada día.

Columnas comunes: `valid_from`, `valid_to`, `is_current`, `version`, `row_hash` y clave sustituta
`sk = sha2(id ‖ valid_from)`.

**Clientes** (`silver_customers_scd2`): en Olist cada pedido crea un `customer_id` nuevo; la persona es
`customer_unique_id`. Cada pedido es una "foto" de la dirección en la fecha de compra. Se ordenan las fotos por
fecha y se abre una versión nueva solo cuando cambia zip, ciudad o estado.

```python
# (simplificado; ver glue/silver/g1_olist_silver_customers_scd2.py)
# 1. Una foto por pedido: dirección + fecha de compra
# 2. Se compara cada foto con la anterior de la misma persona; se abre versión solo si cambió el hash
w = Window.partitionBy("customer_unique_id").orderBy("fecha_compra")
fotos = fotos.withColumn("cambio", F.col("attr_hash") != F.lag("attr_hash").over(w))
# 3. valid_from = compra con esa dirección; valid_to = compra con la dirección siguiente
# 4. MERGE por clave sustituta
(DeltaTable.forPath(spark, DESTINO).alias("t")
    .merge(versiones.alias("s"), "t.sk_cliente = s.sk_cliente")
    .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash OR NOT (t.valid_to <=> s.valid_to)")  # cierra versiones
    .whenNotMatchedInsertAll()                 # versiones nuevas
    .whenNotMatchedBySourceDelete()            # versiones que un pedido atrasado dejó sin efecto
    .execute())
```

**¿Por qué recalcular la historia completa en vez de comparar solo contra la versión vigente?**
Porque los datos llegan **tarde y desordenados** (streaming + lotes). Si un pedido de hace dos meses llega hoy,
la historia correcta cambia en el medio, no al final. Recalcular desde silver y aplicar con `MERGE` da siempre
el mismo resultado (idempotente) y el costo es bajo para el volumen del proyecto.

**Vendedores** (`silver_sellers`): `valid_from` = primer día del periodo del lote del ERP en que llegó el cambio
(no la hora del job, que haría la historia irreproducible).

**Precios de la competencia**: `valid_from` = hora de la captura; solo se historizan capturas que pasaron las
reglas de validación del scraper.

## 4.4 Unión de hechos con dimensiones SCD2

```sql
-- Versión del cliente vigente al momento de la compra
SELECT o.order_id, c.sk_cliente, c.customer_city, c.customer_state
FROM silver_orders o
JOIN silver_customers s       ON s.customer_id = o.customer_id
JOIN silver_customers_scd2 c  ON c.customer_unique_id = s.customer_unique_id
 AND o.order_purchase_timestamp >= c.valid_from
 AND (c.valid_to IS NULL OR o.order_purchase_timestamp < c.valid_to);
```

En gold se hace lo mismo en PySpark; si la compra es anterior a la primera versión conocida se usa la primera y
se marca `sk_cliente_estimada = true`, para no perder hechos ni esconder la aproximación.
