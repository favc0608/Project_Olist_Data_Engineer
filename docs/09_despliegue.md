# 9. Despliegue paso a paso

Guía para levantar el proyecto en una cuenta propia de AWS (región de referencia: `us-east-2`).
Costo estimado de una prueba completa: menos de USD 2.

## 9.0 Requisitos
- Cuenta de AWS con permisos sobre S3, Glue, Lambda, Step Functions, EventBridge, SQS, SNS, Athena, IAM,
  CloudFront y CloudFormation.
- AWS CLI v2 y Python 3.12 (`pip install boto3 pandas pyarrow kagglehub`).
- Dataset de Olist: `python scripts/descargar_dataset.py` (se guarda en `data/raw/olist/`, fuera del repo).

## 9.1 Bucket del lakehouse
Crear un bucket privado (en el proyecto: un solo bucket con prefijos `erp/` y `web/`). Los jobs y plantillas
reciben el nombre del bucket como parámetro o constante `BUCKET` al inicio de cada script.

## 9.2 Stack de streaming (`infra/cloudformation/g1-olist-streaming.yaml`)
Crea la cola SQS + DLQ, la Lambda consumidora, el usuario productor del simulador (solo `SendMessage`), el rol de
Glue del proyecto, el workgroup de Athena y alarmas.

```bash
python infra/scripts/empaquetar_lambda.py build/                     # zips con nombre por contenido
aws s3 cp build/consumidor-<hash>.zip s3://<bucket>/artefactos/lambda/
aws cloudformation deploy --stack-name g1-olist-streaming \
  --template-file infra/cloudformation/g1-olist-streaming.yaml --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides BucketLakehouse=<bucket> CodigoConsumidorKey=artefactos/lambda/consumidor-<hash>.zip
```

## 9.3 Jobs de Glue
```bash
python infra/scripts/crear_jobs_glue.py --rol arn:aws:iam::<ACCOUNT_ID>:role/g1-olist-glue-role \
  --bucket-scripts aws-glue-assets-<ACCOUNT_ID>-us-east-2
```

## 9.4 Stack de orquestación (`infra/cloudformation/g1-olist-orquestacion.yaml`)
Lambda orquestadora, máquina de Step Functions, reglas y horarios de EventBridge, tema SNS de alertas.
Activar `EventBridgeConfiguration` en el bucket para que S3 publique los eventos `Object Created`.

```bash
aws cloudformation deploy --stack-name g1-olist-orquestacion \
  --template-file infra/cloudformation/g1-olist-orquestacion.yaml --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides BucketLakehouse=<bucket> CodigoOrquestadorKey=artefactos/lambda/orquestador-<hash>.zip \
    EstadoDisparadores=ENABLED IntervaloMinimoMin=120 MaxFallosSeguidos=2 CorreoAlertas=<correo>
```

Parámetros útiles: `ExpresionHorario` (frecuencia del respaldo), `IntervaloMinimoMin` (bajarlo a 5 el día de una
demo), `EstadoDisparadores` (DISABLED para apagar todo lo automático).

## 9.5 Primera carga y tablas de Athena
```bash
python scripts/generar_lotes_historicos.py --desde 2016-09 --hasta 2017-12 --salida build/lotes
aws s3 sync build/lotes/erp/landing s3://<bucket>/erp/landing          # dispara el pipeline
```
Cuando la primera ejecución termine, registrar las tablas Delta nativas con
[`infra/athena/erp_tablas_y_controles.sql`](../infra/athena/erp_tablas_y_controles.sql) (las tablas Delta se
registran después de la primera escritura porque Athena lee el esquema del log).

## 9.6 Fuente web (opcional) (`infra/cloudformation/g1-olist-web.yaml`)
```bash
python competencia/generador/construir.py --fecha 2026-11-01          # genera competencia/sitio/
aws cloudformation deploy --stack-name g1-olist-web --template-file infra/cloudformation/g1-olist-web.yaml \
  --capabilities CAPABILITY_NAMED_IAM --parameter-overrides NombreBucketSitio=<bucket-sitio> BucketLakehouse=<bucket> ...
aws s3 sync competencia/sitio s3://<bucket-sitio>/ --cache-control max-age=60
```
Vistas gold de la competencia: [`infra/athena/web_tablas.sql`](../infra/athena/web_tablas.sql).

## 9.7 Pruebas locales
```bash
python -m unittest discover -s tests -v
python -m unittest discover -s competencia/tests -p "test_*.py"
```
