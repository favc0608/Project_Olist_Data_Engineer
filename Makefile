.PHONY: install test lint lotes jobs

install:            ## dependencias locales
	pip install -r requirements.txt -r requirements-dev.txt

test:               ## pruebas unitarias (sin AWS)
	python -m unittest discover -s tests -v
	python -m unittest discover -s competencia/tests -p "test_*.py"

lint:
	ruff check --select E9,F63,F7,F82 .

lotes:              ## lotes mensuales de 2017 en build/lotes
	python scripts/generar_lotes_historicos.py --desde 2017-01 --hasta 2017-12 --salida build/lotes

jobs:               ## crea/actualiza los jobs de Glue (requiere ROL y BUCKET)
	python infra/scripts/crear_jobs_glue.py --rol $(ROL) --bucket-scripts $(BUCKET)
