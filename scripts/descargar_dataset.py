"""Descarga el dataset Olist desde Kaggle y lo copia a data/raw/olist/."""
import shutil
from pathlib import Path

import kagglehub

DESTINO = Path(__file__).resolve().parent.parent / "data" / "raw" / "olist"

# 1. DESCARGAR (kagglehub guarda en su cache local)
ruta_cache = Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))
print(f"Descargado en cache: {ruta_cache}")

# 2. COPIAR CSV AL PROYECTO
DESTINO.mkdir(parents=True, exist_ok=True)
for archivo in sorted(ruta_cache.glob("*.csv")):
    shutil.copy2(archivo, DESTINO / archivo.name)
    print(f"Copiado: {archivo.name} ({archivo.stat().st_size / 1e6:.1f} MB)")

print(f"Dataset disponible en: {DESTINO}")
