#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilidades compartidas para los scrapers individuales de cada tienda competidora.
100% Librería Estándar de Python (Zero-Dependencies: funciona sin instalar pip packages
tanto en local, AWS Lambda, AWS Glue Python Shell o Google Colab).
"""

import csv
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.request import urlopen

ROOT_DIR = Path(__file__).resolve().parent.parent
SEED_PATH = ROOT_DIR / "catalog_seed.json"
OUTPUT_DIR = ROOT_DIR / "scrapers" / "output"


class DOMNode:
    """Nodo ligero del DOM para consultar por tag, clase, id o atributo."""

    def __init__(self, tag: str, attrs: dict, parent=None):
        self.tag = tag
        self.attrs = attrs
        self.parent = parent
        self.children = []
        self.text_parts = []

    @property
    def classes(self) -> set:
        return set(self.attrs.get("class", "").split())

    def get(self, attr_name: str, default=""):
        return self.attrs.get(attr_name, default)

    def text(self, strip: bool = True) -> str:
        pieces = list(self.text_parts)
        for child in self.children:
            pieces.append(child.text(strip=False))
        raw = "".join(pieces)
        return clean_whitespace(raw) if strip else raw

    def matches(self, tag: str = None, class_name: str = None, attr_filter: dict = None) -> bool:
        if tag and self.tag != tag:
            return False
        if class_name and class_name not in self.classes:
            return False
        if attr_filter:
            for k, v in attr_filter.items():
                if v is True and k not in self.attrs:
                    return False
                elif v is not True and self.attrs.get(k) != v:
                    return False
        return True

    def find_all(self, tag: str = None, class_name: str = None, attr_filter: dict = None) -> list:
        results = []
        for child in self.children:
            if child.matches(tag=tag, class_name=class_name, attr_filter=attr_filter):
                results.append(child)
            results.extend(child.find_all(tag=tag, class_name=class_name, attr_filter=attr_filter))
        return results

    def find_one(self, tag: str = None, class_name: str = None, attr_filter: dict = None):
        for child in self.children:
            if child.matches(tag=tag, class_name=class_name, attr_filter=attr_filter):
                return child
            found = child.find_one(tag=tag, class_name=class_name, attr_filter=attr_filter)
            if found is not None:
                return found
        return None


class SimpleDOMParser(HTMLParser):
    """Construye un árbol DOMNode a partir de cualquier documento HTML5."""

    VOID_TAGS = {"meta", "link", "img", "br", "hr", "input", "source", "area", "col", "param"}

    def __init__(self):
        super().__init__()
        self.root = DOMNode("document", {})
        self.current = self.root

    def handle_starttag(self, tag, attrs):
        node = DOMNode(tag.lower(), dict(attrs), parent=self.current)
        self.current.children.append(node)
        if tag.lower() not in self.VOID_TAGS:
            self.current = node

    def handle_endtag(self, tag):
        tag_l = tag.lower()
        node = self.current
        while node.parent is not None:
            if node.tag == tag_l:
                self.current = node.parent
                return
            node = node.parent

    def handle_data(self, data):
        self.current.text_parts.append(data)


def parse_html(html_content: str) -> DOMNode:
    parser = SimpleDOMParser()
    parser.feed(html_content)
    return parser.root


def fetch_html(source_base: str, relative_path: str) -> str:
    """Obtiene el HTML desde el disco local o desde un bucket HTTP/S3."""
    if source_base.startswith("http://") or source_base.startswith("https://"):
        url = f"{source_base.rstrip('/')}/{relative_path.lstrip('/')}"
        with urlopen(url, timeout=15) as response:
            return response.read().decode("utf-8")
    file_path = Path(source_base) / relative_path
    return file_path.read_text(encoding="utf-8")


def clean_whitespace(text: str) -> str:
    """Elimina saltos de línea y espacios repetidos (limpieza de Dirty Data Trap #4)."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_price(raw_text: str) -> float:
    """
    Normaliza precios de distintos formatos regionales:
    - Mercado Abierto: '4.299' (punto como separador de miles) -> 4299.0
    - Selva: 'US$1,129.99' -> 1129.99
    - Compree: 'S/ 4,189.00' -> 4189.0
    - Ridley: 'S/ 4,099' -> 4099.0
    """
    if not raw_text:
        return 0.0
    cleaned = re.sub(r"[^\d.,]", "", raw_text)
    if not cleaned:
        return 0.0
    # Caso punto como separador de miles exacto (ej. 4.299 o 1.249)
    if re.match(r"^\d{1,3}(\.\d{3})+$", cleaned):
        cleaned = cleaned.replace(".", "")
    else:
        cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_sold_k(raw_text: str) -> int:
    """Convierte '1.4k vendidos' -> 1400 o '420 vendidos' -> 420."""
    if not raw_text:
        return 0
    match = re.search(r"([\d.]+)\s*(k)?", raw_text.lower())
    if not match:
        return 0
    val = float(match.group(1))
    if match.group(2) == "k":
        val *= 1000
    return int(round(val))


def load_seed() -> dict:
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


def save_store_output(store_name: str, records: list[dict]):
    """Guarda los resultados de una tienda en CSV y JSON dentro de scrapers/output/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_file = OUTPUT_DIR / f"{store_name}_scraped.json"
    csv_file = OUTPUT_DIR / f"{store_name}_scraped.csv"

    json_file.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")

    if records:
        fieldnames = list(records[0].keys())
        with csv_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

    return json_file, csv_file

