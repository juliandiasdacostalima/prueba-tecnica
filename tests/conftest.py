import pytest


def pytest_addoption(parser):
    parser.addoption("--csv1", default=None, help="Ruta al primer CSV (referencia)")
    parser.addoption("--csv2", default=None, help="Ruta al segundo CSV (a comparar)")
    parser.addoption("--key-cols", default=None, help="Columnas clave separadas por coma")
    parser.addoption("--sep", default=";", help="Separador del CSV (por defecto: ;)")
    parser.addoption("--csv", default=None, help="Ruta al CSV a validar")
