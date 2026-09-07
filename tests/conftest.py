"""Configuracion comun de la suite.

Desde el arreglo del hallazgo 1 (auth que fallaba abierta), el servidor RECHAZA
con 503 cuando no hay API_KEY configurada. Eso es lo correcto en produccion y
rompe una suite que llama a los endpoints sin credencial.

Se usa el MISMO escape que documenta config.py -- `PERMITIR_SIN_AUTH` -- en vez
de inventar un modo de test aparte: si el escape se rompe, la suite se entera.
El comportamiento sin escape lo cubre tests/test_seguridad_cso.py, que lo apaga
a proposito.
"""
import pytest

from venara_discovery import config


@pytest.fixture(autouse=True)
def _permitir_sin_auth(monkeypatch):
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", True)
    monkeypatch.setattr(config, "API_KEY", None)
