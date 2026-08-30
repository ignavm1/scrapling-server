"""G2 y G3 -- la API dice la verdad cuando esta bloqueada, y respeta el tope.

Sin red: se sustituye el pipeline. Lo que se prueba es el CONTRATO, no el
scraping.
"""
import pytest
from fastapi.testclient import TestClient

from venara_discovery import api, pipeline
from venara_discovery.cache import CACHE
from venara_discovery.entity import Empresa


@pytest.fixture(autouse=True)
def _limpiar():
    CACHE.limpiar()
    yield
    CACHE.limpiar()


cliente = TestClient(api.app)


def _falso(empresas, completo, bloqueados=None):
    def _f(nicho, ubicacion, limite):
        return {
            "empresas": empresas[:limite],
            "completo": completo,
            "diagnostico": {"proveedores_bloqueados": bloqueados or {},
                            "proveedores_ok": {} if not completo else {"duckduckgo": 10},
                            "crudos": len(empresas), "filtrados": 0,
                            "duplicados_fusionados": 0, "ranking": {}, "descartes": {},
                            "estrategias": [], "ubicacion": {}, "ms": 5,
                            "completo": completo, "metodos_extraccion": {},
                            "websites_resueltos": 0},
        }
    return _f


def test_blocked_reporta_el_bloqueo_y_no_lo_disfraza_de_cero(monkeypatch):
    # EL FALLO MAS CARO DEL SERVIDOR VIEJO (F1/F4): devolvia 200 con lista
    # vacia y Venara concluia "el nicho no tiene empresas". El nicho tenia
    # empresas; los buscadores estaban bloqueando.
    monkeypatch.setattr(pipeline, "buscar",
                        _falso([], False, {"duckduckgo": "captcha", "bing": "status-429"}))
    r = cliente.post("/search-linkedin-companies",
                     json={"query": "marketing", "location": "Lima", "maxResults": 25})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] == 0
    assert d["complete"] is False
    assert d["error"] == "providers_blocked"
    assert d["blocked_providers"] == {"duckduckgo": "captcha", "bing": "status-429"}
    print("\nCONTRATO BLOQUEO VERIFICADO")


def test_blocked_no_envenena_la_cache(monkeypatch):
    monkeypatch.setattr(pipeline, "buscar", _falso([], False, {"duckduckgo": "captcha"}))
    cliente.post("/search-linkedin-companies", json={"query": "x", "location": "Lima"})
    assert len(CACHE) == 0, "una busqueda bloqueada quedo cacheada 6 horas"


def test_una_busqueda_completa_SI_se_cachea(monkeypatch):
    monkeypatch.setattr(pipeline, "buscar",
                        _falso([Empresa(nombre="Acme", website="https://acme.pe/")], True))
    r1 = cliente.post("/search-linkedin-companies", json={"query": "x", "location": "Lima"})
    assert r1.json()["cached"] is False
    r2 = cliente.post("/search-linkedin-companies", json={"query": "x", "location": "Lima"})
    assert r2.json()["cached"] is True


def test_maxresults_camelCase_controla_el_tope(monkeypatch):
    # F8: el cliente de Venara manda camelCase; el modelo viejo solo declaraba
    # snake_case, asi que Pydantic descartaba el campo y usaba el default.
    # Pedir 50 devolvia 25.
    muchas = [Empresa(nombre="E%d" % i, website="https://e%d.pe/" % i) for i in range(40)]
    monkeypatch.setattr(pipeline, "buscar", _falso(muchas, True))
    r = cliente.post("/search-linkedin-companies",
                     json={"query": "x", "location": "Lima", "maxResults": 7})
    assert r.json()["total"] == 7, "maxResults ignorado"

    CACHE.limpiar()
    r2 = cliente.post("/search-linkedin-companies",
                      json={"query": "x", "location": "Lima", "max_results": 5})
    assert r2.json()["total"] == 5, "max_results ignorado"
    print("\nMAXRESULTS VERIFICADO")


def test_limites_de_request(monkeypatch):
    monkeypatch.setattr(pipeline, "buscar", _falso([], True))
    # Agotamiento de recursos: una query gigante o un tope absurdo se rechazan
    # en el borde, antes de gastar un solo fetch.
    assert cliente.post("/search-linkedin-companies",
                        json={"query": "x" * 5000}).status_code == 422
    assert cliente.post("/search-linkedin-companies",
                        json={"query": "x", "maxResults": 100000}).status_code == 422
    assert cliente.post("/search-linkedin-companies", json={"query": ""}).status_code == 422
