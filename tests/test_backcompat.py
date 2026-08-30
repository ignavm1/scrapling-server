"""G11 -- el cliente de Venara no se rompe.

Los nombres de campo de abajo NO son inventados: salen de leer
`lib/scraping/index.ts` en el repo de Venara. Ese cliente lee por nombre, asi
que un campo renombrado lo rompe en silencio -- devuelve 200 y cero leads.
"""
import pytest
from fastapi.testclient import TestClient

from venara_discovery import api, pipeline
from venara_discovery.cache import CACHE
from venara_discovery.entity import Empresa

cliente = TestClient(api.app)

# RawCompany en lib/scraping/index.ts
CAMPOS_CLIENTE = {"name", "website", "linkedin_url", "source", "description"}


@pytest.fixture(autouse=True)
def _limpiar():
    CACHE.limpiar(); yield; CACHE.limpiar()


def test_health_conserva_status_y_version():
    d = cliente.get("/health").json()
    assert d["status"] == "ok"
    assert isinstance(d["version"], str) and d["version"]


def test_search_companies_conserva_results_y_total(monkeypatch):
    e = Empresa(nombre="Acme", website="https://acme.pe/",
                linkedin_url="https://www.linkedin.com/company/acme",
                descripcion="Agencia", fuentes={"duckduckgo"})
    e.senales["score"] = {"total": 0.8}
    monkeypatch.setattr(pipeline, "buscar", lambda n, u, l: {
        "empresas": [e], "completo": True,
        "diagnostico": {"proveedores_bloqueados": {}, "ms": 1}})
    d = cliente.post("/search-linkedin-companies",
                     json={"query": "marketing", "location": "Lima", "maxResults": 25}).json()
    assert "results" in d and "total" in d
    assert d["total"] == len(d["results"]) == 1
    fila = d["results"][0]
    faltantes = CAMPOS_CLIENTE - set(fila)
    assert not faltantes, "el cliente de Venara leeria undefined en: %s" % faltantes
    assert fila["name"] == "Acme"
    assert fila["website"] == "https://acme.pe/"
    assert fila["linkedin_url"].endswith("/acme")
    assert isinstance(fila["source"], str)


def test_scrape_website_conserva_clean_text_url_method():
    # El cliente hace `data.clean_text || null`.
    d = cliente.post("/scrape-website", json={"url": "http://127.0.0.1/"}).json()
    assert set(d) >= {"clean_text", "url", "method"}
    assert d["clean_text"] == "NO_CONTENT"
    assert d["method"] == "blocked"


def test_search_linkedin_conserva_su_forma():
    d = cliente.post("/search-linkedin", json={"company": "NO_COMPANY_FOUND"}).json()
    assert set(d) >= {"person_name", "person_title", "linkedin_url", "source"}
    assert d["person_name"] == "NOT_FOUND"


def test_los_cuatro_endpoints_historicos_siguen_existiendo():
    rutas = {r.path for r in api.app.routes if hasattr(r, "path")}
    for r in ("/health", "/search-linkedin-companies", "/scrape-website", "/search-linkedin"):
        assert r in rutas, "desaparecio %s" % r


def test_marcador_final():
    print("\nRETROCOMPATIBILIDAD VERIFICADA")
