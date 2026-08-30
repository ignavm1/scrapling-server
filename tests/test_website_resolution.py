"""G8 -- una empresa vista solo en LinkedIn recupera su website.

Importa porque el cliente de Venara DESCARTA toda empresa sin website
(`if (!website) continue` en lib/scraping/index.ts). Cada website recuperado
aca es un lead que antes se perdia entero.
"""
from venara_discovery import website, providers
from venara_discovery.entity import Empresa
from venara_discovery.location import interpretar

UBI = interpretar("Lima, Peru")


def test_pertenece_a_acepta_el_dominio_de_la_empresa():
    assert website.pertenece_a("https://kallpacreativa.pe/", "Kallpa Creativa") >= 0.9
    assert website.pertenece_a("https://acmedigital.com/", "Acme Digital") >= 0.9


def test_pertenece_a_RECHAZA_directorios_y_terceros():
    # Sin esto, "el primer resultado que no sea LinkedIn" se toma como website
    # oficial y se asigna el sitio de un directorio o de un competidor.
    assert website.pertenece_a("https://clutch.co/pe/agencies", "Kallpa Creativa") == 0.0
    assert website.pertenece_a("https://www.linkedin.com/company/kallpa", "Kallpa Creativa") == 0.0
    assert website.pertenece_a("https://otraempresa.pe/", "Kallpa Creativa") < website.UMBRAL_WEBSITE


def test_resuelve_el_website_de_una_empresa_solo_de_linkedin(monkeypatch):
    solo_li = Empresa(nombre="Kallpa Creativa",
                      linkedin_url="https://www.linkedin.com/company/kallpa")
    con_web = Empresa(nombre="Otra", website="https://otra.pe/")

    class RFalsa:
        sirve = True
        page = None
        html = ""
    monkeypatch.setattr(website, "obtener", lambda u, p, s: RFalsa())
    monkeypatch.setattr(website, "extraer", None, raising=False)
    monkeypatch.setattr(website.extraction, "extraer", lambda page, html, motor: ([
        {"url": "https://clutch.co/pe/agencies", "titulo": "Directorio", "snippet": ""},
        {"url": "https://kallpacreativa.pe/", "titulo": "Kallpa Creativa", "snippet": ""},
    ], "css"))

    n = website.resolver_para([solo_li, con_web], UBI)
    assert n == 1
    assert solo_li.website == "https://kallpacreativa.pe/"
    assert solo_li.senales["website_resuelto"] is True
    assert con_web.website == "https://otra.pe/", "no debe tocar las que ya tienen website"


def test_no_asigna_nada_cuando_no_hay_candidato_confiable(monkeypatch):
    solo_li = Empresa(nombre="Kallpa Creativa",
                      linkedin_url="https://www.linkedin.com/company/kallpa")

    class RFalsa:
        sirve = True; page = None; html = ""
    monkeypatch.setattr(website, "obtener", lambda u, p, s: RFalsa())
    monkeypatch.setattr(website.extraction, "extraer", lambda page, html, motor: ([
        {"url": "https://sitio-sin-relacion.com/", "titulo": "Otra cosa", "snippet": ""},
    ], "css"))
    assert website.resolver_para([solo_li], UBI) == 0
    assert solo_li.website == "", "se invento un website"


def test_limpiar_html_quita_scripts():
    t = website.limpiar_html("<html><script>var x=1</script><p>Hola &amp; chau</p></html>")
    assert "var x" not in t and "Hola & chau" in t


def test_marcador_final():
    print("\nWEBSITE RESOLUTION VERIFICADA")
