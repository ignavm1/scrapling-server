"""Proveedores nuevos: Brave y lite-DuckDuckGo, sobre HTML real capturado.

Se agregaron por COBERTURA, no para hacer mas requests: un motor bloqueado ya
no deja la busqueda sin fuentes. El techo de MAX_FETCHES evita que sumar
proveedores multiplique el gasto.
"""
import pathlib
import pytest

from venara_discovery import blocking, config, extraction, filtering, providers
from venara_discovery.location import interpretar

FIX = pathlib.Path(__file__).parent / "fixtures"
leer = lambda n: (FIX / n).read_text(encoding="utf-8")
CTX = "agencia de marketing digital Lima Peru"


def utiles(html: str, motor: str) -> list:
    items, _ = extraction.extraer(None, html, motor)
    return [i for i in items
            if not filtering.motivo_descarte(i["url"], i["titulo"])
            and filtering.relevancia(CTX, i["titulo"], i["snippet"], i["url"]) >= 0.34]


# ── P1: extraen empresas reales ─────────────────────────────────────────────

@pytest.mark.parametrize("fixture,motor,minimo", [
    ("brave_ok.html", "brave", 5),
    ("ddglite_ok.html", "ddglite", 4),
])
def test_los_proveedores_nuevos_extraen_empresas(fixture, motor, minimo):
    u = utiles(leer(fixture), motor)
    assert len(u) >= minimo, "%s solo dio %d empresas utiles" % (fixture, len(u))
    assert all(x["url"].startswith("https://") for x in u)


def test_aportan_empresas_que_los_viejos_no_traen():
    # El valor de sumar una fuente es lo que trae de NUEVO. Si Brave devolviera
    # exactamente lo mismo que DuckDuckGo, seria un request desperdiciado.
    de_ddg = {x["url"] for x in utiles(leer("ddg_companies.html"), "duckduckgo")}
    de_brave = {x["url"] for x in utiles(leer("brave_ok.html"), "brave")}
    nuevas = de_brave - de_ddg
    assert len(nuevas) >= 3, "Brave solo aporto %d empresas nuevas" % len(nuevas)


def test_marcador_extrae():
    print("\nPROVEEDORES EXTRAEN")


# ── P2: URLs bien decodificadas ─────────────────────────────────────────────

def test_urls_de_ddglite_se_desenvuelven():
    # lite.duckduckgo envuelve todo en //duckduckgo.com/l/?uddg=...
    u = utiles(leer("ddglite_ok.html"), "ddglite")
    assert not any("duckduckgo.com/l/" in x["url"] for x in u), "quedaron envoltorios sin decodificar"
    assert any("limadigital.pe" in x["url"] for x in u)


def test_urls_de_brave_son_directas():
    u = utiles(leer("brave_ok.html"), "brave")
    assert not any("search.brave.com" in x["url"] for x in u)


def test_marcador_urls():
    print("\nURLS DECODIFICADAS")


# ── P3: deteccion de bloqueo en los motores nuevos ──────────────────────────

@pytest.mark.parametrize("fixture", ["brave_ok.html", "ddglite_ok.html"])
def test_CONTROL_las_paginas_buenas_no_se_marcan_bloqueadas(fixture):
    v = blocking.analizar(leer(fixture), 200)
    assert v.bloqueado is False, "%s marcado como %s" % (fixture, v.motivo)


def test_el_captcha_de_mojeek_SI_se_detecta():
    """Mojeek quedo fuera por esto, y su pagina destapo un hueco del detector.

    Sus unicos enlaces "externos" son subdominios propios (blog., community.),
    asi que el conteo de anclas cruzaba el umbral y la pagina pasaba por buena.
    El <title> dice literalmente "Captcha": ninguna pagina de resultados
    legitima se titula asi.
    """
    v = blocking.analizar(leer("mojeek_captcha.html"), 200)
    assert v.bloqueado is True and v.motivo == "captcha"


def test_el_titulo_no_marca_paginas_legitimas():
    # Control positivo del criterio nuevo: los 4 motores que funcionan tienen
    # la query en el <title>, no una palabra de bloqueo.
    for f in ("brave_ok.html", "ddglite_ok.html", "ddg_companies.html", "bing_ok.html"):
        assert not blocking._TITULOS_BLOQUEO.match(blocking.titulo_de(leer(f))), f


def test_marcador_bloqueo():
    print("\nBLOQUEO OK EN MOTORES NUEVOS")


# ── Presupuesto: mas fuentes no puede significar mas gasto ──────────────────

def test_hay_techo_de_fetches_por_busqueda():
    from venara_discovery import queries
    plan = queries.construir("agencia de marketing digital", interpretar("Lima, Peru"))
    combinaciones = len(plan) * len(providers.activos())
    assert combinaciones > config.MAX_FETCHES, "el techo no esta ejercitado por este caso"
    assert config.MAX_FETCHES <= 14, "el techo es demasiado alto: provocaria captchas"


def test_las_urls_de_cada_proveedor_se_construyen():
    ubi = interpretar("Lima, Peru")
    for p in providers.activos():
        u = providers.construir_url(p.nombre, "agencia marketing", ubi)
        assert u.startswith("https://"), p.nombre
        assert "agencia" in u


# ── Defectos vistos en la corrida EN VIVO con 4 proveedores ────────────────

@pytest.mark.parametrize("titulo,url,esperado", [
    ("Webtilia webtilia.com en Multicultural Digital Marketing Agency",
     "https://webtilia.com/en", "Webtilia"),
    ("Staff digital staffdigital.pe portada » Agencia Digital en Lima",
     "https://staffdigital.pe/", "Staff digital"),
    ("Impuestos YA! impuestosya.cl", "https://impuestosya.cl/", "Impuestos YA!"),
])
def test_el_titulo_de_brave_se_corta_donde_empieza_el_dominio(titulo, url, esperado):
    # Brave arma el titulo como <nombre> <dominio> <ruta> <descripcion>. Sin
    # cortarlo, el cliente ve "Webtilia webtilia.com en Multicultural..." como
    # razon social del lead.
    from venara_discovery.normalize import mejor_nombre
    assert mejor_nombre(titulo, url, CTX) == esperado


def test_CONTROL_un_titulo_sin_dominio_no_se_recorta():
    from venara_discovery.normalize import mejor_nombre, recortar_en_dominio
    assert recortar_en_dominio("Kallpa Creativa", "https://kallpacreativa.com/") == "Kallpa Creativa"
    assert mejor_nombre("Kallpa Creativa", "https://kallpacreativa.com/", CTX) == "Kallpa Creativa"


@pytest.mark.parametrize("url", [
    "https://ar.jooble.org/trabajo-estudio-contable",
    "https://www.laborum.cl/empleos-contador",
    "https://cl.talent.com/jobs",
])
def test_las_bolsas_de_trabajo_se_descartan(url):
    # Publican vacantes del rubro, asi que rankean alto en cualquier busqueda
    # por nicho+ciudad, y no son la empresa que el cliente quiere contactar.
    assert filtering.motivo_descarte(url) == "directorio", filtering.motivo_descarte(url)


def test_CONTROL_una_empresa_real_no_cae_en_el_filtro_de_empleo():
    for u in ("https://impuestosya.cl/", "https://webtilia.com/", "https://staffdigital.pe/"):
        assert filtering.es_empresa_candidata(u), filtering.motivo_descarte(u)
