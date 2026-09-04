"""Canal PERSONAS: decisores del rubro, sin partir de una empresa.

Lo que estos tests protegen, en orden de dano si se rompe:

  1. Que no entre un nombre inventado. Un falso positivo aca termina en un
     correo que dice "Hola Marketing,".
  2. Que un vacio diga POR QUE esta vacio. "No hay decisores publicados" y "no
     pudimos mirar" mandan a investigar lugares distintos.
  3. Que ninguna query gaste presupuesto en site:linkedin.com/in, medido muerto.
"""
import pathlib

import pytest
from fastapi.testclient import TestClient

from venara_discovery import api, extraction, personas
from venara_discovery.cache import CACHE
from venara_discovery.location import interpretar

FIX = pathlib.Path(__file__).parent / "fixtures"
# Fixture SINTETICO, no capturado. El prefijo lo declara: la medicion F7 de
# test_linkedin.py escanea capturas reales y este archivo, escrito a mano para
# probar el parseo, no puede contar como evidencia sobre el indice publico.
HTML_PERSONAS = (FIX / "sintetico_personas_serp.html").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _limpiar():
    CACHE.limpiar()
    yield
    CACHE.limpiar()


def _items():
    items, _ = extraction.extraer(None, HTML_PERSONAS, "duckduckgo")
    return items


# ── P1: extrae personas reales del HTML de un buscador ──────────────────────

def test_extrae_persona_cargo_y_empresa_de_una_pagina_de_equipo():
    it = [i for i in _items() if "onzamarketing" in i["url"]][0]
    encontrados = personas.extraer_personas(it["titulo"], it["snippet"], it["url"])
    por_nombre = {p["person_name"]: p for p in encontrados}
    assert "Matias Bravo" in por_nombre
    assert por_nombre["Matias Bravo"]["person_title"].lower() == "gerente general"
    assert por_nombre["Matias Bravo"]["company"] == "Onza Marketing"
    # Dos personas en la misma pagina: el canal existe para encontrar decisores,
    # no UNO por sitio.
    assert "Carolina Reyes" in por_nombre


def test_extrae_un_nombramiento_de_prensa():
    # La prensa no escribe "Paula Guzman - gerente comercial"; escribe "asume
    # como". Sin el puente verbal, toda la superficie de prensa se pierde.
    it = [i for i in _items() if "df.cl" in i["url"]][0]
    nombres = {p["person_name"]: p for p in
               personas.extraer_personas(it["titulo"], it["snippet"], it["url"])}
    assert "Paula Guzman" in nombres
    assert nombres["Paula Guzman"]["company"] == "Kadem"


def test_no_atribuye_a_un_resultado_la_persona_del_resultado_vecino():
    # La ventana de snippet del extractor cruzaba al resultado siguiente y le
    # colgaba a una nota de df.cl el gerente de otra empresa. Un snippet corto
    # pierde contexto; uno contaminado inventa un hecho.
    it = [i for i in _items() if "df.cl" in i["url"]][0]
    nombres = {p["person_name"] for p in
               personas.extraer_personas(it["titulo"], it["snippet"], it["url"])}
    assert "Matias Bravo" not in nombres, "el snippet arrastro el resultado vecino"


# ── P2: control negativo — lo que NO es una persona ─────────────────────────

@pytest.mark.parametrize("texto", [
    "Marketing Digital",            # rubro con forma de nombre
    "Santiago Chile",               # ciudad + pais
    "Gerente General",              # el cargo solo
    "Nuestro Equipo",               # encabezado de la pagina
    "Agencia Digital SPA",          # razon social
    "Juan",                         # un solo token
    "SEO SEM ADS",                  # siglas
    "Juan Perez 2026",              # con digitos
    "Enero Febrero",                # meses
    "juan perez",                   # sin mayusculas
])
def test_rechaza_lo_que_no_es_una_persona(texto):
    assert personas.es_nombre_de_persona(texto) is False, texto


@pytest.mark.parametrize("texto", [
    "Matias Bravo",
    "Maria de los Angeles Rojas",
    "Juan Pablo Undurraga",
    "Ignacia Soto",
])
def test_control_positivo_de_nombres_reales(texto):
    # Sin este control, el test de arriba pasaria con un detector que rechaza
    # TODO, y eso no seria un detector.
    assert personas.es_nombre_de_persona(texto) is True, texto


def test_una_pagina_sin_personas_no_produce_candidatos():
    it = [i for i in _items() if "agenciagl" in i["url"]][0]
    assert personas.extraer_personas(it["titulo"], it["snippet"], it["url"]) == []


def test_no_acepta_prosa_como_nombre_de_empresa():
    assert personas.limpiar_empresa("una agencia que trabaja con marcas") == ""
    assert personas.limpiar_empresa("Wenu Digital") == "Wenu Digital"


# ── P3: LinkedIn personal queda fuera, y se demuestra ───────────────────────

def test_descarta_perfiles_personales_de_linkedin():
    assert personas.es_perfil_linkedin("https://www.linkedin.com/in/matias-bravo") is True
    assert personas.es_perfil_linkedin("https://www.linkedin.com/company/onza") is False
    assert personas.es_perfil_linkedin("https://onzamarketing.cl/equipo") is False


def test_ninguna_query_del_canal_usa_linkedin():
    # F7: `site:linkedin.com/in` devuelve cero perfiles con control positivo
    # (el MISMO operador sobre /company devuelve 10). Gastar una query ahi es
    # gastarsela a todas las demas.
    ubi = interpretar("Santiago, Chile")
    plan = personas.construir_plan("agencia de marketing digital",
                                   ["gerente general", "gerente comercial"], ubi)
    assert plan, "el plan quedo vacio"
    for est in plan:
        assert "linkedin" not in est.query.lower(), est.query


def test_ninguna_ronda_reintroduce_linkedin():
    # La rotacion por ronda cambia las palabras de la query. Verificar solo la
    # ronda 0 dejaria una puerta abierta en las demas.
    ubi = interpretar("Santiago, Chile")
    for ronda in range(6):
        plan = personas.construir_plan("agencias", ["gerente general", "ceo"], ubi, ronda)
        assert plan
        for est in plan:
            assert "linkedin" not in est.query.lower(), (ronda, est.query)


def test_las_rondas_no_repiten_las_mismas_queries():
    # Sin rotacion, la ronda 3 pide lo mismo que la 1, el buscador devuelve lo
    # mismo, y la corrida se gasta redescubriendo gente ya guardada.
    ubi = interpretar("Santiago, Chile")
    firma = lambda r: "|".join(sorted(e.query for e in personas.construir_plan(
        "agencias de marketing", ["gerente general", "gerente comercial"], ubi, r)))
    assert firma(0) != firma(1)
    assert firma(1) != firma(2)


def test_el_segundo_cargo_nunca_repite_al_principal():
    # Repetirlo gastaria cuatro fetches (uno por proveedor) en la query que ya
    # se esta haciendo.
    ubi = interpretar("Santiago, Chile")
    for ronda in range(4):
        plan = personas.construir_plan("agencias", ["gerente general", "ceo"], ubi, ronda)
        queries = [e.query for e in plan]
        assert len(queries) == len(set(queries)), (ronda, queries)


def test_un_solo_cargo_no_genera_la_estrategia_duplicada():
    ubi = interpretar("Santiago, Chile")
    plan = personas.construir_plan("agencias", ["gerente general"], ubi, 0)
    assert not any(e.nombre == "equipo-2" for e in plan)


def test_control_positivo_el_detector_de_queries_prohibidas_si_caza_una_plantada():
    # El test de arriba es una ASUSENCIA, y una ausencia pasa tambien cuando el
    # detector esta roto. Se planta la query prohibida y se verifica que la ve.
    plantada = personas.Estrategia("plantada", 'site:linkedin.com/in "acme"', 99)
    assert "linkedin" in plantada.query.lower()


# ── P4: el vacio siempre dice por que ───────────────────────────────────────

class _RespuestaFalsa:
    def __init__(self, html="", bloqueado=False, motivo=""):
        self.html = html
        self.page = None
        self.error = motivo if bloqueado else ""
        self._sirve = bool(html) and not bloqueado

    @property
    def sirve(self):
        return self._sirve

    veredicto = None


def _sin_red(monkeypatch, html="", bloqueados=None):
    def _obtener(url, proveedor, salud, timeout=None):
        if bloqueados and proveedor in bloqueados:
            salud.marcar_bloqueado(proveedor, bloqueados[proveedor])
            return _RespuestaFalsa(bloqueado=True, motivo=bloqueados[proveedor])
        return _RespuestaFalsa(html=html)
    monkeypatch.setattr(personas, "obtener", _obtener)


def test_encuentra_personas_sin_tocar_la_red(monkeypatch):
    _sin_red(monkeypatch, html=HTML_PERSONAS)
    r = personas.buscar("agencia de marketing digital", ["gerente general"],
                        "Santiago, Chile", 25)
    nombres = {p["person_name"] for p in r["personas"]}
    assert {"Matias Bravo", "Carolina Reyes", "Paula Guzman"} <= nombres
    assert r["completo"] is True
    # El perfil de LinkedIn del fixture tiene que quedar contado como descarte
    # deliberado, no desaparecer en silencio.
    assert r["diagnostico"]["descartes"].get("linkedin_in_descartado", 0) >= 1


def test_un_bloqueo_no_se_disfraza_de_rubro_sin_decisores(monkeypatch):
    _sin_red(monkeypatch, html="", bloqueados={
        "duckduckgo": "captcha", "brave": "captcha",
        "bing": "status-429", "ddglite": "captcha"})
    r = personas.buscar("agencia de marketing digital", ["gerente general"],
                        "Santiago, Chile", 25)
    assert r["personas"] == []
    assert r["completo"] is False
    assert r["diagnostico"]["motivo_vacio"] == "providers_blocked"
    assert r["diagnostico"]["proveedores_bloqueados"]


def test_respeta_el_techo_de_fetches():
    # `buscar_persona()` de linkedin.py no tiene techo ni presupuesto y por eso
    # una sola consulta tardo 4m43s medidos contra produccion. Este canal no
    # puede repetir eso.
    ubi = interpretar("Santiago, Chile")
    plan = personas.construir_plan("agencias", ["gerente general", "gerente comercial"], ubi)
    from venara_discovery import config, providers
    assert len(plan) * len(providers.activos()) > config.MAX_FETCHES, \
        "el plan ya no excede el techo: este test dejo de probar algo"


# ── P5: contrato HTTP ───────────────────────────────────────────────────────

cliente = TestClient(api.app)


def test_el_endpoint_devuelve_personas_y_el_contrato_completo(monkeypatch):
    _sin_red(monkeypatch, html=HTML_PERSONAS)
    r = cliente.post("/search-people", json={
        "query": "agencia de marketing digital",
        "titles": ["gerente general", "gerente comercial"],
        "location": "Santiago, Chile", "maxResults": 10})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] >= 3 and d["complete"] is True
    uno = d["results"][0]
    for campo in ("person_name", "person_title", "company", "url", "score", "source"):
        assert campo in uno, campo


def test_el_endpoint_reporta_el_bloqueo_en_vez_de_un_cero_mudo(monkeypatch):
    _sin_red(monkeypatch, html="", bloqueados={"duckduckgo": "captcha"})
    r = cliente.post("/search-people", json={
        "query": "agencia de marketing digital", "titles": ["gerente general"],
        "location": "Santiago, Chile"})
    d = r.json()
    assert d["total"] == 0
    assert d["complete"] is False
    assert d["error"] == "providers_blocked"
    assert d["reason"] == "providers_blocked"


def test_un_bloqueo_no_envenena_la_cache(monkeypatch):
    _sin_red(monkeypatch, html="", bloqueados={"duckduckgo": "captcha"})
    cliente.post("/search-people", json={"query": "x", "titles": [], "location": "Chile"})
    assert len(CACHE) == 0, "una busqueda bloqueada quedo cacheada 6 horas"


def test_los_cargos_se_sanean_y_se_topean():
    req = api.PeopleSearchRequest(query="x", titles=[
        "  Gerente   General  ", "gerente general", "CEO", "a" * 200,
        "Fundador", "COO", "Director Comercial", "Gerente de Ventas"])
    cargos = req.cargos
    assert cargos[0] == "Gerente General"
    assert len(cargos) <= 6
    assert all(len(c) <= 60 for c in cargos)
    assert len({c.lower() for c in cargos}) == len(cargos), "quedaron cargos duplicados"


# ── P6: las paginas donde SI viven los nombres ──────────────────────────────
# Medido en vivo (2026-09-03): 67 resultados crudos de Bing produjeron UN
# candidato, y era falso. El snippet de un buscador casi nunca dice
# "Nombre - Cargo - Empresa". Devolver solo personas tiraba el 99% del valor.

@pytest.mark.parametrize("url,titulo", [
    ("https://onzamarketing.cl/nuestro-equipo/", "Onza"),
    ("https://acme.cl/quienes-somos", "Acme"),
    ("https://x.cl/about", "X"),
    ("https://summit.cl/expositores", "Summit"),
    ("https://gremio.cl/socios", "Gremio"),
    ("https://y.cl/algo", "Nuestro equipo - Y Agencia"),
    ("https://z.cl/pagina", "Plana ejecutiva | Z"),
])
def test_reconoce_una_pagina_que_lista_gente(url, titulo):
    assert personas.es_pagina_de_personas(url, titulo) is True, url


@pytest.mark.parametrize("url,titulo", [
    ("https://acme.cl/servicios", "Servicios de marketing digital"),
    ("https://acme.cl/blog/post-seo", "Como hacer SEO en 2026"),
    ("https://acme.cl/", "Agencia de marketing digital en Santiago"),
])
def test_control_negativo_no_marca_cualquier_pagina(url, titulo):
    # Sin este control, una heuristica que devuelve True siempre pasaria el
    # test de arriba, y el cliente gastaria un scrape por resultado.
    assert personas.es_pagina_de_personas(url, titulo) is False, url


def test_devuelve_las_paginas_aunque_ningun_snippet_nombre_a_nadie(monkeypatch):
    # El caso REAL medido: hay resultados utiles y el parser de snippets no
    # saca a nadie. Si la respuesta viniera vacia, el canal reportaria "no hay
    # decisores" sobre una busqueda que encontro seis paginas de equipo.
    html = HTML_PERSONAS.replace("Matias Bravo - Gerente General - Onza Marketing.", "") \
                        .replace("Carolina Reyes, Gerente Comercial de Onza Marketing,", "") \
                        .replace("Paula Guzman asume como", "alguien asume como") \
                        .replace("Rodrigo Fuentes - Director Comercial - Wenu Digital.", "") \
                        .replace("Ignacia Soto - Fundadora - Trama Agencia.", "")
    _sin_red(monkeypatch, html=html)
    r = personas.buscar("agencia de marketing digital", ["gerente general"],
                        "Santiago, Chile", 25)
    assert r["personas"] == []
    assert len(r["paginas"]) >= 3
    assert r["diagnostico"].get("motivo_vacio") is None, \
        "una busqueda con paginas utiles se reporto como vacia"


def test_las_paginas_de_personas_van_primero(monkeypatch):
    _sin_red(monkeypatch, html=HTML_PERSONAS)
    r = personas.buscar("agencia de marketing digital", ["gerente general"],
                        "Santiago, Chile", 25)
    marcas = [p["people_page"] for p in r["paginas"]]
    # El cliente gasta scrapes de arriba hacia abajo: si las paginas utiles no
    # van primero, el presupuesto se va en paginas de servicios.
    assert marcas == sorted(marcas, reverse=True), marcas
    assert marcas[0] is True


def test_ningun_perfil_de_linkedin_llega_a_las_paginas(monkeypatch):
    _sin_red(monkeypatch, html=HTML_PERSONAS)
    r = personas.buscar("agencia de marketing digital", ["gerente general"],
                        "Santiago, Chile", 25)
    assert not any(personas.es_perfil_linkedin(p["url"]) for p in r["paginas"])


def test_el_endpoint_devuelve_las_paginas(monkeypatch):
    _sin_red(monkeypatch, html=HTML_PERSONAS)
    r = cliente.post("/search-people", json={
        "query": "agencia de marketing digital", "titles": ["gerente general"],
        "location": "Santiago, Chile"})
    d = r.json()
    assert d["pages"], "el endpoint no devolvio paginas"
    for campo in ("url", "titulo", "people_page", "industry_match"):
        assert campo in d["pages"][0], campo


def test_un_cargo_compuesto_no_entra_como_nombre():
    # Medido en vivo: "Secretario General" fue el unico candidato de la primera
    # corrida real, y no es una persona.
    assert personas.es_nombre_de_persona("Secretario General") is False
    assert personas.es_nombre_de_persona("Vicepresidente Ejecutivo") is False
    assert personas.es_nombre_de_persona("Matias Bravo") is True


# ── P7: los agregadores no son paginas de equipo ────────────────────────────
# Medido en vivo (2026-09-03): la primera version marcaba amarillas.cl,
# laborum.cl y directorioempresaschile.cl como "paginas de equipo". Un scrape
# ahi lista gente de OTRAS empresas, no del prospecto.

@pytest.mark.parametrize("url,titulo", [
    ("https://amarillas.cl/", "Directorio de empresas"),
    ("https://laborum.cl/empleos-area-gerencia-y-direccion-general.html",
     "Ofertas de trabajo en Gerencia / Direccion General"),
    ("https://directorioempresaschile.cl/directory-empresas_on/", "Directorio"),
    ("https://chileempresas.com/", "Directorio Empresarial de Chile"),
    ("https://www.linkedin.com/company/acme/people", "Empleados de Acme"),
    ("https://vitria.cl/agencias", "Vitria - Directorio de agencias"),
])
def test_un_agregador_no_es_una_pagina_de_equipo(url, titulo):
    assert personas.es_pagina_de_personas(url, titulo) is False, url


def test_control_positivo_las_paginas_reales_siguen_pasando():
    # Sin este control, el filtro de arriba podria estar rechazando todo.
    assert personas.es_pagina_de_personas(
        "https://wp.interactioncr.com/interactioncr/equipo-de-direccion",
        "Equipo de direccion") is True
    assert personas.es_pagina_de_personas(
        "https://onzamarketing.cl/nuestro-equipo/", "Onza Marketing") is True
    assert personas.es_pagina_de_personas(
        "https://marketingsummit.cl/expositores", "Expositores 2026") is True


def test_un_host_que_se_llama_directorio_no_es_pagina_de_equipo():
    # Medido en vivo: `directorioempresaschile.cl/...` pasaba porque la senal
    # "directorio" se buscaba en la URL COMPLETA, host incluido. En una ruta la
    # palabra significa el directorio de una sociedad; en el host significa que
    # el sitio entero es un agregador.
    assert personas.es_pagina_de_personas(
        "https://directorioempresaschile.cl/directory-empresas_on/ubicaciones/cerro-navia",
        "Cerro Navia") is False


def test_control_positivo_una_ruta_de_socios_en_un_gremio_si_cuenta():
    # Sin este control, la regla de arriba pasaria con un detector que rechaza
    # cualquier cosa parecida a un directorio, y se perderia el caso legitimo:
    # la pagina de socios de un gremio SI lista decisores del rubro.
    assert personas.es_pagina_de_personas(
        "https://achap.cl/socios", "Socios | ACHAP") is True


# ── P8: falsos positivos vistos en la medicion en vivo ──────────────────────
# 2026-09-03, resolutor de decisor sobre Fintual, Buk, Betterfly y Toteat.
# Estos tres entraron como personas y ninguno lo es. Los casos vienen de una
# corrida real, no de imaginar que podria salir mal.

@pytest.mark.parametrize("texto", [
    "Chief Economist",        # cargo en ingles, en el /equipo de Fintual
    "Betterfly's Co",         # recorte de "Betterfly's Co-Founder"
    "Chief Executive",
    "Head Of Growth",
    "Managing Partner",
    "Acme Inc",
    "Onza SpA",
])
def test_no_entra_un_cargo_ni_una_razon_social_como_persona(texto):
    assert personas.es_nombre_de_persona(texto) is False, texto


def test_control_positivo_los_decisores_reales_medidos_siguen_pasando():
    # Sin este control, las reglas de arriba podrian estar rechazando todo. Los
    # cuatro salieron correctamente de la MISMA corrida en vivo.
    for nombre in ["Omar Larre", "Ricardo Sateler", "Jaime Arrieta Boetsch",
                   "Cristobal Della Maggiora"]:
        assert personas.es_nombre_de_persona(nombre) is True, nombre
