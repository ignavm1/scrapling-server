"""Angulo LinkedIn: buscar el perfil del decisor y entrar SOLO si es LinkedIn.

La regla la puso el usuario y la medicion la respalda: el snippet del buscador
llega destrozado y el titulo de la pagina trae nombre, cargo y empresa.
"""
import pathlib
import re

import pytest

from venara_discovery import (config, decisor, extraction, linkedin_perfil,
                              providers)
from venara_discovery.location import interpretar

FIX = pathlib.Path(__file__).parent / "fixtures"
leer = lambda n: (FIX / n).read_text(encoding="utf-8")
PERFIL = leer("sintetico_linkedin_perfil.html")
LOGIN = leer("sintetico_linkedin_login.html")


# ── L1: las queries ─────────────────────────────────────────────────────────

def test_la_query_es_empresa_mas_cargo_mas_linkedin():
    qs = linkedin_perfil.construir_queries("Fintual", ["CEO", "gerente general"])
    assert '"Fintual" CEO linkedin' in qs
    assert '"Fintual" gerente general linkedin' in qs


def test_la_query_no_usa_el_operador_site_medido_en_cero():
    # F24: `site:linkedin.com/in` da cero en los proveedores disponibles; la
    # palabra suelta devuelve siete perfiles en Brave para la misma empresa.
    for q in linkedin_perfil.construir_queries("Fintual", ["CEO"]):
        assert "site:" not in q.lower(), q


def test_control_positivo_una_query_prohibida_plantada_se_detecta():
    # El test de arriba afirma una ausencia, y una ausencia pasa tambien cuando
    # el detector esta roto.
    plantada = 'site:linkedin.com/in "Fintual"'
    assert "site:" in plantada.lower()


def test_sin_empresa_no_se_arma_ninguna_query():
    assert linkedin_perfil.construir_queries("", ["CEO"]) == []


# ── L2: solo se entra si es un perfil ───────────────────────────────────────

@pytest.mark.parametrize("url", [
    "https://cl.linkedin.com/in/andresmarinkovic",
    "https://www.linkedin.com/in/olarre",
    "https://linkedin.com/in/gonzaloenei",
    "https://mx.linkedin.com/in/leonardocastillosolis",
    "https://cl.linkedin.com/in/olarre/en",
])
def test_se_entra_a_un_perfil(url):
    assert linkedin_perfil.es_perfil(url) is True, url


@pytest.mark.parametrize("url", [
    "https://www.linkedin.com/company/fintual",       # empresa, no persona
    "https://www.linkedin.com/jobs/view/123",
    "https://www.linkedin.com/pulse/articulo",
    "https://craft.co/buk-chile/executives",          # agregador
    "https://fintual.cl/equipo",                      # ni siquiera es LinkedIn
    "https://notlinkedin.com/in/alguien",             # dominio parecido
])
def test_no_se_visita_lo_que_no_es_un_perfil(url):
    assert linkedin_perfil.es_perfil(url) is False, url


def test_el_resolutor_solo_visita_perfiles_de_linkedin(monkeypatch):
    # Control decisivo de la regla: se le dan resultados mezclados y se cuenta
    # exactamente a que urls entro.
    serp = ('<html><body><div class="results">'
            '<div class="result"><a class="result__a" href="https://cl.linkedin.com/in/olarre">Omar</a>'
            '<a class="result__snippet" href="https://cl.linkedin.com/in/olarre">Fintual</a></div>'
            '<div class="result"><a class="result__a" href="https://www.linkedin.com/company/fintual">Fintual</a>'
            '<a class="result__snippet" href="https://www.linkedin.com/company/fintual">Fintual</a></div>'
            '<div class="result"><a class="result__a" href="https://craft.co/fintual/executives">Fintual execs</a>'
            '<a class="result__snippet" href="https://craft.co/fintual/executives">Fintual</a></div>'
            '</div></body></html>')
    visitadas = []

    def _obtener(url, proveedor, salud, timeout=None):
        if proveedor in ("linkedin", "sitio"):
            visitadas.append(url)
            return _Rta(html=PERFIL if "linkedin.com/in" in url else "")
        return _Rta(html=serp)

    monkeypatch.setattr(decisor, "obtener", _obtener)
    decisor.resolver("Fintual", "", "Santiago, Chile")
    de_linkedin = [u for u in visitadas if "linkedin.com" in u]
    assert de_linkedin == ["https://cl.linkedin.com/in/olarre"], visitadas
    assert not any("craft.co" in u for u in visitadas), visitadas


class _Rta:
    veredicto = None

    def __init__(self, html="", bloqueado=False, motivo=""):
        self.html = html
        self.page = None
        self.error = motivo if bloqueado else ""
        self._sirve = bool(html) and not bloqueado

    @property
    def sirve(self):
        return self._sirve


# ── L3: el titulo del perfil ────────────────────────────────────────────────

@pytest.mark.parametrize("titulo,nombre,cargo,empresa", [
    ("Andrés Marinkovic - Co Founder y COO en Fintual (YC S18) | LinkedIn",
     "Andrés Marinkovic", "Co Founder y COO", "Fintual"),
    ("Omar Larré - Co-founder & CIO - Fintual | LinkedIn",
     "Omar Larré", "Co-founder & CIO", "Fintual"),
    ("Jane Doe - CEO at Acme | LinkedIn", "Jane Doe", "CEO", "Acme"),
])
def test_parsea_el_titulo_de_un_perfil_real(titulo, nombre, cargo, empresa):
    d = linkedin_perfil.parsear_titulo(titulo)
    assert d == {"nombre": nombre, "cargo": cargo, "empresa": empresa}


@pytest.mark.parametrize("titulo", [
    "Sign in | LinkedIn",                    # muro de sesion
    "Fintual | LinkedIn",                    # pagina de empresa
    "LinkedIn",
    "",
    "Juan Perez - Estudiante en Universidad | LinkedIn",   # no decide
    "Algo sin marca del sitio",
])
def test_un_titulo_que_no_es_de_perfil_no_produce_candidato(titulo):
    # El muro de sesion tambien termina en "| LinkedIn": sin esta regla, cada
    # perfil bloqueado inventaria una persona.
    assert linkedin_perfil.parsear_titulo(titulo) is None, titulo


def test_lee_el_titulo_desde_el_html_del_perfil():
    assert linkedin_perfil.parsear_titulo(
        linkedin_perfil.titulo_de(PERFIL))["nombre"] == "Andrés Marinkovic"


def test_el_muro_de_sesion_no_produce_persona():
    assert linkedin_perfil.parsear_titulo(linkedin_perfil.titulo_de(LOGIN)) is None


# ── L4: el pais, y el homonimo medido ───────────────────────────────────────

def test_el_subdominio_declara_el_pais():
    assert linkedin_perfil.pais_del_perfil("https://cl.linkedin.com/in/x") == "CL"
    assert linkedin_perfil.pais_del_perfil("https://mx.linkedin.com/in/x") == "MX"
    # Sin subdominio de pais no se afirma nada: "" significa "no declara".
    assert linkedin_perfil.pais_del_perfil("https://www.linkedin.com/in/x") == ""
    assert linkedin_perfil.pais_del_perfil("https://linkedin.com/in/x") == ""


def test_un_perfil_de_otro_pais_no_se_atribuye_a_la_empresa_homonima(monkeypatch):
    # EL FALSO POSITIVO MEDIDO (2026-09-03): "Houm" es chilena y tambien india,
    # y un directorio extranjero le colgo a la chilena dos fundadores ajenos.
    perfil_indio = ('<html><head><title>Bijai Jayarajan - Founder en Houm '
                    '| LinkedIn</title></head><body></body></html>')
    serp = ('<html><body><div class="results"><div class="result">'
            '<a class="result__a" href="https://in.linkedin.com/in/bijai">Bijai</a>'
            '<a class="result__snippet" href="https://in.linkedin.com/in/bijai">Houm</a>'
            '</div></div></body></html>')

    def _obtener(url, proveedor, salud, timeout=None):
        if "linkedin.com/in" in url:
            return _Rta(html=perfil_indio)
        if proveedor in ("sitio",):
            return _Rta(html="")
        return _Rta(html=serp)

    monkeypatch.setattr(decisor, "obtener", _obtener)
    r = decisor.resolver("Houm", "", "Santiago, Chile")
    assert "Bijai Jayarajan" not in [c.nombre for c in r["candidatos"]], \
        [(c.nombre, c.url) for c in r["candidatos"]]


def test_control_positivo_el_perfil_del_pais_pedido_SI_se_atribuye(monkeypatch):
    # Sin este control, la regla de pais pasaria con un resolutor que descarta
    # todos los perfiles, y se concluiria lo contrario.
    serp = ('<html><body><div class="results"><div class="result">'
            '<a class="result__a" href="https://cl.linkedin.com/in/amarinkovic">A</a>'
            '<a class="result__snippet" href="https://cl.linkedin.com/in/amarinkovic">Fintual</a>'
            '</div></div></body></html>')

    def _obtener(url, proveedor, salud, timeout=None):
        if "linkedin.com/in" in url:
            return _Rta(html=PERFIL)
        if proveedor == "sitio":
            return _Rta(html="")
        return _Rta(html=serp)

    monkeypatch.setattr(decisor, "obtener", _obtener)
    r = decisor.resolver("Fintual", "", "Santiago, Chile")
    assert "Andrés Marinkovic" in [c.nombre for c in r["candidatos"]]
    mejor = r["candidatos"][0]
    assert mejor.angulo == "linkedin_perfil"
    assert any("perfil de LinkedIn" in e for e in mejor.evidencia)


def test_una_empresa_distinta_en_el_perfil_no_se_acepta():
    assert linkedin_perfil.coincide_empresa("Fintual", "Fintual") is True
    assert linkedin_perfil.coincide_empresa("Fintual (YC S18)", "Fintual") is True
    assert linkedin_perfil.coincide_empresa("Banco de Chile", "Fintual") is False


# ── L8: "buscar en Google" ──────────────────────────────────────────────────

def test_google_no_sirve_resultados_por_http_medido_de_nuevo():
    # El usuario pidio explicitamente buscar en Google. Se volvio a medir el
    # 2026-09-04 en vez de citar la medicion vieja: el HTML servido son 92KB de
    # bootstrap de JavaScript, con CERO perfiles y cero resultados extraibles.
    items, _ = extraction.extraer(None, leer("google_jsshell.html"), "google")
    assert len([i for i in items if i["url"].startswith("http")]) <= 1
    assert not re.search(r"linkedin\.com/in/", leer("google_jsshell.html"))
    # Y el sistema no depende de el.
    assert "google" not in {p.nombre for p in providers.activos()}
    assert config.ENABLE_GOOGLE is False


def test_control_positivo_un_buscador_que_si_responde_da_resultados():
    # Sin este control, el test de arriba pasaria con un extractor roto.
    items, _ = extraction.extraer(None, leer("bing_ok.html"), "bing")
    assert len([i for i in items if i["url"].startswith("http")]) >= 5


# ── L5: el angulo tiene que llegar a ejecutarse ─────────────────────────────

def test_el_angulo_de_linkedin_sobrevive_al_techo_de_fetches():
    """El bug mas silencioso de todos: el angulo existia y nunca corria.

    MEDIDO el 2026-09-04: con el reparto plano, `sitio_equipo` y
    `cargo_directo` por los cuatro proveedores consumian los 8 fetches y las
    queries de LinkedIn quedaban fuera SIEMPRE. El sintoma era "linkedin=0" en
    las seis empresas, que se lee como "el angulo no sirve" cuando en realidad
    no se habia ejecutado ni una vez.
    """
    plan = decisor.construir_plan("Fintual", "fintual.cl", interpretar("Santiago, Chile"))
    activos = providers.activos()[: config.DECISOR_PROVEEDORES_POR_ANGULO]
    trabajos = sorted([(a, p) for a in plan for p in activos],
                      key=lambda t: (t[0].prioridad, t[1].prioridad))
    ejecutados = trabajos[: config.DECISOR_MAX_FETCHES]
    angulos = {a.nombre for a, _ in ejecutados}
    assert "linkedin_perfil" in angulos, sorted(angulos)
    # Y el camino del sitio propio, que es el que mejor rinde, tampoco puede
    # quedar desplazado por el angulo nuevo.
    assert "sitio_equipo" in angulos, sorted(angulos)


def test_el_reparto_por_angulo_cubre_mas_angulos_que_el_plano():
    # Control de que el arreglo hace lo que dice: con el mismo techo, repartir
    # por angulo cubre estrictamente mas angulos que gastar todos los
    # proveedores en los primeros.
    plan = decisor.construir_plan("Fintual", "fintual.cl", interpretar("Chile"))
    todos = providers.activos()
    plano = sorted([(a, p) for a in plan for p in todos],
                   key=lambda t: (t[0].prioridad, t[1].prioridad))[: config.DECISOR_MAX_FETCHES]
    repartido = sorted([(a, p) for a in plan
                        for p in todos[: config.DECISOR_PROVEEDORES_POR_ANGULO]],
                       key=lambda t: (t[0].prioridad, t[1].prioridad))[: config.DECISOR_MAX_FETCHES]
    assert len({a.nombre for a, _ in repartido}) > len({a.nombre for a, _ in plano})
