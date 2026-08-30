"""Segunda pasada: edge cases y seguridad.

Los dos primeros salieron de revisar el codigo ya funcionando, no de un test
que fallara. Es la pasada que el pedido llama "no asumas que el codigo esta
bien solo porque funciona".
"""
import pytest
from venara_discovery import blocking, filtering
from venara_discovery.location import interpretar, confianza
from venara_discovery.normalize import dominio_registrable, normalizar_url, mejor_nombre


# ── Ubicacion: substring vs palabra ─────────────────────────────────────────

@pytest.mark.parametrize("texto,ciudad_esperada,pais_esperado", [
    ("Lima", "lima", "PE"),
    ("Lima, Peru", "lima", "PE"),
    ("Miraflores", "lima", "PE"),          # barrio -> su ciudad
    ("Colima, Mexico", "", "MX"),          # NO es Lima
    ("Salima", "", ""),                    # NO es Lima
    ("Santiago", "santiago", "CL"),
    ("Buenos Aires", "buenos aires", "AR"),
    ("", "", ""),
])
def test_ubicacion_por_palabra_completa(texto, ciudad_esperada, pais_esperado):
    # "Colima" contiene "lima". Con substring, una campana para Mexico se
    # ejecutaba contra Peru y el error aparecia recien en los leads.
    u = interpretar(texto)
    assert u.ciudad == ciudad_esperada, "%s -> %s" % (texto, u.ciudad)
    assert u.pais == pais_esperado


def test_la_confianza_de_ubicacion_tampoco_usa_substring():
    ubi = interpretar("Lima, Peru")
    conf, nivel = confianza(ubi, "Agencia en Colima, Mexico", "https://x.mx/")
    assert nivel != "ciudad", "'Colima' se conto como 'Lima'"


# ── Bloqueo: falso positivo por una palabra suelta ──────────────────────────

def test_una_pagina_legitima_que_menciona_captcha_NO_es_bloqueo():
    # Una empresa que vende servicios anti-captcha, o un articulo sobre el
    # tema. Marcarla apagaba el proveedor para toda la busqueda.
    html = "".join('<a href="https://empresa%d.pe/">Servicios anti captcha</a>' % i
                   for i in range(12))
    assert blocking.analizar(html, 200).bloqueado is False


def test_CONTROL_el_captcha_real_sigue_detectandose():
    import pathlib
    h = (pathlib.Path(__file__).parent / "fixtures" / "ddg_blocked.html").read_text(encoding="utf-8")
    v = blocking.analizar(h, 202)
    assert v.bloqueado is True and v.motivo == "captcha"


# ── Entradas malformadas: nada debe explotar ────────────────────────────────

BASURA = ["", "   ", None, "http://", "https://", "://x", "h" * 3000,
          "https://[[[", "javascript:alert(1)", "data:text/html,<h1>x",
          "https://.", "https://..", "\x00\x01", "https://a b c/"]


@pytest.mark.parametrize("mala", BASURA)
def test_entradas_malformadas_no_lanzan(mala):
    # Un resultado corrupto debe afectar solo a ese resultado, nunca tumbar la
    # busqueda entera.
    assert isinstance(normalizar_url(mala or ""), str)
    assert isinstance(dominio_registrable(mala or ""), str)
    assert isinstance(filtering.motivo_descarte(mala or "", "t"), str)
    assert isinstance(mejor_nombre("t", mala or "", "ctx"), str)


@pytest.mark.parametrize("html", ["", "<html>", "<a href=", "\x00" * 100,
                                  "<a href='https://x.pe/'>" * 500])
def test_html_inesperado_no_lanza(html):
    from venara_discovery import extraction
    assert isinstance(blocking.analizar(html, 200).bloqueado, bool)
    assert isinstance(extraction.extraer_por_regex(html), list)


def test_relevancia_con_query_vacia_no_divide_por_cero():
    assert 0.0 <= filtering.relevancia("", "algo", "", "https://x.pe/") <= 1.0


def test_marcador_final():
    print("\nEDGE CASES VERIFICADOS")


# ── Defectos vistos en la corrida EN VIVO contra Santiago, Chile ────────────

def test_directorio_con_TLD_de_pais_tambien_se_descarta():
    # cylex.cl aparecio en los resultados reales. La lista tenia cylex.com, y
    # la version local es JUSTO la que sale en una busqueda por ciudad.
    assert filtering.motivo_descarte("https://cylex.cl/santiago/estudio+contable") == "directorio"
    assert filtering.motivo_descarte("https://cylex.com/x") == "directorio"
    assert filtering.motivo_descarte("https://paginasamarillas.cl/x") == "directorio"


def test_CONTROL_una_empresa_real_con_el_mismo_TLD_si_pasa():
    # Sin este control, un filtro que descarte todo .cl pasaria el test de arriba.
    assert filtering.es_empresa_candidata("https://impuestosya.cl/")
    assert filtering.es_empresa_candidata("https://contabilidadfr.cl/")


def test_un_TLD_de_otro_pais_contradice_la_ubicacion():
    # `estudiocontablesantiago.com.ar` se colo como resultado "de ciudad" para
    # Santiago de CHILE, porque "santiago" estaba en el dominio. El TLD manda.
    ubi = interpretar("Santiago, Chile")
    conf, nivel = confianza(ubi, "Estudio Contable Santiago",
                            "https://estudiocontablesantiago.com.ar/")
    assert nivel == "otro-pais" and conf < 0.2
    # CONTROL: el mismo texto con TLD correcto si es de ciudad.
    conf2, nivel2 = confianza(ubi, "Estudio Contable Santiago", "https://impuestosya.cl/")
    assert nivel2 == "ciudad" and conf2 > 0.9


@pytest.mark.parametrize("titulo", ["Contacto", "Inicio", "Nosotros", "Home",
                                    "About Us", "Servicios"])
def test_el_titulo_de_una_pagina_interna_no_es_el_nombre_de_la_empresa(titulo):
    # Salio un lead llamado "Contacto" (de mva.cl/contacto). El dominio
    # identifica mejor que el titulo de una seccion.
    n = mejor_nombre(titulo, "https://mva.cl/contacto", "estudio contable Santiago")
    assert n == "Mva", n


def test_CONTROL_un_nombre_real_no_se_reemplaza_por_el_dominio():
    assert mejor_nombre("Impuestos YA!", "https://impuestosya.cl/",
                        "estudio contable Santiago") == "Impuestos YA!"
