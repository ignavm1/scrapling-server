"""G15 -- LinkedIn de empresa: se valida; personas/posts/jobs se rechazan."""
from venara_discovery import linkedin


def test_solo_acepta_paginas_de_empresa():
    assert linkedin.es_url_de_empresa("https://www.linkedin.com/company/acme") is True
    for malo in [
        "https://www.linkedin.com/in/juanperez",          # persona
        "https://www.linkedin.com/company/acme/posts/",   # post
        "https://www.linkedin.com/jobs/view/123",         # empleo
        "https://www.linkedin.com/pulse/articulo",        # articulo
        "https://acme.pe/",                               # ni siquiera es LinkedIn
    ]:
        assert linkedin.es_url_de_empresa(malo) is False, malo


def test_no_confunde_url_de_persona_con_url_de_empresa():
    # El requisito 30 lo prohibe explicitamente y es un error caro: guardar un
    # perfil personal en el campo de empresa contamina la base.
    p = "https://www.linkedin.com/in/juanperez"
    assert linkedin.es_url_de_persona(p) is True
    assert linkedin.es_url_de_empresa(p) is False


def test_la_confianza_baja_cuando_el_nombre_no_coincide():
    alta = linkedin.confianza_empresa("Kallpa Creativa", "Kallpa Creativa | LinkedIn",
                                      "https://www.linkedin.com/company/kallpa-creativa")
    baja = linkedin.confianza_empresa("Kallpa Creativa", "Otra Empresa | LinkedIn",
                                      "https://www.linkedin.com/company/otra-empresa")
    assert alta >= 0.9 and baja <= 0.4


def test_prioridad_de_cargos_para_decidir_quien_compra():
    assert linkedin.puntuar_cargo("Founder & CEO") > linkedin.puntuar_cargo("Marketing Manager")
    assert linkedin.puntuar_cargo("CEO") > linkedin.puntuar_cargo("Director")
    assert linkedin.puntuar_cargo("Director") > linkedin.puntuar_cargo("Analista")
    assert linkedin.puntuar_cargo("Co-Founder") >= 0.95


def test_partir_titulo():
    n, c = linkedin._partir_titulo("Juan Perez - Founder & CEO - Acme | LinkedIn")
    assert n == "Juan Perez" and "Founder" in c
    # Un titulo que no es un nombre no produce una persona inventada.
    assert linkedin._partir_titulo("10 Mejores Agencias de Marketing 2026")[0] == ""
    assert linkedin._partir_titulo("Agencia123 - CEO")[0] == ""


def test_sin_empresa_devuelve_not_found_sin_inventar():
    d = linkedin.buscar_persona("", "")
    assert d["person_name"] == "NOT_FOUND"
    assert d["linkedin_url"] == ""


def test_marcador_final():
    print("\nLINKEDIN VERIFICADO")


# ── G16: la disponibilidad real de perfiles, MEDIDA sobre evidencia congelada ──

def test_MEDICION_los_perfiles_de_persona_no_estan_en_el_indice():
    """Afirmacion de AUSENCIA, con su control positivo en el mismo archivo.

    ddg_linkedin.html se capturo sin bloqueo (blocking.analizar lo confirma) y
    contiene 10 URLs REALES de paginas de empresa. Eso prueba que el operador
    `site:linkedin.com/...` funciona en ese motor y esa sesion.

    En los mismos fixtures no hay UN SOLO perfil /in/. La ausencia es de la
    fuente, no de la query -- que es justo lo que hay que demostrar antes de
    decir "esto no se puede".

    Consecuencia de producto: la busqueda de decision makers via buscadores
    publicos no tiene fuente. Agregar mas variantes de query no lo cambia.
    """
    import pathlib, re
    from venara_discovery import blocking
    fix = pathlib.Path(__file__).parent / "fixtures"

    control = (fix / "ddg_linkedin.html").read_text(encoding="utf-8")
    assert blocking.analizar(control, 200).bloqueado is False, \
        "el control se capturo bloqueado: la medicion no seria valida"
    empresas = set(re.findall(r"linkedin\.com/company/([a-zA-Z0-9\-%_]+)", control))
    assert len(empresas) >= 5, "el control positivo fallo: solo %d paginas de empresa" % len(empresas)

    perfiles = 0
    for f in fix.glob("*.html"):
        html = f.read_text(encoding="utf-8")
        if blocking.analizar(html, 200).bloqueado:
            continue          # una pagina bloqueada no dice nada
        perfiles += len(set(re.findall(r"linkedin\.com/in/([a-zA-Z0-9\-%_]+)", html)))

    assert perfiles == 0, ("aparecieron %d perfiles: la medicion cambio y hay que "
                           "revisar la conclusion de FINDINGS.md F7" % perfiles)
