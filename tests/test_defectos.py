"""Los seis defectos que aparecieron buscando gerentes de agencias reales.

Cada uno tiene el caso EXACTO de la corrida del 2026-09-06 sobre 30 agencias de
marketing, que devolvio 9 candidatos de los cuales 4 eran basura. Ninguno de
estos casos se le habria ocurrido a quien escribe tests desde cero: salieron de
mirar resultados reales.
"""
import pytest

from venara_discovery import decisor, filtering, personas


# ── D1: el nombre de la empresa no es una persona ───────────────────────────

def test_el_nombre_de_la_empresa_no_entra_como_persona():
    # "Flama Creators" entro como decisor de flamacreators.com, y encima se le
    # construyo flama@flamacreators.com por patron: un email inventado sobre
    # algo que no es un nombre.
    assert personas.es_el_nombre_de_la_empresa(
        "Flama Creators", "https://flamacreators.com/nosotros") is True
    assert personas.es_el_nombre_de_la_empresa(
        "Web Development", "https://agencies.semrush.com/x", "Web Development") is True


def test_no_descarta_a_quien_tiene_el_apellido_parecido_a_la_empresa():
    # Hay gente cuyo apellido esta en el nombre de su empresa, y suele ser el
    # fundador -- justo a quien mas queremos. Se compara por igualdad, no por
    # contencion.
    assert personas.es_el_nombre_de_la_empresa(
        "Matias Bravo", "https://bravomarketing.cl/equipo") is False
    assert personas.es_el_nombre_de_la_empresa(
        "Omar Larre", "https://fintual.cl/equipo") is False


# ── D2: cargo con forma de vacante ──────────────────────────────────────────

@pytest.mark.parametrize("cargo", [
    "Director/a de Estrategia Digital",
    "Gerente/a Comercial",
    "Buscamos Gerente General",
    "Vacante: Director Comercial",
    "Hiring Head of Sales",
])
def test_un_cargo_de_vacante_no_es_el_cargo_de_nadie(cargo):
    # "Director/a" con barra de genero es forma de aviso de empleo, no de
    # alguien que ocupa el puesto.
    assert personas.es_cargo_de_vacante(cargo) is True, cargo


@pytest.mark.parametrize("cargo", [
    "Directora de Marketing y Estrategia",
    "Director Ejecutivo",
    "Co-founder & CIO",
    "Socia Fundadora",
])
def test_control_positivo_los_cargos_reales_siguen_pasando(cargo):
    # Sin este control, un detector que rechaza todo pasaria el test de arriba.
    assert personas.es_cargo_de_vacante(cargo) is False, cargo


# ── D3: un directorio no es una agencia ─────────────────────────────────────

@pytest.mark.parametrize("url", [
    "https://agencies.semrush.com/es/agency/x",
    "https://theorg.com/org/buk/org-chart/x",
    "https://craft.co/buk-chile/executives",
    "https://www.g2.com/categories/agencies",
])
def test_un_directorio_de_agencias_no_pasa_como_agencia(url):
    assert filtering.motivo_descarte(url, "") == "directorio", url


def test_control_positivo_una_agencia_real_sigue_pasando():
    for url in ["https://onzamarketing.cl/", "https://nexbu.com/",
                "https://beyondagency.cl/contacto"]:
        assert filtering.motivo_descarte(url, "") == "", url


# ── D4: el pais del dominio ─────────────────────────────────────────────────

def test_pais_dominio_se_lee_del_tld():
    assert decisor.pais_del_dominio("https://marketing-branding.es/x") == "ES"
    assert decisor.pais_del_dominio("https://onzamarketing.cl/x") == "CL"
    assert decisor.pais_del_dominio("https://algo.com.pe/x") == "PE"


def test_pais_dominio_un_com_no_declara_nada():
    # La mayoria de las empresas chilenas usa .com y .cl indistintamente.
    # Tratar el neutro como extranjero tiraria mas de lo que salva.
    assert decisor.pais_del_dominio("https://xepelin.com/x") == ""
    assert decisor.contradice_el_pais("https://xepelin.com/x", "CL") is False


def test_pais_dominio_un_es_contradice_una_busqueda_en_chile():
    # MEDIDO: "Teresa Bergua" de marketing-branding.es entro en una busqueda de
    # Santiago. Eran dos defectos en un candidato: cargo de vacante y pais.
    assert decisor.contradice_el_pais("https://marketing-branding.es/x", "CL") is True


def test_pais_dominio_sin_pais_pedido_no_contradice_nada():
    assert decisor.contradice_el_pais("https://marketing-branding.es/x", "") is False


# ── D5: el homonimo entre paises, sin LinkedIn ──────────────────────────────

def test_homonimo_de_otro_pais_se_detecta_por_el_dominio_de_la_fuente():
    # "Houm" es una empresa chilena y tambien una india. Un directorio indio le
    # colgo a la chilena dos fundadores ajenos. Cuando el candidato NO viene de
    # LinkedIn no hay subdominio de pais, pero el dominio de la fuente si lo
    # declara.
    assert decisor.contradice_el_pais("https://yourstory.in/companies/houm", "CL") is True


def test_homonimo_no_descarta_una_fuente_neutra():
    # yourstory.com no declara pais: descartarlo por sospecha tiraria fuentes
    # legitimas. La liga con la empresa sigue siendo la que decide.
    assert decisor.contradice_el_pais("https://yourstory.com/companies/houm", "CL") is False


# ── D6: la ortografia no confirmada ─────────────────────────────────────────

def _cand(**kw):
    base = dict(nombre="Eduardo X", cargo="CEO", url="https://qed.com/p",
                angulo="a", proveedor="brave", origen="tercero",
                donde="snippet", empresa_en_texto=True)
    base.update(kw)
    return decisor.Candidato(**base)


def test_un_nombre_de_una_sola_fuente_de_tercero_no_esta_confirmado():
    # Aparecio "Eduardo Dillamajora" por Della Maggiora, escrito de oido en un
    # transcript. NO se corrige la ortografia -- adivinar introduce un error
    # peor que el que resuelve -- pero se avisa que no esta confirmada.
    c = _cand()
    c.fuentes = {"qed.com"}
    assert c.nombre_confirmado is False
    assert c.a_dict()["name_confirmed"] is False


def test_dos_fuentes_independientes_confirman_el_nombre():
    c = _cand()
    c.fuentes = {"qed.com", "craft.co"}
    assert c.nombre_confirmado is True


def test_el_sitio_propio_y_linkedin_confirman_por_si_solos():
    # Ahi lo escribe quien lo sabe.
    assert _cand(origen="sitio_propio").nombre_confirmado is True
    assert _cand(origen="linkedin_verificado").nombre_confirmado is True


# ── D7: los cuatro basura de la corrida real, juntos ────────────────────────

def test_corrida_real_los_cuatro_candidatos_basura_ya_no_pasan():
    """Los cuatro que se descartaron a mano el 2026-09-06."""
    # 1) nombre de la empresa
    assert personas.es_el_nombre_de_la_empresa(
        "Flama Creators", "https://flamacreators.com/x") is True
    # 2) nombre de la empresa, en un directorio
    assert filtering.motivo_descarte("https://agencies.semrush.com/x", "") == "directorio"
    # 3) cargo de vacante + pais equivocado
    assert personas.es_cargo_de_vacante("Director/a de Estrategia Digital") is True
    assert decisor.contradice_el_pais("https://marketing-branding.es/x", "CL") is True
    # 4) nombre dudoso de una sola fuente
    c = _cand(nombre="Alias Morris")
    c.fuentes = {"relevantmkt.com"}
    assert c.nombre_confirmado is False


def test_corrida_real_los_cinco_buenos_siguen_pasando():
    """Control positivo: los cinco utilizables de la MISMA corrida.

    Sin esto, los arreglos de arriba pasarian con un sistema que rechaza todo.
    """
    buenos = [
        ("Mario Vera", "https://nexbu.com/nosotros", "Director Ejecutivo"),
        ("Estefania Moreno Bettancourt", "https://beyondagency.cl/contacto", "Socia Fundadora"),
        ("Lorraynne Barbosa", "https://rocketsweb.cl/equipo", "Directora de Marketing"),
        ("Pancho Ordenes", "https://meat.cl/nosotros", "Socio - Director ejecutivo"),
        ("Marcel Acunis", "https://bigbuda.cl/equipo", "fundador"),
    ]
    for nombre, url, cargo in buenos:
        assert personas.es_nombre_de_persona(nombre) is True, nombre
        assert personas.es_el_nombre_de_la_empresa(nombre, url) is False, nombre
        assert personas.es_cargo_de_vacante(cargo) is False, cargo
        assert filtering.motivo_descarte(url, "") == "", url
        assert decisor.contradice_el_pais(url, "CL") is False, url


def test_pais_dominio_se_resuelve_por_regla_y_no_por_lista_blanca():
    """Una lista blanca deja pasar en silencio cualquier pais que falte.

    Asi entro `yourstory.in` -- un directorio indio -- en una busqueda chilena:
    el TLD `.in` no estaba en la tabla y el dominio quedo como "neutro".
    Ahora todo TLD de dos letras es pais, salvo los que se usan de genericos.
    """
    for url, esperado in [
        ("https://yourstory.in/x", "IN"),
        ("https://algo.de/x", "DE"),          # nunca estuvo en ninguna tabla
        ("https://algo.fr/x", "FR"),
        ("https://x.co.uk/a", "GB"),
    ]:
        assert decisor.pais_del_dominio(url) == esperado, url


@pytest.mark.parametrize("url", [
    "https://acme.com/x", "https://acme.io/x", "https://acme.co/x",
    "https://acme.ai/x", "https://acme.net/x",
])
def test_los_tld_genericos_no_declaran_pais(url):
    # `.co` es Colombia y tambien "company"; `.io` es el Oceano Indico y nadie
    # lo usa por eso. Tratarlos como pais descartaria empresas legitimas.
    assert decisor.pais_del_dominio(url) == "", url
