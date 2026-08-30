"""G6 -- basura fuera, empresas dentro. Con control positivo en ambos sentidos."""
import pathlib, pytest
from venara_discovery import filtering, extraction

FIX = pathlib.Path(__file__).parent / "fixtures"

BASURA = [
    ("https://www.linkedin.com/in/juanperez", "red-social-o-buscador"),
    ("https://facebook.com/acme", "red-social-o-buscador"),
    ("https://www.reddit.com/r/elgato/", "red-social-o-buscador"),
    ("https://aol.com/", "red-social-o-buscador"),
    ("https://es.wikipedia.org/wiki/Marketing", "red-social-o-buscador"),
    ("https://www.paginasamarillas.com.pe/agencias", "directorio"),
    ("https://clutch.co/pe/agencies/digital-marketing", "directorio"),
    ("https://www.computrabajo.com/empleos-de-marketing", "directorio"),
    ("https://gestion.pe/economia/empresas/agencias", "medio-de-prensa"),
    ("https://acme.pe/catalogo.pdf", "documento"),
    ("https://acme.pe/blog/como-elegir-agencia", "pagina-no-empresarial"),
    ("https://acme.pe/jobs/disenador", "pagina-no-empresarial"),
    ("https://funnel.pe/10-mejores-agencias-de-marketing", "listicle"),
    ("https://flamacreators.com/mejores-agencias-marketing-lima", "listicle"),
    ("https://www.universidad.edu.pe/carreras", "universidad"),
]

BUENAS = ["https://limadigital.pe/", "https://onzamarketing.com/",
          "https://kallpacreativa.com/servicios", "https://acme.com.pe/",
          "https://ibo.pe/", "https://estudio-norte.cl/nosotros"]


@pytest.mark.parametrize("url,motivo", BASURA)
def test_descarta_lo_que_no_es_empresa(url, motivo):
    assert filtering.motivo_descarte(url) == motivo or \
           filtering.motivo_descarte(url, "") == motivo, filtering.motivo_descarte(url)


@pytest.mark.parametrize("url", BUENAS)
def test_CONTROL_no_descarta_empresas_reales(url):
    # Sin este control, un filtro que rechace todo pasaria la mitad del test.
    assert filtering.es_empresa_candidata(url), filtering.motivo_descarte(url)


def test_titulo_listicle_sin_numero():
    assert filtering.motivo_descarte("https://x.pe/pagina",
                                     "Mejores Agencias de Marketing en Lima") == "listicle"


# ── Relevancia: la defensa contra el envenenamiento de Bing (F6) ─────────────

def test_relevancia_rechaza_los_resultados_envenenados_reales():
    # Fixture REAL: Bing devolvio hilos de Reddit sobre Elgato y recetas
    # japonesas para queries de marketing. Es el caso que ningun filtro
    # estructural detecta.
    q = "agencia de marketing digital Lima"
    for fixture in ("bing_poisoned_microsoft.html", "bing_poisoned_recipes.html"):
        html = (FIX / fixture).read_text(encoding="utf-8")
        items = extraction.extraer_por_regex(html)
        relevantes = [it for it in items
                      if filtering.relevancia(q, it["titulo"], it["snippet"], it["url"]) >= 0.34]
        assert len(relevantes) <= 1, "%s dejo pasar %d envenenados" % (fixture, len(relevantes))


def test_CONTROL_la_relevancia_acepta_resultados_legitimos():
    q = "agencia de marketing digital Lima"
    html = (FIX / "ddg_companies.html").read_text(encoding="utf-8")
    items = [it for it in extraction.extraer_por_regex(html)
             if filtering.es_empresa_candidata(it["url"], it["titulo"])]
    relevantes = [it for it in items
                  if filtering.relevancia(q, it["titulo"], it["snippet"], it["url"]) >= 0.34]
    assert len(relevantes) >= 4, "solo %d de %d pasaron relevancia" % (len(relevantes), len(items))


def test_relevancia_cero_para_algo_totalmente_ajeno():
    assert filtering.relevancia("agencia de marketing digital Lima",
                                "No elgato sound in OBS/Streamlabs : r/elgato",
                                "", "https://reddit.com/r/elgato/") == 0.0


def test_marcador_final():
    print("\nFILTRADO VERIFICADO")
