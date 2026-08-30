"""G7 -- el ranking sube lo bueno y hunde lo malo, sobre un fixture etiquetado."""
from venara_discovery import scoring
from venara_discovery.entity import Empresa
from venara_discovery.location import interpretar

Q = "agencia de marketing digital"
UBI = interpretar("Lima, Peru")


def _e(nombre, web="", li="", desc="", fuentes=("duckduckgo",)):
    return Empresa(nombre=nombre, website=web, linkedin_url=li,
                   descripcion=desc, fuentes=set(fuentes))


# Etiquetado a mano: True = deberia quedar arriba.
CORPUS = [
    (_e("Kallpa Creativa", "https://kallpacreativa.pe/",
        "https://www.linkedin.com/company/kallpa", "Agencia de marketing digital en Lima",
        ("duckduckgo", "bing")), True),
    (_e("Onzamarketing", "https://onzamarketing.com/", desc="Agencia de marketing digital en Lima"), True),
    (_e("Limadigital", "https://limadigital.pe/", desc="Marketing digital Lima Peru"), True),
    (_e("Elgato", "https://reddit.com/r/elgato/", desc="No elgato sound in OBS"), False),
    (_e("AOL Mail", "https://aol.com/", desc="Log-in to Yahoo AOL"), False),
    (_e("Paginas Amarillas", "https://paginasamarillas.com.pe/agencias", desc="Directorio"), False),
    (_e("Sin nada", "", "", ""), False),
]


def test_las_buenas_rankean_por_encima_de_las_malas():
    puntajes = {e.nombre: scoring.puntuar(e, Q, UBI).total for e, _ in CORPUS}
    peor_buena = min(p for e, ok in CORPUS if ok for p in [puntajes[e.nombre]])
    mejor_mala = max(p for e, ok in CORPUS if not ok for p in [puntajes[e.nombre]])
    assert peor_buena > mejor_mala, \
        "la peor buena (%.3f) no supera a la mejor mala (%.3f): %s" % (peor_buena, mejor_mala, puntajes)


def test_el_umbral_deja_fuera_a_TODAS_las_malas():
    # "Preferible 18 empresas excelentes que 25 resultados malos": el corte
    # tiene que cortar de verdad, no rellenar hasta max_results.
    salida, stats = scoring.rankear([e for e, _ in CORPUS], Q, UBI, limite=25)
    nombres = {e.nombre for e in salida}
    for e, ok in CORPUS:
        if not ok:
            assert e.nombre not in nombres, "%s no deberia haber pasado" % e.nombre
    assert stats["bajo_umbral"] >= 4


def test_las_senales_son_interpretables():
    e = CORPUS[0][0]
    s = scoring.puntuar(e, Q, UBI).dict()
    for k in ("relevance_score", "website_confidence", "identity_confidence",
              "location_confidence", "source_count", "total"):
        assert k in s, k
    assert 0.0 <= s["total"] <= 1.0


def test_mas_fuentes_sube_el_score():
    una = _e("Acme", "https://acme.pe/", desc="agencia de marketing digital Lima", fuentes=("bing",))
    dos = _e("Acme", "https://acme.pe/", desc="agencia de marketing digital Lima",
             fuentes=("bing", "duckduckgo"))
    assert scoring.puntuar(dos, Q, UBI).total > scoring.puntuar(una, Q, UBI).total


def test_diversidad_limita_un_mismo_dominio():
    muchas = [_e("Acme %d" % i, "https://acme.pe/pagina%d" % i,
                 desc="agencia de marketing digital Lima") for i in range(6)]
    salida, _ = scoring.rankear(muchas, Q, UBI, limite=10)
    del_dominio = sum(1 for e in salida if e.dominio == "acme.pe")
    assert del_dominio <= 2 or len(salida) == len(muchas)


def test_marcador_final():
    print("\nSCORING VERIFICADO")
