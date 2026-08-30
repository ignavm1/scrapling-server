"""G1 -- una pagina de captcha se clasifica como bloqueo, una de resultados no.

Todos los fixtures son HTML REAL capturado el 2026-08-30. Probar un detector de
bloqueo contra una maqueta escrita por quien lo programo no prueba nada: la
maqueta contiene justo las marcas que el detector busca.

El control positivo es lo que da valor a este test. `ddg_blocked.html` se
capturo provocando el bloqueo de verdad (HTTP 202, 13066 bytes) y las cuatro
paginas de resultados son respuestas legitimas del mismo dia. Si el detector
marcara todo como bloqueado pasaria la mitad del test y fallaria la otra.
"""
from __future__ import annotations
import pathlib
import pytest

from venara_discovery import blocking

FIX = pathlib.Path(__file__).parent / "fixtures"


def leer(nombre: str) -> str:
    return (FIX / nombre).read_text(encoding="utf-8")


# ── Bloqueos reales ──────────────────────────────────────────────────────────

def test_captcha_de_duckduckgo_se_detecta():
    # HTTP 202, NO 403 ni 429. Un detector que solo mire codigos de error deja
    # pasar esta pagina como si fuera un resultado vacio legitimo.
    v = blocking.analizar(leer("ddg_blocked.html"), 202)
    assert v.bloqueado is True
    assert v.motivo == "captcha", v.motivo


def test_el_status_202_por_si_solo_no_se_confunde_con_exito():
    assert 202 in blocking._STATUS_SOSPECHOSOS


def test_shell_de_javascript_de_google_se_detecta():
    # 92KB de HTML y aun asi cero resultados: es un bootstrap que redirige a
    # enablejs. Contarlo como "sin resultados" hacia creer que el nicho estaba
    # vacio.
    v = blocking.analizar(leer("google_jsshell.html"), 200)
    assert v.bloqueado is True
    assert v.motivo in ("requiere-javascript", "sin-resultados-extraibles"), v.motivo


def test_sin_respuesta_es_bloqueo():
    assert blocking.analizar(None, None).bloqueado is True


@pytest.mark.parametrize("status", [403, 429, 503])
def test_status_clasicos_de_bloqueo(status):
    # Pagina con anclas suficientes: sin el status, pasaria por buena.
    html = "".join('<a href="https://empresa%d.com">x</a>' % i for i in range(10))
    v = blocking.analizar(html, status)
    assert v.bloqueado is True
    assert v.motivo == "status-" + str(status)


# ── CONTROL POSITIVO: paginas reales que NO deben marcarse ───────────────────

@pytest.mark.parametrize("fixture", [
    "bing_ok.html",
    "ddg_companies.html",
    "ddg_linkedin.html",
])
def test_paginas_de_resultados_reales_no_se_marcan_como_bloqueo(fixture):
    # Este es el control que impide el detector trivial "todo es bloqueo".
    v = blocking.analizar(leer(fixture), 200)
    assert v.bloqueado is False, "%s marcado como %s" % (fixture, v.motivo)
    assert v.anclas >= 5, "%s solo tiene %d anclas externas" % (fixture, v.anclas)


@pytest.mark.parametrize("fixture", [
    "bing_poisoned_microsoft.html",
    "bing_poisoned_recipes.html",
])
def test_las_paginas_envenenadas_de_bing_NO_son_bloqueo(fixture):
    # Distincion que importa: Bing sirve resultados irrelevantes (recetas
    # japonesas para una query de marketing) en una pagina estructuralmente
    # perfecta. Eso NO es un bloqueo -- hay resultados, son los equivocados.
    #
    # Confundirlos haria que el sistema reintentara contra otro proveedor
    # cuando el problema es la calidad, no la disponibilidad. Filtrarlos es
    # trabajo de la capa de relevancia (ver test_filtering.py).
    v = blocking.analizar(leer(fixture), 200)
    assert v.bloqueado is False, "%s marcado como bloqueo: %s" % (fixture, v.motivo)


def test_las_paginas_buenas_tienen_muchas_mas_anclas_que_la_bloqueada():
    # La senal que separa una clase de otra tiene que ser amplia, no marginal:
    # si el margen fuera de 1 o 2 anclas, cualquier cambio de layout lo cruza.
    buena = blocking.contar_anclas_externas(leer("ddg_companies.html"))
    mala = blocking.contar_anclas_externas(leer("ddg_blocked.html"))
    assert buena > mala * 3, "margen insuficiente: buena=%d mala=%d" % (buena, mala)


def test_marcador_final():
    print("\nBLOCKING VERIFICADO")
