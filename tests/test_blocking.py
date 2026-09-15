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


# --- Cortacircuito por timeout (2026-09-15) ---------------------------------
#
# Un proveedor que no CONTESTA no es lo mismo que uno que nos RECHAZA, y hasta
# esta fecha solo el segundo se marcaba. Scrapling reintenta 3 veces por fetch,
# asi que duckduckgo caido se comia ~18s de los 25 del presupuesto en cada
# angulo, y la empresa terminaba en `sin_acceso` sin que nadie hubiera mirado.

from venara_discovery.fetch import SaludProveedores  # noqa: E402
from venara_discovery import config  # noqa: E402


def test_un_timeout_suelto_no_tumba_al_proveedor():
    """Una falla de red aislada no puede apagar un motor que si funciona."""
    salud = SaludProveedores()
    corto = salud.registrar_timeout("duckduckgo", "Timeout")
    assert corto is False
    assert salud.esta_caido("duckduckgo") == ""


def test_dos_timeouts_seguidos_sacan_al_proveedor_de_la_busqueda():
    salud = SaludProveedores()
    for _ in range(config.MAX_TIMEOUTS_PROVEEDOR):
        corto = salud.registrar_timeout("duckduckgo", "Timeout")
    assert corto is True
    assert salud.esta_caido("duckduckgo") == "Timeout"
    assert "duckduckgo" in salud.caidos()


def test_caido_y_bloqueado_no_se_mezclan():
    """El que no contesta y el que nos rechaza se arreglan en lugares distintos.

    Mezclarlos es el fallo que este repo documenta como el mas caro (F1/F4):
    "no pudimos llegar" pide mirar la red, "nos rechazaron" pide proxy.
    """
    salud = SaludProveedores()
    salud.marcar_bloqueado("brave", "captcha")
    for _ in range(config.MAX_TIMEOUTS_PROVEEDOR):
        salud.registrar_timeout("duckduckgo", "Timeout")

    assert salud.resumen() == {"brave": "captcha"}, "un caido no puede figurar como bloqueado"
    assert salud.caidos() == {"duckduckgo": "Timeout"}, "un bloqueado no puede figurar como caido"
    # Y ninguno contamina al otro proveedor.
    assert salud.esta_caido("brave") == ""
    assert salud.esta_bloqueado("duckduckgo") == ""


def test_el_corte_no_se_repite_ni_sigue_contando():
    """Una vez caido, no vuelve a loguear ni a "cortar" en cada fallo posterior."""
    salud = SaludProveedores()
    for _ in range(config.MAX_TIMEOUTS_PROVEEDOR):
        salud.registrar_timeout("duckduckgo", "Timeout")
    assert salud.registrar_timeout("duckduckgo", "Timeout") is False
    assert salud.caidos() == {"duckduckgo": "Timeout"}


# --- El proxy se aplica de verdad (2026-09-15) ------------------------------


def test_el_proxy_se_aplica_de_verdad(monkeypatch):
    """Guarda contra el no-op silencioso de `FetcherSession(proxy=...)`.

    Scrapling 0.4.8 acepta el parametro singular a nivel sesion y lo IGNORA:
    no lanza y no avisa. Durante toda la vida del repo /health reportaba
    `"proxy": true` mientras las peticiones salian por la IP de casa.

    Este test no sale a la red: verifica que `crear_sesion` pase el proxy por
    la forma que Scrapling si honra (`proxies`, plural), que es lo unico que
    se puede comprobar sin depender de un proxy real.
    """
    from venara_discovery import fetch as mod_fetch

    capturado = {}

    class SesionFalsa:
        def __init__(self, **kwargs):
            capturado.update(kwargs)

    monkeypatch.setattr(mod_fetch, "FetcherSession", SesionFalsa)
    monkeypatch.setattr(mod_fetch.config, "PROXY_URL", "http://usuario:clave@host:8080")

    mod_fetch.crear_sesion()

    assert "proxies" in capturado, (
        "el proxy tiene que viajar como `proxies` (plural): el singular a nivel "
        "sesion lo ignora Scrapling en silencio")
    assert capturado["proxies"] == {
        "http": "http://usuario:clave@host:8080",
        "https": "http://usuario:clave@host:8080",
    }


def test_sin_proxy_no_se_inventa_uno(monkeypatch):
    """Control negativo: sin PROXY_URL la sesion no lleva clave de proxy."""
    from venara_discovery import fetch as mod_fetch

    capturado = {}

    class SesionFalsa:
        def __init__(self, **kwargs):
            capturado.update(kwargs)

    monkeypatch.setattr(mod_fetch, "FetcherSession", SesionFalsa)
    monkeypatch.setattr(mod_fetch.config, "PROXY_URL", None)

    mod_fetch.crear_sesion()

    assert "proxies" not in capturado
    assert "proxy" not in capturado
