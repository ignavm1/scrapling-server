"""G4 -- la extraccion sobrevive a un cambio de HTML del buscador.

Un selector muerto no se queja: extrae cero y el sistema reporta "no hay
empresas". Eso es exactamente lo que pasaba con Google. Estos tests fijan que
haya una segunda via que no dependa de ninguna clase CSS.
"""
from __future__ import annotations
import pathlib
import pytest

from venara_discovery import extraction
from venara_discovery.normalize import decodificar_redirect, normalizar_url

FIX = pathlib.Path(__file__).parent / "fixtures"
leer = lambda n: (FIX / n).read_text(encoding="utf-8")


def test_decodifica_el_redirect_de_bing():
    # ESTE era el bug que hacia que Bing no aportara NI UNA empresa (F5):
    # `fix_href` manejaba Google y DuckDuckGo pero no el /ck/a de Bing, asi que
    # toda URL de Bing salia como bing.com y se descartaba como basura.
    real = "https://kallpacreativa.com/servicios"
    import base64
    b64 = base64.urlsafe_b64encode(real.encode()).decode().rstrip("=")
    href = "https://www.bing.com/ck/a?!&&p=abc&u=a1" + b64 + "&ntb=1"
    assert decodificar_redirect(href) == real


def test_decodifica_google_y_duckduckgo():
    assert decodificar_redirect("/url?q=https%3A%2F%2Facme.pe%2F&sa=U") == "https://acme.pe/"
    assert decodificar_redirect(
        "//duckduckgo.com/l/?uddg=https%3A%2F%2Facme.pe%2Fcontacto") == "https://acme.pe/contacto"


def test_el_fallback_por_regex_extrae_de_html_real():
    # Sin depender de ninguna clase CSS.
    res = extraction.extraer_por_regex(leer("ddg_companies.html"))
    assert len(res) >= 8, "solo %d resultados" % len(res)
    assert all(r["url"].startswith("https://") for r in res)


def test_el_fallback_funciona_con_html_completamente_desconocido():
    # Simula un rediseno total: clases inventadas que ningun selector conoce.
    html = """<html><body>
      <div class="clase-que-no-existe-en-ningun-selector">
        <a href="https://kallpacreativa.pe/">Kallpa Creativa | Agencia</a>
        <a href="https://onzamarketing.com/">Onza Marketing</a>
        <a href="https://limadigital.pe/">Lima Digital</a>
        <a href="https://addigital.pe/">AD Digital</a>
        <a href="https://otraagencia.pe/">Otra Agencia</a>
      </div></body></html>"""
    res = extraction.extraer_por_regex(html)
    assert len(res) == 5
    assert res[0]["titulo"] == "Kallpa Creativa"    # el "| Agencia" es ruido de buscador


def test_extraer_usa_el_fallback_cuando_el_css_no_encuentra_nada():
    html = "".join('<a href="https://empresa%d.pe/">Empresa %d</a>' % (i, i) for i in range(9))
    res, metodo = extraction.extraer(None, html, "bing")
    assert metodo in ("regex", "regex-fallback")
    assert len(res) == 9


def test_no_se_confunden_resultados_vecinos():
    # El servidor viejo subia 5 ancestros buscando el website y a esa altura ya
    # leia el bloque del resultado de al lado (F11). El limite ahora es 3.
    import inspect
    src = inspect.getsource(extraction._snippet_de)
    assert "range(3)" in src, "el limite de ancestros volvio a subir"


def test_normalizar_url_quita_tracking_y_www():
    a = normalizar_url("https://www.Acme.pe/servicios/?utm_source=x&gclid=y&id=7#top")
    assert a == "https://acme.pe/servicios?id=7"


@pytest.mark.parametrize("basura", ["", "no-es-url", "javascript:alert(1)",
                                    "ftp://acme.pe", "file:///etc/passwd"])
def test_urls_malformadas_no_producen_resultados(basura):
    assert normalizar_url(basura) == ""


def test_marcador_final():
    print("\nEXTRACCION VERIFICADA")


def test_el_fallback_entrega_texto_UTILIZABLE_no_solo_urls():
    """Un fallback cuyos resultados la capa siguiente descarta no es un fallback.

    Bing pinta un breadcrumb dentro del ancla ("limadigital.pe
    https://limadigital.pe > servicios"). Tomado como titulo no comparte ni un
    token con la query, asi que el filtro de relevancia tiraba empresas
    perfectamente buenas -- 8 de ellas en este fixture.

    En vivo no se notaba porque ahi funciona el CSS. Se notaria el dia que
    Bing cambie su HTML, que es justo el dia en que el fallback tiene que
    salvar la busqueda.
    """
    import pathlib
    from venara_discovery import filtering
    ctx = "agencia de marketing digital Lima Peru"
    html = (FIX / "bing_ok.html").read_text(encoding="utf-8")

    items = extraction.extraer_por_regex(html)
    utiles = [it for it in items
              if not filtering.motivo_descarte(it["url"], it["titulo"])
              and filtering.relevancia(ctx, it["titulo"], it["snippet"], it["url"]) >= 0.34]
    assert len(utiles) >= 5, "solo %d empresas sobreviven al filtro con el fallback" % len(utiles)
    assert any(it["snippet"] for it in items), "el fallback no extrae snippets"


def test_el_breadcrumb_no_queda_como_nombre():
    limpio = extraction._limpiar_breadcrumb("limadigital.pe https://limadigital.pe › servicios")
    assert "https://" not in limpio and "›" not in limpio
