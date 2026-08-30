"""Extraccion de resultados desde el HTML de un buscador.

Los motores cambian su HTML sin avisar y un selector muerto no se queja: extrae
cero y el sistema reporta "no hay empresas". Por eso hay DOS capas:

  1. Selectores CSS por motor, varios por motor.
  2. Fallback por regex sobre el HTML crudo, que no depende de ninguna clase.

La capa 2 es la que sobrevive a un rediseno. Se usa siempre que la 1 devuelva
mucho menos de lo esperado, no solo cuando devuelva cero: un selector que
todavia matchea el 20% de los resultados es un selector roto a medias, y ese
caso es el que pasa desapercibido.
"""
from __future__ import annotations
import html as _html
import re

from .normalize import decodificar_redirect, limpiar_titulo, normalizar_url

# Selectores por motor, del mas especifico al mas general.
SELECTORES = {
    "bing": [("li.b_algo h2 a", None), ("ol#b_results li.b_algo a", "h2"),
             ("li.b_algo a", None)],
    "duckduckgo": [(".result__title a", None), ("a.result__a", None),
                   (".results .result a", None)],
    "google": [("div.g a", "h3"), ("div.tF2Cxc a", "h3"), ("div.yuRUbf a", "h3")],
    "brave": [("#results .snippet a", None), (".snippet-title", None),
              ("a.result-header", None)],
    # lite.duckduckgo.com es una tabla sin clases: no hay selector util y el
    # fallback por regex es la via correcta, no un parche.
    "ddglite": [("a.result-link", None)],
}

_RX_ANCLA = re.compile(r'<a\b[^>]*\bhref=["\'](.*?)["\'][^>]*>(.*?)</a>', re.I | re.S)
_RX_TAGS = re.compile(r"<[^>]+>")


def _texto(fragmento: str) -> str:
    return re.sub(r"\s+", " ", _html.unescape(_RX_TAGS.sub(" ", fragmento or ""))).strip()


# Bing pinta un breadcrumb dentro del ancla: "limadigital.pe https://limadigital.pe
# › servicios". Tomado como titulo, no comparte NINGUN token con la query y el
# filtro de relevancia tira una empresa perfectamente buena. El fallback tiene
# que entregar texto utilizable o no es un fallback: sus resultados los descarta
# la capa siguiente.
_RX_BREADCRUMB = re.compile(r"https?://\S+|\s›\s|\s>\s", re.I)


def _limpiar_breadcrumb(texto: str) -> str:
    t = _RX_BREADCRUMB.sub(" ", texto or "")
    return re.sub(r"\s+", " ", t).strip()


def extraer_por_regex(html: str) -> list[dict]:
    """Fallback independiente del CSS.

    Recorre TODAS las anclas del documento, desenvuelve el redirect del buscador
    y se queda con las externas. No sabe nada de clases, asi que un rediseno no
    lo rompe.

    Ademas toma el texto que sigue al ancla como snippet: sin el, los resultados
    llegan sin evidencia y la relevancia da 0 aunque la empresa sea correcta.
    """
    salida = []
    vistos = set()
    crudo = html or ""
    for m in _RX_ANCLA.finditer(crudo):
        href, interior = m.group(1), m.group(2)
        url = decodificar_redirect(href)
        if not url.startswith("http"):
            continue
        norm = normalizar_url(url)
        if not norm or norm in vistos:
            continue
        titulo = limpiar_titulo(_limpiar_breadcrumb(_texto(interior)))
        # Ventana de texto posterior al ancla: ahi vive la descripcion del
        # resultado en los tres motores, sin depender de ninguna clase.
        cola = _texto(crudo[m.end(): m.end() + 1200])
        snippet = _limpiar_breadcrumb(cola)[:400]
        vistos.add(norm)
        salida.append({"url": norm, "titulo": titulo, "snippet": snippet})
    return salida


def _snippet_de(anchor) -> str:
    """Texto descriptivo cercano al resultado.

    Se suben como mucho 3 ancestros, no 5. El servidor viejo subia 5 y a esa
    altura ya se esta leyendo el bloque del resultado VECINO — de ahi salian
    websites asignados a la empresa equivocada (F11 en FINDINGS.md).
    """
    try:
        par = anchor
        for _ in range(3):
            par = par.parent
            if par is None:
                break
            txt = " ".join(par.css("::text").getall())
            txt = re.sub(r"\s+", " ", txt).strip()
            if 40 <= len(txt) <= 600:
                return txt[:400]
    except Exception:
        pass
    return ""


def extraer_por_css(page, motor: str) -> list[dict]:
    """Extraccion con los selectores del motor."""
    salida = []
    vistos = set()
    for sel, sel_titulo in SELECTORES.get(motor, []):
        try:
            anclas = page.css(sel)
        except Exception:
            continue
        for a in anclas:
            try:
                href = decodificar_redirect(a.attrib.get("href", ""))
            except Exception:
                continue
            if not href.startswith("http"):
                continue
            norm = normalizar_url(href)
            if not norm or norm in vistos:
                continue
            vistos.add(norm)
            titulo = ""
            try:
                if sel_titulo:
                    t = a.css(sel_titulo)
                    titulo = (t.css("::text").get() if t else None) or a.css("::text").get() or ""
                else:
                    titulo = a.css("::text").get() or ""
            except Exception:
                titulo = ""
            salida.append({
                "url": norm,
                "titulo": limpiar_titulo(_texto(titulo)),
                "snippet": _snippet_de(a),
            })
    return salida


# Debajo de esto se considera que los selectores estan rotos y se completa con
# el fallback. No es "cero" a proposito: un selector que matchea 2 de 10
# resultados esta roto igual, y ese caso es el que pasa desapercibido.
MINIMO_ESPERADO = 5


def extraer(page, html: str, motor: str) -> tuple[list[dict], str]:
    """Devuelve (resultados, metodo). El metodo va al log para poder ver cuando
    un motor cambio su HTML sin que nadie lo note."""
    por_css = []
    if page is not None:
        try:
            por_css = extraer_por_css(page, motor)
        except Exception:
            por_css = []

    if len(por_css) >= MINIMO_ESPERADO:
        return por_css, "css"

    por_regex = extraer_por_regex(html)
    if len(por_regex) > len(por_css):
        metodo = "regex-fallback" if por_css else "regex"
        # Se unen ambos: el CSS aporta titulos mejores cuando funciona.
        por_url = {r["url"]: r for r in por_regex}
        for r in por_css:
            if r["url"] in por_url and r["titulo"]:
                por_url[r["url"]]["titulo"] = r["titulo"]
                por_url[r["url"]]["snippet"] = r["snippet"] or por_url[r["url"]]["snippet"]
        return list(por_url.values()), metodo
    return por_css, "css"
