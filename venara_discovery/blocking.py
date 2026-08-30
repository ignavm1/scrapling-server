"""Deteccion de bloqueo, captcha y paginas sin resultados renderizados.

POR QUE ESTE MODULO ES EL MAS IMPORTANTE DEL SERVIDOR

Medido en produccion el 2026-08-30: `/search-linkedin-companies` devolvia
HTTP 200 con {"results": [], "total": 0} para TODA busqueda, tardando ~47.6s.
El cliente de Venara leia queriesFailed = 0 y concluia "el nicho no tiene
resultados". El nicho tenia resultados; los buscadores estaban bloqueando al
servidor.

El sintoma no se parecia a la causa, y esa es justamente la clase de fallo que
mas cuesta: nadie investiga un 200 con lista vacia.

LA TRAMPA DEL STATUS CODE

DuckDuckGo NO responde 403 ni 429 cuando bloquea. Responde **HTTP 202** con una
pagina de captcha ("Select all squares containing a duck"). Capturada como
fixture: tests/fixtures/ddg_blocked.html, 202, 13066 bytes.

Cualquier chequeo del tipo `if status in (403, 429)` lo deja pasar como exito.
Por eso aca se mira el CUERPO, no solo el status.
"""
from __future__ import annotations
import re
from dataclasses import dataclass

# Marcas FUERTES: frases completas que solo aparecen en una pagina anti-bot.
# Observadas en respuestas reales. Valen por si solas.
_MARCAS_BLOQUEO = (
    "bots use duckduckgo",
    "confirm this search was made by a human",
    "select all squares containing",
    "unusual traffic",
    "detected unusual",
    "our systems have detected",
    "are you a robot",
)

# Marcas DEBILES: palabras que tambien aparecen en paginas legitimas -- una
# empresa que vende servicios anti-captcha, un articulo sobre rate limiting.
# Solo cuentan si ADEMAS no hay resultados que extraer.
#
# Sin esta separacion, una pagina con 12 resultados validos que mencionara
# "captcha" apagaba el proveedor para toda la busqueda (F15).
_MARCAS_DEBILES = (
    "captcha",
    "recaptcha",
    "access denied",
    "too many requests",
    "rate limit",
    "why did this happen",
)

# Marcas de "esta pagina necesita JavaScript para mostrar resultados".
# Google devuelve exactamente esto desde 2026: un bootstrap, no resultados.
_MARCAS_JS_SHELL = (
    "/httpservice/retry/enablejs",
    "enablejs",
    "please click here if you are not redirected",
    "haz clic aqu",
)

# Status que casi siempre son bloqueo. 202 esta incluido a proposito: es el que
# usa DuckDuckGo para su captcha, y es el que rompe los detectores ingenuos.
_STATUS_SOSPECHOSOS = {202, 401, 403, 407, 429, 503}


@dataclass(frozen=True)
class Veredicto:
    """Resultado del analisis de una respuesta de buscador."""
    bloqueado: bool
    motivo: str          # "" cuando no hay bloqueo
    anclas: int          # cuantos <a href> externos trae la pagina

    @property
    def utilizable(self) -> bool:
        return not self.bloqueado


def contar_anclas_externas(html: str) -> int:
    """Anclas que apuntan fuera del propio buscador.

    Es la senal mas dificil de falsear: una pagina de resultados REAL tiene
    decenas; un captcha o un shell de JS tiene una o ninguna.
    """
    if not html:
        return 0
    crudas = re.findall(r'<a\b[^>]*href=["\'](.*?)["\']', html, flags=re.I | re.S)
    n = 0
    for h in crudas:
        h = h.strip()
        bajo = h.lower()
        # Redirects del propio buscador hacia un externo. CUENTAN: son
        # resultados envueltos, no navegacion interna.
        #
        # El `/ck/a` de Bing hay que reconocerlo explicitamente o toda pagina de
        # Bing parece vacia: Bing envuelve el 100% de sus resultados en su
        # propio dominio (F5 en FINDINGS.md).
        if (bajo.startswith("/url?q=") or "//duckduckgo.com/l/" in bajo
                or "/ck/a" in bajo):
            n += 1
            continue
        if not h.startswith("http"):
            continue
        if any(m in bajo for m in ("google.com/", "bing.com/", "duckduckgo.com/",
                                   "microsoft.com/", "microsofttranslator.",
                                   "go.microsoft.com", "support.microsoft.com")):
            continue
        n += 1
    return n


def analizar(html: str | None, status: int | None) -> Veredicto:
    """Decide si una respuesta de buscador sirve.

    El orden importa: primero las senales positivas de bloqueo (texto), despues
    el status, y recien al final el heuristico de "pagina vacia". Asi el motivo
    que se reporta es el mas especifico disponible, que es lo que hace util un
    log a las 3 de la manana.
    """
    if html is None:
        return Veredicto(True, "sin-respuesta", 0)

    bajo = html.lower()
    anclas = contar_anclas_externas(html)

    for marca in _MARCAS_BLOQUEO:
        if marca in bajo:
            return Veredicto(True, "captcha", anclas)

    if anclas < 5:
        for marca in _MARCAS_DEBILES:
            if marca in bajo:
                return Veredicto(True, "captcha", anclas)

    # El shell de JS solo cuenta como bloqueo si ADEMAS no hay resultados. Una
    # pagina real puede mencionar "enablejs" en un script sin ser un shell.
    if anclas < 5:
        for marca in _MARCAS_JS_SHELL:
            if marca in bajo:
                return Veredicto(True, "requiere-javascript", anclas)

    if status in _STATUS_SOSPECHOSOS:
        return Veredicto(True, "status-" + str(status), anclas)

    # Ultimo recurso: 200, sin marcas, pero sin nada que extraer. Es bloqueo
    # silencioso o un cambio de HTML; en ambos casos el resultado NO se debe
    # cachear como "esta busqueda no tiene empresas".
    if anclas < 3:
        return Veredicto(True, "sin-resultados-extraibles", anclas)

    return Veredicto(False, "", anclas)
