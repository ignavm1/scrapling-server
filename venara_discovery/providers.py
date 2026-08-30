"""Proveedores de busqueda.

Un proveedor es solo: como se arma la URL + como se llama + si esta activo.
Agregar uno nuevo es agregar una entrada al registro, sin tocar el pipeline.

DECISIONES MEDIDAS (ver FINDINGS.md)

  google       APAGADO. Devuelve un bootstrap de JavaScript sin resultados en
               el HTML (F3). Ningun selector lo arregla. Consumia un tercio del
               presupuesto de cada busqueda para no aportar nada.

  bing         Necesita `mkt=` y `cc=`. Con `setlang=es` a secas devolvia
               contadores de Guatemala para "contadores Santiago Chile" (F6).
               Sigue siendo erratico: hay que tratarlo como fuente hostil y
               filtrar por relevancia SIEMPRE.

  duckduckgo   El mas fiable para resultados, y el que primero bloquea. Es el
               unico que honra `site:linkedin.com/company` de forma util.

  brave        Indice propio (no revende Google ni Bing). Sirve HTML completo,
               con URLs directas sin envoltorio. Medido: 9 empresas utiles en
               una sola pagina, y NO nos bloqueo.

  ddglite      El endpoint liviano de DuckDuckGo: 17KB en vez de 29KB para los
               mismos resultados. Es un proveedor aparte y no un detalle del
               otro porque tiene su propio presupuesto de tolerancia -- cuando
               el endpoint html empieza a devolver captcha, este todavia
               responde, y esa independencia es justamente lo que aporta.

QUE NO SE HACE, Y POR QUE

  Ningun proveedor intenta resolver ni esquivar un captcha. Cuando un motor
  responde con un desafio, se marca bloqueado y se gasta el presupuesto en los
  que si atienden. Medido el 2026-08-30: Google devuelve HTTP 429 con un
  formulario de captcha ("comprobamos si eres tu quien envia las solicitudes en
  lugar de un robot"). Eso no es un problema de renderizado que se arregle con
  un navegador: es una decision de acceso explicita (F17, F18).

  Brave y ddglite se sumaron porque SI atienden: 10 -> 20 empresas unicas sobre
  el mismo corpus (F19).
"""
from __future__ import annotations
from dataclasses import dataclass
from urllib.parse import quote

from . import config
from .location import Ubicacion


@dataclass(frozen=True)
class Proveedor:
    nombre: str
    activo: bool
    # Peso al ordenar: si el presupuesto no alcanza para todos, se gastan
    # primero los que mas aportan por request.
    prioridad: int


def url_bing(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://www.bing.com/search?q=" + quote(q) + "&count=" + str(n)
    mercado = ubi.mercado
    if mercado:
        # `mkt` fija el mercado y `cc` el pais. Medido: sin esto Bing devuelve
        # otro pais entero.
        return base + "&mkt=" + mercado + "&cc=" + ubi.pais
    return base + "&setlang=es"


def url_ddg(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://html.duckduckgo.com/html/?q=" + quote(q)
    # kl combina region e idioma. Sin region, DuckDuckGo mezcla paises.
    if ubi.pais:
        return base + "&kl=" + ("es-" + ubi.pais.lower() if ubi.pais != "BR" else "br-pt")
    return base + "&kl=es-es"


def url_brave(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://search.brave.com/search?q=" + quote(q)
    # Brave acepta pais con `country=`; sin el mezcla mercados como los demas.
    return base + ("&country=" + ubi.pais.lower() if ubi.pais else "")


def url_ddglite(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://lite.duckduckgo.com/lite/?q=" + quote(q)
    if ubi.pais:
        return base + "&kl=" + ("es-" + ubi.pais.lower() if ubi.pais != "BR" else "br-pt")
    return base + "&kl=es-es"


def url_google(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://www.google.com/search?q=" + quote(q) + "&num=" + str(n) + "&hl=es"
    return base + ("&gl=" + ubi.pais.lower() if ubi.pais else "")


CONSTRUCTORES = {
    "bing": url_bing,
    "duckduckgo": url_ddg,
    "ddglite": url_ddglite,
    "brave": url_brave,
    "google": url_google,
}

# La prioridad ordena el gasto del presupuesto: si el tiempo se acaba, lo que
# queda sin hacer es lo que menos aporta por request.
REGISTRO = [
    Proveedor("duckduckgo", True, 1),
    Proveedor("brave", True, 2),
    Proveedor("bing", True, 3),
    Proveedor("ddglite", True, 4),
    Proveedor("google", config.ENABLE_GOOGLE, 5),
]


def activos() -> list[Proveedor]:
    return sorted([p for p in REGISTRO if p.activo], key=lambda p: p.prioridad)


def construir_url(proveedor: str, q: str, ubi: Ubicacion, n: int = 20) -> str:
    return CONSTRUCTORES[proveedor](q, ubi, n)
