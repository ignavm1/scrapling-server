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


def url_google(q: str, ubi: Ubicacion, n: int = 20) -> str:
    base = "https://www.google.com/search?q=" + quote(q) + "&num=" + str(n) + "&hl=es"
    return base + ("&gl=" + ubi.pais.lower() if ubi.pais else "")


CONSTRUCTORES = {"bing": url_bing, "duckduckgo": url_ddg, "google": url_google}

REGISTRO = [
    Proveedor("duckduckgo", True, 1),
    Proveedor("bing", True, 2),
    Proveedor("google", config.ENABLE_GOOGLE, 3),
]


def activos() -> list[Proveedor]:
    return sorted([p for p in REGISTRO if p.activo], key=lambda p: p.prioridad)


def construir_url(proveedor: str, q: str, ubi: Ubicacion, n: int = 20) -> str:
    return CONSTRUCTORES[proveedor](q, ubi, n)
