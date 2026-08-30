"""Estrategias de busqueda.

Cada estrategia existe por un motivo distinto y aporta algo que las otras no.
Ninguna esta para "hacer mas requests": el presupuesto es chico a proposito
porque el limite real no es la CPU, es cuantas queries tolera un buscador antes
de mandar un captcha (F4 en FINDINGS.md).

Cada resultado se etiqueta con la estrategia que lo trajo, asi se puede medir
cual aporta y cual sobra en vez de discutirlo.
"""
from __future__ import annotations
from dataclasses import dataclass

from .location import Ubicacion

# Variaciones semanticas del nicho. Sirven porque una agencia se describe como
# "agencia", otra como "estudio" y otra como "consultora": buscar una sola
# palabra deja fuera a las demas.
_VARIANTES = {
    "agencia": ["estudio", "consultora"],
    "agency": ["studio", "consultancy"],
    "marketing": ["publicidad", "comunicacion"],
    "contador": ["contable", "contabilidad"],
    "contadores": ["estudio contable", "contabilidad"],
    "abogado": ["estudio juridico", "legal"],
    "abogados": ["estudio juridico", "bufete"],
    "restaurante": ["restaurant"],
    "clinica": ["centro medico"],
    "inmobiliaria": ["bienes raices"],
    "constructora": ["construccion"],
    "software": ["desarrollo de software", "tecnologia"],
    "consultora": ["consultoria"],
}


@dataclass(frozen=True)
class Estrategia:
    nombre: str
    query: str
    # "web" busca el sitio de la empresa; "linkedin" busca su pagina de empresa.
    modo: str
    prioridad: int


def _variante(nicho: str) -> str:
    """Primera variante semantica aplicable, o "" si no hay."""
    bajo = nicho.lower()
    for clave, alternativas in _VARIANTES.items():
        if clave in bajo:
            for alt in alternativas:
                if alt not in bajo:
                    return bajo.replace(clave, alt)
    return ""


def construir(nicho: str, ubi: Ubicacion) -> list[Estrategia]:
    """Plan de busqueda para un (nicho, ubicacion).

    Devuelto en orden de prioridad: si el presupuesto de tiempo se agota, se
    corta por el final y se pierde lo menos valioso.
    """
    nicho = (nicho or "").strip()
    lugar = ubi.ciudad or ubi.texto or ""
    pais = ubi.pais_nombre or ""
    e: list[Estrategia] = []

    # 1. La busqueda directa. Es la que mas empresas reales trae.
    e.append(Estrategia("nicho_ubicacion", (nicho + " " + lugar).strip(), "web", 1))

    # 2. LinkedIn de empresa. Aporta identidad y una URL estable, aunque casi
    #    nunca traiga el website: eso lo resuelve el merge con las otras.
    if lugar:
        e.append(Estrategia("linkedin_empresa",
                            'site:linkedin.com/company ' + nicho + " " + lugar, "linkedin", 2))
    else:
        e.append(Estrategia("linkedin_empresa",
                            'site:linkedin.com/company ' + nicho, "linkedin", 2))

    # 3. Sesgo hacia el sitio propio. "contacto" aparece en la web de una
    #    empresa y casi nunca en un articulo que la menciona, asi que esta
    #    query sube la proporcion de sitios oficiales frente a prensa.
    e.append(Estrategia("contacto", (nicho + " " + lugar + " contacto").strip(), "web", 3))

    # 4. Variante semantica: alcanza empresas que se describen con otra palabra.
    v = _variante(nicho)
    if v:
        e.append(Estrategia("variante_semantica", (v + " " + lugar).strip(), "web", 4))

    # 5. Ampliacion a pais. Solo si hay ciudad: sirve para completar cuando la
    #    ciudad da poco, y se marca aparte para poder medir si aporta ruido.
    if ubi.ciudad and pais:
        e.append(Estrategia("ampliacion_pais", (nicho + " " + pais).strip(), "web", 5))

    return e
