"""LinkedIn: pagina de empresa y personas.

QUE ESTA MEDIDO (F7 en FINDINGS.md, 2026-08-30)

  Paginas de EMPRESA: `site:linkedin.com/company <nicho> <ciudad>` en
  DuckDuckGo devuelve 10 URLs utiles. Funciona.

  Perfiles de PERSONA: `site:linkedin.com/in "<empresa>"` devuelve CERO en
  todas las variantes probadas -- con cargo, con ubicacion, sin operador, y
  con empresas grandes. Coincide con lo ya medido del lado de Venara, que
  tiene un test fijando que ninguna query use ese operador.

  El control positivo es lo que hace valida esa conclusion: el MISMO motor con
  el MISMO operador devuelve resultados para /company. No es que `site:` no
  funcione; son los perfiles personales los que no estan en el indice publico.

Consecuencia honesta: `buscar_persona()` intenta igual y devuelve NOT_FOUND con
un `source` que dice por que. No se inventa un contacto, y no se devuelve un
perfil que solo coincide de nombre. Un decision maker equivocado en una campana
de outbound es peor que ninguno.
"""
from __future__ import annotations
import logging
import re

from . import extraction, providers
from .fetch import SaludProveedores, obtener
from .location import interpretar
from .normalize import clave_nombre, limpiar_titulo, sin_acentos

log = logging.getLogger(__name__)

# Prioridad de cargos. El requisito pide responder "quien puede COMPRAR", y
# para una empresa chica ese es el fundador, no un manager.
PESO_CARGO = [
    (r"\b(founder|fundador[ao]?|co-?founder|cofundador[ao]?)\b", 1.00),
    (r"\b(ceo|chief executive)\b", 0.95),
    (r"\b(owner|dueñ[oa]|propietari[oa]|titular)\b", 0.92),
    (r"\b(managing director|director general|gerente general)\b", 0.88),
    (r"\b(partner|soci[oa])\b", 0.82),
    (r"\b(cto|cmo|coo|cfo|chief)\b", 0.78),
    (r"\b(vp|vice president|vicepresidente)\b", 0.72),
    (r"\b(head of|jefe de|responsable de)\b", 0.66),
    (r"\b(director[ao]?)\b", 0.62),
    (r"\b(gerente|manager)\b", 0.52),
    (r"\b(business development|desarrollo de negocios?)\b", 0.50),
]


def puntuar_cargo(titulo: str) -> float:
    t = sin_acentos(titulo or "").lower()
    for patron, peso in PESO_CARGO:
        if re.search(patron, t):
            return peso
    return 0.15


def es_url_de_empresa(url: str) -> bool:
    """Solo la pagina raiz de una empresa. Rechaza personas, posts y jobs."""
    u = (url or "").lower()
    if "linkedin.com/company/" not in u:
        return False
    return not any(b in u for b in ("/posts/", "/jobs/", "/pulse/", "/feed/",
                                    "/showcase/", "/company/setup"))


def es_url_de_persona(url: str) -> bool:
    return "linkedin.com/in/" in (url or "").lower()


def confianza_empresa(nombre_buscado: str, titulo_resultado: str, url: str) -> float:
    """Cuanto se parece la pagina encontrada a la empresa que se buscaba."""
    if not es_url_de_empresa(url):
        return 0.0
    kb = clave_nombre(nombre_buscado)
    kt = clave_nombre(titulo_resultado)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    ks = clave_nombre(slug.replace("-", " "))
    if not kb:
        return 0.0
    if kb == kt or kb == ks:
        return 0.96
    if (kt and (kb in kt or kt in kb)) or (ks and (kb in ks or ks in kb)):
        return 0.80
    return 0.35


def resolver_empresa(nombre: str, ubicacion: str = "") -> dict:
    """Encuentra la pagina de LinkedIn de una empresa, con confianza."""
    nombre = (nombre or "").strip()
    if len(nombre) < 2:
        return {"linkedin_company_url": "", "linkedin_company_name": "",
                "linkedin_company_confidence": 0.0, "source": "no_company"}
    ubi = interpretar(ubicacion)
    salud = SaludProveedores()
    activos = providers.activos()
    consultas = [
        'site:linkedin.com/company "' + nombre + '"',
        'site:linkedin.com/company ' + nombre + (" " + ubi.ciudad if ubi.ciudad else ""),
    ]
    mejor = {"linkedin_company_url": "", "linkedin_company_name": "",
             "linkedin_company_confidence": 0.0, "source": "not_found"}

    for consulta in consultas:
        for prov in activos:
            url = providers.construir_url(prov.nombre, consulta, ubi)
            r = obtener(url, prov.nombre, salud)
            if not r.sirve:
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            for it in items:
                conf = confianza_empresa(nombre, it.get("titulo", ""), it["url"])
                if conf > mejor["linkedin_company_confidence"]:
                    mejor = {
                        "linkedin_company_url": it["url"],
                        "linkedin_company_name": limpiar_titulo(it.get("titulo", "")),
                        "linkedin_company_confidence": round(conf, 3),
                        "source": prov.nombre,
                    }
            if mejor["linkedin_company_confidence"] >= 0.9:
                return mejor
    return mejor


def buscar_persona(empresa: str, ubicacion: str = "") -> dict:
    """Intenta encontrar un decisor. Devuelve el contrato historico.

    Se intenta de verdad, pero lo medido dice que los perfiles personales no
    estan en el indice publico. Cuando no hay evidencia solida, se devuelve
    NOT_FOUND con el motivo: inventar un contacto o aceptar una coincidencia
    debil manda un correo a la persona equivocada a nombre del cliente.
    """
    # Cortar ANTES de salir a la red. Sin esto una empresa vacia dispara tres
    # queries reales contra buscadores que ya estan cerca de su limite, para
    # buscar a alguien de "".
    empresa = (empresa or "").strip()
    if len(empresa) < 2:
        return {"person_name": "NOT_FOUND", "person_title": "", "linkedin_url": "",
                "source": "no_company", "company_match_confidence": 0.0}

    ubi = interpretar(ubicacion)
    salud = SaludProveedores()
    activos = providers.activos()
    lugar = (" " + ubi.ciudad) if ubi.ciudad else ""
    consultas = [
        'site:linkedin.com/in "' + empresa + '" (founder OR CEO OR director)',
        'linkedin.com/in "' + empresa + '" founder' + lugar,
        '"' + empresa + '" linkedin founder OR CEO' + lugar,
    ]

    mejor_score, mejor = 0.0, None
    for consulta in consultas:
        for prov in activos:
            url = providers.construir_url(prov.nombre, consulta, ubi)
            r = obtener(url, prov.nombre, salud)
            if not r.sirve:
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            for it in items:
                if not es_url_de_persona(it["url"]):
                    continue
                nombre, cargo = _partir_titulo(it.get("titulo", ""))
                if not nombre:
                    continue
                texto = (it.get("titulo", "") + " " + it.get("snippet", "")).lower()
                # La empresa TIENE que aparecer: sin eso es solo una persona
                # que el buscador relaciono por casualidad.
                if clave_nombre(empresa) and clave_nombre(empresa) not in clave_nombre(texto):
                    continue
                s = puntuar_cargo(cargo)
                if s > mejor_score:
                    mejor_score = s
                    mejor = {"person_name": nombre, "person_title": cargo,
                             "linkedin_url": it["url"], "source": prov.nombre,
                             "company_match_confidence": round(min(1.0, 0.5 + s / 2), 3)}
        if mejor_score >= 0.9:
            break

    if mejor:
        return mejor
    bloqueados = salud.resumen()
    return {
        "person_name": "NOT_FOUND", "person_title": "", "linkedin_url": "",
        # El source explica la ausencia. "not_indexed" no es lo mismo que
        # "blocked", y confundirlos manda a investigar el lugar equivocado.
        "source": "providers_blocked" if bloqueados else "not_indexed",
        "company_match_confidence": 0.0,
    }


def _partir_titulo(txt: str) -> tuple[str, str]:
    """"Juan Perez - Founder & CEO - Acme | LinkedIn" -> ("Juan Perez", "Founder & CEO")."""
    limpio = re.sub(r"\s*[|\-–—]\s*LinkedIn\s*$", "", txt or "", flags=re.I).strip()
    if not limpio:
        return "", ""
    partes = re.split(r"\s*[-–—]\s*", limpio)
    nombre = partes[0].strip()
    # Un nombre tiene 2 a 6 palabras y no lleva digitos.
    palabras = nombre.split()
    if not (2 <= len(palabras) <= 6) or len(nombre) > 60 or any(c.isdigit() for c in nombre):
        return "", ""
    cargo = partes[1].strip() if len(partes) > 1 else ""
    return nombre, cargo
