"""Angulo LinkedIn: buscar el perfil del decisor y ENTRAR a leerlo.

POR QUE HAY QUE ENTRAR, Y NO ALCANZA CON EL SNIPPET

Medido el 2026-09-04. Brave devuelve perfiles para `"Fintual" CEO linkedin`,
pero su titulo llega como breadcrumb:

    'LinkedIn cl.linkedin.com in andresmarinkovic Andrés Marinkovic'

Ahi hay un nombre y no hay cargo, asi que el parser de SERP no produce nada. El
titulo de la PAGINA, en cambio, lo tiene todo:

    'Andrés Marinkovic - Co Founder y COO en Fintual (YC S18) | LinkedIn'

Nombre, cargo y empresa en una sola linea. Por eso la regla es entrar.

QUE SE CORRIGE DE LO QUE ESTABA ESCRITO ANTES

F7 concluyo que "los perfiles personales no estan en el indice publico". Eso se
midio sobre Bing y DuckDuckGo con el operador `site:linkedin.com/in`, y se
generalizo de mas: **Brave si los devuelve** cuando la query dice simplemente
"linkedin" en vez de usar el operador. El indice no era el problema; el buscador
si. Ver F24.

QUE NO SE HACE

No se inicia sesion, no se manda cookie y no se resuelve ningun desafio. Se lee
lo que LinkedIn sirve publicamente a cualquiera: el `<title>` y el `og:title`.
El cuerpo del perfil viene detras de un muro de sesion y se deja ahi.
"""
from __future__ import annotations
import html as _html
import logging
import re

from .linkedin import puntuar_cargo
from .normalize import sin_acentos

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Que es un perfil
# ─────────────────────────────────────────────────────────────────────────────
# Se aceptan los subdominios de pais (cl., mx., pe., es., www., o ninguno)
# porque LinkedIn sirve el mismo perfil desde todos, y descartar `cl.` dejaria
# fuera justo a los perfiles chilenos, que son los que mas interesan aca.
_RX_PERFIL = re.compile(r"^https?://([a-z]{2,3}\.)?linkedin\.com/in/([^/?#]+)", re.I)


def es_perfil(url: str) -> bool:
    """La regla del usuario, en una funcion: solo se entra si es un perfil.

    Rechaza /company/, /jobs/, /posts/, /pulse/ y cualquier otro dominio. No es
    una optimizacion: entrar a lo que no es un perfil gasta un fetch y no puede
    devolver un decisor.
    """
    return bool(_RX_PERFIL.match((url or "").strip()))


def pais_del_perfil(url: str) -> str:
    """Codigo de pais del subdominio, o "" si el perfil no lo declara.

    `cl.linkedin.com/in/x` es una senal barata y real de que la persona esta en
    Chile. Es lo que faltaba para el falso positivo medido: dos empresas
    distintas llamadas igual en paises distintos.
    """
    m = _RX_PERFIL.match((url or "").strip())
    if not m or not m.group(1):
        return ""
    sub = m.group(1).rstrip(".").lower()
    return "" if sub == "www" else sub.upper()


# ─────────────────────────────────────────────────────────────────────────────
# El titulo del perfil
# ─────────────────────────────────────────────────────────────────────────────
# Formas reales, verificadas sobre paginas servidas:
#   "Andrés Marinkovic - Co Founder y COO en Fintual (YC S18) | LinkedIn"
#   "Omar Larré - Co-founder & CIO - Fintual | LinkedIn"
#   "Jane Doe - CEO at Acme | LinkedIn"
_RX_TITULO_HTML = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_RX_OG_TITULO = re.compile(
    r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I)

# Cola que LinkedIn agrega siempre. Se corta antes de parsear para que no
# ensucie el nombre de la empresa.
_RX_COLA_LINKEDIN = re.compile(r"\s*[|·]\s*LinkedIn\s*$", re.I)

# Ruido entre parentesis del estilo "(YC S18)" o "(She/Her)": es del perfil, no
# del nombre de la empresa.
_RX_PARENTESIS = re.compile(r"\s*\([^)]{0,40}\)\s*$")

# El separador entre cargo y empresa es " en " (es), " at " (en) o un guion.
_RX_CARGO_EMPRESA = re.compile(r"^(?P<cargo>.+?)\s+(?:en|at|de)\s+(?P<empresa>.+)$", re.I)


def _limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", _html.unescape(txt or "")).strip()


def titulo_de(html: str) -> str:
    """El titulo del perfil. Prefiere og:title, que viene sin la cola del sitio."""
    m = _RX_OG_TITULO.search(html or "")
    if m:
        return _limpiar(m.group(1))
    m = _RX_TITULO_HTML.search(html or "")
    return _limpiar(m.group(1)) if m else ""


def parsear_titulo(titulo: str) -> dict | None:
    """"Nombre - Cargo en Empresa | LinkedIn" -> {nombre, cargo, empresa}.

    Devuelve None cuando el titulo no tiene la forma de un perfil. Es lo que
    impide que una pagina de login o un error 404 de LinkedIn --que tambien
    terminan en "| LinkedIn"-- produzcan un candidato inventado.
    """
    t = _limpiar(titulo)
    if not t:
        return None
    # Se exige la marca de LinkedIn: sin ella no se sabe que se esta leyendo.
    if not re.search(r"\bLinkedIn\b", t, re.I):
        return None
    t = _RX_COLA_LINKEDIN.sub("", t).strip()

    partes = [p.strip() for p in re.split(r"\s+[-–—]\s+", t) if p.strip()]
    if len(partes) < 2:
        return None

    nombre = partes[0]
    resto = " - ".join(partes[1:])
    resto = _RX_PARENTESIS.sub("", resto).strip()

    cargo, empresa = resto, ""
    m = _RX_CARGO_EMPRESA.match(resto)
    if m:
        cargo = m.group("cargo").strip()
        empresa = _RX_PARENTESIS.sub("", m.group("empresa").strip()).strip()
    elif len(partes) >= 3:
        # "Nombre - Cargo - Empresa": el ultimo tramo es la empresa.
        cargo = partes[1].strip()
        empresa = _RX_PARENTESIS.sub("", partes[-1].strip()).strip()

    cargo = re.sub(r"\s+", " ", cargo)[:80].strip(" -–—|·,")
    empresa = re.sub(r"\s+", " ", empresa)[:60].strip(" -–—|·,")
    if not cargo:
        return None
    # Un perfil sin cargo reconocible no sirve: puede ser un estudiante o un
    # jubilado, y el canal existe para encontrar a quien decide.
    if puntuar_cargo(cargo) <= 0.15:
        return None
    return {"nombre": nombre, "cargo": cargo, "empresa": empresa}


def coincide_empresa(empresa_perfil: str, empresa_buscada: str) -> bool:
    """El perfil dice trabajar en la empresa que buscamos?

    Comparacion por palabras y sin genericos: "Fintual (YC S18)" y "Fintual" son
    la misma; "Houm" de India y "Houm" de Chile tambien -- por eso el pais se
    chequea aparte y no aca.
    """
    norm = lambda s: " " + re.sub(r"[^a-z0-9]+", " ", sin_acentos(s or "").lower()).strip() + " "
    a, b = norm(empresa_perfil), norm(empresa_buscada).strip()
    if not b:
        return False
    if " " + b + " " in a:
        return True
    # Token distintivo, para "Fintual SpA" vs "Fintual".
    for palabra in b.split():
        if len(palabra) > 3 and " " + palabra + " " in a:
            return True
    return False


def construir_queries(empresa: str, cargos: list[str]) -> list[str]:
    """Las queries que pidio el usuario: "(empresa) CEO", "(empresa) gerente".

    Se agrega la palabra "linkedin" en vez del operador `site:linkedin.com/in`:
    medido el 2026-09-04, el operador da cero en los proveedores disponibles y
    la palabra suelta devuelve siete perfiles en Brave para la misma empresa.
    """
    e = (empresa or "").strip()
    if not e:
        return []
    vistos, out = set(), []
    for cargo in [c.strip() for c in cargos if c and c.strip()]:
        q = f'"{e}" {cargo} linkedin'
        if q.lower() in vistos:
            continue
        vistos.add(q.lower())
        out.append(q)
    return out
