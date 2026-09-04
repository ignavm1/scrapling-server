"""Resolucion y validacion del website oficial.

Una empresa vista solo en LinkedIn no se descarta por no traer website: se
intenta encontrarlo. Pero NO cualquier URL vale como website — la mitad del
valor de este modulo esta en lo que rechaza.
"""
from __future__ import annotations
import logging
import re

from . import filtering
from .entity import Empresa
from .fetch import SaludProveedores, obtener
from .location import Ubicacion
from .normalize import clave_nombre, dominio_registrable, sin_acentos
from . import extraction, providers

log = logging.getLogger(__name__)

_TAGS = re.compile(r"<(script|style|noscript)[\s\S]*?</\1>", re.I)


def limpiar_html(html: str, max_chars: int = 12000) -> str:
    """HTML -> texto plano acotado."""
    t = _TAGS.sub(" ", html or "")
    t = re.sub(r"<[^>]+>", " ", t)
    t = (t.replace("&nbsp;", " ").replace("&amp;", "&")
          .replace("&quot;", '"').replace("&#39;", "'")
          .replace("&lt;", "<").replace("&gt;", ">"))
    return re.sub(r"\s+", " ", t).strip()[:max_chars]


# Etiquetas que separan una cosa de la siguiente en una pagina. Sin este corte,
# "<h3>Matias Bravo</h3><p>Gerente General</p>" queda como "Matias Bravo Gerente
# General" y no hay forma de saber donde termina el nombre y empieza el cargo.
_BLOQUE = re.compile(
    r"</?(?:p|div|li|tr|td|th|br|h[1-6]|section|article|header|footer|"
    r"figcaption|blockquote|dt|dd|option)\b[^>]*>", re.I)


def texto_por_bloques(html: str, max_chars: int = 20000) -> str:
    """HTML -> texto con UN SALTO DE LINEA por bloque.

    `limpiar_html()` colapsa todo a una linea, que es lo correcto para medir
    relevancia pero destruye justo lo que necesita la extraccion de personas:
    en una pagina de equipo el nombre y el cargo son dos bloques distintos, y
    pegados no se distinguen de una frase cualquiera.
    """
    t = _TAGS.sub(" ", html or "")
    t = _BLOQUE.sub("\n", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = (t.replace("&nbsp;", " ").replace("&amp;", "&")
          .replace("&quot;", '"').replace("&#39;", "'")
          .replace("&lt;", "<").replace("&gt;", ">"))
    # Se colapsan espacios DENTRO de cada linea, no entre lineas.
    lineas = [re.sub(r"[ \t]+", " ", l).strip() for l in t.split("\n")]
    return "\n".join(l for l in lineas if l)[:max_chars]


def pertenece_a(url: str, nombre_empresa: str) -> float:
    """0..1 — cuanta evidencia hay de que este dominio sea de ESTA empresa.

    Sin esto, "el primer resultado que no sea LinkedIn" se toma como website
    oficial, y eso asigna el sitio de un directorio o de un competidor.
    """
    dom = dominio_registrable(url)
    if not dom:
        return 0.0
    if filtering.motivo_descarte(url, ""):
        return 0.0
    # Comparacion "estrecha": solo sin acentos ni puntuacion, SIN quitar
    # genericos. `kallpacreativa.pe` es literalmente "Kallpa Creativa"
    # concatenado, y eso es prueba maxima de pertenencia. La comparacion
    # relajada de abajo lo daba en 0.75 porque "creativa" es un generico del
    # rubro y lo eliminaba de los dos lados de forma desigual.
    import re as _re
    estrecho = lambda x: _re.sub(r"[^a-z0-9]", "", sin_acentos(x or "").lower())
    if estrecho(nombre_empresa) and estrecho(nombre_empresa) == estrecho(dom.split(".")[0]):
        return 1.0

    kn = clave_nombre(nombre_empresa)
    kd = clave_nombre(dom.split(".")[0])
    if not kn or not kd:
        return 0.3
    if kn == kd:
        return 1.0
    if kn in kd or kd in kn:
        return 0.75
    # Iniciales: "Kallpa Creativa" -> "kc". Debil pero no nulo.
    if len(kn) >= 4 and kd.startswith(kn[:4]):
        return 0.55
    return 0.2


UMBRAL_WEBSITE = 0.5


def resolver_para(empresas: list[Empresa], ubi: Ubicacion,
                  maximo: int = 5) -> int:
    """Busca el website de las empresas que solo tienen LinkedIn.

    Se limita a `maximo` porque cada resolucion es una query mas contra un
    buscador que ya esta cerca de su limite de tolerancia. Se priorizan las
    mejor puntuadas: gastar el presupuesto en la empresa numero 40 no cambia
    el resultado del cliente.
    """
    sin_web = [e for e in empresas if not e.website and e.nombre][:maximo]
    if not sin_web:
        return 0

    salud = SaludProveedores()
    activos = providers.activos()
    if not activos:
        return 0
    encontrados = 0

    for e in sin_web:
        consulta = e.nombre + (" " + ubi.ciudad if ubi.ciudad else "") + " sitio web oficial"
        for prov in activos[:1]:      # un solo proveedor: es una consulta de apoyo
            url = providers.construir_url(prov.nombre, consulta, ubi)
            r = obtener(url, prov.nombre, salud)
            if not r.sirve:
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            mejor, mejor_conf = "", 0.0
            for it in items:
                conf = pertenece_a(it["url"], e.nombre)
                if conf > mejor_conf:
                    mejor, mejor_conf = it["url"], conf
            if mejor and mejor_conf >= UMBRAL_WEBSITE:
                e.website = mejor
                e.senales["website_resuelto"] = True
                e.senales["website_resuelto_confianza"] = round(mejor_conf, 3)
                encontrados += 1
                break
    return encontrados
