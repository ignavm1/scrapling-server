"""Filtrado de falsos positivos y validacion de relevancia.

POR QUE LA RELEVANCIA ES OBLIGATORIA Y NO UN EXTRA

Medido el 2026-08-30 (F6 en FINDINGS.md): Bing devuelve paginas
estructuralmente perfectas — `<title>` correcto, 10 contenedores `li.b_algo`
bien formados — con resultados que no tienen NADA que ver con la query:

  "agencia de marketing digital Lima"  ->  hilos de Reddit sobre hardware Elgato
  "contadores Santiago Chile"          ->  contadores de Guatemala
  "restaurantes Buenos Aires"          ->  restaurantes de Panama
  con &mkt=es-CL                       ->  paginas de AOL

Ningun filtro estructural detecta eso: reddit.com y aol.com son dominios
perfectamente validos. Solo comparar el resultado contra la query lo detiene.
Sin esta capa, Reddit y AOL entran al pipeline como "empresas".
"""
from __future__ import annotations
import re

from .normalize import dominio_registrable, sin_acentos, host_de

# ── Dominios que nunca son el sitio de una empresa objetivo ──────────────────

REDES_Y_BUSCADORES = {
    "google.com", "bing.com", "duckduckgo.com", "yahoo.com", "aol.com",
    "baidu.com", "yandex.com", "ecosia.org", "brave.com", "startpage.com",
    "facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com",
    "youtube.com", "tiktok.com", "pinterest.com", "reddit.com", "tumblr.com",
    "whatsapp.com", "telegram.org", "snapchat.com", "threads.net",
    "wikipedia.org", "wikimedia.org", "fandom.com", "quora.com",
    "medium.com", "substack.com", "blogspot.com", "wordpress.org",
    "translate.goog", "webcache.googleusercontent.com", "archive.org",
    "amazonaws.com", "cloudfront.net", "googleusercontent.com",
}

# Directorios, agregadores, marketplaces y sitios de reviews. Aparecen SIEMPRE
# en los primeros resultados de una busqueda por nicho+ciudad, y son
# exactamente lo que no sirve: listan empresas, no son empresas.
DIRECTORIOS = {
    "paginasamarillas.com", "paginasamarillas.com.pe", "amarillas.com",
    "yelp.com", "tripadvisor.com", "tripadvisor.com.pe", "foursquare.com",
    "clutch.co", "sortlist.com", "goodfirms.co", "designrush.com",
    "agencyspotter.com", "upcity.com", "expertise.com", "trustpilot.com",
    "glassdoor.com", "indeed.com", "computrabajo.com", "bumeran.com",
    # Bolsas de trabajo: aparecen alto en cualquier busqueda por nicho+ciudad
    # porque publican vacantes de ese rubro, y no son la empresa buscada.
    "jooble.org", "laborum.cl", "trabajando.com", "zonajobs.com.ar",
    "konzerta.com", "empleosti.com.mx", "occ.com.mx", "elempleo.com",
    "talent.com", "jobrapido.com", "neuvoo.com", "adzuna.com",
    "crunchbase.com", "zoominfo.com", "apollo.io", "rocketreach.co",
    "opencorporates.com", "dnb.com", "bloomberg.com", "owler.com",
    "mercadolibre.com", "amazon.com", "ebay.com", "alibaba.com",
    "booking.com", "airbnb.com", "doplim.com", "olx.com",
    "cylex.com", "infoisinfo.com", "hotfrog.com", "tuugo.com", "opendi.com",
    "yellowpages.com", "guiatelefonica.com", "encuentra24.com",
    "restaurantguru.com", "degusta.com", "thefork.com", "zomato.com",
}

# Medios y prensa: mencionan empresas, no son empresas.
MEDIOS = {
    "elcomercio.pe", "larepublica.pe", "gestion.pe", "rpp.pe", "peru21.pe",
    "emol.com", "latercera.com", "df.cl", "clarin.com", "lanacion.com.ar",
    "infobae.com", "eltiempo.com", "elespectador.com", "semana.com",
    "eluniversal.com.mx", "milenio.com", "forbes.com", "entrepreneur.com",
    "techcrunch.com", "businessinsider.com", "cnn.com", "bbc.com",
}

# Extensiones que no son la home de una empresa.
_EXT_DOCUMENTO = re.compile(
    r"\.(pdf|docx?|xlsx?|pptx?|csv|zip|rar|7z|tar|gz|mp[34]|avi|mov|jpe?g|png|gif|svg)$",
    re.I,
)

# Rutas que delatan un articulo, un listado o una oferta de empleo.
_RUTA_NO_EMPRESA = re.compile(
    r"/(blog|noticias?|news|articulos?|articles?|posts?|prensa|press|"
    r"jobs?|empleos?|trabajo|careers?|vacantes?|"
    r"foro|forum|comments?|thread|tag|tags|category|categoria|"
    r"search|busqueda|resultados|listado|directorio|ranking|top-?\d+)(/|$)",
    re.I,
)

# Titulos de listicle: "Los 10 mejores...", "Top 20 de...". Son directorios
# aunque el dominio no este en la lista.
_TITULO_LISTICLE = re.compile(
    r"^\s*(los?\s+|las?\s+)?(top\s*)?\d{1,3}\s+(mejores|best|top|principales)|"
    r"^\s*(top|los?\s+mejores|las?\s+mejores|best)\s+\d{1,3}\b|"
    r"\bmejores\s+\d{1,3}\b|\branking\b|\bdirectorio\b|"
    # Sin numero tambien: "Mejores Agencias de Marketing en Lima" es un
    # listicle igual, y era el caso que se colaba.
    r"^\s*(los?\s+|las?\s+)?(mejores|best|top)\s+(agencias|empresas|companies|"
    r"agencies|estudios|servicios|proveedores)\b|"
    r"\b(comparativa|guia\s+de|listado\s+de)\b",
    re.I,
)

# Rutas de listicle. `/mejores-agencias-marketing` no tiene /blog/ ni /news/,
# asi que el patron de ruta generico no lo agarraba.
_RUTA_LISTICLE = re.compile(
    # El numero puede ir ANTES ("/10-mejores-agencias") o DESPUES
    # ("/mejores-10-agencias"), y a veces no esta.
    r"/(\d{1,3}[-_])?(mejores|best|top|ranking)([-_]?\d{1,3})?[-_]?"
    r"(de[-_])?(agencias?|empresas?|agencies|companies|"
    r"estudios?|servicios?|proveedores?|herramientas?)",
    re.I,
)

# Marca = primer segmento del dominio registrable. Se derivan de las listas de
# arriba para no mantener dos catalogos en paralelo.
_MARCAS_DIRECTORIO = {d.split(".")[0] for d in DIRECTORIOS}
_MARCAS_MEDIO = {d.split(".")[0] for d in MEDIOS}

_UNIVERSIDAD = re.compile(r"\b(universidad|university|\.edu\b|\.edu\.|instituto|facultad)\b", re.I)


def motivo_descarte(url: str, titulo: str = "") -> str:
    """Devuelve el motivo por el que esto NO es una empresa objetivo, o "".

    Se devuelve el motivo y no un booleano a proposito: el requisito de
    observabilidad pide poder responder "por que se descarto este resultado",
    y un False no responde nada.
    """
    if not url:
        return "sin-url"
    dom = dominio_registrable(url)
    host = host_de(url)
    if not dom:
        return "url-invalida"

    if dom in REDES_Y_BUSCADORES or any(host.endswith("." + d) for d in REDES_Y_BUSCADORES):
        return "red-social-o-buscador"
    # Comparacion por MARCA, no por dominio exacto: los directorios operan la
    # misma marca en cada pais (cylex.com, cylex.cl, cylex.com.mx). Listar solo
    # el .com dejaba pasar la version local, que es justo la que aparece en una
    # busqueda por ciudad (F16).
    marca = dom.split(".")[0]
    if dom in DIRECTORIOS or marca in _MARCAS_DIRECTORIO:
        return "directorio"
    if dom in MEDIOS or marca in _MARCAS_MEDIO:
        return "medio-de-prensa"
    if _EXT_DOCUMENTO.search(url.split("?")[0]):
        return "documento"
    if _RUTA_NO_EMPRESA.search(url):
        return "pagina-no-empresarial"
    if _RUTA_LISTICLE.search(url):
        return "listicle"
    if _UNIVERSIDAD.search(host):
        return "universidad"
    if titulo and _TITULO_LISTICLE.search(titulo):
        return "listicle"
    if host.count(".") > 3:
        return "host-sospechoso"
    return ""


def es_empresa_candidata(url: str, titulo: str = "") -> bool:
    return motivo_descarte(url, titulo) == ""


# ── Relevancia contra la query ───────────────────────────────────────────────

_PALABRAS_VACIAS = {
    "de", "del", "la", "el", "los", "las", "y", "en", "para", "con", "por",
    "the", "of", "and", "in", "for", "a", "an", "to", "sitio", "web", "site",
    "empresa", "empresas", "company", "companies", "mejores", "best", "top",
}


def tokens(texto: str) -> set[str]:
    """Palabras significativas, sin acentos ni vacias."""
    t = sin_acentos(texto or "").lower()
    crudas = re.findall(r"[a-z0-9]{3,}", t)
    return {w for w in crudas if w not in _PALABRAS_VACIAS}


def relevancia(query: str, titulo: str, snippet: str, url: str) -> float:
    """0..1 — cuanto se parece este resultado a lo que se pidio.

    Se mira titulo, snippet Y url: una agencia puede no repetir el nicho en el
    titulo pero tenerlo en el dominio, y al reves.

    Esta es la funcion que detiene el envenenamiento de Bing. Para la query
    "agencia de marketing digital Lima", un hilo de r/elgato comparte cero
    tokens y queda en 0.0.
    """
    objetivo = tokens(query)
    if not objetivo:
        return 0.5            # sin query no hay nada que exigir
    texto = " ".join([titulo or "", snippet or "", (url or "").replace("/", " ").replace("-", " ")])
    presentes = tokens(texto)
    if not presentes:
        return 0.0
    comunes = objetivo & presentes
    return len(comunes) / len(objetivo)
