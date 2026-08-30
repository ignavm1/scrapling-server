"""Normalizacion de URLs, dominios y nombres.

Es la base de la deduplicacion: si dos formas de la misma empresa no se
normalizan igual, ningun algoritmo de entity resolution las va a unir.
"""
from __future__ import annotations
import base64
import html as _html
import re
import unicodedata
from urllib.parse import urlparse, parse_qs, unquote, urlunparse

# Parametros de tracking. Se quitan para que la misma pagina con y sin campana
# de marketing sea la misma clave.
_TRACKING = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "gclid", "fbclid", "msclkid", "mc_cid", "mc_eid", "ref", "trk",
    "trkinfo", "originalsubdomain", "_ga", "igshid", "si", "spm",
}

# Sufijos compuestos donde el dominio registrable tiene 3 partes. Sin esto,
# "com.pe" o "com.ar" se tomarian como el dominio y TODA empresa peruana
# colapsaria en una sola entidad.
_SUFIJOS_COMPUESTOS = {
    "com.pe", "com.ar", "com.br", "com.mx", "com.co", "com.uy", "com.ve",
    "com.ec", "com.bo", "com.py", "com.do", "com.gt", "com.pa", "com.sv",
    "co.uk", "com.au", "co.nz", "com.tr", "co.il", "com.sg", "co.za",
    "org.pe", "org.ar", "org.mx", "org.co", "net.pe", "net.ar", "net.mx",
    "gob.pe", "gob.mx", "gov.ar", "edu.pe", "edu.ar", "edu.mx", "edu.co",
}

# Hosting compartido: el dominio NO identifica a la empresa, el subdominio si.
# Sin esta lista, veinte negocios en wixsite.com colapsan en una sola entidad.
HOSTING_COMPARTIDO = {
    "wixsite.com", "mystrikingly.com", "weebly.com", "squarespace.com",
    "webflow.io", "wordpress.com", "blogspot.com", "github.io", "netlify.app",
    "vercel.app", "herokuapp.com", "myshopify.com", "business.site",
    "negocio.site", "godaddysites.com", "jimdosite.com", "tilda.ws",
}


def limpiar_entidades(s: str) -> str:
    return _html.unescape(s or "")


def decodificar_redirect(href: str) -> str:
    """Convierte el redirect de un buscador en la URL real.

    Cada motor usa su propio envoltorio y hay que desenvolver los TRES:

      Google      /url?q=<url>
      DuckDuckGo  //duckduckgo.com/l/?uddg=<url-encodeada>
      Bing        /ck/a?...&u=a1<base64url>

    El de Bing faltaba, y esa omision valia caro: toda URL de Bing salia como
    `bing.com/...` y se descartaba como basura, asi que Bing no aportaba ni una
    empresa. Ver F5 en FINDINGS.md.
    """
    if not href:
        return ""
    href = limpiar_entidades(href.strip())

    # Google
    if "/url?q=" in href:
        try:
            q = parse_qs(urlparse(href).query).get("q")
            if q:
                href = unquote(q[0])
        except Exception:
            pass

    # DuckDuckGo
    if "uddg=" in href:
        try:
            q = parse_qs(urlparse(href).query).get("uddg")
            if q:
                href = unquote(q[0])
        except Exception:
            pass

    # Bing: el parametro u lleva la URL en base64url con prefijo "a1".
    if "/ck/a" in href and "u=" in href:
        m = re.search(r"[?&]u=([^&]+)", href)
        if m:
            crudo = m.group(1)
            if crudo.startswith("a1"):
                crudo = crudo[2:]
            try:
                d = base64.urlsafe_b64decode(crudo + "=" * (-len(crudo) % 4))
                texto = d.decode("utf-8", errors="ignore")
                if texto.startswith("http"):
                    href = texto
            except Exception:
                pass

    return href.strip().rstrip(".,);]")


def normalizar_url(url: str) -> str:
    """URL canonica: sin tracking, sin fragmento, sin puerto por defecto."""
    if not url:
        return ""
    url = decodificar_redirect(url)
    try:
        p = urlparse(url)
    except Exception:
        return ""
    if p.scheme not in ("http", "https"):
        return ""
    host = (p.hostname or "").lower()
    if not host or "." not in host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    netloc = host
    if p.port and p.port not in (80, 443):
        netloc = host + ":" + str(p.port)
    try:
        qs = [(k, v) for k, v in parse_qs(p.query, keep_blank_values=True).items()
              if k.lower() not in _TRACKING]
        query = "&".join(k + "=" + v[0] for k, v in sorted(qs)) if qs else ""
    except Exception:
        query = ""
    ruta = p.path.rstrip("/") or "/"
    return urlunparse(("https", netloc, ruta, "", query, ""))


def host_de(url: str) -> str:
    """Host sin www, en minusculas."""
    try:
        h = (urlparse(decodificar_redirect(url)).hostname or "").lower()
    except Exception:
        return ""
    return h[4:] if h.startswith("www.") else h


def dominio_registrable(url_o_host: str) -> str:
    """Dominio que identifica al DUENO del sitio.

    En hosting compartido devuelve el subdominio completo, porque ahi el
    dominio base pertenece al proveedor y no a la empresa: sin esta rama,
    `panaderia.wixsite.com` y `ferreteria.wixsite.com` serian la misma entidad.
    """
    host = url_o_host if "/" not in url_o_host and ":" not in url_o_host else host_de(url_o_host)
    host = (host or "").lower().lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return ""
    partes = host.split(".")
    base2 = ".".join(partes[-2:])
    base3 = ".".join(partes[-3:]) if len(partes) >= 3 else base2
    if base2 in HOSTING_COMPARTIDO or base3 in HOSTING_COMPARTIDO:
        return host
    if len(partes) >= 3 and base2 in _SUFIJOS_COMPUESTOS:
        return ".".join(partes[-3:])
    return base2


# ── Nombres ──────────────────────────────────────────────────────────────────

# Ruido que los buscadores agregan al titulo. El orden no importa porque se
# aplica como alternancia sobre el final de la cadena.
_COLA_RUIDO = re.compile(
    r"\s*[|\-–—:]\s*(linkedin|inicio|home|about|contact[oa]?|"
    r"sitio\s+web|website|p[aá]gina\s+oficial|official\s+site|company|empresa)\s*$",
    re.I,
)

_SUFIJOS_LEGALES = re.compile(
    r"\b(s\.?a\.?c\.?|s\.?a\.?s\.?|s\.?r\.?l\.?|s\.?a\.?|e\.?i\.?r\.?l\.?|"
    r"ltda?\.?|inc\.?|llc\.?|corp\.?|co\.?|gmbh|b\.?v\.?|pty|plc)\b\.?",
    re.I,
)

_GENERICOS = re.compile(
    r"\b(agencia|agency|estudio|studio|grupo|group|consultora|consulting|"
    r"marketing|digital|creativa?|solutions|soluciones|services|servicios|"
    r"company|empresa|the|de|del|la|el|los|las|and|y)\b",
    re.I,
)


def limpiar_titulo(txt: str) -> str:
    """Titulo de resultado -> nombre de empresa legible."""
    if not txt:
        return ""
    t = limpiar_entidades(txt)
    t = re.sub(r"\s+", " ", t).strip()
    anterior = None
    while anterior != t:            # el ruido puede venir encadenado
        anterior = t
        t = _COLA_RUIDO.sub("", t).strip()

    # La barra vertical practicamente nunca esta DENTRO del nombre de una
    # empresa; es el separador que usan los sitios para su tagline:
    #   "Kallpa Creativa | Agencia de Marketing Digital en Lima Peru"
    # El cliente muestra este campo como nombre, asi que quedarse con la frase
    # entera da una linea de marketing en vez de un nombre.
    if "|" in t:
        cabeza = t.split("|")[0].strip()
        if len(cabeza) >= 3:
            t = cabeza

    # Con guion se corta solo si el titulo es largo: hay nombres reales con
    # guion ("Coca-Cola", "Jorge-Luis Servicios") y partirlos siempre romperia
    # mas de lo que arregla.
    if len(t) > 60:
        t = re.split(r"\s[–—]\s|\s-\s", t)[0].strip()
    return t[:120]


def sin_acentos(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s or "")
                   if unicodedata.category(c) != "Mn")


def clave_nombre(nombre: str) -> str:
    """Forma canonica para comparar nombres de empresa.

    "Acme Digital", "ACME Digital Agency" y "Acme Digital S.A.C." colapsan en
    "acme". Quitar los genericos es lo que hace que "Agencia Acme" y "Acme"
    se reconozcan; dejarlos haria que toda agencia empezara distinto.
    """
    s = sin_acentos(limpiar_titulo(nombre)).lower()
    s = _SUFIJOS_LEGALES.sub(" ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = _GENERICOS.sub(" ", s)
    return re.sub(r"\s+", "", s).strip()


def nombre_desde_dominio(url: str) -> str:
    """Nombre de respaldo cuando el titulo no sirve."""
    d = dominio_registrable(url)
    if not d:
        return ""
    base = d.split(".")[0]
    base = re.sub(r"[-_]+", " ", base).strip()
    return " ".join(p[:1].upper() + p[1:] for p in base.split() if p)[:80]


def titulo_es_generico(titulo: str, contexto: str = "") -> bool:
    """True si el titulo no aporta nada mas alla de lo que se pidio.

    "Agencia de Marketing Digital en Lima Peru" para la busqueda
    "agencia de marketing digital / Lima" describe el rubro y la ciudad: no
    identifica a NADIE. El cliente muestra este campo como nombre de empresa,
    asi que devolver eso hace que veinte leads distintos se vean iguales en la
    pantalla y sean imposibles de reconocer.

    Comparar contra el contexto (nicho + ubicacion) es lo que lo detecta:
    quitar solo los genericos del rubro dejaba "lima" y parecia un nombre.
    """
    import re as _re
    def _tok(x):
        return set(_re.findall(r"[a-z0-9]{3,}", sin_acentos(x or "").lower()))
    # Titulos que son el nombre de una SECCION, no de la empresa. Salen de
    # paginas internas ("Contacto", "Inicio", "Nosotros") y dejan al cliente
    # con un lead llamado "Contacto".
    if sin_acentos(titulo or "").strip().lower() in {
            "contacto", "contactenos", "contact", "contact us", "inicio", "home",
            "nosotros", "about", "about us", "quienes somos", "servicios",
            "services", "blog", "empresa", "index"}:
        return True

    propios = _tok(titulo) - _tok(contexto)
    if not propios:
        return True
    # Queda algo, pero si es solo ruido corto tampoco identifica.
    return len("".join(propios)) < 4


def recortar_en_dominio(titulo: str, url: str) -> str:
    """Corta el titulo donde el buscador inserto el dominio del resultado.

    Brave arma el titulo como `<nombre> <dominio> <segmentos de ruta> <desc>`:

        "Webtilia webtilia.com en Multicultural Digital Marketing Agency"
        "MK agenciamk.com Agencia de Marketing Digital en Lima Peru"

    El nombre es lo que va ANTES del dominio. Sin este corte el cliente ve
    "Webtilia webtilia.com en Multicultural..." como razon social.
    """
    dom = dominio_registrable(url)
    if not dom or not titulo:
        return titulo
    # Se busca el dominio (con o sin www) como token dentro del titulo.
    for variante in (dom, "www." + dom, dom.split(".")[0] + "." + dom.split(".", 1)[1] if "." in dom else dom):
        idx = titulo.lower().find(variante.lower())
        if idx > 0:
            cabeza = titulo[:idx].strip(" -|·–—:,")
            if len(cabeza) >= 2:
                return cabeza
    return titulo


def mejor_nombre(titulo: str, url: str, contexto: str = "") -> str:
    """Elige entre el titulo y el nombre derivado del dominio.

    El dominio identifica mejor que una frase: limadigital.pe -> "Limadigital"
    se distingue de onzamarketing.com -> "Onzamarketing", mientras que los dos
    titulos empiezan igual.
    """
    limpio = limpiar_titulo(recortar_en_dominio(titulo, url))
    if limpio and not titulo_es_generico(limpio, contexto):
        return limpio
    return nombre_desde_dominio(url) or limpio
