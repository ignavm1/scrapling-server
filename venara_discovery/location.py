"""Inteligencia de ubicacion.

Dos trabajos distintos:

  1. Traducir "Lima" o "Miraflores" al MERCADO que hay que pedirle al buscador.
     Medido (F6): `setlang=es` NO fija pais. Bing devolvia contadores de
     Guatemala para "contadores Santiago Chile" y restaurantes de Panama para
     "restaurantes Buenos Aires". Con `mkt=es-AR` la busqueda de Buenos Aires
     se corrigio. El mercado no es un detalle: es la diferencia entre leads del
     pais del cliente y leads de otro continente.

  2. Medir CUANTA confianza hay de que un resultado sea de esa ubicacion, en
     niveles (ciudad exacta / area metro / pais / desconocida) en vez de un si-no.
     Exigir que el website mencione la ciudad exacta descarta empresas
     legitimas; no exigir nada trae otro pais entero.

Las ciudades de abajo NO son un hardcodeo de empresas ni un catalogo cerrado:
son el mapeo pais->mercado que los buscadores necesitan, y cualquier ubicacion
que no este cae con gracia a "desconocida" sin romper la busqueda.
"""
from __future__ import annotations
from dataclasses import dataclass

from .normalize import sin_acentos

# ciudad/region -> (codigo de pais, nombre de pais, barrios y sinonimos)
_CIUDADES = {
    "lima":          ("PE", "Peru", ("miraflores", "san isidro", "surco", "barranco",
                                     "la molina", "san borja", "magdalena", "jesus maria",
                                     "pueblo libre", "chorrillos", "callao", "lince")),
    "arequipa":      ("PE", "Peru", ()),
    "trujillo":      ("PE", "Peru", ()),
    "santiago":      ("CL", "Chile", ("providencia", "las condes", "vitacura", "nunoa",
                                      "la reina", "maipu", "san miguel", "lo barnechea")),
    "valparaiso":    ("CL", "Chile", ("vina del mar",)),
    "concepcion":    ("CL", "Chile", ()),
    "buenos aires":  ("AR", "Argentina", ("palermo", "belgrano", "recoleta", "caballito",
                                          "villa crespo", "microcentro", "puerto madero",
                                          "capital federal", "caba", "vicente lopez")),
    "cordoba":       ("AR", "Argentina", ()),
    "rosario":       ("AR", "Argentina", ()),
    "bogota":        ("CO", "Colombia", ("chapinero", "usaquen", "chico", "teusaquillo")),
    "medellin":      ("CO", "Colombia", ("el poblado", "laureles", "envigado")),
    "cali":          ("CO", "Colombia", ()),
    "barranquilla":  ("CO", "Colombia", ()),
    "ciudad de mexico": ("MX", "Mexico", ("cdmx", "polanco", "condesa", "roma norte",
                                          "santa fe", "coyoacan", "df")),
    "mexico city":   ("MX", "Mexico", ("cdmx", "polanco", "condesa")),
    "guadalajara":   ("MX", "Mexico", ("zapopan",)),
    "monterrey":     ("MX", "Mexico", ("san pedro garza",)),
    "quito":         ("EC", "Ecuador", ()),
    "guayaquil":     ("EC", "Ecuador", ()),
    "montevideo":    ("UY", "Uruguay", ()),
    "asuncion":      ("PY", "Paraguay", ()),
    "la paz":        ("BO", "Bolivia", ()),
    "santa cruz":    ("BO", "Bolivia", ()),
    "san jose":      ("CR", "Costa Rica", ()),
    "panama":        ("PA", "Panama", ("ciudad de panama",)),
    "sao paulo":     ("BR", "Brasil", ()),
    "rio de janeiro": ("BR", "Brasil", ()),
    "madrid":        ("ES", "Espana", ()),
    "barcelona":     ("ES", "Espana", ()),
    "miami":         ("US", "Estados Unidos", ()),
}

# Nombre de pais suelto -> codigo. Permite buscar por pais sin ciudad.
_PAISES = {
    "peru": "PE", "chile": "CL", "argentina": "AR", "colombia": "CO",
    "mexico": "MX", "ecuador": "EC", "uruguay": "UY", "paraguay": "PY",
    "bolivia": "BO", "costa rica": "CR", "panama": "PA", "brasil": "BR",
    "brazil": "BR", "espana": "ES", "spain": "ES", "estados unidos": "US",
    "usa": "US", "united states": "US", "guatemala": "GT", "honduras": "HN",
    "el salvador": "SV", "nicaragua": "NI", "republica dominicana": "DO",
    "venezuela": "VE",
}

# Mercado de Bing por pais. es-XX donde aplica.
_MERCADO = {
    "PE": "es-PE", "CL": "es-CL", "AR": "es-AR", "CO": "es-CO", "MX": "es-MX",
    "EC": "es-EC", "UY": "es-UY", "PY": "es-PY", "BO": "es-BO", "CR": "es-CR",
    "PA": "es-PA", "VE": "es-VE", "GT": "es-GT", "DO": "es-DO", "HN": "es-HN",
    "SV": "es-SV", "NI": "es-NI", "ES": "es-ES", "BR": "pt-BR", "US": "en-US",
}

# TLD que implican pais. Senal fuerte y barata.
_TLD_PAIS = {
    ".pe": "PE", ".cl": "CL", ".ar": "AR", ".co": "CO", ".mx": "MX",
    ".ec": "EC", ".uy": "UY", ".py": "PY", ".bo": "BO", ".cr": "CR",
    ".pa": "PA", ".ve": "VE", ".gt": "GT", ".do": "DO", ".br": "BR", ".es": "ES",
}

NIVELES = ("desconocida", "pais", "metro", "ciudad")


def _contiene_palabra(texto: str, frase: str) -> bool:
    """`frase` aparece como palabra completa dentro de `texto`.

    "lima" NO debe matchear dentro de "colima" ni de "salima".
    """
    import re as _re
    return bool(_re.search(r"(?<![a-z0-9])" + _re.escape(frase) + r"(?![a-z0-9])", texto))


@dataclass(frozen=True)
class Ubicacion:
    """Ubicacion pedida, ya interpretada."""
    texto: str          # lo que escribio el usuario
    ciudad: str         # "" si no se reconocio
    pais: str           # codigo ISO, "" si no se reconocio
    pais_nombre: str
    barrios: tuple[str, ...]

    @property
    def mercado(self) -> str:
        """Mercado para el buscador. "" cuando no se pudo determinar el pais."""
        return _MERCADO.get(self.pais, "")

    @property
    def reconocida(self) -> bool:
        return bool(self.ciudad or self.pais)


def interpretar(texto: str) -> Ubicacion:
    """"Lima, Peru" / "Miraflores" / "Chile" -> Ubicacion."""
    crudo = (texto or "").strip()
    plano = sin_acentos(crudo).lower()
    if not plano:
        return Ubicacion("", "", "", "", ())

    partes = [p.strip() for p in plano.replace("/", ",").split(",") if p.strip()]

    # 1) Ciudad conocida, por PALABRA COMPLETA.
    #
    # El substring suelto era un error de geografia grave: "Colima, Mexico"
    # contiene "lima" y resolvia a Lima, Peru -- una campana entera al pais
    # equivocado, con el sintoma apareciendo recien en los leads (F14).
    for ciudad, (cc, nombre, barrios) in _CIUDADES.items():
        if _contiene_palabra(plano, ciudad):
            return Ubicacion(crudo, ciudad, cc, nombre, barrios)

    # 2) Barrio conocido -> su ciudad. "Miraflores" debe resolver a Lima.
    for ciudad, (cc, nombre, barrios) in _CIUDADES.items():
        for b in barrios:
            if b and _contiene_palabra(plano, b):
                return Ubicacion(crudo, ciudad, cc, nombre, barrios)

    # 3) Pais suelto.
    for p in partes + [plano]:
        if p in _PAISES:
            cc = _PAISES[p]
            return Ubicacion(crudo, "", cc, p.title(), ())

    return Ubicacion(crudo, "", "", "", ())


def confianza(ubi: Ubicacion, texto_resultado: str, url: str = "") -> tuple[float, str]:
    """Cuanta evidencia hay de que este resultado sea de la ubicacion pedida.

    Devuelve (0..1, nivel). Un resultado sin senales NO se descarta: se marca
    "desconocida" y se le baja el score. Descartarlo perderia empresas
    legitimas cuyo sitio simplemente no repite la ciudad.
    """
    if not ubi.reconocida:
        return 0.5, "desconocida"

    plano = sin_acentos(texto_resultado or "").lower()
    host = sin_acentos(url or "").lower()

    # Un TLD de OTRO pais contradice la ubicacion pedida, aunque el texto
    # mencione la ciudad. Caso real: `estudiocontablesantiago.com.ar` aparecio
    # como resultado "de ciudad" para Santiago de Chile, porque "santiago"
    # estaba en el dominio. El TLD manda sobre la coincidencia de texto (F16).
    for sufijo, cc in _TLD_PAIS.items():
        if host.endswith(sufijo) or (sufijo + "/") in host:
            if ubi.pais and cc != ubi.pais:
                return 0.1, "otro-pais"
            break

    if ubi.ciudad and _contiene_palabra(plano, ubi.ciudad):
        return 1.0, "ciudad"
    for b in ubi.barrios:
        if b and _contiene_palabra(plano, b):
            return 0.9, "metro"

    tld = "." + ubi.pais.lower() if ubi.pais else ""
    if tld and (host.endswith(tld) or (tld + "/") in host or ("." + ubi.pais.lower() + ".") in host):
        return 0.75, "pais"
    if ubi.pais_nombre and sin_acentos(ubi.pais_nombre).lower() in plano:
        return 0.75, "pais"
    for sufijo, cc in _TLD_PAIS.items():
        if host.endswith(sufijo) and cc == ubi.pais:
            return 0.75, "pais"

    return 0.35, "desconocida"
