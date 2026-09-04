"""Canal PERSONAS: buscar decisores del rubro, no de una empresa concreta.

QUE PROBLEMA RESUELVE

El pipeline de empresas va nicho -> empresa -> sitio -> persona. Sirve, pero
tiene un techo medido: solo el 17% de los sitios publica a su equipo, asi que
el 83% de las empresas descubiertas termina sin nombre de decisor.

Este modulo invierte la direccion: busca en la web abierta PAGINAS QUE YA
NOMBRAN a un decisor del rubro, y recien despues se pregunta de que empresa es.

POR QUE NO ES OTRO INTENTO DE LINKEDIN

`site:linkedin.com/in` esta medido y muerto (F7): cero perfiles, con control
positivo -- el MISMO operador sobre /company devuelve 10. Este canal no gasta
ni una query ahi, y ademas DESCARTA cualquier resultado /in/ que aparezca por
su cuenta, porque un titulo de LinkedIn indexado sin la pagina detras es un
nombre sin nada que lo respalde.

Las superficies que si estan indexadas y si nombran personas con cargo:

  equipo        "nuestro equipo" / "quienes somos" del sitio de la empresa
  prensa        nombramientos: "asume como gerente general de X"
  expositores   ponentes y panelistas de conferencias del rubro
  directorio    socios de gremios y asociaciones
  entrevista    notas y podcasts del rubro

LA REGLA QUE GOBIERNA EL PARSEO

Un nombre suelto no es una persona: "Juan Perez" hay miles y cualquier texto
tiene mayusculas. Un candidato existe solo cuando el MISMO fragmento trae un
cargo reconocido pegado al nombre. Sin cargo no hay candidato, aunque el texto
parezca un nombre perfecto. Es la misma regla de oro del scoring de Venara,
aplicada una capa antes.
"""
from __future__ import annotations
import concurrent.futures
import logging
import re
import time

from . import config, extraction, filtering, providers
from .fetch import SaludProveedores, obtener
from .linkedin import puntuar_cargo
from .location import Ubicacion, interpretar
from .normalize import clave_nombre, host_de, sin_acentos

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Vocabulario de cargos
# ─────────────────────────────────────────────────────────────────────────────
# Lo que dispara el reconocimiento de un candidato. Deliberadamente mas ancho
# que PESO_CARGO de linkedin.py: aca sirve para DETECTAR (encontrar el patron
# en el texto) y alla para PRIORIZAR. Un "head of growth" se detecta igual, y
# despues puntua bajo -- que es lo correcto: verlo y descartarlo es distinto de
# no verlo nunca.
CARGOS_DETECTABLES = [
    "founder", "co-founder", "cofounder", "fundador", "fundadora",
    "socio fundador", "socia fundadora", "cofundador", "cofundadora",
    "ceo", "chief executive officer", "cto", "cmo", "coo", "cfo",
    "owner", "dueno", "duena", "propietario", "propietaria", "titular",
    "managing director", "director general", "directora general",
    "gerente general", "gerenta general",
    "director comercial", "directora comercial", "gerente comercial",
    "gerenta comercial", "gerente de ventas", "director de ventas",
    "gerente de marketing", "director de marketing", "directora de marketing",
    "jefe de ventas", "jefa de ventas", "jefe comercial", "jefa comercial",
    "vp de ventas", "vicepresidente", "vicepresidenta",
    "head of growth", "head of sales", "head of marketing",
    "partner", "socio", "socia", "director", "directora", "gerente", "gerenta",
]

# Ordenado de mas largo a mas corto: sin esto "director" gana antes que
# "director comercial" y el cargo llega recortado al scorer, que lo puntua como
# si fuera un director cualquiera.
_CARGOS_ORD = sorted(CARGOS_DETECTABLES, key=len, reverse=True)
_CARGO_ALT = "|".join(re.escape(c) for c in _CARGOS_ORD)

# ─────────────────────────────────────────────────────────────────────────────
# Reconocimiento de nombres propios
# ─────────────────────────────────────────────────────────────────────────────
# Un token de nombre: empieza en mayuscula (con o sin tilde) y sigue en
# minusculas. "GERENTE" no matchea (todo mayusculas) y "iPhone" tampoco.
_TOKEN = r"[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü'`]{1,20}"
# Conectores que SI van dentro de un nombre hispano: "Maria de los Angeles",
# "Juan del Rio". Van en minuscula y no cuentan como token propio.
_CONECTOR = r"(?:de|del|la|las|los|y|da|do|van|von)"
RX_NOMBRE = re.compile(
    r"\b(" + _TOKEN + r"(?:\s+(?:" + _CONECTOR + r"\s+)?" + _TOKEN + r"){1,3})\b"
)

# Palabras que tienen forma de nombre y no lo son. Sin esta lista, "Marketing
# Digital" y "Santiago Chile" entran como personas -- y "Santiago" es a la vez
# una ciudad y un nombre real, asi que el filtro no puede ser solo geografico.
_NO_ES_NOMBRE = {
    "gerente", "gerenta", "director", "directora", "jefe", "jefa", "socio",
    "socia", "fundador", "fundadora", "presidente", "presidenta", "equipo",
    # Medido en vivo (2026-09-03): "Secretario General" entro como persona en
    # la primera corrida real contra Bing. Un cargo compuesto pasa cualquier
    # regla token a token si sus palabras no estan listadas.
    "secretario", "secretaria", "general", "ejecutivo", "ejecutiva",
    # MEDIDO EN VIVO (2026-09-03) sobre empresas reales: "Chief Economist"
    # entro como persona en la pagina de equipo de Fintual. Un cargo en ingles
    # compuesto por dos palabras capitalizadas pasa cualquier regla token a
    # token si sus palabras no estan listadas.
    "chief", "economist", "officer", "executive", "head", "founder",
    "cofounder", "partner", "principal", "chairman", "chairwoman", "board",
    "manager", "president", "owner", "lead", "senior", "junior", "staff",
    "vicepresidente", "vicepresidenta", "tesorero", "tesorera", "consejero",
    "consejera", "vocal", "coordinador", "coordinadora", "encargado",
    "encargada", "administrador", "administradora",
    "nuestro", "nuestra", "quienes", "somos", "contacto", "inicio", "home",
    "empresa", "empresas", "agencia", "agencias", "marketing", "digital",
    "ventas", "comercial", "publicidad", "consultora", "consultoria",
    "servicios", "soluciones", "grupo", "compania", "corporacion", "holding",
    "linkedin", "facebook", "instagram", "twitter", "youtube", "google",
    "chile", "peru", "mexico", "colombia", "argentina", "espana", "brasil",
    "bolivia", "uruguay", "paraguay", "ecuador", "venezuela", "panama",
    "santiago", "lima", "bogota", "medellin", "buenos", "aires", "monterrey",
    "guadalajara", "quito", "montevideo", "valparaiso", "concepcion",
    "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
    "septiembre", "setiembre", "octubre", "noviembre", "diciembre",
    "sociedad", "limitada", "anonima", "spa", "sac", "sas", "ltda",
    "conferencia", "congreso", "seminario", "webinar", "evento", "expositores",
    "panelistas", "speakers", "programa", "entrevista", "noticias", "prensa",
    "sobre", "acerca", "team", "about", "leadership", "management",
}

# Abreviaturas de sociedad. Son cortas y capitalizadas, asi que pasan cualquier
# regla de forma; lo que las delata es que nombran a la empresa, no a nadie.
_ABREVIATURAS_SOCIETARIAS = {
    "co", "inc", "ltd", "llc", "plc", "sa", "spa", "sac", "sas", "srl",
    "ltda", "gmbh", "bv", "nv", "ag", "corp", "sl",
}

# Largo maximo razonable de un nombre completo. Por encima de esto lo que se
# capturo es una frase, no una persona.
MAX_LARGO_NOMBRE = 60

# ─────────────────────────────────────────────────────────────────────────────
# Patrones de extraccion
# ─────────────────────────────────────────────────────────────────────────────
# NINGUN patron usa re.I global, y no es un olvido: con IGNORECASE la clase
# [A-Z...] del token de nombre matchea minusculas y "anuncio que Paula Guzman"
# entra como nombre de cuatro palabras. El cargo SI necesita ser insensible, asi
# que lleva su propio flag local `(?i:...)`.
#
# A) "Juan Perez - Gerente General - Acme"    (separadores)
#    "Juan Perez, Gerente Comercial de Acme"  (coma + preposicion)
RX_PERSONA_CARGO = re.compile(
    r"(?P<nombre>" + _TOKEN + r"(?:\s+(?:" + _CONECTOR + r"\s+)?" + _TOKEN + r"){1,3})"
    # Separadores y puentes verbales. Los verbos importan tanto como los
    # guiones: la prensa de nombramientos casi nunca escribe "Paula Guzman -
    # gerente comercial", escribe "Paula Guzman asume como gerente comercial".
    r"\s*(?:[-–—,|·]|\s+(?:es|sera|fue|asume|asumio|asumira|llega|se\s+incorpora)"
    r"(?:\s+como)?\s+|\s+como\s+)\s*"
    r"(?P<cargo>(?i:" + _CARGO_ALT + r"))"
    r"(?:\s*(?:[-–—,|·]|\s+de\s+|\s+en\s+)\s*(?P<empresa>[^-–—|·,\.]{2,60}))?",
)

# B) "El nuevo gerente general de Acme, Juan Perez"  (cargo primero)
#    "asume como gerente general de Acme Juan Perez"
RX_CARGO_PERSONA = re.compile(
    r"(?P<cargo>(?i:" + _CARGO_ALT + r"))"
    r"\s+(?:de|en)\s+(?P<empresa>[^,\.;:]{2,60})"
    r"\s*[,:]\s*(?P<nombre>" + _TOKEN + r"(?:\s+(?:" + _CONECTOR + r"\s+)?" + _TOKEN + r"){1,3})",
)

# Ruido que los sitios pegan al nombre de la empresa en el titulo.
_RX_COLA_EMPRESA = re.compile(
    r"\s*(?:\||\bhttps?://|\bwww\.|\b(?:linkedin|facebook|instagram)\b).*$", re.I)

# La prensa sigue escribiendo despues del nombre de la empresa: "...gerente
# comercial de Kadem a partir de marzo". Sin cortar en el conector, la empresa
# queda "Kadem a partir de marzo" y deja de servir para deduplicar o para
# buscar el sitio.
_RX_CLAUSULA = re.compile(
    r"\s+(?:a\s+partir|desde|tras|luego|para|durante|hasta|donde|quien|que|"
    r"con\s+el|con\s+la|en\s+reemplazo|este|el\s+proximo)\b.*$", re.I)

# Tope de palabras del nombre de una empresa. Cinco cubre "Grupo Editorial La
# Tercera S.A."; mas alla de eso lo capturado es una frase.
MAX_PALABRAS_EMPRESA = 5


def es_nombre_de_persona(txt: str) -> bool:
    """Filtro estricto. Ante la duda, NO es una persona.

    Un falso positivo aca no se queda quieto: se convierte en un lead con
    nombre inventado al que despues se le escribe "Hola Marketing,".
    """
    t = (txt or "").strip()
    if not t or len(t) > MAX_LARGO_NOMBRE:
        return False
    if any(c.isdigit() for c in t):
        return False
    palabras = t.split()
    # Los conectores no cuentan: "Maria de los Angeles Rojas" son 3 tokens.
    tokens = [p for p in palabras if sin_acentos(p).lower() not in
              {"de", "del", "la", "las", "los", "y", "da", "do", "van", "von"}]
    if not (2 <= len(tokens) <= 4):
        return False
    # Una tira de siglas ("SEO SEM ADS") pasa cualquier regla token a token: son
    # cortas, empiezan en mayuscula y no son palabras prohibidas. Lo que la
    # delata es que TODAS estan en mayuscula. Un "JUAN PEREZ" real tambien se
    # pierde, y esa es la direccion segura del error: perder un lead cuesta un
    # lead; inventar uno cuesta un correo a "Hola SEO,".
    if all(tok.isupper() for tok in tokens):
        return False
    for tok in tokens:
        base = sin_acentos(tok).lower().strip("'`")
        # MEDIDO EN VIVO: "Betterfly's Co" entro como persona, recortado de
        # "Betterfly's Co-Founder". El posesivo delata que la palabra es la
        # EMPRESA, no alguien; y una abreviatura societaria no es un apellido.
        if base.endswith("'s") or tok.endswith("’s"):
            return False
        if base in _ABREVIATURAS_SOCIETARIAS:
            return False
        if base in _NO_ES_NOMBRE:
            return False
        if len(base) < 2:
            return False
        # Todo en mayusculas es una sigla o un grito de titular, no un nombre.
        if tok.isupper() and len(tok) > 3:
            return False
        if not tok[:1].isupper():
            return False
    return True


def limpiar_empresa(txt: str) -> str:
    """Deja el nombre de la empresa utilizable, o "" si no quedo nada."""
    t = _RX_COLA_EMPRESA.sub("", (txt or "")).strip(" -–—|·,.\t")
    t = _RX_CLAUSULA.sub("", t).strip(" -–—|·,.\t")
    t = re.sub(r"\s+", " ", t)
    if len(t) < 2 or len(t) > 60:
        return ""
    if len(t.split()) > MAX_PALABRAS_EMPRESA:
        return ""
    # Un "empresa" que en realidad es otro cargo no aporta: pasa cuando el
    # titulo encadena "Juan Perez - CEO - Fundador".
    if puntuar_cargo(t) > 0.15:
        return ""
    # Un nombre propio tiene al menos una mayuscula. Sin esta regla entra la
    # prosa que rodea al cargo -- "gerente comercial de una agencia que..."
    # dejaba "una agencia" como si fuera el nombre de la empresa.
    if not any(p[:1].isupper() for p in t.split()):
        return ""
    return t


def _extraer_de(fragmentos: list[tuple[str, str]], url: str) -> list[dict]:
    """Candidatos persona de una lista de (texto, de-donde-salio).

    Cada fragmento se mira POR SEPARADO y nunca concatenado: pegar dos textos
    crea fronteras falsas donde el final de uno y el principio del otro forman
    un par nombre-cargo que no existe en ningun lado.
    """
    salida: list[dict] = []
    vistos: set[str] = set()
    dominio = host_de(url)

    for texto, donde in fragmentos:
        for rx in (RX_PERSONA_CARGO, RX_CARGO_PERSONA):
            for m in rx.finditer(texto or ""):
                nombre = re.sub(r"\s+", " ", (m.group("nombre") or "")).strip()
                if not es_nombre_de_persona(nombre):
                    continue
                cargo = re.sub(r"\s+", " ", (m.group("cargo") or "")).strip()
                empresa = limpiar_empresa(m.groupdict().get("empresa") or "")
                clave = clave_nombre(nombre)
                if not clave or clave in vistos:
                    continue
                vistos.add(clave)
                salida.append({
                    "person_name": nombre,
                    "person_title": cargo,
                    "company": empresa,
                    "url": url,
                    "domain": dominio,
                    "evidence": donde,
                })
    return salida


def extraer_personas(titulo: str, snippet: str, url: str) -> list[dict]:
    """Candidatos persona de UN resultado de buscador."""
    return _extraer_de([(titulo or "", "titulo"), (snippet or "", "snippet")], url)


# Un bloque que EMPIEZA con un cargo. En una pagina de equipo el cargo suele
# ser el bloque siguiente al nombre, no una frase que lo contiene.
_RX_CARGO_INICIO = re.compile(r"^\s*(?i:" + _CARGO_ALT + r")\b")

# Cuantos bloques despues del nombre se acepta el cargo. Dos cubre el caso
# comun (nombre, cargo) y el que mete una foto o un separador en el medio; mas
# alla de eso se empieza a leer al miembro siguiente del equipo.
MAX_BLOQUES_DE_DISTANCIA = 2


def extraer_de_texto(texto: str, url: str) -> list[dict]:
    """Candidatos persona del TEXTO POR BLOQUES de una pagina.

    Existe porque el snippet del buscador casi nunca nombra a nadie (F21): los
    nombres estan dentro de la pagina de equipo. Y hace falta que sea POR
    BLOQUES porque ahi el nombre y el cargo son dos elementos distintos:

        <h3>Matias Bravo</h3><p>Gerente General</p>

    Aplanado a una linea eso es "Matias Bravo Gerente General", que no se
    distingue de una frase cualquiera. Con los bloques separados, la estructura
    ES el dato.
    """
    bloques = [b.strip() for b in re.split(r"[\n\r]+", texto or "") if b and b.strip()]
    if not bloques:
        return []

    # 1) El patron inline, por si el bloque trae "Nombre - Cargo" junto.
    salida = _extraer_de([(b, "pagina") for b in bloques], url)
    vistos = {clave_nombre(p["person_name"]) for p in salida}

    # 2) El patron estructural: un bloque que es un nombre, seguido de cerca
    #    por un bloque que empieza con un cargo.
    for i, bloque in enumerate(bloques):
        if not es_nombre_de_persona(bloque):
            continue
        clave = clave_nombre(bloque)
        if not clave or clave in vistos:
            continue
        for j in range(i + 1, min(i + 1 + MAX_BLOQUES_DE_DISTANCIA, len(bloques))):
            siguiente = bloques[j]
            if not _RX_CARGO_INICIO.match(siguiente):
                continue
            # El cargo se corta en el primer punto: el bloque suele seguir con
            # la bio ("Gerente General. Lidera la agencia desde 2018.").
            cargo = re.split(r"[.·|]", siguiente, 1)[0].strip()[:60]
            vistos.add(clave)
            salida.append({
                "person_name": bloque,
                "person_title": cargo,
                "company": "",
                "url": url,
                "domain": host_de(url),
                "evidence": "pagina",
            })
            break
    return salida


# ─────────────────────────────────────────────────────────────────────────────
# Plan de busqueda
# ─────────────────────────────────────────────────────────────────────────────
# Como se nombra cada superficie. Rotan por ronda porque el indice no cambia
# entre una corrida y la siguiente: preguntar lo mismo devuelve lo mismo, y la
# ronda 3 se gastaria redescubriendo a la gente que ya esta guardada.
EQUIPO_VARIANTES = [
    '"nuestro equipo" OR "quienes somos"',
    '"equipo directivo" OR "plana ejecutiva"',
    '"nuestro equipo" OR "conoce al equipo"',
]
PRENSA_VARIANTES = ["asume como", "es el nuevo", "fue designado"]
FORO_VARIANTES = [
    "expositores OR panelistas OR speakers",
    "ponentes OR charla OR keynote",
    "webinar OR seminario OR conversatorio",
]


class Estrategia:
    __slots__ = ("nombre", "query", "prioridad")

    def __init__(self, nombre: str, query: str, prioridad: int) -> None:
        self.nombre = nombre
        self.query = re.sub(r"\s+", " ", query).strip()
        self.prioridad = prioridad


def construir_plan(nicho: str, cargos: list[str], ubi: Ubicacion,
                   ronda: int = 0) -> list[Estrategia]:
    """Queries del canal, ordenadas por lo que mas rinde por request.

    Ninguna usa `site:linkedin.com/in`. No es una omision: esta medido que
    devuelve cero (F7), asi que gastar una query ahi es gastar el presupuesto
    de todas las demas.

    LA RONDA NO ES DECORACION. El motor de Venara vuelve a correr sobre el
    mismo ICP cada pocas horas. Con un plan fijo, la ronda 3 pide exactamente lo
    mismo que la ronda 1, el buscador devuelve exactamente lo mismo, y la
    corrida entera se gasta en redescubrir gente ya guardada. La ronda rota dos
    cosas a la vez: que cargo lidera y con que palabras se nombra la superficie.
    """
    lugar = ubi.ciudad or ubi.pais_nombre or ubi.texto or ""
    r = max(0, int(ronda))
    lista = [c.strip() for c in cargos if c and c.strip()] or ["gerente general"]
    principal = lista[r % len(lista)]
    # El segundo cargo nunca es el mismo que el principal: repetirlo gastaria
    # cuatro fetches (uno por proveedor) en la query que ya se esta haciendo.
    secundario = lista[(r + 1) % len(lista)] if len(lista) > 1 else ""

    equipo = EQUIPO_VARIANTES[r % len(EQUIPO_VARIANTES)]
    prensa = PRENSA_VARIANTES[r % len(PRENSA_VARIANTES)]
    foro = FORO_VARIANTES[r % len(FORO_VARIANTES)]

    plan = [
        Estrategia("equipo", f'"{principal}" {nicho} {lugar} ({equipo})', 1),
        Estrategia("prensa", f'{nicho} {lugar} "{prensa} {principal}"', 2),
        Estrategia("foro", f'{nicho} {lugar} ({foro}) "{principal}"', 3),
        Estrategia("directorio",
                   f'{nicho} {lugar} (directorio OR socios OR asociados) "{principal}"', 4),
        Estrategia("entrevista", f'"{principal}" {nicho} {lugar} entrevista', 5),
    ]
    if secundario:
        plan.append(Estrategia("equipo-2", f'"{secundario}" {nicho} {lugar} ({equipo})', 6))
    return plan


def es_perfil_linkedin(url: str) -> bool:
    return "linkedin.com/in/" in (url or "").lower()


# ─────────────────────────────────────────────────────────────────────────────
# Paginas donde viven los decisores
# ─────────────────────────────────────────────────────────────────────────────
# MEDIDO EN VIVO (2026-09-03, IP residencial, Bing respondiendo): 67 resultados
# crudos produjeron UN candidato, y era un falso positivo. La conclusion no es
# que el canal no sirva: es que el snippet de un buscador casi nunca contiene
# "Nombre - Cargo - Empresa". Los nombres estan EN LA PAGINA.
#
# Por eso el endpoint devuelve dos cosas distintas:
#
#   results   personas ya parseadas del snippet. Gratis cuando aparecen, pero
#             son la minoria.
#   pages     las paginas que MERECEN una visita porque tienen forma de listar
#             gente. El cliente las scrapea y extrae de ahi.
#
# Devolver solo `results` era tirar el 99% del valor de la busqueda.
# "directorio" NO esta como senal de ruta, y no por olvido: `/directorio` ya
# esta en `_RUTA_NO_EMPRESA` de filtering.py como pagina de listado, asi que
# ningun resultado con esa ruta llega hasta aca. Dejarla escrita sugeriria una
# cobertura que no existe. El directorio de un gremio se alcanza igual por
# `/socios`, que si pasa el filtro.
_SENALES_URL = ("equipo", "nosotros", "quienes-somos", "quienessomos", "team",
                "about", "socios", "expositores", "panelistas",
                "speakers", "ponentes", "staff", "plana", "directiva",
                "leadership", "management", "gerencia", "ejecutivos")
# "directorio" y "socios" NO estan aca, y es una decision medida. En un TITULO
# casi siempre significan "Directorio de empresas de Chile" -- un agregador --
# y no el directorio de una sociedad. En la primera medicion en vivo esa sola
# palabra metio amarillas.cl, chilepymes.com y directorioempresaschile.cl como
# "paginas de equipo". Como segmento de URL (/directorio, /socios) el sentido
# es el otro, asi que ahi si se aceptan.
_SENALES_TITULO = ("nuestro equipo", "quienes somos", "equipo directivo",
                   "plana ejecutiva", "conoce al equipo", "our team",
                   "expositores", "panelistas", "speakers", "ponentes",
                   "nosotros", "gerencia")

# Un titulo que anuncia una LISTA de empresas no es una pagina de equipo, por
# mas que contenga la palabra que buscamos.
# Un host que se llama a si mismo directorio, guia o paginas amarillas ES un
# agregador, aunque no este en la lista de filtering.py: no hay forma de que
# sea el sitio del prospecto.
_RX_HOST_LISTADO = re.compile(
    r"(directorio|directory|amarillas|guiaempresas|guia-?empresas|"
    r"paginas|listado|empleos|trabajos|jobs)", re.I)

_RX_LISTADO = re.compile(
    r"\b(directorio|listado|guia|ranking|catalogo|ofertas|empleos|vacantes)\s+(de|en)\b", re.I)


def es_pagina_de_personas(url: str, titulo: str) -> bool:
    """Heuristica barata: vale la pena gastar un scrape en esta pagina?

    Es deliberadamente permisiva con las senales y estricta con el costo: un
    falso positivo cuesta UN scrape; un falso negativo pierde a todo el equipo
    de una empresa que no volvera a aparecer.
    """
    # El filtro que ya protege al canal de empresas protege tambien a este:
    # directorios, bolsas de trabajo y agregadores listan gente de OTRAS
    # empresas, asi que un scrape ahi no produce un lead del rubro pedido.
    if filtering.motivo_descarte(url, titulo):
        return False
    t = sin_acentos((titulo or "").lower())
    if _RX_LISTADO.search(t):
        return False

    # Las senales se buscan en la RUTA, nunca en el host. Medido en vivo:
    # `directorioempresaschile.cl` entraba como pagina de equipo porque su
    # NOMBRE contiene "directorio". En una ruta (`/directorio`, `/socios`) la
    # palabra significa el directorio de una sociedad; en el host significa que
    # el sitio entero es un directorio de empresas, que es lo contrario.
    host = sin_acentos(host_de(url).lower())
    if _RX_HOST_LISTADO.search(host):
        return False
    ruta = sin_acentos((url or "").lower())
    corte = ruta.find(host) if host else -1
    ruta = ruta[corte + len(host):] if corte >= 0 else ruta

    if any(s in ruta for s in _SENALES_URL):
        return True
    return any(s in t for s in _SENALES_TITULO)


def buscar(nicho: str, cargos: list[str], ubicacion: str, limite: int,
           ronda: int = 0) -> dict:
    """Busqueda completa del canal personas. Devuelve candidatos + diagnostico.

    Igual que el pipeline de empresas: una busqueda que no pudo mirar NO es una
    busqueda sin resultados. Si los proveedores bloquearon, la respuesta lo
    dice y `completo` queda en False.
    """
    t0 = time.monotonic()
    ubi = interpretar(ubicacion)
    plan = construir_plan(nicho, cargos, ubi, ronda)
    r_efectiva = max(0, int(ronda))
    activos = providers.activos()
    salud = SaludProveedores()

    trabajos = [(est, prov) for est in plan for prov in activos]
    trabajos.sort(key=lambda t: (t[0].prioridad, t[1].prioridad))
    descartados_por_techo = max(0, len(trabajos) - config.MAX_FETCHES)
    trabajos = trabajos[: config.MAX_FETCHES]

    crudos: list[tuple[dict, Estrategia, str]] = []
    por_proveedor: dict[str, int] = {}
    descartes: dict[str, int] = {}

    def tarea(est: Estrategia, prov):
        url = providers.construir_url(prov.nombre, est.query, ubi)
        return est, prov, obtener(url, prov.nombre, salud)

    limite_hilos = min(config.MAX_CONCURRENCY, max(1, len(trabajos)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=limite_hilos) as ex:
        futuros = [ex.submit(tarea, e, p) for e, p in trabajos]
        for fut in concurrent.futures.as_completed(futuros):
            # Presupuesto total. `buscar_persona()` de linkedin.py no lo tenia
            # y por eso una sola consulta tardaba 4m43s medidos contra
            # produccion: recorria 3 queries x 4 proveedores en serie sin
            # ningun tope. El cliente ya habia cortado hacia rato.
            if time.monotonic() - t0 > config.SEARCH_BUDGET_S:
                log.warning("presupuesto agotado, se corta la busqueda de personas")
                for f in futuros:
                    f.cancel()
                break
            try:
                est, prov, r = fut.result()
            except Exception as e:
                log.warning("tarea fallo: %s", str(e)[:100])
                continue
            if not r.sirve:
                motivo = r.error or (r.veredicto.motivo if r.veredicto else "desconocido")
                clave = prov.nombre + ":" + motivo
                descartes[clave] = descartes.get(clave, 0) + 1
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            por_proveedor[prov.nombre] = por_proveedor.get(prov.nombre, 0) + len(items)
            for it in items:
                crudos.append((it, est, prov.nombre))

    candidatos: dict[str, dict] = {}
    paginas: dict[str, dict] = {}
    for it, est, prov in crudos:
        url = it.get("url", "")
        if es_perfil_linkedin(url):
            # Medido muerto: si aparece, es un titulo indexado sin la pagina
            # detras. Se cuenta para poder demostrar que se descarto a
            # proposito y no que el parser fallo.
            descartes["linkedin_in_descartado"] = descartes.get("linkedin_in_descartado", 0) + 1
            continue
        # La pagina se registra ANTES de intentar parsear: que el snippet no
        # nombre a nadie es la norma, no la excepcion, y es justo cuando hay
        # que ir a mirar la pagina.
        titulo_it = it.get("titulo", "")
        texto_it = sin_acentos((titulo_it + " " + it.get("snippet", "")).lower())
        del_rubro_it = any(w for w in sin_acentos(nicho.lower()).split()
                           if len(w) > 3 and w in texto_it)
        if url and url not in paginas:
            paginas[url] = {
                "url": url,
                "titulo": titulo_it,
                "snippet": it.get("snippet", ""),
                "people_page": es_pagina_de_personas(url, titulo_it),
                "industry_match": del_rubro_it,
                "query": est.nombre,
                "source": prov,
            }
        for p in extraer_personas(titulo_it, it.get("snippet", ""), url):
            clave = clave_nombre(p["person_name"]) + "|" + clave_nombre(p["company"])
            score = puntuar_cargo(p["person_title"])
            # El nicho en el texto es lo que liga a la persona con el rubro
            # pedido. Sin esto entra cualquier gerente de cualquier industria.
            del_rubro = del_rubro_it
            if del_rubro:
                score = min(1.0, score + 0.10)
            p.update({"score": round(score, 3), "source": prov, "query": est.nombre,
                      "industry_match": del_rubro})
            previo = candidatos.get(clave)
            if previo is None or p["score"] > previo["score"]:
                candidatos[clave] = p

    ordenados = sorted(candidatos.values(), key=lambda c: -c["score"])[:limite]
    # Las paginas se ordenan por lo que las hace valiosas: primero las que
    # tienen forma de listar gente, despues las del rubro pedido. El cliente
    # gasta sus scrapes de arriba hacia abajo.
    ordenadas = sorted(paginas.values(),
                       key=lambda p: (not p["people_page"], not p["industry_match"]))[:limite]

    bloqueados = salud.resumen()
    hubo_respuesta = bool(por_proveedor)
    completo = hubo_respuesta and not bloqueados

    diag = {
        "proveedores_ok": por_proveedor,
        "proveedores_bloqueados": bloqueados,
        "crudos": len(crudos),
        "candidatos": len(candidatos),
        "paginas": len(paginas),
        "paginas_de_personas": sum(1 for p in paginas.values() if p["people_page"]),
        "descartes": descartes,
        "estrategias": [e.nombre for e in plan],
        "ronda": r_efectiva,
        "fetches_planificados": len(trabajos),
        "fetches_descartados_por_techo": descartados_por_techo,
        "ubicacion": {"texto": ubi.texto, "ciudad": ubi.ciudad, "pais": ubi.pais,
                      "reconocida": ubi.reconocida},
        "ms": int((time.monotonic() - t0) * 1000),
        "completo": completo,
    }
    # El motivo de un vacio NO es opcional: "no hay personas publicadas" y "no
    # pudimos mirar" mandan a investigar lugares distintos.
    # El vacio se juzga sobre las DOS salidas: una corrida que no nombro a
    # nadie en los snippets pero devolvio doce paginas de equipo no esta vacia,
    # y decir que si lo esta manda a investigar el lugar equivocado.
    if not ordenados and not ordenadas:
        diag["motivo_vacio"] = "providers_blocked" if bloqueados else (
            "sin_resultados" if not crudos else "not_indexed")

    log.info("personas '%s' %s -> %d candidatos + %d paginas (%d crudos) en %dms%s",
             nicho, ubicacion, len(ordenados), len(ordenadas), len(crudos), diag["ms"],
             " BLOQUEADO:" + ",".join(bloqueados) if bloqueados else "")
    return {"personas": ordenados, "paginas": ordenadas,
            "diagnostico": diag, "completo": completo}
