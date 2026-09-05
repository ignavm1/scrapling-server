"""Resolutor de decisor: de un NOMBRE DE EMPRESA a la persona que decide.

QUE RESUELVE, Y POR QUE NO ALCANZABA LO QUE HABIA

`linkedin.buscar_persona()` intentaba esto con tres queries en serie contra
`site:linkedin.com/in`, sin techo ni presupuesto. Medido contra produccion: una
sola consulta tardaba **4m43s** y devolvia NOT_FOUND, porque los perfiles
personales no estan en el indice publico (F7). Buscaba en el unico lugar donde
esta demostrado que no hay nada, y tardaba una eternidad en no encontrarlo.

LOS ANGULOS

El indice no responde igual a la misma pregunta hecha de formas distintas. Una
sola query encuentra siempre lo mismo; varias, cada una apuntando a una
superficie distinta, encuentran gente que ninguna sola alcanza:

  cargo_directo   "<empresa>" CEO / gerente general / fundador
  sitio_equipo    site:<dominio> (equipo OR nosotros OR "quienes somos")
  prensa          "<empresa>" ("asume como" OR "es el nuevo" OR "fue designado")
  entrevista      "<empresa>" entrevista (fundador OR CEO OR gerente)
  pagina          el TEXTO de la pagina de equipo del propio sitio

El ultimo no es una query: es una VISITA. Esta medido (F21) que el snippet de un
buscador casi nunca dice "Nombre - Cargo - Empresa" -- 67 resultados crudos
dieron un candidato, y era falso. Los nombres estan dentro de la pagina, y la
unica forma de leerlos es entrar.

LA REGLA QUE EVITA EL FALSO POSITIVO

Un candidato necesita quedar LIGADO a la empresa por una de dos vias: o salio
del sitio propio de la empresa, o el texto donde aparece nombra a la empresa.
Sin ninguna de las dos se descarta, por mas que el cargo sea perfecto. Un
decision maker equivocado en una campana de outbound es peor que ninguno: se le
escribe a una persona real, a nombre del cliente, sobre una empresa que no es la
suya.
"""
from __future__ import annotations
import concurrent.futures
import logging
import re
import time
from urllib.parse import urljoin, urlsplit
from dataclasses import dataclass, field

# `cargos` y `contacto` van con alias porque `resolver()` recibe parametros con
# esos mismos nombres, y sin el alias el parametro tapa al modulo -- el fallo
# aparece recien en tiempo de ejecucion, como 'NoneType' has no attribute.
from . import (cargos as mod_cargos, config, contacto as mod_contacto,
               extraction, filtering, linkedin_perfil, personas, providers,
               website)
from .fetch import SaludProveedores, obtener
from .linkedin import puntuar_cargo
from .location import Ubicacion, interpretar
from .normalize import clave_nombre, sin_acentos

log = logging.getLogger(__name__)

# Cargos que se preguntan de forma directa. Son los que firman en una PyME
# LATAM; preguntar por "coordinador" gastaria un angulo en quien no decide.
CARGOS_DIRECTOS = ["CEO", "gerente general", "fundador", "director comercial"]

# Cuantos angulos de cargo directo se arman. Cada uno cuesta un fetch por
# proveedor, y DECISOR_MAX_FETCHES corta antes de que sean gratis.
MAX_CARGOS_DIRECTOS = 3

# Queries de perfil por consulta. Dos cargos abren dos angulos distintos; el
# tercero repite en gran medida los resultados de los dos primeros.
MAX_QUERIES_LINKEDIN = 2

# Score a partir del cual no se sale a buscar. Un candidato que llega aca ya
# tiene cargo de decisor Y esta publicado en el sitio de la propia empresa: ocho
# busquedas encima no lo mejoran, y si queman el presupuesto de scraping del
# lote entero, porque esto se llama UNA VEZ POR EMPRESA.
UMBRAL_SUFICIENTE = 0.85


@dataclass(frozen=True)
class Angulo:
    nombre: str
    query: str
    prioridad: int


@dataclass
class Candidato:
    nombre: str
    cargo: str
    url: str
    angulo: str
    proveedor: str
    # 'sitio_propio' cuando el dominio es de la empresa buscada; si no, 'tercero'.
    origen: str
    # 'pagina' cuando salio del texto de la pagina; 'titulo'/'snippet' del SERP.
    donde: str
    empresa_en_texto: bool
    score: float = 0.0
    evidencia: list[str] = field(default_factory=list)
    # Por que canal se alcanza. Se llena al final, cuando ya se sabe quien es.
    contacto: dict = field(default_factory=dict)
    # URL del perfil, cuando el candidato salio de uno. Es un CANAL por derecho
    # propio -- se le puede escribir por ahi -- y por eso no alcanza con dejarlo
    # en `url`, que significa "de donde salio el dato" y puede ser un diario.
    perfil_linkedin: str = ""

    def a_dict(self) -> dict:
        return {
            "person_name": self.nombre,
            "person_title": self.cargo,
            "url": self.url,
            "angle": self.angulo,
            "source": self.proveedor,
            "origin": self.origen,
            "found_in": self.donde,
            "confidence": round(self.score, 3),
            "seniority": mod_cargos.nivel(self.cargo),
            "evidence": self.evidencia,
            # Cada dato de contacto viaja con su procedencia: un email sin
            # origen no se puede auditar el dia que rebota.
            "contact": ({**self.contacto, "linkedin_url": self.perfil_linkedin}
                        if self.contacto else
                        ({"linkedin_url": self.perfil_linkedin}
                         if self.perfil_linkedin else None)),
        }


def construir_plan(empresa: str, dominio: str, ubi: Ubicacion,
                   cargos: list[str] | None = None) -> list[Angulo]:
    """Los angulos de ataque, ordenados por lo que mas rinde por request.

    Ninguno usa `site:linkedin.com/in`: esta medido que devuelve cero perfiles
    con control positivo (el MISMO operador sobre /company devuelve diez). Un
    angulo que no puede traer nada es un angulo menos para los que si pueden.
    """
    e = (empresa or "").strip()
    d = (dominio or "").strip().lower().replace("https://", "").replace("http://", "")
    d = d.replace("www.", "").split("/")[0]
    lugar = ubi.ciudad or ubi.pais_nombre or ""
    lista = [c.strip() for c in (cargos or CARGOS_DIRECTOS) if c and c.strip()]
    lista = lista[:MAX_CARGOS_DIRECTOS] or CARGOS_DIRECTOS[:1]

    plan: list[Angulo] = []

    # 1) El sitio propio primero. Es la fuente con menos ruido que existe: si la
    #    empresa publica a su equipo, ahi esta, con el cargo escrito por ella.
    if d:
        plan.append(Angulo("sitio_equipo",
                           f'site:{d} (equipo OR nosotros OR "quienes somos")', 1))
    else:
        plan.append(Angulo("sitio_equipo",
                           f'"{e}" ("nuestro equipo" OR "quienes somos")', 1))

    # 2) La pregunta directa: "<empresa> CEO". Va DESPUES del angulo de
    #    LinkedIn porque un perfil declara cargo y empresa de forma explicita,
    #    mientras que un resultado suelto hay que interpretarlo.
    for i, cargo in enumerate(lista):
        plan.append(Angulo("cargo_directo", f'"{e}" {cargo}', 4 + i))

    # 3) El perfil de LinkedIn. Se pide "<empresa> <cargo> linkedin" y NO con el
    #    operador `site:linkedin.com/in`: medido el 2026-09-04, el operador da
    #    cero en los proveedores disponibles y la palabra suelta devuelve siete
    #    perfiles en Brave para la misma empresa (F24).
    for i, q in enumerate(linkedin_perfil.construir_queries(e, lista)[:MAX_QUERIES_LINKEDIN]):
        plan.append(Angulo("linkedin_perfil", q, 2 + i))

    # 4) Prensa de nombramientos: nombra persona, cargo y empresa en una frase.
    plan.append(Angulo("prensa",
                       f'"{e}" ("asume como" OR "es el nuevo" OR "fue designado")', 6))

    # 5) Entrevistas: el fundador hablando de su propia empresa.
    plan.append(Angulo("entrevista",
                       f'"{e}" entrevista (fundador OR CEO OR "gerente general")'
                       + (f" {lugar}" if lugar else ""), 7))

    # 6) Directorios de ejecutivos. No es una fuente teorica: en la medicion del
    #    2026-09-03 fueron craft.co y theorg.com los que dieron al CEO de Buk y
    #    al co-fundador correcto de Betterfly. Antes se llegaba ahi por
    #    casualidad, desde el angulo de cargo directo; ahora se los busca a
    #    proposito.
    plan.append(Angulo("directorio_ejecutivo",
                       f'"{e}" (executives OR "leadership team" OR '
                       f'"equipo directivo" OR organigrama)', 8))

    # 7) Representante legal. Es el angulo mas LATAM de todos: en licitaciones,
    #    avisos y registros publicos la empresa declara QUIEN LA REPRESENTA, y
    #    esa persona es por definicion la que firma. Ningun otro angulo mira
    #    esos documentos.
    plan.append(Angulo("representante_legal",
                       f'"{e}" ("representante legal" OR "socio fundador" OR '
                       f'"director ejecutivo")', 9))

    return plan


# Palabras del nombre de una empresa que no la identifican. "Marketing" aparece
# en cualquier pagina del rubro, asi que encontrarla no prueba nada.
_PALABRAS_GENERICAS = {
    "agencia", "agency", "grupo", "group", "studio", "estudio", "consultora",
    "consulting", "marketing", "digital", "empresa", "servicios", "soluciones",
    "compania", "corporacion", "holding", "spa", "sac", "sas", "ltda", "srl",
}


def _normalizar_palabras(texto: str) -> str:
    """Texto -> palabras separadas por un espacio, sin acentos ni puntuacion.

    NO se usa `clave_nombre()` aca, y la diferencia importa: esa funcion esta
    hecha para un NOMBRE corto -- pasa por `limpiar_titulo()`, que recorta en el
    primer separador. Aplicada al texto de una pagina entera devolvia
    "nuestroequipo" y toda liga con la empresa daba False.
    """
    return " " + re.sub(r"[^a-z0-9]+", " ", sin_acentos(texto or "").lower()).strip() + " "


def _liga_con_la_empresa(texto: str, empresa: str) -> bool:
    """El texto nombra a la empresa buscada?

    Es la mitad de la regla anti-falso-positivo. La otra mitad es que la URL
    pertenezca al dominio de la empresa. Sin ninguna de las dos, el candidato
    es "un gerente que el buscador relaciono por casualidad".
    """
    t = _normalizar_palabras(texto)
    e = _normalizar_palabras(empresa).strip()
    if not e:
        return False
    # El nombre completo, palabra por palabra: la prueba mas fuerte.
    if " " + e + " " in t:
        return True
    # Si no, alcanza un token DISTINTIVO del nombre. Se exige que no sea
    # generico: "marketing" aparece en toda pagina del rubro y encontrarlo
    # ligaria a la empresa con su competencia entera.
    for palabra in e.split():
        if len(palabra) > 3 and palabra not in _PALABRAS_GENERICAS:
            if " " + palabra + " " in t:
                return True
    return False


# Pesos del score. Suman EXACTAMENTE 1.0 y esa es la razon de que existan como
# constantes en vez de numeros sueltos: la primera version sumaba el peso del
# cargo (hasta 1.0) mas los bonus, y saturaba en 1.0 antes de terminar de
# contar. El efecto no era cosmetico -- dos candidatos que debian ordenarse
# quedaban empatados, y el empate se daba justo entre los candidatos fuertes,
# que es donde el orden importa.
PESO_CARGO = 0.50
PESO_SITIO_PROPIO = 0.25
PESO_EMPRESA_EN_TEXTO = 0.15
PESO_LEIDO_DE_PAGINA = 0.10


def puntuar(c: Candidato) -> Candidato:
    """Score y evidencia legible.

    Suma ponderada de senales, no promedio: un promedio deja que el cargo
    perfecto tape la ausencia total de liga con la empresa. La mitad del score
    la pone el cargo y la otra mitad la PROCEDENCIA, porque un "CEO" leido en
    un blog cualquiera y uno leido en el /equipo de la empresa no valen igual
    aunque digan la misma palabra.
    """
    peso_cargo = puntuar_cargo(c.cargo)
    s = PESO_CARGO * peso_cargo
    ev = [f"cargo: {c.cargo or 'sin cargo'} ({peso_cargo:.2f})"]

    if c.origen == "sitio_propio":
        # La senal mas fuerte disponible sin pagar: la empresa lo publica en su
        # propio dominio. Nadie pone al gerente de otra empresa en su /equipo.
        s += PESO_SITIO_PROPIO
        ev.append("publicado en el sitio de la propia empresa")
    elif c.origen == "linkedin_verificado":
        # Usa la misma ranura que el sitio propio porque es la misma clase de
        # evidencia: la fuente es la parte interesada declarandolo. Aca lo
        # declara la persona en su perfil, y ademas dice la empresa.
        s += PESO_SITIO_PROPIO
        ev.append("perfil de LinkedIn que declara ese cargo en esa empresa")
    if c.empresa_en_texto:
        s += PESO_EMPRESA_EN_TEXTO
        ev.append("el texto nombra a la empresa buscada")
    if c.donde == "pagina":
        # Leido del cuerpo de la pagina, no de un resumen del buscador que pudo
        # mezclar dos resultados.
        s += PESO_LEIDO_DE_PAGINA
        ev.append("leido del texto de la pagina, no del snippet")

    c.score = round(min(1.0, s), 3)
    c.evidencia = ev
    return c



# ─────────────────────────────────────────────────────────────────────────────
# Entrar por el sitio, sin pasar por ningun buscador
# ─────────────────────────────────────────────────────────────────────────────
# MEDIDO EL 2026-09-03: `site:fintual.cl (equipo OR nosotros)` en Bing devolvio
# diez resultados de zhihu.com y foros franceses sobre Instagram. Bing ignora el
# operador y sirve cualquier cosa (F6), y los demas proveedores estaban en
# captcha (F1). Con eso, un resolutor que solo sabe buscar no encuentra nada,
# por mas angulos que tenga.
#
# Pero cuando se conoce el dominio no hace falta ningun buscador: la pagina de
# equipo esta enlazada desde la home. Este camino no depende de nadie mas que
# del sitio del propio prospecto, que es justamente quien SI quiere ser leido.
_RX_ENLACE = re.compile(r'<a\b[^>]*\bhref=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.S)
_RX_TAGS = re.compile(r"<[^>]+>")


def _texto_ancla(fragmento: str) -> str:
    return re.sub(r"\s+", " ", _RX_TAGS.sub(" ", fragmento or "")).strip()


def _paginas_del_sitio(dominio: str, salud: SaludProveedores) -> tuple[list[str], int]:
    """Paginas de equipo enlazadas desde la home. Devuelve (urls, fetches).

    Un fetch a la home compra hasta DECISOR_MAX_PAGINAS candidatas, y ninguna
    depende de que un buscador nos atienda.
    """
    if not dominio:
        return [], 0
    base = "https://" + dominio
    r = obtener(base + "/", "sitio", salud, timeout=config.DECISOR_FETCH_TIMEOUT)
    if not r.html:
        return [], 1

    urls: list[str] = []
    vistas: set[str] = set()
    for m in _RX_ENLACE.finditer(r.html):
        href = (m.group(1) or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        # urljoin y no concatenacion a mano: la primera version armaba
        # `base + href.lstrip("./")` y `lstrip` come TAMBIEN la barra inicial,
        # asi que "/nuestro-equipo" producia "https://onzamarketing.clnuestro-equipo".
        # El error no rompia nada visible -- solo hacia que el fetch fallara y
        # la pagina de equipo no se leyera nunca.
        url = urljoin(base + "/", href).split("#")[0]
        # Solo el dominio propio: seguir un enlace externo desde la home lleva
        # al equipo de otra empresa, o a una red social.
        host = urlsplit(url).hostname or ""
        if not (host == dominio or host.endswith("." + dominio)):
            continue
        if url in vistas:
            continue
        if not personas.es_pagina_de_personas(url, _texto_ancla(m.group(2))):
            continue
        vistas.add(url)
        urls.append(url)
        if len(urls) >= config.DECISOR_MAX_PAGINAS:
            break
    return urls, 1


def _paginas_de_contacto(dominio: str, salud: SaludProveedores) -> list[str]:
    """Paginas de contacto enlazadas desde la home. Ahi viven los emails."""
    if not dominio:
        return []
    base = "https://" + dominio
    r = obtener(base + "/", "sitio", salud, timeout=config.DECISOR_FETCH_TIMEOUT)
    if not r.html:
        return []
    out, vistas = [], set()
    for m in _RX_ENLACE.finditer(r.html):
        href = (m.group(1) or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        url = urljoin(base + "/", href).split("#")[0]
        host = urlsplit(url).hostname or ""
        if not (host == dominio or host.endswith("." + dominio)):
            continue
        if url in vistas or not mod_contacto.es_pagina_de_contacto(url, _texto_ancla(m.group(2))):
            continue
        vistas.add(url)
        out.append(url)
        if len(out) >= 2:
            break
    # MEDIDO (2026-09-04): fintual.cl y xepelin.com no enlazan ninguna pagina
    # con la palabra "contacto" en la home -- usan formulario o chat. Probar las
    # rutas habituales cuesta un fetch y es lo unico que queda cuando el enlace
    # no existe.
    if not out:
        out = [base + "/" + r for r in ("contacto", "contact")]
    return out


def _paginas_a_visitar(items: list[tuple[dict, str, str]], empresa: str,
                       dominio: str) -> list[str]:
    """URLs del SITIO DE LA EMPRESA que parecen listar gente.

    Solo del sitio propio: entrar a la pagina de equipo de un tercero gasta un
    fetch para leer el equipo de otra empresa.
    """
    urls: list[str] = []
    vistas: set[str] = set()
    for it, _ang, _prov in items:
        url = it.get("url", "")
        if not url or url in vistas:
            continue
        if personas.es_perfil_linkedin(url):
            continue
        propio = website.pertenece_a(url, empresa) >= 0.8
        if dominio and dominio in url.lower():
            propio = True
        if not propio:
            continue
        if not personas.es_pagina_de_personas(url, it.get("titulo", "")):
            continue
        vistas.add(url)
        urls.append(url)
        if len(urls) >= config.DECISOR_MAX_PAGINAS:
            break
    return urls



# Largo minimo para aceptar que un nombre corto esta contenido en uno largo.
# Con menos, "Ana" fusionaria con "Ana Maria Rojas Perez" -- y tambien con
# "Anabel Soto", que es otra persona.
MIN_CLAVE_PARA_FUSION = 8


def _fusionar_por_apellido_en_la_misma_pagina(
        candidatos: dict[str, Candidato]) -> dict[str, Candidato]:
    """Une variantes del mismo nombre leidas de la MISMA url.

    Se exige la misma url a proposito: dos personas del mismo apellido y misma
    inicial existen (hermanos, familia duena de la empresa), y fusionarlas
    entre fuentes distintas borraria a una de verdad. Dentro de una sola
    pagina, en cambio, es el parser leyendo dos veces.
    """
    por_firma: dict[tuple, str] = {}
    fuera: set[str] = set()
    for k, c in candidatos.items():
        partes = [p for p in c.nombre.split() if len(p) > 1]
        if len(partes) < 2:
            continue
        firma = (c.url, sin_acentos(partes[-1]).lower(), sin_acentos(partes[0])[:1].lower())
        previo = por_firma.get(firma)
        if previo is None:
            por_firma[firma] = k
            continue
        gana, pierde = ((k, previo) if candidatos[k].score > candidatos[previo].score
                        or (candidatos[k].score == candidatos[previo].score
                            and len(candidatos[k].nombre) > len(candidatos[previo].nombre))
                        else (previo, k))
        candidatos[gana].evidencia = candidatos[gana].evidencia + [
            'tambien leido como "%s" en la misma pagina' % candidatos[pierde].nombre]
        por_firma[firma] = gana
        fuera.add(pierde)
    return {k: v for k, v in candidatos.items() if k not in fuera}


def fusionar_mismo_humano(candidatos: dict[str, Candidato]) -> dict[str, Candidato]:
    """Une las formas del MISMO nombre en un solo candidato.

    MEDIDO EN VIVO (2026-09-03): la corrida sobre Buk devolvio "Jaime Arrieta" y
    "Jaime Arrieta Boetsch" como dos decisores. Son la misma persona, y dos
    leads de la misma persona son dos mensajes al mismo humano a nombre del
    cliente.

    Gana el de mayor score; si empatan, el nombre mas largo, que es el mas
    completo para saludar.
    """
    # Segunda pasada, sobre la MISMA fuente: "Karim Pichara" y "Kim Pichara"
    # salieron los dos de notco.ai/about. Ninguna es substring de la otra
    # ("Kim" no es prefijo de "Karim"), asi que la regla de contencion no las
    # une. Dentro de una sola pagina, mismo apellido + misma inicial ES la
    # misma persona: el parser la leyo dos veces, no hay dos CTOs.
    candidatos = _fusionar_por_apellido_en_la_misma_pagina(candidatos)
    claves = sorted(candidatos, key=len, reverse=True)
    fusionadas: dict[str, Candidato] = {}
    for k in claves:
        c = candidatos[k]
        destino = None
        for otra in fusionadas:
            corto, largo = (k, otra) if len(k) <= len(otra) else (otra, k)
            if len(corto) >= MIN_CLAVE_PARA_FUSION and corto in largo:
                destino = otra
                break
        if destino is None:
            fusionadas[k] = c
            continue
        actual = fusionadas[destino]
        gana = (c.score > actual.score
                or (c.score == actual.score and len(c.nombre) > len(actual.nombre)))
        if gana:
            # Se conserva la evidencia de las dos vistas: que la misma persona
            # aparezca en dos fuentes es informacion, no ruido.
            c.evidencia = c.evidencia + [f"tambien visto como \"{actual.nombre}\""]
            # Y el perfil NO se pierde: si la lectura ganadora no lo traia, el
            # canal de LinkedIn desapareceria por haber fusionado.
            c.perfil_linkedin = c.perfil_linkedin or actual.perfil_linkedin
            fusionadas[destino] = c
        else:
            actual.evidencia = actual.evidencia + [f"tambien visto como \"{c.nombre}\""]
            actual.perfil_linkedin = actual.perfil_linkedin or c.perfil_linkedin
    return fusionadas


def resolver(empresa: str, dominio: str = "", ubicacion: str = "",
             cargos: list[str] | None = None, limite: int = 5) -> dict:
    """Encuentra al decisor de UNA empresa. Devuelve candidatos + diagnostico.

    EL ORDEN NO ES ARBITRARIO. Primero se entra al sitio de la empresa, y solo
    si eso no alcanza se gastan busquedas. Medido el 2026-09-03: los buscadores
    o estaban en captcha o servian resultados de otro planeta (`site:fintual.cl
    equipo` en Bing devolvio foros franceses sobre Instagram). El sitio del
    propio prospecto es la unica fuente que siempre atiende, y ademas es la que
    menos ruido tiene: nadie publica al gerente de otra empresa en su /equipo.

    Igual que el resto del servidor: una busqueda que no pudo mirar NO es una
    busqueda sin resultados.
    """
    t0 = time.monotonic()
    empresa = (empresa or "").strip()
    if len(empresa) < 2:
        return {"candidatos": [], "completo": False,
                "diagnostico": {"motivo_vacio": "no_company", "ms": 0,
                                "proveedores_bloqueados": {}, "angulos": [],
                                "queries": [], "fetches": 0, "crudos": 0,
                                "paginas_visitadas": 0, "candidatos": 0,
                                "busco_en_internet": False, "completo": False}}

    ubi = interpretar(ubicacion)
    dominio = (dominio or "").strip().lower()
    dominio = dominio.replace("https://", "").replace("http://", "")
    dominio = dominio.replace("www.", "").split("/")[0]
    plan = construir_plan(empresa, dominio, ubi, cargos)
    activos = providers.activos()
    salud = SaludProveedores()

    candidatos: dict[str, Candidato] = {}
    visitadas: set[str] = set()
    # Texto de las paginas del PROPIO sitio. Es de donde sale el contacto: los
    # emails no estan en el buscador, estan en la pagina de contacto.
    texto_sitio: list[str] = []
    fetches = 0
    crudos: list[tuple[dict, str, str]] = []
    por_proveedor: dict[str, int] = {}
    # Un fetch que se cae por timeout NO marca al proveedor como bloqueado
    # (solo lo hacen el captcha y los status), asi que desaparecia en silencio y
    # el veredicto terminaba diciendo "no_publicado". Contarlos es lo que
    # permite distinguir "esta empresa no publica a nadie" de "no pudimos
    # mirar" -- que es el fallo mas caro que documenta este repo (F1/F4).
    fallidos = 0
    presupuesto_agotado = False

    def agregar(c: Candidato) -> None:
        # Se exige la liga con la empresa. Sin ella el candidato es "un gerente
        # que el buscador relaciono por casualidad" -- y ese falso positivo
        # cuesta un correo real, a nombre del cliente, a quien no corresponde.
        if c.origen != "sitio_propio" and not c.empresa_en_texto:
            return
        k = clave_nombre(c.nombre)
        if not k:
            return
        puntuar(c)
        previo = candidatos.get(k)
        if previo is None or c.score > previo.score:
            candidatos[k] = c

    def leer_pagina(url: str, angulo: str = "sitio_directo") -> None:
        nonlocal fetches
        if url in visitadas:
            return
        visitadas.add(url)
        r = obtener(url, "sitio", salud, timeout=config.DECISOR_FETCH_TIMEOUT)
        fetches += 1
        if not r.html:
            return
        # Por BLOQUES, no aplanado: en una pagina de equipo el nombre y el
        # cargo son dos elementos distintos, y pegados no se distinguen de una
        # frase cualquiera.
        texto = website.texto_por_bloques(r.html)
        texto_sitio.append(texto)
        liga = _liga_con_la_empresa(texto, empresa)
        for p in personas.extraer_de_texto(texto, url):
            agregar(Candidato(
                nombre=p["person_name"], cargo=p["person_title"], url=url,
                angulo=angulo, proveedor="sitio",
                origen="sitio_propio", donde="pagina", empresa_en_texto=liga))

    # ── Fase 1: el sitio de la empresa, sin pasar por ningun buscador ────────
    enlazadas, gasto = _paginas_del_sitio(dominio, salud)
    fetches += gasto
    for url in enlazadas:
        if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
            break
        leer_pagina(url)

    candidatos = fusionar_mismo_humano(candidatos)
    mejor_ahora = max((c.score for c in candidatos.values()), default=0.0)
    if mejor_ahora >= UMBRAL_SUFICIENTE:
        # Ya hay un decisor con evidencia fuerte. Gastar ocho busquedas encima
        # no lo mejora y si quema el presupuesto de scraping del lote.
        if dominio:
            ya = mod_contacto.emails_del_dominio("\n".join(texto_sitio), dominio)
            if not ya:
                for url in _paginas_de_contacto(dominio, salud):
                    leer_pagina(url, angulo="contacto")
                    break
        ordenados = mod_cargos.elegir_mejor(list(candidatos.values()))[:limite]
        texto = "\n".join(texto_sitio)
        emails = mod_contacto.emails_del_dominio(texto, dominio)
        pares = mod_contacto.emparejar(emails, [c.nombre for c in candidatos.values()])
        for c in ordenados:
            c.contacto = mod_contacto.contacto_de(
                c.nombre, dominio, texto, ubi.pais or "CL", pares)
        diag = _diagnostico(plan, {}, salud, 0, fetches, len(visitadas),
                            len(candidatos), t0, busco=False)
        log.info("decisor '%s' -> %d desde el sitio, sin buscar, en %dms",
                 empresa, len(ordenados), diag["ms"])
        return {"candidatos": ordenados, "completo": True, "diagnostico": diag}

    # ── Fase 2: los buscadores ───────────────────────────────────────────────
    # Reparto POR RONDAS, no por angulo completo. La ronda 1 le da un proveedor
    # a CADA angulo; recien la ronda 2 reparte el segundo. Es lo unico que
    # garantiza que los siete angulos se ejecuten al menos una vez: con el
    # reparto anterior, los primeros angulos consumian el techo y los ultimos
    # no corrian nunca -- el fallo silencioso que ya habia matado al angulo de
    # LinkedIn (F24.3) y que con siete angulos habria matado a tres.
    orden_plan = sorted(plan, key=lambda a: a.prioridad)
    usables = activos[: config.DECISOR_PROVEEDORES_POR_ANGULO]
    trabajos = [(a, usables[r]) for r in range(len(usables)) for a in orden_plan]
    descartados_por_techo = max(0, len(trabajos) - config.DECISOR_MAX_FETCHES)
    trabajos = trabajos[: config.DECISOR_MAX_FETCHES]

    def tarea(ang: Angulo, prov):
        url = providers.construir_url(prov.nombre, ang.query, ubi)
        # Timeout propio, mas corto que el global: Scrapling reintenta 3 veces
        # por su cuenta, asi que con FETCH_TIMEOUT un solo fetch colgado podia
        # costar 45s -- mas que el presupuesto entero de la consulta.
        return ang, prov, obtener(url, prov.nombre, salud,
                                  timeout=config.DECISOR_FETCH_TIMEOUT)

    limite_hilos = min(config.MAX_CONCURRENCY, max(1, len(trabajos)))
    # El executor NO va en un `with`: al salir, el context manager espera a los
    # hilos vivos (shutdown(wait=True)) y eso hacia inutil el presupuesto --
    # medido, una corrida con todos los buscadores colgados tardaba 48s con el
    # presupuesto en 25. Se cierra con wait=False para volver a tiempo.
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=limite_hilos)
    futuros = [ex.submit(tarea, a, p) for a, p in trabajos]
    try:
        restante = max(0.1, config.DECISOR_BUDGET_S - (time.monotonic() - t0))
        for fut in concurrent.futures.as_completed(futuros, timeout=restante):
            if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
                presupuesto_agotado = True
                break
            try:
                ang, prov, r = fut.result()
            except Exception as e:
                log.warning("tarea fallo: %s", str(e)[:100])
                continue
            fetches += 1
            if not r.sirve:
                fallidos += 1
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            por_proveedor[prov.nombre] = por_proveedor.get(prov.nombre, 0) + len(items)
            for it in items:
                crudos.append((it, ang.nombre, prov.nombre))
    except concurrent.futures.TimeoutError:
        # No es un error: es el presupuesto haciendo su trabajo. Lo ya extraido
        # se usa igual.
        log.warning("presupuesto agotado resolviendo '%s'", empresa)
        presupuesto_agotado = True
    finally:
        for f in futuros:
            f.cancel()
        ex.shutdown(wait=False, cancel_futures=True)

    for it, ang, prov in crudos:
        url = it.get("url", "")
        if personas.es_perfil_linkedin(url):
            continue
        # MEDIDO (2026-09-03): un post de Instagram y un video de Facebook
        # entraron como fuente de decisores. Una red social no publica el
        # organigrama de nadie: lo que hay ahi es texto suelto que casualmente
        # contiene un nombre y una palabra que parece cargo.
        #
        # Se filtra SOLO redes y buscadores, no `motivo_descarte()` entero: ese
        # tambien descarta medios de prensa, y la prensa es justamente uno de
        # los angulos que mas rinde para nombramientos.
        if filtering.motivo_descarte(url, "") == "red-social-o-buscador":
            continue
        texto = it.get("titulo", "") + " " + it.get("snippet", "")
        propio = website.pertenece_a(url, empresa) >= 0.8 or (
            bool(dominio) and dominio in url.lower())
        liga = _liga_con_la_empresa(texto, empresa)
        for p in personas.extraer_personas(it.get("titulo", ""), it.get("snippet", ""), url):
            agregar(Candidato(
                nombre=p["person_name"], cargo=p["person_title"], url=url,
                angulo=ang, proveedor=prov,
                origen="sitio_propio" if propio else "tercero",
                donde=p["evidence"], empresa_en_texto=liga))

    # ── Fase 2b: los perfiles de LinkedIn ────────────────────────────────────
    # REGLA EXPLICITA: entre los resultados de busqueda se entra UNICAMENTE a
    # perfiles de LinkedIn. El snippet no alcanza -- Brave devuelve el titulo
    # como breadcrumb ("LinkedIn cl.linkedin.com in andresmarinkovic Andrés
    # Marinkovic"), con nombre y sin cargo. El titulo de la PAGINA si trae
    # nombre, cargo y empresa.
    perfiles_leidos = 0
    for it, ang, prov in crudos:
        if perfiles_leidos >= config.DECISOR_MAX_PERFILES:
            break
        if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
            break
        url = it.get("url", "")
        if not linkedin_perfil.es_perfil(url) or url in visitadas:
            continue
        visitadas.add(url)
        r = obtener(url, "linkedin", salud, timeout=config.DECISOR_FETCH_TIMEOUT)
        fetches += 1
        perfiles_leidos += 1
        if not r.html:
            continue
        datos = linkedin_perfil.parsear_titulo(linkedin_perfil.titulo_de(r.html))
        if not datos:
            continue

        coincide = linkedin_perfil.coincide_empresa(datos["empresa"], empresa)
        pais_perfil = linkedin_perfil.pais_del_perfil(url)
        # EL FALSO POSITIVO MEDIDO: "Houm" es una empresa chilena y tambien una
        # india, y un directorio extranjero le colgo a la chilena dos fundadores
        # que no son suyos. Cuando el perfil declara pais y NO es el pedido, no
        # se atribuye. Se pierde a quien se mudo de pais; se gana no escribirle
        # al fundador equivocado a nombre del cliente.
        if pais_perfil and ubi.pais and pais_perfil != ubi.pais:
            log.info("perfil %s descartado: pais %s != %s", url, pais_perfil, ubi.pais)
            continue
        if not coincide:
            continue

        agregar(Candidato(
            nombre=datos["nombre"], cargo=datos["cargo"], url=url,
            angulo="linkedin_perfil", proveedor="linkedin",
            origen="linkedin_verificado", donde="pagina",
            empresa_en_texto=True, perfil_linkedin=url))

    # ── Fase 3: paginas de equipo que aparecieron en la busqueda ─────────────
    for url in _paginas_a_visitar(crudos, empresa, dominio):
        if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
            break
        if len(visitadas) >= config.DECISOR_MAX_PAGINAS:
            break
        # Angulo distinto al de la fase 1, y no es un detalle cosmetico: esta
        # pagina se ENCONTRO BUSCANDO. Etiquetarla "sitio_directo" hacia leer
        # la evidencia como si el sistema hubiera entrado solo por la home, y
        # ocultaba que ese candidato desaparece cuando los buscadores bloquean
        # -- que es exactamente lo que paso al repetir la medicion.
        leer_pagina(url, angulo="pagina_desde_busqueda")

    # ── Fase 4: la pagina de contacto ────────────────────────────────────────
    # Se visita solo si hace falta: si el texto ya recogido trae un email del
    # dominio, gastar otro fetch no agrega nada.
    if dominio and time.monotonic() - t0 < config.DECISOR_BUDGET_S:
        ya = mod_contacto.emails_del_dominio("\n".join(texto_sitio), dominio)
        if not ya:
            for url in _paginas_de_contacto(dominio, salud):
                leer_pagina(url, angulo="contacto")
                break

    candidatos = fusionar_mismo_humano(candidatos)
    # El primero es el que MEJOR CARGO tiene, no el mejor documentado: a quien
    # hay que escribirle es al que firma.
    ordenados = mod_cargos.elegir_mejor(list(candidatos.values()))[:limite]

    # ── Fase 4b: el perfil de LinkedIn de QUIEN YA ENCONTRAMOS ──────────────
    # MEDIDO (2026-09-04): Fintual y Xepelin se resolvieron por el sitio propio
    # -- Omar Larre y Sebastian Kreis, score 1.0 -- y quedaron con CERO canales:
    # esos sitios no publican email ni telefono, y como la persona no vino de un
    # perfil, tampoco habia LinkedIn. Encontrar al decisor y no tener por donde
    # escribirle vale lo mismo que no haberlo encontrado.
    #
    # Con el nombre en la mano la consulta es mucho mas precisa que la de la
    # fase 2: "Omar Larre" "Fintual" linkedin apunta a una persona concreta, no
    # a un cargo generico. Se hace SOLO para el mejor candidato y SOLO si le
    # falta el canal: uno o dos fetches, no uno por persona.
    if ordenados and not ordenados[0].perfil_linkedin:
        mejor = ordenados[0]
        consulta = f'"{mejor.nombre}" "{empresa}" linkedin'
        for prov in activos[: config.DECISOR_PROVEEDORES_POR_ANGULO]:
            if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
                break
            r = obtener(providers.construir_url(prov.nombre, consulta, ubi),
                        prov.nombre, salud, timeout=config.DECISOR_FETCH_TIMEOUT)
            fetches += 1
            if not r.sirve:
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            encontrado = False
            for it in items:
                url = it.get("url", "")
                if not linkedin_perfil.es_perfil(url) or url in visitadas:
                    continue
                visitadas.add(url)
                rp = obtener(url, "linkedin", salud,
                             timeout=config.DECISOR_FETCH_TIMEOUT)
                fetches += 1
                datos = linkedin_perfil.parsear_titulo(
                    linkedin_perfil.titulo_de(rp.html or ""))
                if not datos:
                    continue
                # Se exige que el perfil sea de ESTA persona y de ESTA empresa.
                # Sin las dos condiciones se le colgaria a nuestro decisor el
                # perfil de un homonimo, que es peor que no tener perfil.
                mismo = clave_nombre(datos["nombre"]) == clave_nombre(mejor.nombre)
                if mismo and linkedin_perfil.coincide_empresa(datos["empresa"], empresa):
                    mejor.perfil_linkedin = url
                    mejor.evidencia = mejor.evidencia + [
                        "perfil de LinkedIn confirmado: mismo nombre y misma empresa"]
                    encontrado = True
                    break
            if encontrado:
                break

    # ── Fase 5: buscar un email del dominio en la web abierta ───────────────
    # Cuando el sitio no publica ninguno -- que es lo normal en startups, con
    # formulario en vez de correo -- la unica muestra posible esta afuera: una
    # nota de prensa, un directorio, un PDF. Con UNA muestra se deduce la
    # convencion y toda persona de esa empresa sale gratis; sin ninguna, la
    # regla del repo prohibe construir nada.
    #
    # Se dispara solo si hace falta: si ya hay un email, otra busqueda no
    # agrega nada.
    if (dominio and ordenados
            and not mod_contacto.emails_del_dominio("\n".join(texto_sitio), dominio)
            and time.monotonic() - t0 < config.DECISOR_BUDGET_S):
        for prov in activos[: config.DECISOR_PROVEEDORES_POR_ANGULO]:
            if time.monotonic() - t0 > config.DECISOR_BUDGET_S:
                break
            url = providers.construir_url(prov.nombre, f'"@{dominio}"', ubi)
            r = obtener(url, prov.nombre, salud, timeout=config.DECISOR_FETCH_TIMEOUT)
            fetches += 1
            if not r.sirve:
                continue
            items, _ = extraction.extraer(r.page, r.html, prov.nombre)
            hallado = "\n".join((it.get("titulo", "") + " " + it.get("snippet", ""))
                                 for it in items)
            if mod_contacto.emails_del_dominio(hallado, dominio):
                texto_sitio.append(hallado)
                break

    # ── El contacto de cada decisor ─────────────────────────────────────────
    texto = "\n".join(texto_sitio)
    nombres_vistos = [c.nombre for c in candidatos.values()]
    emails = mod_contacto.emails_del_dominio(texto, dominio)
    pares = mod_contacto.emparejar(emails, nombres_vistos)
    for c in ordenados:
        c.contacto = mod_contacto.contacto_de(
            c.nombre, dominio, texto, ubi.pais or "CL", pares)

    diag = _diagnostico(plan, por_proveedor, salud, len(crudos), fetches,
                        len(visitadas), len(candidatos), t0, busco=True,
                        descartados_por_techo=descartados_por_techo)
    completo = diag["completo"]

    diag["fetches_fallidos"] = fallidos
    diag["presupuesto_agotado"] = presupuesto_agotado
    if not ordenados:
        # El motivo del vacio no es opcional: "esta empresa no publica a nadie"
        # y "los buscadores no nos dejaron mirar" se arreglan en lugares
        # distintos, y confundirlos ya costo una investigacion entera (F1/F4).
        #
        # `sin_acceso` es el caso que faltaba y que la medicion del 2026-09-04
        # dejo a la vista: cuatro empresas reportaron "no_publicado" con el
        # presupuesto agotado a los 25s. No es que no publiquen -- es que los
        # proveedores agotaron el tiempo sin devolver nada, y por timeout no
        # quedan marcados como bloqueados.
        if diag["proveedores_bloqueados"]:
            motivo = "providers_blocked"
        elif presupuesto_agotado or (fetches and fallidos >= max(1, fetches // 2)):
            motivo = "sin_acceso"
        elif not crudos and not visitadas:
            motivo = "sin_resultados"
        else:
            motivo = "no_publicado"
        diag["motivo_vacio"] = motivo

    log.info("decisor '%s' -> %d candidatos (%d crudos, %d fetches, %d paginas) en %dms%s",
             empresa, len(ordenados), len(crudos), fetches, len(visitadas), diag["ms"],
             " BLOQUEADO:" + ",".join(diag["proveedores_bloqueados"])
             if diag["proveedores_bloqueados"] else "")
    return {"candidatos": ordenados, "completo": completo, "diagnostico": diag}


def _diagnostico(plan, por_proveedor, salud, crudos, fetches, paginas, cands,
                 t0, busco: bool, descartados_por_techo: int = 0) -> dict:
    """El diagnostico en un solo lugar: los dos caminos de salida lo arman igual.

    Cuando se resolvio SIN buscar, `completo` es True aunque `proveedores_ok`
    este vacio -- no hubo nada bloqueado porque no se pidio nada.
    """
    bloqueados = salud.resumen()
    return {
        "angulos": [a.nombre for a in plan],
        "queries": [a.query for a in plan],
        "proveedores_ok": por_proveedor,
        "proveedores_bloqueados": bloqueados,
        "crudos": crudos,
        "fetches": fetches,
        "fetches_descartados_por_techo": descartados_por_techo,
        "paginas_visitadas": paginas,
        "candidatos": cands,
        "busco_en_internet": busco,
        "ms": int((time.monotonic() - t0) * 1000),
        "completo": (bool(por_proveedor) and not bloqueados) if busco else True,
    }
