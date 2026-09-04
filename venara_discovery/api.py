"""API HTTP. Mantiene el contrato que Venara ya consume.

RETROCOMPATIBILIDAD: los cuatro endpoints conservan su forma. Se AGREGAN campos
(nunca se quitan ni se renombran), porque el cliente lee por nombre y un campo
que desaparece lo rompe en silencio.

El cambio de comportamiento que si importa, y es deliberado: cuando los
proveedores estan bloqueados, la respuesta lo DICE. Antes devolvia 200 con
lista vacia y Venara concluia que el nicho no tenia empresas (F1/F4).
"""
from __future__ import annotations
import logging
import secrets
import urllib.request
from contextlib import asynccontextmanager

import anyio
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from . import config, decisor, linkedin, personas, pipeline, security
from .cache import CACHE
from .fetch import crear_sesion
from .normalize import normalizar_url

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        anyio.to_thread.current_default_thread_limiter().total_tokens = 100
    except Exception as e:
        log.warning("no se pudo ajustar el threadpool: %s", e)
    if not config.PROXY_URL:
        log.warning("PROXY_URL sin configurar: los buscadores bloquean IPs de datacenter "
                    "(medido: 0 resultados desde Render, ver FINDINGS.md F1)")
    if not config.API_KEY:
        log.warning("API_KEY sin configurar: endpoints SIN autenticacion")
    yield


app = FastAPI(title="Venara Discovery Engine", version=config.VERSION, lifespan=lifespan)


def require_api_key(x_api_key: str = Header(default="")):
    # Comparar BYTES, no str: compare_digest sobre str lanza TypeError con
    # cualquier caracter no-ASCII, y ese TypeError sale como 500. Eso convertia
    # al propio chequeo de auth en un vector de denegacion de servicio:
    # bastaba mandar "X-API-Key: cafe" con acento para tumbar el endpoint.
    if config.API_KEY and not secrets.compare_digest(
            x_api_key.encode("utf-8"), config.API_KEY.encode("utf-8")):
        raise HTTPException(status_code=401, detail="invalid or missing api key")


class CompanySearchRequest(BaseModel):
    """Acepta snake_case Y camelCase para el tope de resultados.

    El cliente de Venara manda `maxResults` (camelCase) y el modelo viejo solo
    declaraba `max_results`: Pydantic descartaba el campo desconocido y usaba
    siempre el default. Pedir 50 devolvia 25 (F8 en FINDINGS.md).

    Se aceptan los dos y gana el que venga, sin romper a ningun cliente.
    """
    query: str = Field(min_length=1, max_length=config.MAX_QUERY_LEN)
    location: str = Field(default="", max_length=config.MAX_LOCATION_LEN)
    max_results: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)
    maxResults: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)

    @property
    def limite(self) -> int:
        return self.maxResults or self.max_results or 25


class WebsiteRequest(BaseModel):
    url: str = Field(min_length=1, max_length=config.MAX_URL_LEN)


class PeopleSearchRequest(BaseModel):
    """Entrada del canal PERSONAS.

    `query` es el rubro (no una empresa) y `titles` los cargos que el usuario
    quiere alcanzar. Es la diferencia con LinkedInRequest, que parte de una
    empresa ya elegida: aca no hay empresa todavia -- se descubre junto con la
    persona.
    """
    query: str = Field(min_length=1, max_length=config.MAX_QUERY_LEN)
    titles: list[str] = Field(default_factory=list)
    location: str = Field(default="", max_length=config.MAX_LOCATION_LEN)
    max_results: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)
    maxResults: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)
    # La ronda viaja desde el motor de Venara: es lo que evita que la corrida
    # de las 16:00 pida exactamente las mismas queries que la de las 12:00.
    round: int = Field(default=0, ge=0, le=999)

    @property
    def limite(self) -> int:
        return self.maxResults or self.max_results or 25

    @property
    def cargos(self) -> list[str]:
        """Cargos saneados. El tope no es cosmetico: cada cargo extra abre su
        propia estrategia y multiplica los fetches contra MAX_FETCHES."""
        out: list[str] = []
        vistos = set()
        for t in self.titles:
            if not isinstance(t, str):
                continue
            limpio = " ".join(t.split())[: config.MAX_TITLE_LEN]
            clave = limpio.lower()
            if not limpio or clave in vistos:
                continue
            vistos.add(clave)
            out.append(limpio)
            if len(out) >= config.MAX_TITLES:
                break
        return out


class DecisionMakerRequest(BaseModel):
    """Entrada del resolutor de decisor.

    `domain` es opcional pero cambia el resultado de forma grande: con el, el
    primer angulo pasa a ser `site:<dominio>`, que es la fuente con menos ruido
    que existe. Sin el hay que preguntarle al indice por el nombre, y dos
    empresas pueden llamarse igual.
    """
    company: str = Field(default="", max_length=config.MAX_QUERY_LEN)
    domain: str = Field(default="", max_length=config.MAX_URL_LEN)
    location: str = Field(default="", max_length=config.MAX_LOCATION_LEN)
    titles: list[str] = Field(default_factory=list)
    max_results: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)
    maxResults: int | None = Field(default=None, ge=1, le=config.MAX_RESULTS_CAP)

    @property
    def limite(self) -> int:
        return self.maxResults or self.max_results or 5

    @property
    def cargos(self) -> list[str]:
        out: list[str] = []
        vistos = set()
        for t in self.titles:
            if not isinstance(t, str):
                continue
            limpio = " ".join(t.split())[: config.MAX_TITLE_LEN]
            if not limpio or limpio.lower() in vistos:
                continue
            vistos.add(limpio.lower())
            out.append(limpio)
            if len(out) >= config.MAX_TITLES:
                break
        return out


class LinkedInRequest(BaseModel):
    company: str = Field(default="", max_length=config.MAX_QUERY_LEN)
    location: str = Field(default="", max_length=config.MAX_LOCATION_LEN)
    fallback_name: str = Field(default="", max_length=config.MAX_QUERY_LEN)
    # AGREGADO, no obligatorio: sin dominio el resolutor no puede entrar al
    # sitio de la empresa, que es el unico camino que funciona cuando los
    # buscadores bloquean. Medido en produccion el 2026-09-03: con dominio,
    # Fintual se resuelve en 2,7s sin una sola busqueda; sin dominio, la misma
    # empresa devuelve NOT_FOUND por captcha de los proveedores.
    #
    # Es opcional a proposito: un cliente viejo que no lo manda sigue
    # funcionando exactamente igual que antes.
    domain: str = Field(default="", max_length=config.MAX_URL_LEN)


@app.get("/health")
def health():
    # Se mantienen status y version tal cual; lo demas es agregado.
    return {
        "status": "ok",
        "version": config.VERSION,
        "proxy": bool(config.PROXY_URL),
        "auth": bool(config.API_KEY),
        "cache": len(CACHE),
    }


@app.post("/search-linkedin-companies", dependencies=[Depends(require_api_key)])
def search_linkedin_companies(req: CompanySearchRequest):
    nicho = req.query.strip()
    ubicacion = req.location.strip()
    limite = req.limite

    clave = CACHE.clave(nicho, ubicacion)
    cacheado = CACHE.obtener(clave)
    if cacheado is not None:
        log.info("cache hit: %s / %s", nicho, ubicacion)
        recortado = cacheado[:limite]
        return {"results": recortado, "total": len(recortado), "cached": True}

    salida = pipeline.buscar(nicho, ubicacion, limite)
    diag = salida["diagnostico"]

    resultados = []
    for e in salida["empresas"]:
        resultados.append({
            # ── Campos historicos: el cliente lee estos nombres ──────────────
            "name": e.nombre,
            "website": e.website,
            "linkedin_url": e.linkedin_url,
            "source": ",".join(sorted(e.fuentes)) or "scrapling",
            "description": e.descripcion,
            # ── Agregados (no rompen a nadie) ───────────────────────────────
            "score": e.senales.get("score", {}).get("total"),
            "signals": e.senales.get("score", {}),
            "queries": sorted(e.queries),
        })

    # Solo se cachea una busqueda COMPLETA. Cachear un bloqueo dejaba ese nicho
    # muerto seis horas (F9).
    CACHE.guardar(clave, resultados, completo=salida["completo"])

    cuerpo = {
        "results": resultados,
        "total": len(resultados),
        "cached": False,
        # Lo que faltaba: distinguir "no hay empresas" de "no pudimos mirar".
        "complete": salida["completo"],
        "blocked_providers": diag["proveedores_bloqueados"],
        "diagnostics": diag,
    }
    if not salida["completo"] and not resultados:
        cuerpo["error"] = "providers_blocked"
        cuerpo["message"] = ("Los buscadores bloquearon las consultas; esto NO significa "
                             "que el nicho no tenga empresas. Revisar PROXY_URL.")
    return cuerpo


@app.post("/scrape-website", dependencies=[Depends(require_api_key)])
def scrape_website(req: WebsiteRequest):
    url = req.url.strip()
    if not url:
        return {"clean_text": "NO_CONTENT", "url": url}
    if not security.is_safe_public_url(url):
        log.warning("scraping rechazado: URL no permitida")
        return {"clean_text": "NO_CONTENT", "url": url, "method": "blocked"}

    from .website import limpiar_html
    try:
        with crear_sesion() as s:
            page = s.get(url, stealthy_headers=True, follow_redirects="safe",
                         timeout=config.FETCH_TIMEOUT)
        html = (page.html_content or "")[: config.MAX_HTML_BYTES]
        if len(html) > 100:
            return {"clean_text": limpiar_html(html), "url": url, "method": "fetcher_session"}
    except Exception as e:
        log.warning("fetcher_session fallo: %s", str(e)[:120])

    try:
        pedido = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with security.OPENER_SEGURO.open(pedido, timeout=config.FETCH_TIMEOUT) as resp:
            # Revalidar el destino FINAL: un 302 hacia 169.254.169.254 haria
            # inutil la validacion inicial.
            if not security.is_safe_public_url(resp.geturl()):
                raise ValueError("url final no permitida")
            html = security.leer_acotado(resp)
        if len(html) > 100:
            return {"clean_text": limpiar_html(html), "url": url, "method": "urllib"}
    except Exception as e:
        log.warning("urllib fallo: %s", str(e)[:120])

    return {"clean_text": "NO_CONTENT", "url": url, "method": "failed"}


@app.post("/search-linkedin", dependencies=[Depends(require_api_key)])
def search_linkedin(req: LinkedInRequest):
    """Busca a un decisor de la empresa. FORMA HISTORICA INTACTA.

    Por dentro ya no es la busqueda vieja contra `site:linkedin.com/in`, que
    tardaba 4m43s medidos en produccion para devolver NOT_FOUND: ahora llama al
    resolutor de decisor. El cliente de Venara no se entera --lee los mismos
    cuatro campos-- pero deja de recibir un vacio garantizado.

    `linkedin_url` se devuelve SIEMPRE vacio, y eso no es una regresion: los
    perfiles personales no estan en el indice publico (F7), asi que el campo
    nunca tuvo con que llenarse. Se conserva porque el cliente lo lee.

    MANDAR `domain` CAMBIA EL RESULTADO, y mucho: habilita el camino que entra
    al sitio de la empresa, el unico que no depende de que un buscador nos
    atienda. Sin el, desde una IP de datacenter la respuesta suele ser
    NOT_FOUND por captcha (F1).
    """
    empresa = req.company.strip()
    if not empresa or empresa in {"NO_COMPANY_FOUND", "NOT_FOUND"}:
        return {"person_name": "NOT_FOUND", "person_title": "",
                "linkedin_url": "", "source": "no_company"}

    salida = decisor.resolver(empresa, req.domain.strip(), req.location.strip(), None, 1)
    diag = salida["diagnostico"]
    if not salida["candidatos"]:
        return {"person_name": "NOT_FOUND", "person_title": "", "linkedin_url": "",
                "source": diag.get("motivo_vacio", "no_publicado"),
                "company_match_confidence": 0.0}

    mejor = salida["candidatos"][0]
    return {"person_name": mejor.nombre, "person_title": mejor.cargo,
            "linkedin_url": "", "source": mejor.proveedor,
            "company_match_confidence": round(mejor.score, 3)}


@app.post("/resolve-company-linkedin", dependencies=[Depends(require_api_key)])
def resolve_company_linkedin(req: LinkedInRequest):
    """Endpoint NUEVO: la pagina de LinkedIn de una empresa, con confianza.

    Separado de /search-linkedin a proposito: mezclar la URL de una empresa con
    la de una persona en el mismo endpoint es como se terminan guardando
    perfiles personales en el campo de empresa.
    """
    empresa = req.company.strip()
    if not empresa:
        return {"linkedin_company_url": "", "linkedin_company_name": "",
                "linkedin_company_confidence": 0.0, "source": "no_company"}
    return linkedin.resolver_empresa(empresa, req.location.strip())


@app.post("/search-people", dependencies=[Depends(require_api_key)])
def search_people(req: PeopleSearchRequest):
    """Canal PERSONAS: decisores del rubro, sin partir de una empresa.

    Es el complemento del canal de empresas, no su reemplazo. Aquel encuentra
    la empresa y despues busca a alguien adentro (y falla en el 83% de los
    sitios, que no publican equipo); este parte de paginas que YA nombran a un
    decisor -- equipo, prensa de nombramientos, expositores, gremios.

    Devuelve `complete=False` y un motivo cuando no pudo mirar. Un vacio sin
    motivo es lo que hacia concluir "el nicho no tiene decisores" cuando lo
    que habia era un captcha.
    """
    nicho = req.query.strip()
    cargos = req.cargos
    ubicacion = req.location.strip()

    # La ronda entra en la clave: dos rondas hacen queries distintas, asi que
    # compartir cache entre ellas serviria a la ronda 3 el resultado de la 1 y
    # anularia la rotacion entera.
    clave = CACHE.clave("personas|r%d|" % req.round + nicho + "|"
                        + ",".join(sorted(c.lower() for c in cargos)), ubicacion)
    cacheado = CACHE.obtener(clave)
    if cacheado is not None:
        return {"results": cacheado["results"][: req.limite],
                "pages": cacheado["pages"][: req.limite],
                "total": len(cacheado["results"][: req.limite]), "cached": True,
                "complete": True, "blocked_providers": {}}

    salida = personas.buscar(nicho, cargos, ubicacion, req.limite, req.round)
    diag = salida["diagnostico"]
    resultados = salida["personas"]
    paginas = salida["paginas"]

    # Se cachean las DOS salidas juntas: servir personas sin sus paginas haria
    # que un cache hit valiera mucho menos que la busqueda que lo lleno.
    CACHE.guardar(clave, {"results": resultados, "pages": paginas},
                  completo=salida["completo"])

    cuerpo = {
        "results": resultados,
        "pages": paginas,
        "total": len(resultados),
        "cached": False,
        "complete": salida["completo"],
        "blocked_providers": diag["proveedores_bloqueados"],
        "diagnostics": diag,
    }
    if not resultados and not paginas:
        # El motivo viaja al cliente para que pueda distinguir un rubro sin
        # decisores publicados de una corrida que no pudo mirar.
        cuerpo["reason"] = diag.get("motivo_vacio", "not_indexed")
        if diag["proveedores_bloqueados"]:
            cuerpo["error"] = "providers_blocked"
            cuerpo["message"] = ("Los buscadores bloquearon las consultas; esto NO significa "
                                 "que el rubro no tenga decisores publicados.")
    return cuerpo


@app.post("/find-decision-maker", dependencies=[Depends(require_api_key)])
def find_decision_maker(req: DecisionMakerRequest):
    """De un nombre de empresa a la persona que decide.

    Ataca el indice desde varios angulos --"<empresa> CEO", la pagina de equipo
    del propio sitio, la prensa de nombramientos, las entrevistas-- y ademas
    ENTRA a la pagina de equipo, porque esta medido (F21) que el snippet del
    buscador casi nunca nombra a nadie.

    Devuelve `found: false` con un `reason` cuando no encuentra: "no_publicado"
    y "providers_blocked" mandan a hacer cosas distintas.
    """
    empresa = req.company.strip()
    if not empresa or empresa in {"NO_COMPANY_FOUND", "NOT_FOUND"}:
        return {"found": False, "person": None, "alternatives": [],
                "reason": "no_company", "complete": False, "blocked_providers": {}}

    salida = decisor.resolver(empresa, req.domain.strip(), req.location.strip(),
                              req.cargos, req.limite)
    diag = salida["diagnostico"]
    candidatos = [c.a_dict() for c in salida["candidatos"]]

    cuerpo = {
        "found": bool(candidatos),
        "person": candidatos[0] if candidatos else None,
        # Las alternativas viajan: en una PyME el "gerente general" y el
        # "fundador" suelen ser dos personas distintas y ambas deciden.
        "alternatives": candidatos[1:],
        "company": empresa,
        "complete": salida["completo"],
        "blocked_providers": diag["proveedores_bloqueados"],
        "diagnostics": diag,
    }
    if not candidatos:
        cuerpo["reason"] = diag.get("motivo_vacio", "no_publicado")
        if diag["proveedores_bloqueados"]:
            cuerpo["error"] = "providers_blocked"
            cuerpo["message"] = ("Los buscadores bloquearon las consultas; esto NO significa "
                                 "que la empresa no publique a su decisor.")
    return cuerpo
