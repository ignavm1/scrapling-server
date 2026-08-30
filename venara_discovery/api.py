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

from . import config, linkedin, pipeline, security
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


class LinkedInRequest(BaseModel):
    company: str = Field(default="", max_length=config.MAX_QUERY_LEN)
    location: str = Field(default="", max_length=config.MAX_LOCATION_LEN)
    fallback_name: str = Field(default="", max_length=config.MAX_QUERY_LEN)


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
    """Busca a un decisor de la empresa.

    Contrato historico intacto. Ver linkedin.py para lo que esta medido sobre
    la disponibilidad real de perfiles personales en buscadores publicos.
    """
    empresa = req.company.strip()
    if not empresa or empresa in {"NO_COMPANY_FOUND", "NOT_FOUND"}:
        return {"person_name": "NOT_FOUND", "person_title": "",
                "linkedin_url": "", "source": "no_company"}
    return linkedin.buscar_persona(empresa, req.location.strip())


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
