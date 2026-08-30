"""El pipeline: discover -> normalize -> resolve -> score -> deduplicate -> rank.

Un principio gobierna todo el archivo: **una busqueda que no pudo mirar no es
una busqueda sin resultados**. Si los proveedores estuvieron bloqueados, eso
sale en la respuesta y NO se cachea. Ese era el fallo mas caro del servidor
viejo (F1 y F4 en FINDINGS.md): devolvia 200 con lista vacia y Venara concluia
que el nicho no tenia empresas.
"""
from __future__ import annotations
import concurrent.futures
import logging
import time

from . import config, extraction, filtering, providers, queries as qmod, scoring
from .entity import Empresa, Resolutor
from .fetch import SaludProveedores, obtener
from .location import interpretar

log = logging.getLogger(__name__)


def _linkedin_de(url: str) -> str:
    """URL de PAGINA DE EMPRESA de LinkedIn, o "".

    Rechaza perfiles personales, posts y ofertas: /company/ es lo unico que
    identifica a una empresa. Mezclar una URL de persona con una de empresa es
    justo lo que el requisito 30 prohibe.
    """
    u = (url or "").lower()
    if "linkedin.com/company/" not in u:
        return ""
    for basura in ("/posts/", "/jobs/", "/pulse/", "/feed/", "/showcase/"):
        if basura in u:
            return ""
    slug = u.split("linkedin.com/company/", 1)[1].split("?")[0].split("#")[0].strip("/")
    if not slug or "/" in slug.rstrip("/"):
        # /company/acme/about o /company/acme/people no son la pagina raiz
        slug = slug.split("/")[0]
    return "https://www.linkedin.com/company/" + slug if slug else ""


def _a_empresa(item: dict, estrategia, proveedor: str, contexto: str = "") -> Empresa | None:
    """Convierte un resultado crudo en una observacion de empresa, o None."""
    url = item.get("url") or ""
    titulo = item.get("titulo") or ""
    snippet = item.get("snippet") or ""

    li = _linkedin_de(url)
    if li:
        from .normalize import limpiar_titulo
        nombre = limpiar_titulo(titulo)
        if not nombre or len(nombre) < 2:
            nombre = li.rsplit("/", 1)[-1].replace("-", " ").title()
        return Empresa(nombre=nombre, linkedin_url=li, descripcion=snippet,
                       texto_fuente=(titulo + " " + snippet).strip()[:400],
                       fuentes={proveedor}, queries={estrategia.nombre})

    motivo = filtering.motivo_descarte(url, titulo)
    if motivo:
        return None

    from .normalize import mejor_nombre
    nombre = mejor_nombre(titulo, url, contexto)
    if not nombre or len(nombre) < 2:
        return None
    return Empresa(nombre=nombre, website=url, descripcion=snippet,
                   texto_fuente=(titulo + " " + snippet).strip()[:400],
                   fuentes={proveedor}, queries={estrategia.nombre})


def buscar(nicho: str, ubicacion: str, limite: int) -> dict:
    """Ejecuta una busqueda completa y devuelve resultados + diagnostico."""
    t0 = time.monotonic()
    ubi = interpretar(ubicacion)
    plan = qmod.construir(nicho, ubi)
    activos = providers.activos()
    salud = SaludProveedores()

    # Un trabajo = (estrategia, proveedor). Se ordena por prioridad de ambos
    # para que, si el presupuesto se agota, lo que quede sin hacer sea lo
    # menos valioso.
    trabajos = []
    for est in plan:
        for prov in activos:
            trabajos.append((est, prov))
    trabajos.sort(key=lambda t: (t[0].prioridad, t[1].prioridad))

    crudos: list[tuple] = []
    por_proveedor: dict[str, int] = {}
    descartes: dict[str, int] = {}
    metodos: dict[str, int] = {}

    def tarea(est, prov):
        url = providers.construir_url(prov.nombre, est.query, ubi)
        r = obtener(url, prov.nombre, salud)
        return est, prov, r

    limite_hilos = min(config.MAX_CONCURRENCY, max(1, len(trabajos)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=limite_hilos) as ex:
        futuros = [ex.submit(tarea, e, p) for e, p in trabajos]
        for fut in concurrent.futures.as_completed(futuros):
            # Presupuesto total: el cliente de Venara corta a los 45s. Seguir
            # despues de eso gasta proxy para nadie.
            if time.monotonic() - t0 > config.SEARCH_BUDGET_S:
                log.warning("presupuesto agotado, se corta la busqueda")
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
                descartes[prov.nombre + ":" + motivo] = descartes.get(prov.nombre + ":" + motivo, 0) + 1
                continue
            items, metodo = extraction.extraer(r.page, r.html, prov.nombre)
            metodos[metodo] = metodos.get(metodo, 0) + 1
            por_proveedor[prov.nombre] = por_proveedor.get(prov.nombre, 0) + len(items)
            for it in items:
                crudos.append((it, est, prov.nombre))

    # ── Normalizar y resolver identidad ──────────────────────────────────────
    resolutor = Resolutor()
    filtrados = 0
    # Contexto = lo que se pidio. Un titulo que solo repite esto no es nombre.
    contexto = nicho + " " + (ubi.ciudad or ubi.texto or "") + " " + (ubi.pais_nombre or "")
    for it, est, prov in crudos:
        e = _a_empresa(it, est, prov, contexto)
        if e is None:
            filtrados += 1
            motivo = filtering.motivo_descarte(it.get("url", ""), it.get("titulo", ""))
            if motivo:
                descartes["filtro:" + motivo] = descartes.get("filtro:" + motivo, 0) + 1
            continue
        resolutor.agregar(e)

    unicas = resolutor.empresas()
    duplicados = max(0, (len(crudos) - filtrados) - len(unicas))

    # Una empresa vista solo en LinkedIn no se descarta por no traer website:
    # se intenta encontrarlo. El cliente de Venara descarta toda empresa sin
    # website, asi que cada una recuperada aca es un lead que antes se perdia.
    from .website import resolver_para
    websites_resueltos = 0
    if time.monotonic() - t0 < config.SEARCH_BUDGET_S * 0.7:
        try:
            websites_resueltos = resolver_para(unicas, ubi)
        except Exception as e:
            log.warning("resolucion de websites fallo: %s", str(e)[:100])

    # ── Puntuar, filtrar por umbral, diversificar y cortar ───────────────────
    rankeadas, stats_rank = scoring.rankear(unicas, nicho, ubi, limite)

    bloqueados = salud.resumen()
    # "Completo" = al menos un proveedor respondio bien Y ninguno quedo
    # bloqueado. Solo un resultado completo se cachea.
    hubo_respuesta = bool(por_proveedor)
    completo = hubo_respuesta and not bloqueados

    diag = {
        "proveedores_ok": por_proveedor,
        "proveedores_bloqueados": bloqueados,
        "metodos_extraccion": metodos,
        "crudos": len(crudos),
        "filtrados": filtrados,
        "duplicados_fusionados": duplicados,
        "websites_resueltos": websites_resueltos,
        "ranking": stats_rank,
        "descartes": descartes,
        "estrategias": [e.nombre for e in plan],
        "ubicacion": {
            "texto": ubi.texto, "ciudad": ubi.ciudad, "pais": ubi.pais,
            "mercado": ubi.mercado, "reconocida": ubi.reconocida,
        },
        "ms": int((time.monotonic() - t0) * 1000),
        "completo": completo,
    }
    log.info("busqueda '%s' %s -> %d empresas (%d crudos, %d filtrados, %d dup) en %dms%s",
             nicho, ubicacion, len(rankeadas), len(crudos), filtrados, duplicados,
             diag["ms"], " BLOQUEADO:" + ",".join(bloqueados) if bloqueados else "")
    return {"empresas": rankeadas, "diagnostico": diag, "completo": completo}
