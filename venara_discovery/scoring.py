"""Scoring interpretable de empresas.

"Interpretable" significa que ante un resultado malo se puede responder POR QUE
quedo arriba, mirando las senales y no el codigo. Cada componente esta en 0..1 y
se pesa explicitamente; el total nunca es un numero magico.

El orden de prioridad viene del pedido: calidad de empresa > relevancia >
website > deduplicacion. Los pesos lo reflejan.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict

from . import filtering
from .entity import Empresa
from .location import Ubicacion, confianza as confianza_ubicacion
from .normalize import dominio_registrable

# Pesos. Suman 1.0 en la parte positiva; las penalizaciones restan aparte.
W_RELEVANCIA = 0.34
W_WEBSITE = 0.26
W_IDENTIDAD = 0.18
W_UBICACION = 0.12
W_FUENTES = 0.10


@dataclass(frozen=True)
class Senales:
    """Las senales internas que pide el requisito 5. Viajan con la empresa
    para poder depurar el ranking sin re-ejecutar la busqueda."""
    relevance_score: float
    website_confidence: float
    identity_confidence: float
    location_confidence: float
    location_level: str
    source_count: int
    penalizacion: float
    total: float

    def dict(self) -> dict:
        return {k: (round(v, 3) if isinstance(v, float) else v)
                for k, v in asdict(self).items()}


def confianza_website(e: Empresa) -> float:
    """Cuanta confianza hay de que `website` sea el sitio oficial de ESTA empresa.

    Sin website la empresa no se descarta (el cliente si lo hace, pero esa es su
    decision): se puntua 0 en este eje y puede seguir compitiendo por el resto.
    """
    if not e.website:
        return 0.0
    dom = dominio_registrable(e.website)
    if not dom:
        return 0.0
    base = 0.55
    # Que el dominio se parezca al nombre es la mejor prueba barata de que el
    # sitio pertenece a la empresa y no es un tercero que la menciona.
    from .normalize import clave_nombre
    kn = clave_nombre(e.nombre)
    kd = clave_nombre(dom.split(".")[0])
    if kn and kd:
        if kn == kd:
            base += 0.35
        elif kn in kd or kd in kn:
            base += 0.22
    if e.senales.get("website_validado"):
        base += 0.10
    if e.senales.get("website_caido"):
        base -= 0.30       # existe evidencia de la empresa, pero el sitio no responde
    return max(0.0, min(1.0, base))


def confianza_identidad(e: Empresa) -> float:
    """Que tan seguros estamos de que esto es una empresa identificada, y no
    un titulo suelto."""
    v = 0.25
    if e.nombre and len(e.nombre) >= 3:
        v += 0.25
    if e.linkedin_url:
        v += 0.25          # una pagina de empresa en LinkedIn es identidad fuerte
    if e.website:
        v += 0.15
    if e.descripcion:
        v += 0.10
    return min(1.0, v)


def puntuar(e: Empresa, query: str, ubi: Ubicacion) -> Senales:
    # El texto de la fuente, no el nombre limpio: "Limadigital" no comparte
    # tokens con "agencia de marketing digital", pero el titulo del que salio
    # si. Puntuar sobre el nombre daria relevancia 0 a empresas relevantes.
    evidencia = " ".join([e.texto_fuente or "", e.nombre or "",
                          e.descripcion or "", e.ubicacion_texto or ""])
    texto = evidencia

    rel = filtering.relevancia(query, evidencia, e.descripcion,
                               e.website or e.linkedin_url)
    web = confianza_website(e)
    ident = confianza_identidad(e)
    loc, nivel = confianza_ubicacion(ubi, texto, e.website or "")

    # Aparecer en varias fuentes independientes es evidencia real; el beneficio
    # se satura en 3 para que 6 apariciones no le ganen a una empresa mejor.
    nf = len(e.fuentes)
    fuentes = min(1.0, (nf - 1) / 2.0) if nf > 1 else 0.0

    pen = 0.0
    motivo = filtering.motivo_descarte(e.website or "", e.nombre) if e.website else ""
    if motivo:
        pen += 0.55        # llego hasta aca pero huele a directorio/blog
    if not e.website and not e.linkedin_url:
        pen += 0.30
    if rel < 0.2:
        # Defensa contra el envenenamiento de Bing (F6): un resultado que no
        # comparte casi nada con la query no puede rankear alto por mucho que
        # su ficha se vea completa.
        pen += 0.45

    total = (W_RELEVANCIA * rel + W_WEBSITE * web + W_IDENTIDAD * ident
             + W_UBICACION * loc + W_FUENTES * fuentes) - pen
    return Senales(rel, web, ident, loc, nivel, nf, pen, max(0.0, min(1.0, total)))


# Piso de aceptacion. Preferimos 18 empresas buenas a 25 con basura, asi que
# esto corta de verdad en vez de rellenar hasta max_results.
UMBRAL_ACEPTACION = 0.34


def rankear(empresas: list[Empresa], query: str, ubi: Ubicacion,
            limite: int) -> tuple[list[Empresa], dict]:
    """Puntua, filtra bajo umbral, diversifica y corta.

    Devuelve (empresas, estadisticas) — las estadisticas son lo que permite
    responder "por que se descarto tal resultado" sin re-ejecutar nada.
    """
    puntuadas = []
    for e in empresas:
        s = puntuar(e, query, ubi)
        e.senales["score"] = s.dict()
        puntuadas.append((s.total, e))

    bajo_umbral = sum(1 for t, _ in puntuadas if t < UMBRAL_ACEPTACION)
    aceptadas = [(t, e) for t, e in puntuadas if t >= UMBRAL_ACEPTACION]
    aceptadas.sort(key=lambda p: p[0], reverse=True)

    # Diversidad: como mucho 2 resultados por dominio registrable. Sin esto una
    # empresa con muchas subpaginas indexadas ocupa la mitad de la lista.
    vistos: dict[str, int] = {}
    salida: list[Empresa] = []
    pospuestas: list[Empresa] = []
    for _, e in aceptadas:
        d = e.dominio or ("li:" + e.clave_linkedin)
        if vistos.get(d, 0) >= 2:
            pospuestas.append(e)
            continue
        vistos[d] = vistos.get(d, 0) + 1
        salida.append(e)
        if len(salida) >= limite:
            break

    # Si la diversidad dejo la lista corta, se completa con las pospuestas
    # antes que devolver menos de lo que hay.
    if len(salida) < limite:
        salida.extend(pospuestas[:limite - len(salida)])

    stats = {
        "evaluadas": len(puntuadas),
        "bajo_umbral": bajo_umbral,
        "aceptadas": len(aceptadas),
        "devueltas": len(salida),
        "umbral": UMBRAL_ACEPTACION,
    }
    return salida, stats
