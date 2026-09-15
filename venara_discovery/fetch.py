"""Capa de red: sesion, cooldown por proveedor, reintentos y salud.

Lo que hace distinta a esta capa de un `requests.get` suelto:

  - Cooldown POR PROVEEDOR. Medido: DuckDuckGo pasa a captcha tras pocas
    queries con operador `site:` disparadas seguidas (F4). Nueve fetches
    simultaneos contra tres motores es la forma mas rapida de autobloquearse y
    despues reportar "el nicho no tiene resultados".

  - Salud del proveedor. Si un motor ya devolvio captcha en esta busqueda, no
    se le insiste: se gasta el presupuesto en los que responden.

  - Nunca devuelve "vacio" sin decir por que. Un fallo tiene motivo, y ese
    motivo llega hasta la respuesta de la API.
"""
from __future__ import annotations
import logging
import threading
import time
from dataclasses import dataclass, field

from scrapling.fetchers import FetcherSession

from . import blocking, config

log = logging.getLogger(__name__)


def crear_sesion():
    if config.PROXY_URL:
        return FetcherSession(impersonate="chrome", proxy=config.PROXY_URL)
    return FetcherSession(impersonate="chrome")


@dataclass
class Respuesta:
    proveedor: str
    url: str
    html: str = ""
    status: int | None = None
    page: object = None
    veredicto: blocking.Veredicto | None = None
    error: str = ""
    ms: int = 0

    @property
    def sirve(self) -> bool:
        return bool(self.html) and self.veredicto is not None and not self.veredicto.bloqueado


class SaludProveedores:
    """Estado de los proveedores durante UNA busqueda.

    Es por busqueda y no global a proposito: un bloqueo momentaneo no debe
    dejar un motor apagado para todos los clientes durante horas.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ultimo: dict[str, float] = {}
        self._bloqueados: dict[str, str] = {}
        # Proveedores que no CONTESTAN, que no es lo mismo que bloqueados. Se
        # cuentan aparte a proposito: "nos rechazaron" y "no pudimos llegar" se
        # arreglan en lugares distintos, y mezclarlos es el fallo mas caro que
        # documenta este repo (F1/F4).
        self._timeouts: dict[str, int] = {}
        self._caidos: dict[str, str] = {}

    def esta_bloqueado(self, proveedor: str) -> str:
        with self._lock:
            return self._bloqueados.get(proveedor, "")

    def esta_caido(self, proveedor: str) -> str:
        with self._lock:
            return self._caidos.get(proveedor, "")

    def registrar_timeout(self, proveedor: str, motivo: str) -> bool:
        """Cuenta un fetch que no llego, y corta el proveedor si ya van varios.

        Medido el 2026-09-15: duckduckgo no conectaba desde este host y
        Scrapling reintenta 3 veces por fetch. Con DECISOR_FETCH_TIMEOUT=6 eso
        son ~18s de los 25 del presupuesto gastados en UN proveedor muerto, y
        en CADA angulo. El resultado era que casi todas las empresas terminaban
        en `sin_acceso` sin que nadie hubiera mirado de verdad.

        Devuelve True si este timeout dejo al proveedor fuera de la busqueda.
        """
        with self._lock:
            if proveedor in self._caidos:
                return False
            n = self._timeouts.get(proveedor, 0) + 1
            self._timeouts[proveedor] = n
            if n < config.MAX_TIMEOUTS_PROVEEDOR:
                return False
            self._caidos[proveedor] = motivo
        log.warning("proveedor %s caido tras %d timeouts: %s", proveedor, n, motivo)
        return True

    def caidos(self) -> dict:
        with self._lock:
            return dict(self._caidos)

    def marcar_bloqueado(self, proveedor: str, motivo: str) -> None:
        with self._lock:
            self._bloqueados[proveedor] = motivo
        log.warning("proveedor %s bloqueado: %s", proveedor, motivo)

    def esperar_turno(self, proveedor: str) -> None:
        """Serializa los pedidos al MISMO proveedor con un minimo de separacion.

        Proveedores distintos no se estorban: el paralelismo real esta entre
        motores, no dentro de uno.
        """
        with self._lock:
            ahora = time.monotonic()
            ultimo = self._ultimo.get(proveedor, 0.0)
            espera = max(0.0, config.PROVIDER_COOLDOWN_S - (ahora - ultimo))
            self._ultimo[proveedor] = ahora + espera
        if espera > 0:
            time.sleep(espera)

    def resumen(self) -> dict:
        with self._lock:
            return dict(self._bloqueados)


def obtener(url: str, proveedor: str, salud: SaludProveedores,
            timeout: int | None = None) -> Respuesta:
    """Un fetch, con analisis de bloqueo incluido."""
    motivo = salud.esta_bloqueado(proveedor)
    if motivo:
        return Respuesta(proveedor, url, error="omitido:" + motivo)
    # Un proveedor que ya no contesta no se vuelve a intentar en esta busqueda:
    # cada reintento cuesta el timeout completo por tres, y ese tiempo le falta
    # a los proveedores que SI atienden.
    caido = salud.esta_caido(proveedor)
    if caido:
        return Respuesta(proveedor, url, error="omitido:caido:" + caido)

    salud.esperar_turno(proveedor)
    t0 = time.monotonic()
    try:
        with crear_sesion() as s:
            page = s.get(url, stealthy_headers=True,
                         timeout=timeout or config.FETCH_TIMEOUT)
        html = page.html_content or ""
        status = getattr(page, "status", None)
        v = blocking.analizar(html, status)
        r = Respuesta(proveedor, url, html, status, page, v,
                      ms=int((time.monotonic() - t0) * 1000))
        if v.bloqueado:
            # Un captcha condena al proveedor para el resto de esta busqueda;
            # "sin resultados extraibles" puede ser solo esta query.
            if v.motivo in ("captcha", "requiere-javascript") or str(v.motivo).startswith("status-"):
                salud.marcar_bloqueado(proveedor, v.motivo)
        return r
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        log.warning("fetch %s fallo en %dms: %s", proveedor, ms, str(e)[:120])
        # Un fallo de conexion no es un rechazo del proveedor, pero si se
        # repite hay que dejar de gastarle presupuesto. El corte lo decide
        # SaludProveedores, que lo cuenta aparte de los bloqueos.
        if proveedor != "sitio":
            salud.registrar_timeout(proveedor, type(e).__name__)
        return Respuesta(proveedor, url, error=type(e).__name__, ms=ms)
