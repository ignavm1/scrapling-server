"""Cache TTL en memoria, con una regla que el servidor viejo no tenia:
NUNCA se cachea un resultado que salio de una busqueda bloqueada.

Antes (F9 en FINDINGS.md) una sola busqueda bloqueada guardaba `[]` con TTL de
6 horas. Ese (nicho, ubicacion) quedaba muerto seis horas, y como la respuesta
era HTTP 200 nadie se enteraba. La cache convertia un fallo transitorio en uno
persistente.

Tampoco se usa Redis: para un unico contenedor en Render, un dict con TTL y
tope alcanza, y una dependencia mas es una cosa mas que puede fallar en el
arranque. Si algun dia hay varias instancias, este modulo es el unico que hay
que cambiar.
"""
from __future__ import annotations
import threading
import time

from . import config


class CacheBusqueda:
    def __init__(self, ttl: int | None = None, maximo: int | None = None) -> None:
        self._ttl = ttl if ttl is not None else config.CACHE_TTL_S
        self._max = maximo if maximo is not None else config.CACHE_MAX
        self._datos: dict[tuple, tuple[float, object]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def clave(nicho: str, ubicacion: str) -> tuple:
        """La clave NO incluye max_results.

        Pedir 20 y pedir 25 es el mismo trabajo de scraping; incluirlo hacia
        dos entradas distintas y duplicaba el gasto de proxy (F9). El recorte
        se aplica al servir, no al guardar.
        """
        return ((nicho or "").strip().lower(), (ubicacion or "").strip().lower())

    def obtener(self, clave: tuple):
        with self._lock:
            v = self._datos.get(clave)
            if not v:
                return None
            ts, datos = v
            if time.time() - ts > self._ttl:
                self._datos.pop(clave, None)
                return None
            return datos

    def guardar(self, clave: tuple, datos, completo: bool) -> None:
        """`completo=False` NO guarda nada.

        Es el corazon del arreglo: un resultado parcial o bloqueado se
        descarta en vez de fijarse por 6 horas. Preferimos re-scrapear a
        servir un vacio falso.
        """
        if not completo:
            return
        with self._lock:
            if len(self._datos) >= self._max:
                # Se descarta el 20% mas viejo. Sin tope, la cache crece hasta
                # el OOM del contenedor.
                viejos = sorted(self._datos, key=lambda k: self._datos[k][0])
                for k in viejos[: max(1, self._max // 5)]:
                    self._datos.pop(k, None)
            self._datos[clave] = (time.time(), datos)

    def limpiar(self) -> None:
        with self._lock:
            self._datos.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._datos)


CACHE = CacheBusqueda()
