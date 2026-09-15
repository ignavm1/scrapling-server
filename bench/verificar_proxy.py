"""Verifica que el proxy configurado sirva de verdad, contra los proveedores reales.

Existe porque "el proxy esta puesto" y "el proxy funciona" son cosas distintas,
y hasta ahora la unica forma de saberlo era correr una busqueda entera y
adivinar por que dio cero.

    python bench/verificar_proxy.py

Codigos de salida, pensados para encadenar en un script o un hook:

    0   hay proxy Y al menos un proveedor mas responde que sin el
    1   cualquier otra cosa: sin PROXY_URL, proxy muerto, o no mejora nada

NO imprime la URL del proxy: suele llevar usuario y contrasena embebidos.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Se importan despues de tocar sys.path a proposito.
from venara_discovery import config  # noqa: E402

# Una query trivial por proveedor: no interesa el resultado, interesa si
# atienden. La URL es la misma que usa el pipeline real.
OBJETIVOS = {
    "duckduckgo": "https://html.duckduckgo.com/html/?q=test",
    "brave": "https://search.brave.com/search?q=test",
    "bing": "https://www.bing.com/search?q=test",
}

TIMEOUT_S = 20


def _enmascarar(url: str) -> str:
    """Deja ver el host del proxy sin exponer usuario ni contrasena."""
    try:
        resto = url.split("://", 1)[1]
        host = resto.rsplit("@", 1)[-1]
        return url.split("://", 1)[0] + "://***@" + host if "@" in resto else url
    except (IndexError, AttributeError):
        return "(url ilegible)"


def _probar(nombre: str, url: str, usar_proxy: bool) -> tuple[bool, str]:
    """Un intento contra un proveedor. Devuelve (atendio, detalle).

    "Atendio" es cualquier respuesta HTTP, incluido un 429: significa que
    llegamos. Lo que se mide aca es alcance de red, no exito de la busqueda;
    un 429 con proxy sigue siendo mejor que un timeout sin el.

    Pasa por `fetch.crear_sesion()` A PROPOSITO, en vez de armar la sesion
    aca. La primera version de este verificador se armaba la suya con
    `FetcherSession(proxy=...)` y por lo tanto reproducia el mismo no-op que
    venia a detectar: daba "igual" con un proxy muerto y el bug quedaba
    invisible. Un verificador que no ejecuta el camino de produccion no
    verifica nada.
    """
    from venara_discovery import fetch as mod_fetch

    guardado = config.PROXY_URL
    if not usar_proxy:
        mod_fetch.config.PROXY_URL = None
    t0 = time.monotonic()
    try:
        with mod_fetch.crear_sesion() as s:
            page = s.get(url, stealthy_headers=True, timeout=TIMEOUT_S)
        ms = int((time.monotonic() - t0) * 1000)
        return True, f"HTTP {getattr(page, 'status', None)} en {ms}ms"
    except Exception as e:  # noqa: BLE001 -- cualquier fallo de red cuenta igual
        ms = int((time.monotonic() - t0) * 1000)
        return False, f"{type(e).__name__} en {ms}ms"
    finally:
        mod_fetch.config.PROXY_URL = guardado


def main() -> int:
    if not config.PROXY_URL:
        print("FALLO: PROXY_URL no esta configurado.")
        print()
        print("Opciones, cualquiera sirve:")
        print("  1. Crear un archivo .env junto a este repo con la linea:")
        print("       PROXY_URL=http://usuario:clave@host:puerto")
        print("  2. Exportar PROXY_URL en el entorno antes de lanzar el server.")
        print()
        print("Despues: npx pm2 restart scrapling && python bench/verificar_proxy.py")
        return 1

    print(f"Proxy configurado: {_enmascarar(config.PROXY_URL)}")
    print(f"Probando {len(OBJETIVOS)} proveedores, con y sin proxy. Timeout {TIMEOUT_S}s.")
    print()

    sin_proxy: dict[str, bool] = {}
    con_proxy: dict[str, bool] = {}

    for nombre, url in OBJETIVOS.items():
        ok_sin, det_sin = _probar(nombre, url, usar_proxy=False)
        ok_con, det_con = _probar(nombre, url, usar_proxy=True)
        sin_proxy[nombre] = ok_sin
        con_proxy[nombre] = ok_con
        marca = "MEJORA" if (ok_con and not ok_sin) else ("pierde" if (ok_sin and not ok_con) else "igual")
        print(f"  {nombre:12} sin proxy: {det_sin:28} con proxy: {det_con:28} {marca}")

    n_sin = sum(sin_proxy.values())
    n_con = sum(con_proxy.values())
    print()
    print(f"Proveedores que atienden: {n_sin} sin proxy, {n_con} con proxy.")

    perdidos = [n for n in OBJETIVOS if sin_proxy[n] and not con_proxy[n]]
    if perdidos:
        print(f"ATENCION: el proxy ROMPIO {', '.join(perdidos)}, que sin el funcionaban.")

    if n_con > n_sin:
        print("RESULTADO: el proxy sirve. Vale la pena dejarlo puesto.")
        return 0

    if n_con == n_sin:
        print("RESULTADO: el proxy no cambia nada. No lo pagues por esto.")
    else:
        print("RESULTADO: el proxy EMPEORA el alcance. Revisar la URL o el proveedor.")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrumpido.")
        sys.exit(1)
