"""Punto de entrada del servidor de descubrimiento de Venara.

El codigo vive en el paquete `venara_discovery/`. Este archivo se mantiene con
el mismo nombre a proposito: el Dockerfile lo ejecuta y cambiarlo romperia el
deploy sin avisar.

Por que se partio el modulo unico: el archivo de 445 lineas no se podia testear
por partes. La deduplicacion, el scoring y la deteccion de bloqueo solo eran
observables corriendo una busqueda real contra Google -- es decir, no eran
observables. Los modulos existen para poder verificarlos, no por estetica.
"""
from __future__ import annotations
import logging
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from venara_discovery.api import app          # noqa: E402  (tras configurar logging)
from venara_discovery import config           # noqa: E402

if __name__ == "__main__":
    logging.getLogger(__name__).info(
        "Venara Discovery Engine v%s - puerto 8765", config.VERSION)
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
