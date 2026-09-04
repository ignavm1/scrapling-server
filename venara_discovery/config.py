"""Configuracion central. Un solo lugar donde mirar que se puede tocar sin leer codigo."""
from __future__ import annotations
import os

VERSION = "6.0.0"

# ── Red ──────────────────────────────────────────────────────────────────────
PROXY_URL = os.environ.get("PROXY_URL") or None
API_KEY = os.environ.get("API_KEY") or None

FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "15"))
# Presupuesto TOTAL de una busqueda. El cliente de Venara corta a los 45s; sin
# este tope el server sigue trabajando (y gastando proxy) para nadie -- F12.
SEARCH_BUDGET_S = int(os.environ.get("SEARCH_BUDGET_S", "38"))
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "6"))

# Espera entre requests al MISMO proveedor. Medido: DuckDuckGo devuelve captcha
# tras pocas queries con operador `site:` disparadas seguidas. Sin este freno el
# sistema se autobloquea y despues reporta "el nicho no tiene resultados".
PROVIDER_COOLDOWN_S = float(os.environ.get("PROVIDER_COOLDOWN_S", "1.2"))

# Techo de fetches por busqueda. Con 5 estrategias y 4 proveedores el producto
# da 20, y dispararlos todos hace dos danos a la vez: revienta el presupuesto de
# tiempo y provoca el captcha que despues nos deja sin ninguna fuente.
#
# Sumar proveedores sirve para COBERTURA (que uno bloqueado no mate la
# busqueda), no para multiplicar el gasto. Los trabajos se ordenan por valor y
# se corta aca.
MAX_FETCHES = int(os.environ.get("MAX_FETCHES", "12"))

# ── Cache ────────────────────────────────────────────────────────────────────
CACHE_TTL_S = int(os.environ.get("CACHE_TTL_S", str(6 * 60 * 60)))
CACHE_MAX = int(os.environ.get("CACHE_MAX", "500"))

# ── Limites de request (agotamiento de recursos) ─────────────────────────────
MAX_QUERY_LEN = 200
MAX_LOCATION_LEN = 120
MAX_RESULTS_CAP = 60
# Cargos del canal personas: largo de cada uno y cuantos se aceptan. El tope de
# cantidad protege MAX_FETCHES -- cada cargo extra abre su propia estrategia.
MAX_TITLE_LEN = 60
MAX_TITLES = 6

# ── Resolutor de decisor (empresa -> persona) ────────────────────────────────
# Presupuesto PROPIO, mas chico que el de una busqueda de nicho, y no es un
# detalle de tuning: resolver el decisor se llama UNA VEZ POR EMPRESA, asi que
# su costo se multiplica por el tamano del lote. `buscar_persona()` no tenia
# ningun tope y una sola consulta tardo 4m43s medidos contra produccion.
DECISOR_MAX_FETCHES = int(os.environ.get("DECISOR_MAX_FETCHES", "8"))
# Paginas del sitio de la empresa que se visitan para leer el equipo. Cada una
# es un fetch completo, y la tercera casi nunca aporta lo que no dieron las dos
# primeras.
DECISOR_MAX_PAGINAS = int(os.environ.get("DECISOR_MAX_PAGINAS", "3"))
# Presupuesto de tiempo. Mas corto que SEARCH_BUDGET_S por la misma razon.
DECISOR_BUDGET_S = int(os.environ.get("DECISOR_BUDGET_S", "25"))
# Timeout por fetch del resolutor, mas corto que FETCH_TIMEOUT. Scrapling
# reintenta 3 veces por su cuenta, asi que 15s por intento son 45s por fetch --
# mas que el presupuesto ENTERO de la consulta. Medido: una corrida con los
# buscadores colgados tardaba 48s con el presupuesto puesto en 25.
DECISOR_FETCH_TIMEOUT = int(os.environ.get("DECISOR_FETCH_TIMEOUT", "6"))
MAX_URL_LEN = 2048
# Techo de bytes por respuesta de scraping. Sin el, un servidor hostil puede
# mandar un stream infinito y llenar la memoria del contenedor.
MAX_HTML_BYTES = int(os.environ.get("MAX_HTML_BYTES", str(3 * 1024 * 1024)))

# ── Proveedores ──────────────────────────────────────────────────────────────
# Google esta APAGADO por defecto y no es un descuido.
#
# Medido el 2026-08-30 desde IP residencial (sin bloqueo de por medio):
# devuelve 92KB de HTML con 1 solo <a href>, 0 clases CSS y un <noscript> que
# redirige a /httpservice/retry/enablejs. Es un bootstrap de JavaScript: los
# resultados no vienen renderizados en el HTML. `div.g a` y `div.tF2Cxc a`
# extraen CERO, y ningun selector puede arreglar eso porque el contenido no
# esta ahi.
#
# Dejarlo activo gastaba UN TERCIO del presupuesto de requests de cada busqueda
# para no traer nada. Se puede reactivar con ENABLE_GOOGLE=1 si algun dia
# vuelve a renderizar en servidor.
ENABLE_GOOGLE = os.environ.get("ENABLE_GOOGLE", "0") == "1"
