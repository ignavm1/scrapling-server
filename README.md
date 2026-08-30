# Venara — Company Discovery Engine

Convierte `nicho + ubicacion` en empresas reales con website, listas para
enriquecer y convertir en leads.

```
discover -> normalize -> resolve -> score -> deduplicate -> rank -> return
```

## Lo primero que hay que saber

**Una busqueda que no pudo mirar NO es una busqueda sin resultados.**

El servidor anterior devolvia `HTTP 200 {"results": [], "total": 0}` cuando los
buscadores lo bloqueaban, y Venara concluia que el nicho no tenia empresas.
Estaba pasando en produccion para TODA busqueda (ver [FINDINGS.md](FINDINGS.md)).

Ahora la respuesta lo dice:

```json
{ "results": [], "total": 0, "complete": false,
  "error": "providers_blocked",
  "blocked_providers": {"duckduckgo": "captcha"} }
```

`complete: false` tambien impide que ese vacio se cachee 6 horas.

## Modulos

| archivo | responsabilidad |
|---|---|
| `blocking.py` | captcha, shell de JS, bloqueo silencioso. **El mas importante.** |
| `providers.py` | Bing, DuckDuckGo, Google (apagado y por que) |
| `extraction.py` | selectores CSS + fallback por regex, decodificacion de redirects |
| `normalize.py` | URL/dominio/nombre canonicos |
| `filtering.py` | falsos positivos + **validacion de relevancia** |
| `location.py` | ciudad/pais -> mercado del buscador, y confianza por niveles |
| `entity.py` | entity resolution (union-find + reconciliacion por nombre) |
| `scoring.py` | score interpretable con senales explicitas |
| `website.py` | resolucion del website oficial |
| `linkedin.py` | pagina de empresa; personas (ver limitacion) |
| `cache.py` | TTL, tope, y **nunca cachea un bloqueo** |
| `pipeline.py` | orquestacion + diagnostico |
| `api.py` | HTTP, retrocompatible con el cliente de Venara |

## Verificar

```bash
.venv/bin/python bench/run_suite.py       # suite completa, sin tests desactivados
.venv/bin/python bench/benchmark.py       # metricas antes vs despues
.venv/bin/python bench/check_docker.py    # Dockerfile + entrypoint
.venv/bin/python bench/check_findings.py  # los hallazgos siguen citados
```

Contrato de completitud en [GATES.md](GATES.md):

```bash
node <unlazy>/scripts/gate-check.mjs --approve --reverify GATES.md
```

## Configuracion

| variable | por defecto | para que |
|---|---|---|
| `PROXY_URL` | — | **Critica en produccion.** Sin proxy residencial los buscadores bloquean la IP de datacenter y no sale ninguna empresa. |
| `API_KEY` | — | Autenticacion. Sin ella los endpoints quedan abiertos. |
| `ENABLE_GOOGLE` | `0` | Google sirve un shell de JS sin resultados; encenderlo gasta requests para nada. |
| `SEARCH_BUDGET_S` | `38` | Techo total. El cliente corta a los 45s. |
| `PROVIDER_COOLDOWN_S` | `1.2` | Espera entre pedidos al mismo motor. Bajarlo provoca captchas. |
| `CACHE_TTL_S` | `21600` | 6 horas. |

## Limitaciones conocidas

- **Los perfiles personales de LinkedIn no estan en el indice publico.** Medido
  con control positivo (F7 y G16): el mismo motor y operador devuelve paginas de
  empresa y cero perfiles. `/search-linkedin` intenta igual y devuelve
  `NOT_FOUND` con `source: "not_indexed"` en vez de inventar un contacto.
- **Bing es una fuente hostil**: devuelve resultados irrelevantes en paginas
  bien formadas. Por eso la relevancia se valida siempre.
- **Sin `PROXY_URL` en produccion no hay resultados.** No es un bug del codigo.
