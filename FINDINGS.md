# Auditoria del servidor de discovery — 30 de agosto de 2026

Todo lo de abajo esta **medido**, no inferido. Cada hallazgo dice como
reproducirlo. Los fixtures en `tests/fixtures/` son HTML real capturado ese dia.

---

## F1 — Produccion devuelve CERO empresas para toda busqueda

Contra `scrapling-server.onrender.com`, con API key valida:

| query | ubicacion | resultados | tiempo |
|---|---|---|---|
| marketing agencies | Lima | **0** | 47.7s |
| marketing digital | Lima | **0** | 47.6s |
| agencia de marketing | Lima, Peru | **0** | 47.6s |
| contadores | Santiago | **0** | 47.5s |

HTTP 200 con `{"results": [], "total": 0}`. El descubrimiento de leads de Venara
no esta degradado: **esta muerto**.

Los ~47.6s constantes son firma de timeout, no de busqueda: 9 fetches, 6
workers, 15s de timeout cada uno.

## F2 — La salida de red de Render esta sana; el problema son los buscadores

`POST /scrape-website` con `https://example.com` responde en **0.18s** y
`https://www.python.org` devuelve 6855 caracteres. Descarta "Render sin red" y
localiza la falla en los buscadores.

## F3 — Google es estructuralmente inservible para HTTP puro

Desde IP **residencial** (sin bloqueo de por medio), `google.com/search`
devuelve 92KB con:

- **1** `<a href>` en total, **0** externos
- **0** atributos `class`
- `<noscript>` con `<meta http-equiv="refresh" url="/httpservice/retry/enablejs">`

Es un bootstrap de JavaScript. Los selectores `div.g a` y `div.tF2Cxc a` del
servidor extraen **cero**, y ningun selector lo arregla porque el contenido no
esta en el HTML.

Google consumia **un tercio** del presupuesto de requests de cada busqueda para
no aportar nada. Fixture: `tests/fixtures/google_jsshell.html`.

## F4 — Un bloqueo se disfrazaba de "el nicho no tiene resultados"

DuckDuckGo **no** responde 403 ni 429 al bloquear. Responde **HTTP 202** con una
pagina de captcha:

> "Unfortunately, bots use DuckDuckGo too. Please complete the following
> challenge... Select all squares containing a duck"

Fixture: `tests/fixtures/ddg_blocked.html` (202, 13066 bytes).

El servidor no miraba ni el status ni el cuerpo: extraia 0 y devolvia HTTP 200
con lista vacia. El cliente de Venara leia `queriesFailed: 0` y concluia que el
nicho estaba vacio. **El sintoma no se parecia a la causa.**

Cualquier deteccion basada en `status in (403, 429)` deja pasar esto.

## F5 — Bing no aportaba NINGUNA empresa, por construccion

Bing envuelve cada resultado en un redirect propio:

```
https://www.bing.com/ck/a?...&u=a1<base64-de-la-URL-real>
```

`fix_href()` decodificaba `/url?q=` (Google) y `uddg=` (DuckDuckGo) pero **no**
el `ck/a` de Bing. Resultado: toda URL de Bing salia como `bing.com/...`, y
`_is_business_site()` la descartaba como basura porque `"bing."` esta en
`JUNK_HOST_SUBSTR`.

Con Google sirviendo un shell de JS y Bing descartandose a si mismo, **solo
DuckDuckGo aportaba algo** — y es el que primero se bloquea.

## F6 — Bing devuelve resultados ENVENENADOS que parecen validos

Peor que un bloqueo, porque la pagina es estructuralmente correcta.

| query enviada | `<title>` devuelto | resultados reales |
|---|---|---|
| agencia de marketing digital Lima sitio web | correcto | **hilos de Reddit sobre hardware Elgato** |
| contadores Santiago Chile | correcto | contadores de **Guatemala** |
| restaurantes Buenos Aires | correcto | restaurantes de **Panama** |
| contadores Santiago Chile `&mkt=es-CL` | correcto | paginas de **AOL** |

El `<title>` confirma que Bing recibio la query correcta. Aun asi sirve otra
cosa, manteniendo 10 contenedores `li.b_algo` bien formados.

Consecuencia de diseno: **la validacion de relevancia no es opcional.** Sin
comparar el resultado contra la query, Reddit y AOL entran al pipeline como
"empresas". Un filtro estructural no detecta nada de esto.

`setlang=es` **no** fija pais. `mkt=es-AR` si corrigio Buenos Aires; `mkt=es-CL`
devolvio AOL. Bing es inconsistente y hay que tratarlo como fuente hostil.

## F7 — Los perfiles personales de LinkedIn no estan en el indice publico

Control positivo: `site:linkedin.com/company agencia marketing Lima` en
DuckDuckGo devuelve **10 URLs de empresa**. El operador `site:` funciona.

Con el mismo motor y operador, `site:linkedin.com/in "<empresa>"` devuelve
**0 perfiles** en todas las variantes probadas (con cargo, con ubicacion, con
empresas grandes, sin operador).

Coincide con lo ya medido en Venara (`lib/discovery/queries.ts`), que tiene un
test fijando que ninguna query use `site:linkedin.com/in`.

## F8 — `maxResults` del cliente se ignora

El cliente manda `maxResults` (camelCase); el modelo declara `max_results`.
Pydantic descarta el campo desconocido y usa el default. Pedir 50 devolvia 25.

## F9 — La cache envenenaba busquedas por 6 horas

`_cache_set()` guardaba el resultado vacio de una busqueda bloqueada con el
mismo TTL de 6h que uno bueno. Una sola busqueda bloqueada dejaba ese
(nicho, ubicacion) muerto durante seis horas.

Ademas la clave incluia `max_results`, asi que pedir 20 y pedir 25 eran dos
entradas distintas para el mismo trabajo.

## F10 — La deduplicacion unia empresas distintas

La clave de merge era `_domain(website)`. Dos negocios en el mismo hosting
compartido (`algo.wixsite.com`, `algo.mystrikingly.com`) colapsaban en uno.
Y sin website usaba la `linkedin_url` completa, asi que la misma empresa con y
sin `?trk=` quedaban como dos.

## F11 — `_website_near()` asignaba el website de otro resultado

Subia hasta 5 ancestros del ancla y tomaba la primera URL que encontrara. A esa
altura del DOM ya se esta leyendo el bloque del resultado vecino.

## F12 — Sin techo de lectura ni presupuesto total

`resp.read()` sin argumento lee hasta que el otro lado cierre: un servidor
hostil llena la memoria del contenedor. Es consistente con el OOM que ya tumbo
el servicio en Render.

Y sin presupuesto total, el servidor seguia trabajando despues de que el cliente
cortara a los 45s.

## F13 — SSRF: la validacion por nombre no cierra el rebinding

`is_safe_public_url()` resuelve DNS, valida, y luego la libreria vuelve a
resolver para conectar. Entre las dos resoluciones el atacante puede cambiar el
registro. Ninguna validacion por nombre cierra eso sola.

Tampoco cubria CGNAT (100.64/10), IPv4 mapeada en IPv6 (`::ffff:0:0/96`) ni
NAT64 (`64:ff9b::/96`).

---

## Como se reproduce

```bash
.venv/bin/python -m pytest tests/ -q          # toda la evidencia como tests
.venv/bin/python bench/benchmark.py           # metricas antes/despues
```

---

# Segunda pasada — edge cases y seguridad

Estos NO salieron de un test que fallara: salieron de revisar el codigo ya
funcionando y de mirar los resultados de una corrida real. Es la parte del
pedido que dice "no asumas que el codigo esta bien solo porque funciona".

## F14 — "Colima, Mexico" resolvia a Lima, Peru

La ubicacion se buscaba por substring: `"lima" in "colima"` es verdadero. Una
campana para Mexico se ejecutaba contra el mercado peruano, y el error solo se
veia al mirar los leads uno por uno.

Corregido con coincidencia por palabra completa (`_contiene_palabra`), en la
interpretacion Y en el calculo de confianza.

## F15 — Una pagina legitima que mencionara "captcha" apagaba el proveedor

El detector trataba `"captcha"` como marca de bloqueo por si sola. Una empresa
que vende servicios anti-captcha, o un articulo sobre el tema, mataba a ese
buscador para toda la busqueda.

Corregido separando marcas FUERTES (frases completas que solo aparecen en una
pagina anti-bot) de marcas DEBILES (palabras sueltas), que ahora solo cuentan
cuando ademas no hay resultados que extraer.

## F16 — Tres defectos vistos en la corrida real contra Santiago de Chile

1. **`cylex.cl` pasaba el filtro.** La lista tenia `cylex.com`; los directorios
   operan la misma marca en cada pais y la version local es justo la que sale
   en una busqueda por ciudad. Ahora se compara por marca.

2. **`estudiocontablesantiago.com.ar` figuraba como resultado "de ciudad"** en
   una busqueda de Santiago de CHILE, porque "santiago" estaba en el dominio.
   Ahora un TLD de otro pais contradice la ubicacion y baja la confianza a
   `otro-pais`.

3. **Un lead se llamaba "Contacto"** (de `mva.cl/contacto`): el titulo de una
   pagina interna no es el nombre de la empresa. Ahora esos titulos caen al
   nombre derivado del dominio.

Los tres estan fijados en `tests/test_edge_cases.py`, cada uno con su control
positivo para que el arreglo no se pase de largo y descarte empresas buenas.
