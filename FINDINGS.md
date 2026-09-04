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

---

# Tercera tanda — "que Google no nos detecte"

## F17 — Google no falla al renderizar: nos rechaza explicitamente

Se probo lo unico que podia cambiar el resultado: **ejecutar la pagina con un
navegador real** (Chromium via Playwright, `DynamicFetcher`), porque en HTTP
puro Google devuelve un bootstrap de JavaScript sin resultados (F3).

Resultado medido:

| via | tiempo | bytes | anclas externas | status |
|---|---|---|---|---|
| HTTP puro | 213ms | 91.943 | **0** | 200 |
| Chromium renderizando | 7.957ms | 6.839 | **0** | **429** |

El HTML renderizado dice, textual:

> "Nuestros sistemas han detectado tráfico inusual procedente de tu red de
> ordenadores. En esta página se comprueba si eres tú quien envía las
> solicitudes en lugar de un robot."

…y trae un `captcha-form`.

**Conclusion:** no es una limitacion tecnica que se cierre con un navegador
mejor. Google tomo una decision de acceso y pide verificacion humana. Pasar de
ahi es resolver el desafio, y eso no se hace. Google queda apagado
(`ENABLE_GOOGLE=0`) y el presupuesto se gasta en fuentes que si atienden.

Fijado por `bench/check_no_captcha_bypass.py`, que ademas verifica lo contrario:
que `blocking.py` **si** reconozca captchas, porque reconocer un desafio es lo
que permite retirarse de el.

## F18 — Renderizar por navegador no aporta nada medible en este pipeline

Ya con el navegador funcionando, se midio si servia para lo otro que hace el
servidor: leer los sitios de los prospectos.

Sobre 15 sitios reales descubiertos por el motor:

- HTTP puro no puede leer **2 de 15** (13%).
- El navegador rescata **0 de esos 2**. Uno devuelve 267 caracteres por las dos
  vias (es un sitio flaco, no un problema de JavaScript); el otro cierra la
  conexion a ambos.
- Sobre los 13 que si funcionan, renderizar aporta datos extra en **1**.

Costo de adoptarlo: dependencia de Playwright, ~300MB de RAM por navegador y
segundos por fetch, en un servicio que ya tuvo un OOM en Render.

**Decision: no se adopta.** Se deja registrado para no volver a pagar la
investigacion.

## F19 — Lo que SI subio el outcome: mas fuentes, no mas sigilo

Se midieron siete buscadores que sirven HTML deliberadamente. Resultado:

| motor | status | utilizable | empresas utiles |
|---|---|---|---|
| **brave** | 200 | si | **9** |
| **lite.duckduckgo** | 200 | si | **6** |
| marginalia | 200 | si | envuelve las URLs; queda pendiente |
| mojeek | 200 | **no** | `<title>Captcha</title>` |
| startpage | 200 | no | bloqueado |
| ecosia | 403 | no | bloqueado |
| yep | 200 | no | bloqueado |

Se agregaron **brave** y **ddglite**. Medido sobre el mismo corpus:

```
2 proveedores (ddg + bing)          10 empresas unicas
4 proveedores (+ brave + ddglite)   20 empresas unicas
solo aportadas por los nuevos       10
perdidas                             0
```

**El doble de empresas, sin tocar una sola defensa de nadie.**

Con techo: `MAX_FETCHES=12`. Cinco estrategias por cuatro proveedores son 20
combinaciones, y dispararlas todas revienta el presupuesto y provoca el captcha
que nos deja sin fuentes. Sumar proveedores es para COBERTURA -- que un motor
bloqueado no mate la busqueda -- no para gastar mas.

## F20 — La pagina de captcha de Mojeek destapo un hueco del detector

Sus unicos enlaces "externos" son subdominios propios (`blog.mojeek.com`,
`community.mojeek.com`), asi que el conteo de anclas cruzaba el umbral y la
pagina pasaba por buena.

Corregido agregando el `<title>` como senal fuerte: ninguna pagina de
resultados legitima se titula "Captcha". Con control positivo sobre los cuatro
motores que si funcionan, cuyo titulo lleva la query.

## F21 — Los nombres estan en la PAGINA, no en el snippet del buscador

Medido el 2026-09-03 (IP residencial, sin proxy, Bing respondiendo 200 en 18/18
fetches): una busqueda del canal personas devolvio **67 resultados crudos** y el
parser de snippets saco **UN candidato, que ademas era falso** ("Secretario
General" es un cargo, no una persona).

La conclusion no es que el canal no sirva. Es que el snippet de un buscador casi
nunca tiene la forma "Nombre - Cargo - Empresa": tiene la descripcion comercial
del sitio. Los nombres estan **dentro** de la pagina de equipo, y para leerlos
hay que visitarla.

Consecuencia de diseno: `/search-people` devuelve **dos** salidas.

  results   personas parseadas del snippet. Gratis cuando aparecen, minoria.
  pages     paginas que merecen un scrape porque tienen forma de listar gente.

Devolver solo `results` tiraba el 99% del valor de cada busqueda, y hacia que el
canal reportara "no hay decisores" sobre corridas que habian encontrado doce
paginas de equipo.

### F21.1 — La heuristica de "pagina de personas" necesito dos correcciones, las dos medidas

La primera version marco como paginas de equipo a `amarillas.cl`, `laborum.cl`,
`chilepymes.com` y `directorioempresaschile.cl`. Dos causas distintas:

1. **"directorio" y "socios" en el TITULO.** En un titulo casi siempre
   significan "Directorio de empresas de Chile" -- un agregador. Se quitaron de
   las senales de titulo y se aplico `filtering.motivo_descarte()`, el mismo
   filtro que ya protege al pipeline de empresas.
2. **Senales buscadas en la URL COMPLETA, host incluido.** `directorioempresaschile.cl`
   entraba porque su NOMBRE contiene "directorio". Las senales pasaron a
   buscarse solo en la RUTA: `/directorio` es el directorio de una sociedad, un
   host llamado "directorio…" es un agregador.

### F21.2 — Sin proxy no se puede medir la viabilidad del canal

En las corridas del 2026-09-03 desde IP residencial sin `PROXY_URL`, DuckDuckGo,
Brave y lite-DDG pasaron a captcha tras pocas busquedas y quedo respondiendo
solo Bing, que este mismo documento ya clasifica como fuente hostil (F6). Con
solo Bing, ninguno de los 30 resultados por query era una pagina de equipo.

**Eso NO mide el canal: mide la ausencia de proxy.** La viabilidad hay que
medirla con `PROXY_URL` configurado, que es la condicion de produccion.

## F22 — El sitio del prospecto es la fuente que SIEMPRE atiende

Medido el 2026-09-03 resolviendo el decisor de cuatro empresas chilenas reales
(Fintual, Buk, Betterfly, Toteat) desde IP residencial sin proxy.

### F22.1 — Buscar no alcanza, y a veces no sirve para nada

`site:fintual.cl (equipo OR nosotros OR "quienes somos")` en Bing devolvio diez
resultados: **zhihu.com y foros franceses sobre Instagram**. Bing ignora el
operador `site:` y sirve cualquier cosa -- es el mismo F6 de siempre. Los otros
tres proveedores estaban en captcha (F1).

Con eso, un resolutor que solo sabe buscar no encuentra nada por mas angulos que
tenga. La correccion no fue agregar un angulo mas: fue **entrar al sitio**. La
pagina de equipo esta enlazada desde la home, y ese camino no depende de que
ningun buscador nos atienda -- depende del prospecto, que es justamente quien SI
quiere ser leido.

Orden resultante: primero el sitio; las busquedas solo si el sitio no alcanzo.
Cuando el sitio da un decisor con evidencia fuerte, la consulta **no gasta una
sola busqueda**.

### F22.2 — Los numeros

    empresa      veredicto   decisores  fetches  paginas    ms
    Fintual      decisor     1          3        2          2.859
    Buk          decisor     3          12       3          5.498
    Betterfly    bloqueado   0          12       3          5.458
    Toteat       bloqueado   0          10       1          3.713

Fintual se resolvio **sin buscar**: 3 fetches al propio sitio, 2,8 segundos.
Comparar con el `buscar_persona()` anterior, que tardaba **4m43s** para devolver
NOT_FOUND.

### F22.3 — Tres falsos positivos que solo aparecen con datos reales

La primera corrida devolvio como personas:

  "Chief Economist"   un cargo en ingles, en el /equipo de Fintual
  "Betterfly's Co"    recorte de "Betterfly's Co-Founder"
  "Jaime Arrieta" + "Jaime Arrieta Boetsch"   la MISMA persona, dos veces

Ninguno se le habria ocurrido a quien escribe los tests desde cero: salieron de
mirar HTML real. Los tres estan corregidos y fijados con tests que citan el caso
medido, y con control positivo sobre los decisores reales que la misma corrida
encontro (Omar Larre, Ricardo Sateler, Jaime Arrieta Boetsch, Cristobal Della
Maggiora).

### F22.4 — El presupuesto no acotaba nada

La corrida medida tardo **48s con DECISOR_BUDGET_S en 25**. Dos causas
independientes, las dos invisibles en test unitario:

1. `as_completed()` sin `timeout` esperaba a los futuros para siempre.
2. Salir del `with` del ThreadPoolExecutor hace `shutdown(wait=True)`, que
   espera a los hilos vivos.

Ademas Scrapling reintenta 3 veces por su cuenta: con `FETCH_TIMEOUT` de 15s, un
solo fetch colgado costaba 45s -- mas que el presupuesto entero. De ahi
`DECISOR_FETCH_TIMEOUT`.

### F22.5 — El sistema reporta lo que la fuente dice, incluso cuando la fuente se equivoca

En una corrida aparecio **"Eduardo Dillamajora"** como fundador de Betterfly. El
apellido real es Della Maggiora. Se verifico que NO es el parser: el codigo
captura tramos de texto y no transforma letras (`sin_acentos` solo quita
diacriticos), asi que la cadena estaba literal en el snippet -- probablemente un
transcript de podcast escrito de oido.

No se agrega correccion difusa de nombres: adivinar la ortografia correcta
introduce una clase de error peor (cambiar un apellido que SI estaba bien).
Lo que si mitiga el problema es el ranking, que ya prefiere el sitio propio de
la empresa (1.0) sobre un tercero (0.65).

Consecuencia operativa: un candidato de score 0.65 venido de un tercero vale
para investigar, no para saludar por nombre sin mirar.

## F23 — En produccion el cuello de botella es el proxy, no el codigo

Medido el 2026-09-03 contra el servicio desplegado en Render (IP de datacenter,
`/health` reporta `proxy: false`), resolviendo el decisor de seis empresas con
dominio conocido.

    Xepelin      OK   Sebastian Kreis, CEO   0.975   2.761ms   sin buscar
    Lagencia     NO   providers_blocked              21.890ms  paginas=0
    Agencia GL   NO   providers_blocked              21.595ms  paginas=0
    Agensa       NO   providers_blocked              24.810ms  paginas=0
    Khipu        NO   providers_blocked              23.980ms  paginas=2
    Destacame    NO   providers_blocked              21.549ms  paginas=0

**1 de 6 (17%).** El numero no es casualidad: coincide con lo ya medido del lado
de Venara -- "sitio propio (paginas de equipo) ... 17% da una persona
identificada". Dos mediciones independientes, con metodos distintos, dan lo
mismo.

### F23.1 — `paginas=0` no es un fallo del detector

Se verifico sobre el HTML real. `lagencia.cl` sirve 209KB y 59 anclas, y sus 36
rutas internas son: Inicio, Servicios (x4), Blog, Contacto, Agenda, Politica de
Privacidad y notas del blog. **No existe pagina de equipo.** Idem agenciagl.cl y
destacame.cl. La agencia no publica a su gente, y ningun parseo arregla eso.

Donde SI existe, el camino funciona y es barato: Xepelin y Fintual se
resolvieron en menos de 3 segundos sin gastar una sola busqueda.

### F23.2 — La cobertura del 83% restante depende del proxy

Para las empresas sin pagina de equipo el unico camino es buscar, y desde la IP
de datacenter de Render los cuatro proveedores devuelven captcha (F1, que ya lo
habia medido: "0 resultados desde Render"). Por eso las cinco fallidas dicen
`providers_blocked` y tardan ~22s en decirlo.

**Consecuencia: la palanca mas grande de cobertura hoy no es mas codigo, es
configurar `PROXY_URL` en Render.** Sin proxy, el sistema entrega el ~17% que da
el sitio propio; el resto queda a merced de un captcha.

## F24 — CORRIGE A F7: los perfiles SI estan en el indice; el operador y el buscador eran el problema

F7 concluyo, el 2026-08-30, que "los perfiles personales no estan en el indice
publico". Esa conclusion se **generalizo de mas** y hay que corregirla.

Lo que F7 midio realmente: `site:linkedin.com/in "<empresa>"` en **DuckDuckGo y
Bing** devuelve cero. Eso sigue siendo cierto.

Lo que se midio el 2026-09-04, sobre la misma empresa, cambiando dos cosas:

    buscador   query                          perfiles /in/ en el HTML
    brave      "Fintual" CEO linkedin         7
    bing       "Fintual" CEO linkedin         0
    ddg        "Fintual" CEO linkedin         (bloqueado)

Brave devuelve siete perfiles. **El indice no era el problema: lo eran el
operador `site:` y el buscador al que se le preguntaba.** F7 nunca probo Brave
con la palabra suelta -- Brave se sumo despues, en F19.

### F24.1 — Hay que ENTRAR al perfil; el snippet no alcanza

Brave sirve el titulo del resultado como breadcrumb:

    'LinkedIn cl.linkedin.com in andresmarinkovic Andrés Marinkovic'

Hay nombre y no hay cargo, asi que el parser de SERP no produce nada. El titulo
de la PAGINA, en cambio, lo trae todo:

    'Andrés Marinkovic - Co Founder y COO en Fintual (YC S18) | LinkedIn'

Se entra sin sesion, sin cookie y sin resolver ningun desafio: LinkedIn sirve el
`<title>` y el `og:title` a cualquiera. El cuerpo esta detras de un muro de
sesion y ahi se queda.

### F24.2 — El subdominio de pais resuelve el homonimo

`cl.linkedin.com/in/...` declara Chile. Es lo que faltaba para el falso positivo
de F23: "Houm" es una empresa chilena y tambien una india, y un directorio
extranjero le colgo a la chilena dos fundadores ajenos. Cuando el perfil declara
un pais distinto al pedido, no se atribuye.

### F24.3 — El angulo existia y no se ejecutaba nunca

La primera medicion dio `linkedin=0` en las seis empresas. No era que el angulo
fallara: **no habia corrido ni una vez**. Con el techo de fetches repartido de
forma plana, `sitio_equipo` y `cargo_directo` por los cuatro proveedores se
comian los 8 fetches y las queries de LinkedIn quedaban siempre fuera.

Se reparte por angulo (2 proveedores cada uno): el mismo gasto cubre el doble de
angulos. Angulos distintos alcanzan documentos distintos; proveedores distintos
se solapan, y su valor es la resiliencia ante un bloqueo, no la cobertura.

Tras el arreglo, sobre las mismas empresas:

    Betterfly   Cristobal della Maggiora, Co-Founder & President   score 1.0
    Buk         Jaime Arrieta, Founder                             via perfil

En Betterfly el perfil verificado ademas DESPLAZO al falso positivo de F22.5
("Eduardo Dillamajora", con el apellido mal escrito por la fuente).

### F24.4 — Google sigue sin servir, y se volvio a medir

No se cito la medicion vieja: el 2026-09-04 se pidio de nuevo
`"Fintual" CEO site:linkedin.com/in` a Google desde IP residencial. Devuelve
**92.457 bytes, status 200, cero perfiles y cero resultados extraibles**; el
veredicto de `blocking.analizar` es `requiere-javascript`. Los resultados no
estan en el HTML.

Para usar Google literalmente haria falta su **API oficial de Custom Search**
(una API key y un Search Engine ID). No hay ninguno configurado en el proyecto;
el `GOOGLE_AI_API_KEY` que existe es de Gemini y no sirve para esto sin crear
antes el buscador programable.
