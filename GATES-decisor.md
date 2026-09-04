# Gates: resolutor de decisor (empresa -> persona)

OWNS: venara_discovery/decisor.py, venara_discovery/personas.py, venara_discovery/website.py, venara_discovery/api.py, venara_discovery/config.py, tests/test_decisor.py, tests/fixtures/sintetico_*, bench/medir_decisor.py, GATES-decisor.md

Scope: dado el NOMBRE de una empresa (y su dominio si se conoce), encontrar a su
decisor atacando el indice desde varios angulos distintos --"<empresa> CEO",
"<empresa> gerente general", la pagina de equipo del propio sitio, la prensa de
nombramientos y las entrevistas-- leyendo tanto el snippet del buscador como el
TEXTO de la pagina, con ranking que prefiere el sitio propio, techo de gasto, y
un NOT_FOUND que dice por que.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-decisor.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=f2847208530939d244acf519d4cdafac8ee22a75a32644056f638a8e8c04986d; output-bytes=159

- [x] G1: el plan ataca el indice desde angulos DISTINTOS — incluye "<empresa> CEO" y "<empresa> gerente", aprovecha el dominio cuando se conoce, y ninguna query usa linkedin.com/in (con control positivo que planta una prohibida)
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "plan or angulo or query or linkedin" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=89e76dc692245bbe98217174940f343658b5bc785ad3f3887564a86690b4e4ff; output-bytes=635

- [x] G2: extrae decisores del TEXTO de una pagina de equipo, no solo del snippet — que es donde F21 midio que estan los nombres — y no inventa personas en una pagina sin gente
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "pagina or texto" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=5e44317468066a3e143062cbd12b3684a45aaa6c4bc1e44a21a00928a1d4f677; output-bytes=635

- [x] G3: el ranking prefiere la evidencia mas fuerte — el sitio propio de la empresa gana sobre un tercero, el cargo que decide gana sobre el que no, y un candidato cuya empresa no aparece en ningun lado se descarta
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "ranking or prefiere or descarta" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=35d29415ad1702f7e4a25d831cd79f0a5242aa44b3a34d3ba9ed504c3b049a91; output-bytes=634

- [x] G4: el resolutor completo encuentra al decisor sin tocar la red, y cuando no lo encuentra distingue "no publicado" de "los buscadores bloquearon"
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "resolver or motivo or bloque" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ee657cd77db30c987cb21856c6f8033afc281c4d1ff5e8468fab75543f83e19b; output-bytes=634

- [x] G5: el endpoint nuevo devuelve el decisor con su evidencia y /search-linkedin conserva EXACTAMENTE su forma historica, que Venara ya consume
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py tests/test_backcompat.py tests/test_api_contract.py -k "endpoint or contrato or backcompat or historic or compat or blocked or tope or cache" && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ac2e46cebdd775f40f35ceeee5de1725744340975a8eecf49765639f2194b9e5; output-bytes=635

- [x] G6: una consulta no puede volver a costar 4m43s — el gasto esta acotado por techo de fetches, presupuesto de tiempo y tope de paginas visitadas, y hay un control que falla si esos topes desaparecen
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "presupuesto or techo or tope or gasto" && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=781001d139c7dbabb7d626383c3d756f337351433b0043e4f7bcb80558d6365d; output-bytes=634

- [x] G7: la suite completa del servidor sigue verde, incluidos los tests que ya existian
  CHECK: .venv/bin/python -m pytest -q && echo G7_OK
  EXPECT: G7_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=3b7216c79b73fb3dcaac70dcfdc36c2e7c86cb3de1e2d4cf5ba849ff76ededa5; output-bytes=1022

- [x] G8: la medicion en vivo sobre empresas reales produce un veredicto decisivo (decisores encontrados, o proveedores bloqueados nombrados) y lo deja escrito en un reporte
  CHECK: bash bench/medir_decisor.sh
  EXPECT: MEDICION REGISTRADA
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=4ad415d727e21b3a9e66fab7ea803ad1e7fcdf181ec836398aa135c4da72edea; output-bytes=15178

- [x] G9: el pedido explicito de "buscar en Google" queda resuelto de forma honesta — se verifica contra el fixture capturado que Google no sirve resultados en el HTML, el sistema NO depende de el, y sigue funcionando con los buscadores que si responden
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "google" && echo G9_OK
  EXPECT: G9_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=073b73ed0bcaf99cdc46434996576c3e0cc1f7b5e6b0a1793058ea122bc91a17; output-bytes=634

- [x] G11: los falsos positivos que aparecieron en la medicion en vivo no vuelven — un cargo ("Chief Economist") ni una razon social ("Betterfly's Co") entran como persona — con control positivo de los decisores reales que la MISMA corrida encontro
  CHECK: .venv/bin/python -m pytest -q tests/test_personas.py -k "cargo_ni_una_razon or decisores_reales_medidos" && echo G11_OK
  EXPECT: G11_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=6f1761eb62da790e0761e620a22e269aafcbddee6f03a882437ba2ba751d7597; output-bytes=635

- [x] G12: la misma persona no se devuelve dos veces — "Jaime Arrieta" y "Jaime Arrieta Boetsch" se fusionan — sin colapsar a dos personas distintas
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "fusiona" && echo G12_OK
  EXPECT: G12_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=0599da6dac64b5094a160ec76723b97957b032c79c65e69cd8d43c2972a6641b; output-bytes=635

- [x] G13: /search-linkedin acepta un dominio OPCIONAL y con el usa el camino que entra al sitio — que es el unico que funciona cuando los buscadores bloquean — sin que un cliente viejo que no lo manda cambie de comportamiento
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py -k "search_linkedin" && echo G13_OK
  EXPECT: G13_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=b20dad9ae473360a860ba6a39e76325810133bbc7cfd0eac687a1cd8f4484586; output-bytes=635

- [x] G10: el numero medido queda registrado con su limitacion declarada, y los cambios quedan aplicados en el repo scrapling-server
  EVIDENCE: Medicion del 2026-09-03 sobre 4 empresas chilenas reales con dominio conocido (Fintual, Buk, Betterfly, Toteat), IP residencial SIN proxy. NUMEROS: 2 de 4 resueltas con decisor (Fintual 1 en 2.859ms con 3 fetches y CERO busquedas; Buk 3 en 5.498ms), 2 bloqueadas por captcha de proveedores. Personas verificadas como reales y correctamente atribuidas: Omar Larre (Fintual, Co-founder & CIO), Ricardo Sateler (Buk, Co-Founder), Jaime Arrieta Boetsch (Buk, CEO). Comparacion con el sistema anterior: `buscar_persona()` tardaba 4m43s medidos contra produccion para devolver NOT_FOUND. LIMITACION DECLARADA: sin PROXY_URL los proveedores caen en captcha a los pocos requests (F1) y Bing ignora `site:` sirviendo resultados de otro idioma y otro tema (F6, verificado en vivo: `site:fintual.cl equipo` devolvio zhihu.com y foros franceses). Por eso el veredicto de 2 de 4 empresas es "bloqueado" y NO significa que no publiquen a su decisor; con proxy la cobertura seria mayor y esta SIN MEDIR. El camino sitio-directo no depende de proxy y es el que resolvio Fintual. Registrado en FINDINGS.md F22/F22.1/F22.2/F22.3/F22.4 y en MEDICION-decisor.md. CAMBIOS APLICADOS en el repo local de github.com/ignavm1/scrapling-server (rama feat/decisor-empresa-a-persona); el push queda pendiente de autorizacion del dueno del repo.
