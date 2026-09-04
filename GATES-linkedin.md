# Gates: angulo LinkedIn (buscar el perfil y entrar solo si es LinkedIn)

OWNS: venara_discovery/linkedin_perfil.py, venara_discovery/decisor.py, venara_discovery/personas.py, venara_discovery/api.py, venara_discovery/config.py, tests/test_linkedin_perfil.py, tests/fixtures/sintetico_linkedin_*, bench/medir_linkedin.py, bench/medir_linkedin.sh, GATES-linkedin.md

Scope: buscar el perfil de LinkedIn del decisor de una empresa ("<empresa> CEO
linkedin"), ENTRAR unicamente a los resultados que son perfiles de LinkedIn --
regla explicita del usuario-- y leer del titulo de esa pagina el nombre, el
cargo y la empresa, usando el subdominio de pais como senal para no atribuir a
una empresa homonima de otro pais.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-linkedin.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=6a590b8cc88efe13ea8560227272b4a5fbbcff7beaabd2be11cdf60be505eded; output-bytes=159

- [x] G1: el angulo pide el perfil como lo pidio el usuario -- "<empresa> <cargo> linkedin" -- y NO usa site:linkedin.com/in, que esta medido inutil en los proveedores disponibles, con control positivo que planta la query prohibida
  CHECK: .venv/bin/python -m pytest -q tests/test_linkedin_perfil.py -k "query or plan or prohibida" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=a9402e617efde49cb63a6841b3492d6812dc71b595d451bb77f6c4d0adde3d0b; output-bytes=119

- [x] G2: entre los resultados de busqueda se entra UNICAMENTE a perfiles de LinkedIn, y se demuestra con control negativo que una url que no es perfil no se visita
  CHECK: .venv/bin/python -m pytest -q tests/test_linkedin_perfil.py -k "entra or visita" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=d5c9e66ee2fab2beeedf4160ae0314cfdcd652c4a98d249f25afc58e118fe77f; output-bytes=120

- [x] G3: del titulo de un perfil real se extraen nombre, cargo y empresa, y un titulo que no es de perfil no produce candidato
  CHECK: .venv/bin/python -m pytest -q tests/test_linkedin_perfil.py -k "titulo or parse" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=25c345a349672397e2db9882def977133596b4604450983f47f957c6bc366759; output-bytes=120

- [x] G4: el subdominio de pais del perfil evita el falso positivo medido -- una persona de otro pais no se atribuye a la empresa chilena homonima -- sin descartar a la persona correcta del pais pedido
  CHECK: .venv/bin/python -m pytest -q tests/test_linkedin_perfil.py -k "pais or homonim" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=0e78887a0ad9780a3e0355e7b682ccbc20ee5cb97017a577b8b44d3534bb242c; output-bytes=119

- [x] G5: el resolutor integra el angulo sin romper el camino del sitio propio, que es el que mejor rinde
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=123fff4c6b513893e83a30d6dabdb5aa8db88a6b8d619d42f77bbdda4b6b1ef4; output-bytes=620

- [x] G6: la suite completa del servidor sigue verde
  CHECK: .venv/bin/python -m pytest -q && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=f2c433f80b5a3127f55f554d3e0237de020041da605bc0680af0824993a92482; output-bytes=942

- [x] G7: la medicion en vivo sobre empresas reales produce un veredicto decisivo por empresa y lo deja escrito en un reporte
  CHECK: bash bench/medir_linkedin.sh
  EXPECT: MEDICION REGISTRADA
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=309ad0bd9b9ea37773a6a674c75950f6c9fe5e70a1e5278116100db885cd3931; output-bytes=8839

- [x] G8: el pedido de "buscar en Google" queda resuelto con medicion FRESCA, no citando una vieja -- se verifica hoy que Google no sirve resultados por HTTP y que el sistema no depende de el
  CHECK: .venv/bin/python -m pytest -q tests/test_linkedin_perfil.py -k "google" && echo G8_OK
  EXPECT: G8_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=4071c558d3a5/24 entries; EXPECT=matched; output-sha256=831585bd0379b22483ea9c651f89d355c3ff6b9049f79bfa0737b8a944020419; output-bytes=119

- [x] G9: los numeros medidos quedan registrados con su limitacion, y queda dicho que haria falta para usar Google literalmente
  EVIDENCE: Medicion del 2026-09-04 sobre 6 empresas chilenas reales (Fintual, Buk, Xepelin, Betterfly, Houm, Toteat), IP residencial SIN proxy. RESULTADO: 4 de 6 resueltas; 2 decisores salieron del PERFIL de LinkedIn -- Betterfly: Cristobal della Maggiora, Co-Founder & President, score 1.0; Buk: Jaime Arrieta, Founder. Fintual (Omar Larre, 1.0) y Xepelin (Sebastian Kreis, 0.975) siguen resolviendose por el sitio propio, sin buscar. Houm y Toteat: providers_blocked. EFECTO COLATERAL VERIFICADO: en Betterfly el perfil verificado desplazo al falso positivo de F22.5 ("Eduardo Dillamajora", apellido mal escrito en la fuente), que ya no aparece. CORRECCION A F7 registrada como F24: los perfiles SI estan en el indice -- Brave devuelve 7 para "Fintual" CEO linkedin -- y lo que fallaba era el operador site: y el buscador al que se preguntaba. DEFECTO PROPIO ENCONTRADO Y CORREGIDO (F24.3): la primera medicion dio linkedin=0 en las seis empresas porque el angulo NO SE EJECUTABA NUNCA -- el techo plano de 8 fetches se consumia con 2 angulos x 4 proveedores; ahora se reparten 2 proveedores por angulo y hay un test que falla si el angulo vuelve a quedar fuera. LIMITACION DECLARADA: sin PROXY_URL los proveedores caen en captcha (Houm y Toteat en esta corrida), y Brave -- el unico que devuelve perfiles -- es de los primeros en bloquear; la cobertura con proxy queda SIN MEDIR. GOOGLE: medido de nuevo hoy, no citado de antes -- 92.457 bytes, status 200, cero perfiles, veredicto requiere-javascript; los resultados no estan en el HTML. Para usarlo literalmente hace falta la API oficial de Custom Search (API key + Search Engine ID); el GOOGLE_AI_API_KEY existente es de Gemini y no sirve sin crear antes el buscador programable. Registrado en FINDINGS.md F24/F24.1/F24.2/F24.3/F24.4 y en MEDICION-linkedin.md.
