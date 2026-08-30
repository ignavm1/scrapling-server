# Gates: mas fuentes, y la decision sobre renderizar

OWNS: venara_discovery/providers.py, venara_discovery/extraction.py, tests/test_providers.py, tests/fixtures/brave_ok.html, tests/fixtures/ddglite_ok.html, tests/fixtures/mojeek_ok.html, bench/check_no_captcha_bypass.py, FINDINGS.md

Scope: subir el outcome sumando buscadores que sirven HTML deliberadamente
(Brave y lite-DuckDuckGo) en vez de intentar esquivar a los que explicitamente
nos rechazan; y dejar registrada, con medicion, la decision de NO agregar
renderizado por navegador. Mojeek se evaluo y quedo FUERA: devolvio una pagina
de captcha, que ahora sirve de fixture para el detector.

Toolchain: los CHECK corren con `.venv/bin/python` (3.11 + pytest 9) desde la
raiz del repo.

- [x] P0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-providers.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=48630b7361dd44ee870917b12c3d19b9d7bdea738aaca16bb04d4cab83b772d2; output-bytes=8

- [x] P1: los proveedores nuevos extraen empresas reales de HTML real
  CHECK: .venv/bin/python -m pytest tests/test_providers.py -q --no-header -s -k extrae
  EXPECT: /PROVEEDORES EXTRAEN/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=8a85b3d019c80e0e546b9bf8484384775f9bdaa44787c2b06857e1d68236a211; output-bytes=58

- [x] P2: sus URLs de resultado se decodifican (Brave y Mojeek envuelven o acortan)
  CHECK: .venv/bin/python -m pytest tests/test_providers.py -q --no-header -s -k urls
  EXPECT: /URLS DECODIFICADAS/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=b406bad39df31e2bad0eb07f08fca3ad3f4faa64ec1c627590041f725891724e; output-bytes=58

- [x] P3: el detector de bloqueo no marca como bloqueadas las paginas buenas de los motores nuevos
  CHECK: .venv/bin/python -m pytest tests/test_providers.py -q --no-header -s -k bloqueo
  EXPECT: /BLOQUEO OK EN MOTORES NUEVOS/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=a9ece12f4d22c003b5ebec633c82323adc599393132046b14b718b0013e8c074; output-bytes=65

- [x] P4: con 4 proveedores se descubren MAS empresas unicas que con 2, medido sobre el mismo corpus
  CHECK: .venv/bin/python bench/bench_providers.py --assert-improvement
  EXPECT: /COBERTURA VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=5e055ddff24a6dfda3c79dcceb41c05abf8a49c916fc22ee65554a125eb44240; output-bytes=427

- [x] P5: el repositorio NO contiene resolucion ni bypass de CAPTCHA
  CHECK: .venv/bin/python bench/check_no_captcha_bypass.py
  EXPECT: /SIN BYPASS DE CAPTCHA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=b57acdcf3b20c2ce85ead829e300b214fc3567ed636780905b649a1bbb1d9773; output-bytes=73

- [x] P6: la suite completa pasa sin tests desactivados tras sumar proveedores
  CHECK: .venv/bin/python bench/run_suite.py
  EXPECT: /SUITE VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=ed15c0d542c5ddc5a05f41b3f4e9539d7f4710af561686c66664d1e5db4c8b4b; output-bytes=834

- [x] P7: la decision de no renderizar queda documentada con su medicion
  CHECK: .venv/bin/python bench/check_findings.py
  EXPECT: /HALLAZGOS VERIFICADOS/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=468baddb0c25986f07f05da5007762af8ec766c16d36814a331e03c5852676d3; output-bytes=66
