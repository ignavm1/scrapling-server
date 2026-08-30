# Gates: Company + Decision Maker Discovery Engine

OWNS: venara_discovery/**, tests/**, bench/**, scrapling_server.py, Dockerfile, requirements.txt, requirements-dev.txt, README.md, FINDINGS.md

Scope: convertir el servidor de scraping en un motor de descubrimiento de empresas
que reporte honestamente cuando esta bloqueado, deduplique entidades reales,
filtre falsos positivos, resuelva websites y rankee por calidad — sin romper el
contrato que ya consume Venara.

Toolchain: los CHECK corren con `.venv/bin/python` (Python 3.11 + pytest 9) desde
la raiz del repo. Un `python3` del sistema (3.9) NO sirve: el codigo usa sintaxis
3.10+. Declararlo aca hace que un mismatch de entorno se vea como fallo y no como
evidencia.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=48630b7361dd44ee870917b12c3d19b9d7bdea738aaca16bb04d4cab83b772d2; output-bytes=8

- [x] G1: una pagina de captcha se clasifica como BLOQUEO, y una de resultados reales NO
  CHECK: .venv/bin/python -m pytest tests/test_blocking.py -q --no-header -s
  EXPECT: /BLOCKING VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=3ee89a9be6d2536360dbbefcc407211ec22a5e0424ca8f6dab87b34d3960515d; output-bytes=55

- [x] G2: con todos los proveedores bloqueados la API reporta el bloqueo en vez de "cero resultados"
  CHECK: .venv/bin/python -m pytest tests/test_api_contract.py -q --no-header -s -k blocked
  EXPECT: /CONTRATO BLOQUEO VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=ad24ed143ac70fff692066d4324dda4316658e0f97fa9980c450ad7b3a7f5892; output-bytes=579

- [x] G3: el cuerpo camelCase que manda Venara hoy controla de verdad el tope de resultados
  CHECK: .venv/bin/python -m pytest tests/test_api_contract.py -q --no-header -s -k maxresults
  EXPECT: /MAXRESULTS VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=dd414857e266857b71477afa0e01ef66fd1bb2eb76b17eeeeff6cf308e7574b0; output-bytes=572

- [x] G4: la extraccion sobrevive a un cambio de HTML del buscador
  CHECK: .venv/bin/python -m pytest tests/test_extraction.py -q --no-header -s
  EXPECT: /EXTRACCION VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=eeebf645509c7e69fdd42ca53e06669d726a18276465abeaed43eaf5ff21bb1c; output-bytes=58

- [x] G5: la misma empresa vista en 4 formas se une en 1, y dos empresas distintas en el mismo hosting NO se unen
  CHECK: .venv/bin/python -m pytest tests/test_entity.py -q --no-header -s
  EXPECT: /ENTITY RESOLUTION VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=f31873b708025044c6e174ea3c843c8c1de0ae58fcc14964ce3207efa18274f9; output-bytes=61

- [x] G6: directorios, blogs, PDFs, redes y agregadores quedan fuera; empresas reales quedan dentro
  CHECK: .venv/bin/python -m pytest tests/test_filtering.py -q --no-header -s
  EXPECT: /FILTRADO VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=6388322bfa2b4b0306e40077c51d940c42d4372dd09b19d90bd5959a23b8d701; output-bytes=67

- [x] G7: el ranking pone las empresas buenas arriba en un fixture etiquetado a mano
  CHECK: .venv/bin/python -m pytest tests/test_scoring.py -q --no-header -s
  EXPECT: /SCORING VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=db30228178412be62e04e21d2c2c58bb5f6beac59715358270217a5889c522cf; output-bytes=45

- [x] G8: una empresa vista solo en LinkedIn recupera su website oficial por otra fuente
  CHECK: .venv/bin/python -m pytest tests/test_website_resolution.py -q --no-header -s
  EXPECT: /WEBSITE RESOLUTION VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=767e9293391129972809965e98670b760c56bbecda137edee8619ee1f4326121; output-bytes=56

- [x] G9: la cache nunca guarda un resultado bloqueado ni sirve datos vencidos, y no crece sin limite
  CHECK: .venv/bin/python -m pytest tests/test_cache.py -q --no-header -s
  EXPECT: /CACHE VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=6283786127f3ba1b51a970da1b6352af90ad6b76fd07262fa187fddb3d8215a3; output-bytes=44

- [x] G10: SSRF cerrado: metadata, IP privadas, rebinding DNS y redirects hacia interno
  CHECK: .venv/bin/python -m pytest tests/test_security.py -q --no-header -s
  EXPECT: /SEGURIDAD VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=d656a3874a9394e0f4992f069031593336efe0e7f8dcd2c556c78c23911521b9; output-bytes=74

- [x] G11: los 4 endpoints existentes mantienen su schema (el cliente de Venara no se rompe)
  CHECK: .venv/bin/python -m pytest tests/test_backcompat.py -q --no-header -s
  EXPECT: /RETROCOMPATIBILIDAD VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=9511c61fdf69a619475bcc449cea7ff453f6853aa4b1be8d9ebcb076acdc3293; output-bytes=572

- [x] G12: el benchmark mide antes-vs-despues sobre el mismo corpus y el motor nuevo gana en precision
  CHECK: .venv/bin/python bench/benchmark.py --assert-improvement
  EXPECT: /BENCHMARK VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=80a90668af8981ac06c7029ac0c3d9fd7f4ca6a30791a762ab7bc4d4d29f681f; output-bytes=547

- [x] G13: la suite completa pasa y ningun test quedo desactivado
  CHECK: .venv/bin/python bench/run_suite.py
  EXPECT: /SUITE VERIFICADA/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=bbaeea15c9948bb215edc6a9f663260964999f9e7114b9c86ca4aa0c4b8a9457; output-bytes=834

- [x] G14: el Dockerfile lleva todo lo necesario y el entrypoint del contenedor sirve /health
  CHECK: .venv/bin/python bench/check_docker.py
  EXPECT: /DOCKER VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=3da1de073bb7815148a4da53325f6de7d06320be023f29b5359fab51b8839282; output-bytes=314

- [x] G15: LinkedIn de empresa se resuelve y se valida; se rechazan perfiles personales, posts y jobs
  CHECK: .venv/bin/python -m pytest tests/test_linkedin.py -q --no-header -s
  EXPECT: /LINKEDIN VERIFICADO/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=be92d6f3bdc4c44ca4af50df1a523ac23b07071c35e96544e6d4ec3ac8634dbf; output-bytes=48

- [x] G16: la ausencia de perfiles de persona esta MEDIDA con control positivo, no asumida
  CHECK: .venv/bin/python -m pytest tests/test_linkedin.py -q --no-header -s -k MEDICION
  EXPECT: /1 passed/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=ba4b3d13f36b759b7da4a3a22286cfd01370a14cb227fb4f4b4788dd782603da; output-bytes=34

- [x] G17: los hallazgos de la auditoria quedan escritos con su evidencia reproducible
  CHECK: .venv/bin/python bench/check_findings.py
  EXPECT: /HALLAZGOS VERIFICADOS/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=468baddb0c25986f07f05da5007762af8ec766c16d36814a331e03c5852676d3; output-bytes=66

- [x] G18: la segunda pasada de edge cases y seguridad esta fijada como tests
  CHECK: .venv/bin/python -m pytest tests/test_edge_cases.py -q --no-header -s
  EXPECT: /EDGE CASES VERIFICADOS/
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e6813ac092b6/22 entries; EXPECT=matched; output-sha256=13850523c4166c39cdf4e035f909467a80573cacbbdb33f3cd62ba6b187aa97a; output-bytes=86
