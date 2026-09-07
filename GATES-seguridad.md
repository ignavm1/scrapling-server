# Gates: los tres hallazgos de la auditoria

OWNS: venara_discovery/api.py, venara_discovery/fetch.py, venara_discovery/security.py, venara_discovery/config.py, tests/test_seguridad_cso.py, GATES-seguridad.md

Scope: cerrar los tres hallazgos del /cso del 2026-09-07. Los dos criticos se
encadenan -- la auth que falla abierta expone el SSRF a internet -- asi que se
arreglan juntos o no sirve.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-seguridad.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=428d00263e0ba0ece9b048817e3ae8f8bc68312de0508cb2835e29a3ce6d9aa4; output-bytes=160

- [x] G1: sin API_KEY configurada el servidor RECHAZA en vez de dejar pasar, y lo dice con un motivo que se puede diagnosticar
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "fail_closed or sin_api_key" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=66ad56790afc665738eaa76294837f09c096f2432883462f84d6424ff59c5375; output-bytes=634

- [x] G2: con API_KEY configurada, la key correcta pasa y la incorrecta o ausente recibe 401 -- el arreglo no puede romper al cliente legitimo
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "key_correcta or key_incorrecta" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=9af94bc256df1ad0f3e541f7f4a20f047283da2a38e353b3258a7a712947ea0e; output-bytes=634

- [x] G3: existe un escape explicito para desarrollo local, y NO se activa solo -- tiene que declararlo el operador
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "escape_local" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=349e8099a232a26a7fd01f677b9c44ce33a0fefec2400a47e7257694e755533d; output-bytes=634

- [x] G4: fetch.obtener rechaza una URL que apunta a la red interna -- es el cuello unico por donde salen las 15 llamadas, asi que se valida ahi y no en cada sitio
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "obtener_bloquea or metadata" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=1214a7c71078061a7e560d13b6699c99d5c6555a7d1bab981c7e11aebe8e8522; output-bytes=635

- [x] G5: el campo domain del request se valida ANTES de llegar al resolutor, con control de que un dominio legitimo sigue pasando
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "domain_valida" && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ff55c50588f11ac5728678a8e093ea824d301273e35a8e61340f016deaf3c366; output-bytes=635

- [x] G6: url_con_ip_fijada ya NO es codigo muerto: esta llamada, y hay un control que falla si alguien la vuelve a dejar sin llamadas
  CHECK: .venv/bin/python -m pytest -q tests/test_seguridad_cso.py -k "rebinding or codigo_muerto" && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=7d5b707545e623b869ab65318e2825d23d74c8b92040988bfffb2a95ac092caa; output-bytes=634

- [x] G7: la suite completa del servidor sigue verde -- el arreglo no puede romper el scraping legitimo
  CHECK: .venv/bin/python -m pytest -q && echo G7_OK
  EXPECT: G7_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=70dba2d2ee2423998552efc83837cfccdcaeffb9876853491fa8ecf03c9f597c; output-bytes=1102

- [ ] G8: verificado en vivo contra el servidor real que los endpoints ya NO responden sin credencial
  EVIDENCE: pending
