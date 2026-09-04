# Gates: siete angulos y eleccion por cargo

OWNS: venara_discovery/decisor.py, venara_discovery/cargos.py, venara_discovery/api.py, tests/test_cargos.py, tests/test_angulos.py, bench/medir_angulos.py, bench/medir_angulos.sh, GATES-angulos.md

Scope: llevar el plan de busqueda de la persona a SIETE angulos distintos, cada
uno apuntando a una superficie que los demas no alcanzan, y elegir entre todos
los candidatos al que tiene el MEJOR CARGO -- el que de verdad decide -- en vez
de al que casualmente tuvo mejor evidencia.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-angulos.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=6d4642a5a8b31b0f41d53fd55dd02c3dd0000fe5ccea27c9566b2baf22d23a87; output-bytes=158

- [x] G1: el plan tiene SIETE angulos con nombre distinto, ninguno repite la query de otro, y los dos nuevos apuntan a superficies que los cinco viejos no cubrian
  CHECK: .venv/bin/python -m pytest -q tests/test_angulos.py -k "siete or distintos or nuevos" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ef659f3dd658fcf22124fdd71bf0dbbce0c5be021fe6258e122b06609227dc40; output-bytes=118

- [x] G2: los siete angulos llegan a EJECUTARSE con el techo de fetches real, no solo a existir en el plan
  CHECK: .venv/bin/python -m pytest -q tests/test_angulos.py -k "ejecutan or techo" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=159157919571e519d08899e346aa9c03ca8ad8728fa0e5f4bb00fc3ac371731c; output-bytes=118

- [x] G3: el cargo se clasifica en niveles y un fundador vale mas que un gerente de area, con control de que la escala no colapsa todo al mismo nivel
  CHECK: .venv/bin/python -m pytest -q tests/test_cargos.py -k "nivel or escala" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ca40aff5407d462f657cc5f8d84080dc851c41e33fed411a1057a7c3873bc0fe; output-bytes=119

- [x] G4: entre varios candidatos se devuelve primero al del MEJOR CARGO, aunque otro tenga mejor evidencia
  CHECK: .venv/bin/python -m pytest -q tests/test_cargos.py -k "elige or mejor_cargo" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=b64a95d71bb7ace0fcfe15a8200ec569088e000d880bb57c232f35f77b269e73; output-bytes=119

- [x] G5: un cargo alto sin evidencia suficiente NO desplaza a uno mas bajo bien respaldado -- hay un piso, y se demuestra que sin el la eleccion se rompe
  CHECK: .venv/bin/python -m pytest -q tests/test_cargos.py -k "piso or basura" && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=e56e544f7bec5bf32898168e71ef3b75b1b32ad1218fd157bdcd971becc6cebf; output-bytes=119

- [x] G6: el resolutor y el endpoint devuelven al mejor cargo como `person` y el resto como alternativas ordenadas, sin romper los caminos que ya funcionaban
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py tests/test_linkedin_perfil.py tests/test_contacto.py && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=cbab5e93812eff89d153431a776a7a154b6c6d7ebb944ece81e67bcec66e3e83; output-bytes=701

- [x] G7: la suite completa del servidor sigue verde
  CHECK: .venv/bin/python -m pytest -q && echo G7_OK
  EXPECT: G7_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=baba2373adac04bd2069dc9c7b9fe3aa4c5efbcb0fed74bce319f977eb5f0ffa; output-bytes=1022

- [x] G8: la medicion en vivo dice que angulo aporto cada decisor y si el elegido fue el de mejor cargo, con veredicto decisivo por empresa
  CHECK: bash bench/medir_angulos.sh
  EXPECT: MEDICION REGISTRADA
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=9283cee626bd8e4927fcd658f9c3a2ebe8d54759137e172efc74544860d17786; output-bytes=20803

- [x] G9: los numeros medidos quedan registrados con su limitacion declarada
  EVIDENCE: Medicion del 2026-09-04 sobre 6 empresas chilenas reales, IP residencial SIN proxy. ANGULOS: de 5 a 7 (se agregan directorio_ejecutivo y representante_legal); los siete se EJECUTAN con el techo real (12 fetches, reparto por rondas), verificado por test que reproduce el reparto del resolutor y que falla si el techo baja del numero de angulos. ELECCION POR CARGO: implementada en dos pasos -- nivel de cargo primero (fundador > ejecutivo > c_level > area > mando > otro), evidencia solo para desempatar dentro del nivel, con piso de 0.55 para que un cargo alto sin respaldo no desplace a uno solido; hay un control que demuestra que SIN el piso la eleccion se rompe. RESULTADO EN VIVO: 2 de 6 resueltas (Fintual: Omar Larre, Co-founder & CIO, nivel fundador, score 1.0; Xepelin: Sebastian Kreis, CEO, nivel fundador, 0.975), las dos por el sitio propio y las dos con el cargo mas alto disponible. Las otras 4 sin acceso. DEFECTO PROPIO ENCONTRADO POR ESTA MEDICION Y CORREGIDO: cuatro empresas reportaban "no_publicado" con el presupuesto agotado a los 25s -- un timeout no marca proveedor bloqueado, asi que desaparecia en silencio y el veredicto mentia (F1/F4 por otra puerta). Se agrego el motivo sin_acceso y el conteo de fetches fallidos; registrado como F26.4. LIMITACION DECLARADA: los angulos nuevos (directorio_ejecutivo, representante_legal) NO pudieron validarse en vivo -- en esta corrida los proveedores estaban en captcha o timeout desde esta IP tras dias de medicion, y sin buscadores esos dos angulos no tienen por donde correr; su comportamiento esta fijado con oraculos deterministas pero su RENDIMIENTO real queda sin medir hasta que haya PROXY_URL. Registrado en FINDINGS.md F26 y en MEDICION-angulos.md.
