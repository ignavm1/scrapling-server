# Gates: los seis defectos medidos sobre agencias reales

OWNS: venara_discovery/personas.py, venara_discovery/filtering.py, venara_discovery/decisor.py, venara_discovery/linkedin_perfil.py, tests/test_defectos.py, GATES-defectos.md

Scope: la corrida sobre 30 agencias de marketing devolvio 9 candidatos y 4 eran
basura. Cada defecto de abajo tiene un caso REAL de esa corrida, y ese caso es
el test. No se arregla lo que no se vio fallar.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-defectos.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=98f5e1cd112c12b65e7dc3bc2e71acfaff91f3fad8ed67fadd07e451af3720c9; output-bytes=159

- [x] G1: el nombre de la EMPRESA no entra como persona -- "Flama Creators" en flamacreators.com se descarta -- sin descartar a una persona cuyo apellido se parezca al de la empresa
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "empresa_como_persona or apellido_parecido" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ea9cccf656a4debc1b77180d54babb011cc85588eb2f8b5755d1fa80c99ac6c1; output-bytes=119

- [x] G2: un cargo con forma de VACANTE no pasa como cargo real -- "Director/a de Estrategia" es una oferta de empleo, no alguien que ocupa el puesto
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "vacante" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=a893b43278f3de28410b1459cb2e322c64b78708f3707bd37151d0104be3aaf7; output-bytes=119

- [x] G3: un directorio de agencias no pasa como agencia -- agencies.semrush.com se descarta -- con control de que una agencia real sigue pasando
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "directorio" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=a31ab40c5b3b59c6454d26d910a1bef2506fa1d25db5d057f2e51f2e14f775d8; output-bytes=119

- [x] G4: un dominio de otro pais no se acepta cuando se pidio uno concreto -- un .es no entra en una busqueda de Santiago -- y un dominio neutro (.com) sigue entrando
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "pais_dominio" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=329439002ca0dce421c7e64da3c254feeb951afb7685e4a7148fe6a6e115469c; output-bytes=119

- [x] G5: el homonimo entre paises se detecta AUNQUE el candidato no venga de LinkedIn, usando el pais del dominio de la fuente
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "homonimo" && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=5a939115cff29af66426947e78d3a4ed3f4cc9bea635a4368ffad647009cec7c; output-bytes=119

- [x] G6: un nombre que solo aparece en UNA fuente de tercero se marca como no confirmado, para que no se le escriba por nombre sin mirar -- sin inventar correcciones de ortografia
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "confirmado" && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=6ac52419ca1f968c24dd1043d76b62c277b1c10ed7c3f707fc9a869492f67ece; output-bytes=119

- [x] G7: los cuatro candidatos basura de la corrida real ya no pasan, y los cinco buenos siguen pasando
  CHECK: .venv/bin/python -m pytest -q tests/test_defectos.py -k "corrida_real" && echo G7_OK
  EXPECT: G7_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=a9e9644664b8f53f7333cf93119c20a538e42d196a10d7c2788d1fb5dba2dadf; output-bytes=119

- [x] G8: la suite completa del servidor sigue verde
  CHECK: .venv/bin/python -m pytest -q && echo G8_OK
  EXPECT: G8_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ec1fd7783a3d223345d9dae416eaaa1ef1cc335982df06a6f4b7703875eb66cf; output-bytes=1102

- [x] G9: lo arreglado queda registrado con lo que quedo sin arreglar y por que
  EVIDENCE: Los seis defectos de la corrida del 2026-09-06 sobre 30 agencias (9 candidatos, 4 basura) estan corregidos y fijados con el caso REAL de cada uno: (1) el nombre de la empresa entrando como persona -- "Flama Creators" en flamacreators.com, que ademas genero un email inventado por patron; se compara por IGUALDAD contra dominio y nombre, no por contencion, para no descartar a quien tiene el apellido en el nombre de su empresa (suele ser el fundador). (2) cargo con forma de vacante -- "Director/a de Estrategia Digital". (3) directorio como agencia -- agencies.semrush.com, mas theorg/craft/g2/capterra/producthunt/wellfound. (4) y (5) pais del dominio, que cubre a la vez el .es en una busqueda de Santiago y el homonimo "Houm" chileno vs indio SIN depender de LinkedIn. (6) ortografia no confirmada -- "Eduardo Dillamajora" por Della Maggiora: NO se corrige, se marca `name_confirmed: false` cuando el nombre viene de una sola fuente de tercero. DEFECTO PROPIO ENCONTRADO ESCRIBIENDO EL ARREGLO: la primera version del pais usaba lista blanca de TLDs y `.in` no estaba, asi que yourstory.in -- el directorio indio del caso Houm -- seguia pasando como neutro. Se cambio a regla: todo TLD de dos letras es pais salvo los genericos (.co, .io, .ai, .me...). LO QUE QUEDA SIN ARREGLAR A PROPOSITO: no se corrige la ortografia de un nombre mal escrito en la fuente. Adivinar introduce un error peor que el que resuelve -- cambiar un apellido que SI estaba bien -- y el ranking ya prefiere el sitio propio (1.0) sobre un tercero (0.65). SIN MEDIR: cuanto sube la calidad en una corrida real. Los arreglos estan verificados contra los casos medidos, pero la proxima corrida sobre 30 agencias es la que dice si aparecen defectos nuevos. 441 tests.
