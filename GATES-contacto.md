# Gates: sacar el contacto del decisor

OWNS: venara_discovery/contacto.py, venara_discovery/decisor.py, venara_discovery/api.py, venara_discovery/config.py, tests/test_contacto.py, tests/fixtures/sintetico_contacto*, bench/medir_contacto.py, bench/medir_contacto.sh, GATES-contacto.md

Scope: una vez confirmado el decisor, entregar POR QUE CANAL alcanzarlo -- email
y telefono -- sacandolos del sitio de la empresa, deduciendo el patron de correo
solo cuando hay una muestra real del dominio, y diciendo SIEMPRE de donde salio
cada dato. Nunca se inventa un buzon: la regla del repo es que un patron
adivinado mas un nombre es loteria, y cada rebote degrada el dominio del cliente.

- [x] G0: este ledger declara resultados que pueden fallar
  CHECK: node /Users/ignaciovenegas/.claude/skills/unlazy/scripts/gate-lint.mjs GATES-contacto.md
  EXPECT: LINT OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=ecae37477280f05eccaf76ccc14fa1bdbb355a3120fb5022fd45fc2d469f51bf; output-bytes=159

- [x] G1: si el sitio publica el email de ESA persona, se devuelve tal cual con fuente "publicado", y un email de otro empleado NO se le atribuye
  CHECK: .venv/bin/python -m pytest -q tests/test_contacto.py -k "publicado or otro_empleado" && echo G1_OK
  EXPECT: G1_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=2cfa0e48a8f7cc4bd3ee7a7c8e6485bc1530abc264aa0fbd72fa2abb05cf9578; output-bytes=119

- [x] G2: con una muestra real del dominio se deduce el patron y se construye el email de la persona citando la muestra; SIN muestra no se construye ninguno
  CHECK: .venv/bin/python -m pytest -q tests/test_contacto.py -k "patron or muestra" && echo G2_OK
  EXPECT: G2_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=964b98ce10fd4314fb2bf8fd2174756e57ca0a723484120c1417ab161857b5b4; output-bytes=119

- [x] G3: cuando no hay nada personal se devuelve el buzon generico etiquetado como generico, nunca como si fuera de la persona
  CHECK: .venv/bin/python -m pytest -q tests/test_contacto.py -k "generico" && echo G3_OK
  EXPECT: G3_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=5eb19ce2e48f413c451d3340a0531b9c4c5344c5a67b246618d72da4388d7d7b; output-bytes=119

- [x] G4: el telefono se normaliza a E.164 y se distingue movil de fijo, porque un fijo no tiene WhatsApp y guardarlo como si lo tuviera genera una tarea que nunca llega
  CHECK: .venv/bin/python -m pytest -q tests/test_contacto.py -k "telefono or movil or whatsapp" && echo G4_OK
  EXPECT: G4_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=99827cf0769076cb87569d9563a1b57151db2a960a58a55454f5a1da5cfd8193; output-bytes=119

- [x] G5: ningun contacto se devuelve sin su procedencia y su confianza -- un dato sin origen no se puede auditar cuando rebota
  CHECK: .venv/bin/python -m pytest -q tests/test_contacto.py -k "procedencia or evidencia or auditar" && echo G5_OK
  EXPECT: G5_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=8e8b35f624873493b0201e4401870ae514a9bf57e34b68ffa7f50ea84ad20c9a; output-bytes=119

- [x] G6: el resolutor entrega el contacto junto al decisor y el endpoint lo expone, sin romper el camino del sitio propio ni el angulo de LinkedIn
  CHECK: .venv/bin/python -m pytest -q tests/test_decisor.py tests/test_linkedin_perfil.py -k "contacto or endpoint or sitio or linkedin" && echo G6_OK
  EXPECT: G6_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=b6b12bf3e6a857b6838fc4a5691914fb1441c54847dcdcab6c2a1e4e8ca46d4e; output-bytes=635

- [x] G7: la suite completa del servidor sigue verde
  CHECK: .venv/bin/python -m pytest -q && echo G7_OK
  EXPECT: G7_OK
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=7b20d475ae1aa5f74efa428f870beb86024abc4d153a723349593943bca522c2; output-bytes=1022

- [x] G8: la medicion en vivo sobre empresas reales dice cuantos decisores quedaron con canal de contacto y de que tipo, con veredicto decisivo por empresa
  CHECK: bash bench/medir_contacto.sh
  EXPECT: MEDICION REGISTRADA
  EVIDENCE: exit=0; shell=/bin/sh; cwd=/Users/ignaciovenegas/Desktop/scrapling-server; path=e95c5e8a51af/24 entries; EXPECT=matched; output-sha256=afd9eca055e6f41341bb0e72a9cb6ec697489c7982eeaaba64f79eec13b5a9c9; output-bytes=20733

- [x] G9: los numeros medidos quedan registrados con su limitacion declarada
  EVIDENCE: Medicion del 2026-09-04 sobre 6 empresas chilenas reales, IP residencial SIN proxy. RESULTADO CRUDO: 2 decisores encontrados (Omar Larre / Fintual, Sebastian Kreis / Xepelin) y CERO alcanzables. DIAGNOSTICO VERIFICADO sobre el HTML servido, no asumido: fintual.cl entrega 20.000 chars de texto con 0 mailto y 0 emails; xepelin.com 6.954 chars con 0 mailto y 0 emails; ninguna de las dos enlaza una pagina de contacto desde la home. No es fallo del parser -- esas empresas no publican correo, usan formulario. Registrado como F25. RESPUESTA IMPLEMENTADA: (a) si la home no enlaza contacto se prueban /contacto y /contact; (b) si el sitio no publica ninguna direccion se busca "@<dominio>" en los buscadores, porque con UNA muestra real la convencion queda deducida y toda persona de esa empresa sale gratis. LIMITACION DECLARADA: los dos caminos estan verificados con oraculos deterministas (tests que fijan el comportamiento con red simulada), pero NO se pudieron validar en vivo -- en la corrida los cuatro proveedores estaban en captcha o timeout desde esta IP tras horas de medicion, que es el mismo cuello de botella de F23. Sin PROXY_URL el sistema se queda sin buscadores, y aqui eso significa quedarse sin la muestra. REGLA RESPETADA: no se construye ningun email sin muestra del dominio; cada dato viaja con email_source y email_confidence para que Venara decida que envia.
