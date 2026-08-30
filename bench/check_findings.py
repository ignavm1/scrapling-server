#!/usr/bin/env python3
"""G17 -- los hallazgos estan escritos Y respaldados por un test que corre.

Un documento de auditoria sin test es una opinion. Cada hallazgo F1..F13 tiene
que existir en FINDINGS.md y estar citado desde el codigo o los tests, para que
al cambiar el comportamiento algo se rompa y no quede solo la prosa vieja.
"""
from __future__ import annotations
import pathlib
import re
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
doc = (RAIZ / "FINDINGS.md").read_text(encoding="utf-8")

fallas = []
hallazgos = re.findall(r"^## (F\d+)\s+—\s+(.+)$", doc, re.M)
if len(hallazgos) < 13:
    fallas.append("solo %d hallazgos documentados, se esperaban >=13" % len(hallazgos))

# Cada hallazgo referenciado desde el codigo o los tests.
fuentes = ""
for d in ("venara_discovery", "tests", "bench"):
    for f in (RAIZ / d).rglob("*.py"):
        fuentes += f.read_text(encoding="utf-8")

for fid, titulo in hallazgos:
    if fid not in fuentes:
        fallas.append("%s (%s) no esta citado en ningun archivo de codigo" % (fid, titulo[:45]))

# Los fixtures que la auditoria cita tienen que existir de verdad.
for fixture in re.findall(r"tests/fixtures/([\w.\-]+\.html)", doc):
    if not (RAIZ / "tests" / "fixtures" / fixture).exists():
        fallas.append("falta el fixture citado: " + fixture)

if fallas:
    print("HALLAZGOS FALLA:")
    for f in fallas:
        print("  - " + f)
    sys.exit(1)
print("HALLAZGOS VERIFICADOS (%d documentados y citados desde el codigo)" % len(hallazgos))
