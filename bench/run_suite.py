#!/usr/bin/env python3
"""G13 -- la suite entera pasa y NINGUN test quedo desactivado.

El segundo chequeo importa tanto como el primero: una suite verde con tests
saltados es una suite que no prueba lo que dice probar, y ese es justo el modo
en que una verificacion se vuelve decorativa.
"""
from __future__ import annotations
import pathlib
import re
import subprocess
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
PY = str(RAIZ / ".venv" / "bin" / "python")

r = subprocess.run([PY, "-m", "pytest", "tests/", "-q", "--no-header"],
                   cwd=RAIZ, capture_output=True, text=True)
salida = r.stdout + r.stderr
print(salida[-2500:])

if r.returncode != 0:
    sys.exit("la suite fallo (exit %d)" % r.returncode)

# Nadie desactivado.
for marca in ("skip", "xfail", "deselected"):
    if re.search(r"\d+\s+" + marca, salida):
        sys.exit("hay tests %s: la suite no prueba lo que dice" % marca)

fuentes = list((RAIZ / "tests").glob("test_*.py"))
for f in fuentes:
    txt = f.read_text(encoding="utf-8")
    for prohibido in ("@pytest.mark.skip", "@pytest.mark.xfail", "pytest.skip("):
        if prohibido in txt:
            sys.exit("%s contiene %s" % (f.name, prohibido))

m = re.search(r"(\d+) passed", salida)
n = int(m.group(1)) if m else 0
if n < 80:
    sys.exit("solo %d tests: la cobertura declarada no esta" % n)

print("SUITE VERIFICADA (%d tests, %d archivos, 0 desactivados)" % (n, len(fuentes)))
