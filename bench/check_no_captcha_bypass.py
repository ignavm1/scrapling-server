#!/usr/bin/env python3
"""P5 -- el repositorio NO resuelve ni esquiva CAPTCHAs.

Por que existe este chequeo y no solo una promesa en un README: la peticion que
origino esta tanda fue literalmente "hacer que Google no detecte nuestro
scrape". La respuesta correcta fue sumar fuentes que SI atienden, no romper el
desafio de las que no. Un chequeo ejecutable evita que esa linea se cruce mas
adelante por conveniencia, en un commit apurado.

Que se busca: uso de servicios de resolucion de captcha, del solver de
Cloudflare que trae la libreria, o de codigo propio que envie la respuesta de
un desafio.

Que NO se busca: la palabra "captcha" a secas. El detector de bloqueo TIENE que
nombrarla para reconocer una pagina de desafio y retirarse -- reconocer un
captcha es lo contrario de resolverlo.
"""
from __future__ import annotations
import pathlib
import re
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent

# Servicios comerciales de resolucion.
_SERVICIOS = (
    "2captcha", "anticaptcha", "anti-captcha", "capsolver", "capmonster",
    "deathbycaptcha", "captcha.guru", "solvecaptcha", "nopecha", "captchaai",
)

# APIs de resolucion dentro de la libreria o de terceros.
_APIS = (
    "solve_cloudflare",          # el solver que trae Scrapling
    "solve_captcha",
    "captcha_solver",
    "recaptcha_response",
    "g-recaptcha-response",
    "hcaptcha_response",
    "cf_clearance",              # cookie de bypass de Cloudflare
    "turnstile_token",
)

# Envio del formulario de desafio.
_ENVIO = re.compile(r"captcha[-_]?form[\"']?\s*\)?\s*\.\s*submit|submit\(\s*\)\s*;?\s*}?\s*//\s*captcha", re.I)

REVISAR = ["venara_discovery", "bench", "scrapling_server.py"]
# Este archivo se excluye: contiene la lista de patrones y se detectaria solo.
EXCLUIR = {"check_no_captcha_bypass.py"}


def _codigo_efectivo(txt: str) -> str:
    """Solo lo que el interprete EJECUTA: imports, nombres y literales.

    Descarta comentarios y docstrings a proposito. Mencionar "captcha" en una
    explicacion no es usarlo -- de hecho blocking.py TIENE que nombrarlos para
    reconocer un desafio y retirarse. Un escaner que confunde hablar de algo
    con hacerlo produce ruido y termina desactivado, que es peor que no tenerlo.
    """
    import ast
    try:
        arbol = ast.parse(txt)
    except SyntaxError:
        return txt          # ante la duda, se revisa todo

    docstrings = set()
    for n in ast.walk(arbol):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            d = ast.get_docstring(n, clean=False)
            if d:
                docstrings.add(d)

    piezas = []
    for n in ast.walk(arbol):
        if isinstance(n, ast.Import):
            piezas += [a.name for a in n.names]
        elif isinstance(n, ast.ImportFrom):
            piezas.append(n.module or "")
            piezas += [a.name for a in n.names]
        elif isinstance(n, ast.Name):
            piezas.append(n.id)
        elif isinstance(n, ast.Attribute):
            piezas.append(n.attr)
        elif isinstance(n, ast.Constant) and isinstance(n.value, str):
            if n.value not in docstrings:
                piezas.append(n.value)
    return "\n".join(piezas)


def escanear() -> list[str]:
    hallazgos = []
    for objetivo in REVISAR:
        p = RAIZ / objetivo
        archivos = [p] if p.is_file() else sorted(p.rglob("*.py"))
        for f in archivos:
            if f.name in EXCLUIR:
                continue
            codigo = _codigo_efectivo(f.read_text(encoding="utf-8", errors="ignore"))
            bajo = codigo.lower()
            for s in _SERVICIOS:
                if s in bajo:
                    hallazgos.append("%s usa el servicio de resolucion '%s'" % (f.name, s))
            for a in _APIS:
                if a.lower() in bajo:
                    hallazgos.append("%s invoca '%s'" % (f.name, a))
            if _ENVIO.search(codigo):
                hallazgos.append("%s envia un formulario de captcha" % f.name)
    return hallazgos


def control_positivo() -> bool:
    """El escaner detecta de verdad, o siempre dice que si?

    Se escribe un archivo con una llamada a un solver, se escanea, y se borra.
    Sin este control, un escaner con una ruta mal puesta certifica cualquier
    cosa como limpia.
    """
    trampa = RAIZ / "venara_discovery" / "_control_positivo_tmp.py"
    trampa.write_text("from twocaptcha import TwoCaptcha\nsolve_captcha()\n", encoding="utf-8")
    try:
        return bool(escanear())
    finally:
        trampa.unlink(missing_ok=True)


def main() -> None:
    if not control_positivo():
        sys.exit("el escaner no detecta un bypass plantado: no prueba nada")

    hallazgos = escanear()
    if hallazgos:
        print("BYPASS DETECTADO:")
        for h in hallazgos:
            print("  - " + h)
        sys.exit(1)

    # Y la contraparte: el detector de bloqueo SI tiene que reconocer captchas.
    bl = (RAIZ / "venara_discovery" / "blocking.py").read_text(encoding="utf-8")
    if "captcha" not in bl.lower():
        sys.exit("blocking.py ya no reconoce captchas: dejaria de retirarse ante un desafio")

    print("SIN BYPASS DE CAPTCHA (control positivo OK; el detector si los reconoce)")


if __name__ == "__main__":
    main()
