#!/usr/bin/env python3
"""G16 -- MEDIR si los perfiles de persona de LinkedIn estan en el indice.

Por que hace falta medirlo y no asumirlo: el pedido dice que la busqueda de
personas es prioritaria. Si la fuente no existe, construir mas queries encima no
cambia nada, y prometerlo seria vender humo.

Metodo, con la disciplina que exige una afirmacion de AUSENCIA:

  1. CONTROL POSITIVO en la misma sesion y motor: `site:linkedin.com/company`.
     Si el control tambien da cero, la corrida NO es concluyente -- el motor
     esta bloqueando, no es que los perfiles no existan. (Ese error casi lo
     cometo la primera vez: DuckDuckGo paso a captcha a mitad de la medicion.)
  2. Enfriamiento entre consultas para no provocar el bloqueo que invalidaria
     la medicion.
  3. Deteccion de bloqueo en cada respuesta.
"""
from __future__ import annotations
import pathlib
import re
import sys
import time
from urllib.parse import quote

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
from scrapling.fetchers import FetcherSession          # noqa: E402
from venara_discovery import blocking                  # noqa: E402

ESPERA = 12


def traer(url: str):
    with FetcherSession(impersonate="chrome") as s:
        p = s.get(url, stealthy_headers=True, timeout=20)
    h = p.html_content or ""
    return h, getattr(p, "status", None), blocking.analizar(h, getattr(p, "status", None))


def ddg(q):
    return "https://html.duckduckgo.com/html/?q=" + quote(q) + "&kl=us-en"


def bing(q):
    return "https://www.bing.com/search?q=" + quote(q) + "&count=20"


CONTROLES = [("ddg", ddg, 'site:linkedin.com/company marketing agency'),
             ("bing", bing, 'site:linkedin.com/company marketing agency')]
PRUEBAS = [
    ("ddg", ddg, 'site:linkedin.com/in "Fahrenheit DDB"'),
    ("ddg", ddg, 'site:linkedin.com/in founder Lima'),
    ("bing", bing, 'site:linkedin.com/in "Fahrenheit DDB" CEO'),
    ("bing", bing, 'linkedin.com/in founder marketing Lima'),
]


def contar(h):
    return (len(set(re.findall(r"linkedin\.com/in/([a-zA-Z0-9\-%_]+)", h))),
            len(set(re.findall(r"linkedin\.com/company/([a-zA-Z0-9\-%_]+)", h))))


def main():
    print("CONTROL POSITIVO (paginas de empresa)")
    control_ok = False
    for motor, mk, q in CONTROLES:
        h, st, v = traer(mk(q))
        ins, cos = contar(h)
        print("  %-5s status=%s bloqueado=%-5s /in=%d /company=%d" % (motor, st, v.bloqueado, ins, cos))
        if not v.bloqueado and cos > 0:
            control_ok = True
        time.sleep(ESPERA)

    if not control_ok:
        print("\nNO CONCLUYENTE: el control positivo no devolvio paginas de empresa.")
        print("Los motores estan bloqueando; no se puede afirmar nada sobre /in.")
        return 2

    print("\nPRUEBA (perfiles de persona)")
    total_in = 0
    for motor, mk, q in PRUEBAS:
        h, st, v = traer(mk(q))
        ins, cos = contar(h)
        total_in += ins
        print("  %-5s status=%s bloqueado=%-5s /in=%d  <- %s" % (motor, st, v.bloqueado, ins, q[:44]))
        time.sleep(ESPERA)

    print("\nRESULTADO: el control positivo funciono (hay /company) y las %d consultas"
          % len(PRUEBAS))
    print("de perfiles devolvieron %d perfiles en total." % total_in)
    print("CONCLUSION: los perfiles personales de LinkedIn %s en el indice publico."
          % ("SI estan" if total_in > 0 else "NO estan"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
