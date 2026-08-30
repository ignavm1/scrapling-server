#!/usr/bin/env python3
"""P4 -- sumar fuentes descubre MAS empresas unicas, sobre el mismo corpus.

La metrica es empresas UNICAS tras entity resolution, no filas devueltas: dos
motores que repiten los mismos diez sitios no aportan nada y solo gastan
requests. Lo que justifica un proveedor nuevo es lo que trae que ningun otro
trajo.
"""
from __future__ import annotations
import argparse
import pathlib
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from venara_discovery import extraction, filtering, scoring          # noqa: E402
from venara_discovery.entity import Empresa, Resolutor               # noqa: E402
from venara_discovery.location import interpretar                    # noqa: E402
from venara_discovery.normalize import mejor_nombre                  # noqa: E402

FIX = RAIZ / "tests" / "fixtures"
QUERY = "agencia de marketing digital"
UBI = interpretar("Lima, Peru")
CTX = QUERY + " Lima Peru"

# (fixture, motor) por proveedor. Solo paginas NO bloqueadas: comparar
# cobertura usando una pagina de captcha mediria el bloqueo, no la fuente.
POR_PROVEEDOR = {
    "duckduckgo": [("ddg_companies.html", "duckduckgo")],
    "bing": [("bing_ok.html", "bing")],
    "brave": [("brave_ok.html", "brave")],
    "ddglite": [("ddglite_ok.html", "ddglite")],
}


def descubrir(nombres: list[str]) -> set[str]:
    """Empresas unicas (por dominio o LinkedIn) que aporta ese conjunto."""
    r = Resolutor()
    for prov in nombres:
        for fixture, motor in POR_PROVEEDOR[prov]:
            html = (FIX / fixture).read_text(encoding="utf-8")
            items, _ = extraction.extraer(None, html, motor)
            for it in items:
                if filtering.motivo_descarte(it["url"], it["titulo"]):
                    continue
                nombre = mejor_nombre(it["titulo"], it["url"], CTX)
                if not nombre:
                    continue
                r.agregar(Empresa(
                    nombre=nombre, website=it["url"], descripcion=it["snippet"],
                    texto_fuente=(it["titulo"] + " " + it["snippet"]).strip()[:400],
                    fuentes={motor}, queries={QUERY}))
    rank, _ = scoring.rankear(r.empresas(), QUERY, UBI, limite=50)
    return {e.dominio or ("li:" + e.clave_linkedin) for e in rank if e.dominio or e.clave_linkedin}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--assert-improvement", action="store_true")
    args = ap.parse_args()

    antes = descubrir(["duckduckgo", "bing"])
    despues = descubrir(["duckduckgo", "bing", "brave", "ddglite"])
    nuevas = despues - antes
    perdidas = antes - despues

    print("Cobertura sobre el mismo corpus de HTML real\n")
    print("  %-34s %s" % ("2 proveedores (ddg + bing)", len(antes)))
    print("  %-34s %s" % ("4 proveedores (+ brave + ddglite)", len(despues)))
    print("  %-34s %s" % ("empresas que solo aportan los nuevos", len(nuevas)))
    print()
    for d in sorted(nuevas)[:10]:
        print("     +", d)

    if not args.assert_improvement:
        return

    fallas = []
    if len(despues) <= len(antes):
        fallas.append("sumar proveedores no aumento la cobertura (%d -> %d)" % (len(antes), len(despues)))
    if len(nuevas) < 3:
        fallas.append("los proveedores nuevos solo aportaron %d empresas propias" % len(nuevas))
    if perdidas:
        fallas.append("se PERDIERON %d empresas al sumar fuentes: %s" % (len(perdidas), sorted(perdidas)[:5]))
    if fallas:
        for f in fallas:
            print("FALLA: " + f)
        sys.exit(1)
    print("\nCOBERTURA VERIFICADA")


if __name__ == "__main__":
    main()
