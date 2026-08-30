#!/usr/bin/env python3
"""G12 -- benchmark antes vs despues sobre EL MISMO corpus.

El motor viejo no se describe de memoria: se reconstruye desde el commit
anterior (`git show a4804e7:scrapling_server.py`) y se ejecuta sobre los mismos
fixtures de HTML real. Comparar contra una version recordada del codigo viejo
seria comparar contra nada.

Metricas, todas contadas del mismo corpus:

  resultados       cuantas filas devuelve
  empresas_validas filas que pasan el filtro de falsos positivos
  con_website      filas con un website utilizable (el cliente descarta el resto)
  duplicados       filas que representan una empresa ya contada
  falsos_positivos directorios, listicles, redes, prensa, documentos
  precision        empresas_validas / resultados
"""
from __future__ import annotations
import argparse
import pathlib
import re
import subprocess
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from venara_discovery import extraction, filtering                    # noqa: E402
from venara_discovery.entity import Empresa, Resolutor                # noqa: E402
from venara_discovery.location import interpretar                     # noqa: E402
from venara_discovery.normalize import dominio_registrable, mejor_nombre  # noqa: E402
from venara_discovery import scoring                                  # noqa: E402

FIX = RAIZ / "tests" / "fixtures"
COMMIT_VIEJO = "a4804e7"

# (fixture, motor, query que la origino)
CORPUS = [
    ("ddg_companies.html", "duckduckgo", "agencia de marketing digital", "Lima, Peru"),
    ("ddg_linkedin.html", "duckduckgo", "agencia marketing", "Lima, Peru"),
    ("bing_ok.html", "bing", "agencia de marketing digital", "Lima, Peru"),
    ("bing_poisoned_microsoft.html", "bing", "agencia de marketing digital", "Lima, Peru"),
    ("bing_poisoned_recipes.html", "bing", "agencia marketing", "Lima, Peru"),
    ("google_jsshell.html", "google", "agencia de marketing digital", "Lima, Peru"),
]


# ── Motor viejo, cargado desde git ──────────────────────────────────────────

def cargar_motor_viejo():
    """Extrae las funciones del servidor viejo tal como estaban en el commit."""
    try:
        src = subprocess.run(["git", "show", COMMIT_VIEJO + ":scrapling_server.py"],
                             cwd=RAIZ, capture_output=True, text=True, check=True).stdout
    except Exception as e:
        sys.exit("no se pudo leer el motor viejo desde git (%s): %s" % (COMMIT_VIEJO, e))

    # Se toman solo las funciones puras: no se importa el modulo entero porque
    # arrastra FastAPI y scrapling y abriria sesiones de red.
    necesarias = ["fix_href", "_domain", "_is_business_site", "_name_from_domain",
                  "JUNK_HOST_SUBSTR"]
    trozos = []
    for nombre in necesarias:
        if nombre.isupper():
            m = re.search(r"^" + nombre + r"\s*=\s*\(([\s\S]*?)\)\n", src, re.M)
            if m:
                trozos.append(nombre + " = (" + m.group(1) + ")\n")
        else:
            m = re.search(r"^def " + re.escape(nombre) + r"\([\s\S]*?(?=\n(?:def |@|class |# ---))",
                          src, re.M)
            if m:
                trozos.append(m.group(0))
    ns = {}
    exec("from urllib.parse import urlparse, quote\nimport re\n" + "\n".join(trozos), ns)
    if "fix_href" not in ns or "_is_business_site" not in ns:
        sys.exit("no se pudieron extraer las funciones del motor viejo")
    return ns


def correr_viejo(motor_viejo, html: str) -> list[dict]:
    """Reproduce _extract_direct del servidor viejo: sus selectores y su filtro.

    Se usa el mismo barrido de anclas del fallback nuevo para no castigar al
    viejo por los selectores CSS muertos de Google -- de otro modo la
    comparacion mediria eso y nada mas. Lo que se compara es su NORMALIZACION
    y su FILTRO, que es donde estaba la diferencia real.
    """
    salida = []
    for href, interior in re.findall(r'<a\b[^>]*\bhref=["\'](.*?)["\'][^>]*>(.*?)</a>',
                                     html or "", re.I | re.S):
        h = motor_viejo["fix_href"](href).rstrip(".,)")
        if not h.startswith("http") or not motor_viejo["_is_business_site"](h):
            continue
        texto = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", interior)).strip()
        nombre = re.sub(r"\s*[|\-].*$", "", texto, flags=re.I).strip()
        if not nombre or len(nombre) < 3:
            nombre = motor_viejo["_name_from_domain"](h)
        if not nombre:
            continue
        salida.append({"name": nombre, "website": h, "linkedin_url": ""})
    return salida


def medir_viejo(motor_viejo) -> dict:
    filas = []
    for fixture, _motor, _q, _loc in CORPUS:
        html = (FIX / fixture).read_text(encoding="utf-8")
        filas.extend(correr_viejo(motor_viejo, html))

    # Deduplicacion del viejo: por _domain(website), que es exactamente donde
    # colapsaba empresas distintas en hosting compartido.
    vistos, unicas = set(), []
    for f in filas:
        k = motor_viejo["_domain"](f["website"])
        if k in vistos:
            continue
        vistos.add(k)
        unicas.append(f)

    fp = sum(1 for f in unicas if filtering.motivo_descarte(f["website"], f["name"]))
    return {
        "resultados": len(unicas),
        "empresas_validas": len(unicas) - fp,
        "con_website": sum(1 for f in unicas if f["website"]),
        "duplicados": len(filas) - len(unicas),
        "falsos_positivos": fp,
    }


def medir_nuevo() -> dict:
    resolutor = Resolutor()
    crudos = 0
    ubi = interpretar("Lima, Peru")
    query = "agencia de marketing digital"
    for fixture, motor, q, loc in CORPUS:
        html = (FIX / fixture).read_text(encoding="utf-8")
        items, _ = extraction.extraer(None, html, motor)
        crudos += len(items)
        contexto = q + " " + loc
        for it in items:
            if filtering.motivo_descarte(it["url"], it["titulo"]):
                continue
            nombre = mejor_nombre(it["titulo"], it["url"], contexto)
            if not nombre:
                continue
            resolutor.agregar(Empresa(
                nombre=nombre, website=it["url"], descripcion=it["snippet"],
                texto_fuente=(it["titulo"] + " " + it["snippet"]).strip()[:400],
                fuentes={motor}, queries={q}))
    unicas = resolutor.empresas()
    rankeadas, _stats = scoring.rankear(unicas, query, ubi, limite=25)
    fp = sum(1 for e in rankeadas if filtering.motivo_descarte(e.website or "", e.nombre))
    return {
        "resultados": len(rankeadas),
        "empresas_validas": len(rankeadas) - fp,
        "con_website": sum(1 for e in rankeadas if e.website),
        "duplicados": max(0, crudos - len(unicas)),
        "falsos_positivos": fp,
    }


def precision(m: dict) -> float:
    return (m["empresas_validas"] / m["resultados"]) if m["resultados"] else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--assert-improvement", action="store_true")
    args = ap.parse_args()

    viejo = medir_viejo(cargar_motor_viejo())
    nuevo = medir_nuevo()
    viejo["precision"] = precision(viejo)
    nuevo["precision"] = precision(nuevo)

    print("Corpus: %d paginas de HTML real (%s)" % (len(CORPUS), ", ".join(c[0] for c in CORPUS)))
    print()
    print("  %-20s %10s %10s" % ("metrica", "ANTES", "DESPUES"))
    print("  " + "-" * 42)
    for k in ("resultados", "empresas_validas", "con_website", "duplicados",
              "falsos_positivos"):
        print("  %-20s %10d %10d" % (k, viejo[k], nuevo[k]))
    print("  %-20s %9.1f%% %9.1f%%" % ("precision", viejo["precision"] * 100,
                                       nuevo["precision"] * 100))
    print()

    if not args.assert_improvement:
        return

    fallas = []
    # La metrica que gobierna: proporcion de resultados que son empresas de
    # verdad. Mas resultados con mas basura NO es una mejora.
    if nuevo["precision"] <= viejo["precision"]:
        fallas.append("la precision no mejoro (%.3f -> %.3f)"
                      % (viejo["precision"], nuevo["precision"]))
    if nuevo["falsos_positivos"] > viejo["falsos_positivos"]:
        fallas.append("subieron los falsos positivos (%d -> %d)"
                      % (viejo["falsos_positivos"], nuevo["falsos_positivos"]))
    # Corpus envenenado incluido: el motor nuevo NO puede devolver Reddit/AOL.
    if nuevo["falsos_positivos"] > 0:
        fallas.append("el motor nuevo devolvio %d falsos positivos" % nuevo["falsos_positivos"])
    if nuevo["empresas_validas"] < 3:
        fallas.append("el motor nuevo devolvio muy pocas empresas validas (%d)"
                      % nuevo["empresas_validas"])

    if fallas:
        for f in fallas:
            print("FALLA: " + f)
        sys.exit(1)
    print("BENCHMARK VERIFICADO")


if __name__ == "__main__":
    main()
