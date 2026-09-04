"""Medicion en vivo del resolutor de decisor sobre empresas REALES.

POR QUE NO EXIGE ENCONTRAR A NADIE

Un gate que exigiera "N > 0" mediria el humor de los buscadores, no el
resolutor: una corrida con captcha fallaria igual que un sistema roto, y las dos
cosas se arreglan en lugares distintos. Lo que se verifica aca es que la
medicion PRODUZCA UN VEREDICTO DECISIVO por empresa y lo deje escrito:

  decisor        se encontro a la persona (con nombre, cargo, score y fuente)
  bloqueado      los buscadores no dejaron mirar (con que proveedor y por que)
  no_publicado   se pudo mirar y esta empresa no publica a su decisor
  error          la medicion misma fallo -- no es un veredicto, y hace fallar

LIMITACION QUE GOBIERNA EL RESULTADO: sin PROXY_URL, DuckDuckGo, Brave y
lite-DDG pasan a captcha a los pocos requests (F1) y queda respondiendo solo
Bing, que este repo ya clasifica como fuente hostil (F6). Un "bloqueado" desde
una IP sin proxy no dice nada sobre el resolutor: dice que falto el proxy.
"""
from __future__ import annotations
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from venara_discovery import config, decisor  # noqa: E402

SALIDA = pathlib.Path(__file__).resolve().parent.parent / "MEDICION-decisor.md"

# Empresas reales, con dominio conocido: es el caso que el sistema resuelve en
# produccion, donde el canal de empresas ya descubrio el sitio.
CASOS = [
    ("Fintual", "fintual.cl", "Santiago, Chile"),
    ("Buk", "buk.cl", "Santiago, Chile"),
    ("Betterfly", "betterfly.com", "Santiago, Chile"),
    ("Toteat", "toteat.com", "Santiago, Chile"),
]


def medir(empresa: str, dominio: str, ubicacion: str) -> dict:
    t0 = time.monotonic()
    try:
        r = decisor.resolver(empresa, dominio, ubicacion)
    except Exception as e:                       # la medicion misma fallo
        return {"empresa": empresa, "veredicto": "error", "error": str(e)[:200],
                "ms": int((time.monotonic() - t0) * 1000), "candidatos": [],
                "bloqueados": {}}
    diag = r["diagnostico"]
    cands = r["candidatos"]
# El veredicto lo pone el RESOLUTOR, no este script. Reproducirlo aca
# significaba repetir su logica -- y cuando el resolutor aprendio a
# distinguir "no publica" de "no pudimos mirar" (F26.4), el reporte
# siguio diciendo "no publicado" sobre corridas sin acceso.
    veredicto = "decisor" if cands else diag.get("motivo_vacio", "sin_datos")
    return {"empresa": empresa, "dominio": dominio, "veredicto": veredicto,
            "candidatos": cands, "bloqueados": diag["proveedores_bloqueados"],
            "fetches": diag["fetches"], "paginas": diag["paginas_visitadas"],
            "crudos": diag["crudos"], "ms": diag["ms"]}


def main() -> int:
    filas = [medir(*c) for c in CASOS]
    errores = [f for f in filas if f["veredicto"] == "error"]
    con_decisor = [f for f in filas if f["veredicto"] == "decisor"]
    total = sum(len(f["candidatos"]) for f in con_decisor)

    lineas = [
        "# Medicion en vivo — resolutor de decisor",
        "",
        f"Fecha: **{datetime.date.today().isoformat()}**. "
        f"Proxy configurado: **{'si' if config.PROXY_URL else 'NO'}**.",
        "Generado por `bench/medir_decisor.py`. No editar a mano: se reescribe",
        "en cada corrida.",
        "",
        "| Empresa | Dominio | Veredicto | Decisores | Fetches | Paginas | Crudos | Bloqueados | ms |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for f in filas:
        lineas.append(
            f"| {f['empresa']} | {f.get('dominio','—')} | {f['veredicto']} | "
            f"{len(f['candidatos'])} | {f.get('fetches','—')} | {f.get('paginas','—')} | "
            f"{f.get('crudos','—')} | {', '.join(f['bloqueados']) or '—'} | {f['ms']} |")
    lineas += ["", f"**Empresas con decisor encontrado: {len(con_decisor)} de {len(filas)} "
                   f"({total} personas).**", ""]

    for f in con_decisor:
        lineas.append(f"## {f['empresa']}")
        lineas.append("")
        for c in f["candidatos"]:
            lineas.append(f"- **{c.nombre}** — {c.cargo or 'sin cargo'} · "
                          f"score {c.score} · via {c.angulo}/{c.proveedor} · {c.url}")
            for e in c.evidencia:
                lineas.append(f"  - {e}")
        lineas.append("")

    bloqueadas = [f for f in filas if f["veredicto"] == "bloqueado"]
    if bloqueadas:
        lineas += ["## Corridas que no pudieron mirar", "",
                   "Un vacio por bloqueo **no** significa que la empresa no publique a su",
                   "decisor. Sin `PROXY_URL` los buscadores bloquean a los pocos requests",
                   "(F1) y queda solo Bing, ya clasificado como fuente hostil (F6).", ""]
        for f in bloqueadas:
            lineas.append(f"- {f['empresa']}: "
                          + ", ".join(f"{k}={v}" for k, v in f["bloqueados"].items()))
        lineas.append("")

    if errores:
        lineas += ["## Errores de la medicion", ""]
        lineas += [f"- {f['empresa']}: {f['error']}" for f in errores]
        lineas.append("")

    SALIDA.write_text("\n".join(lineas), encoding="utf-8")

    for f in filas:
        print(f"{f['veredicto']:<13} {f['empresa']:<12} decisores={len(f['candidatos'])} "
              f"fetches={f.get('fetches','—')} paginas={f.get('paginas','—')} ms={f['ms']}")
    print(f"\nReporte: {SALIDA}")

    if errores:
        print(f"\n{len(errores)} de {len(filas)} corridas fallaron: la medicion no es "
              "concluyente.", file=sys.stderr)
        return 1
    print("MEDICION REGISTRADA")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
