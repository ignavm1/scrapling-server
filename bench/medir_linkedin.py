"""Medicion en vivo del angulo LinkedIn sobre empresas reales.

Que verifica: que la medicion produzca un VEREDICTO DECISIVO por empresa y lo
deje escrito. No exige encontrar a nadie -- eso mediria el humor de los
buscadores, no el angulo. Falla solo si la medicion misma se rompe.

Interesa especialmente cuantos decisores salieron DEL PERFIL de LinkedIn, que
es lo que este angulo agrega sobre lo que ya habia.
"""
from __future__ import annotations
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from venara_discovery import config, decisor  # noqa: E402

SALIDA = pathlib.Path(__file__).resolve().parent.parent / "MEDICION-linkedin.md"

CASOS = [
    ("Fintual",   "fintual.cl",    "Santiago, Chile"),
    ("Buk",       "buk.cl",        "Santiago, Chile"),
    ("Xepelin",   "xepelin.com",   "Santiago, Chile"),
    ("Betterfly", "betterfly.com", "Santiago, Chile"),
    ("Houm",      "houm.com",      "Santiago, Chile"),
    ("Toteat",    "toteat.com",    "Santiago, Chile"),
]


def main() -> int:
    filas, errores = [], []
    for i, (nombre, dom, loc) in enumerate(CASOS):
        if i:
            time.sleep(4)          # respiro: no autobloquear a los proveedores
        try:
            r = decisor.resolver(nombre, dom, loc)
        except Exception as e:
            errores.append((nombre, str(e)[:200]))
            filas.append({"empresa": nombre, "veredicto": "error", "cands": [],
                          "bloq": {}, "ms": 0, "li": 0})
            continue
        d, cands = r["diagnostico"], r["candidatos"]
        li = [c for c in cands if c.angulo == "linkedin_perfil"]
        filas.append({
            "empresa": nombre, "dominio": dom, "cands": cands, "li": len(li),
# El veredicto lo pone el RESOLUTOR, no este script. Reproducirlo aca
# significaba repetir su logica -- y cuando el resolutor aprendio a
# distinguir "no publica" de "no pudimos mirar" (F26.4), el reporte
# siguio diciendo "no publicado" sobre corridas sin acceso.
            "veredicto": "decisor" if cands else d.get("motivo_vacio", "sin_datos"),
            "bloq": d["proveedores_bloqueados"], "ms": d["ms"],
            "fetches": d["fetches"], "pag": d["paginas_visitadas"],
        })

    ok = [f for f in filas if f["cands"]]
    total_li = sum(f["li"] for f in filas)

    lineas = [
        "# Medicion en vivo — angulo LinkedIn",
        "",
        f"Fecha: **{datetime.date.today().isoformat()}**. "
        f"Proxy configurado: **{'si' if config.PROXY_URL else 'NO'}**.",
        "Generado por `bench/medir_linkedin.py`. No editar a mano.",
        "",
        "| Empresa | Veredicto | Decisores | De perfil LinkedIn | Fetches | Bloqueados | ms |",
        "|---|---|---|---|---|---|---|",
    ]
    for f in filas:
        lineas.append(f"| {f['empresa']} | {f['veredicto']} | {len(f['cands'])} | "
                      f"{f['li']} | {f.get('fetches','—')} | "
                      f"{', '.join(f['bloq']) or '—'} | {f['ms']} |")
    lineas += ["", f"**Empresas resueltas: {len(ok)} de {len(filas)}. "
                   f"Decisores desde un perfil de LinkedIn: {total_li}.**", ""]

    for f in ok:
        lineas.append(f"## {f['empresa']}")
        lineas.append("")
        for c in f["cands"]:
            lineas.append(f"- **{c.nombre}** — {c.cargo or 'sin cargo'} · score {c.score} · "
                          f"via {c.angulo} · {c.url}")
        lineas.append("")

    SALIDA.write_text("\n".join(lineas), encoding="utf-8")

    for f in filas:
        print(f"{f['veredicto']:<13} {f['empresa']:<11} n={len(f['cands'])} "
              f"linkedin={f['li']} ms={f['ms']}")
        for c in f["cands"][:3]:
            print(f"      -> {c.nombre} | {c.cargo} | {c.score} | {c.angulo}")
    print(f"\nReporte: {SALIDA}")

    if errores:
        for n, e in errores:
            print(f"ERROR {n}: {e}", file=sys.stderr)
        return 1
    print("MEDICION REGISTRADA")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
