"""Medicion en vivo de los SIETE angulos y de la eleccion por cargo.

Que verifica: que la medicion produzca un VEREDICTO DECISIVO por empresa y lo
deje escrito. No exige encontrar a nadie -- eso mediria el humor de los
buscadores, no el angulo. Falla solo si la medicion misma se rompe.

Lo que interesa medir: que angulo aporto cada decisor -- para saber cuales
ganan su lugar y cuales no aportan nada -- y si el que quedo primero es el del
mejor cargo, que es la regla nueva.
"""
from __future__ import annotations
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from venara_discovery import config, decisor  # noqa: E402

SALIDA = pathlib.Path(__file__).resolve().parent.parent / "MEDICION-angulos.md"

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
        por_angulo = {}
        for c in cands:
            por_angulo[c.angulo] = por_angulo.get(c.angulo, 0) + 1
        filas.append({
            "empresa": nombre, "dominio": dom, "cands": cands, "li": len(li),
            "veredicto": ("decisor" if cands
                          else "bloqueado" if d["proveedores_bloqueados"]
                          else "no_publicado"),
            "bloq": d["proveedores_bloqueados"], "ms": d["ms"],
            "fetches": d["fetches"], "pag": d["paginas_visitadas"],
            "por_angulo": por_angulo, "angulos_plan": d["angulos"],
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
    aporte = {}
    for f in filas:
        for a, n in f.get("por_angulo", {}).items():
            aporte[a] = aporte.get(a, 0) + n
    planeados = sorted({a for f in filas for a in f.get("angulos_plan", [])})
    mudos = [a for a in planeados if a not in aporte]
    lineas += ["", f"**Empresas resueltas: {len(ok)} de {len(filas)}. "
                   f"Decisores desde un perfil de LinkedIn: {total_li}.**", "",
               "### Que aporto cada angulo", "",
               "| Angulo | Decisores aportados |", "|---|---|"]
    for a in sorted(aporte, key=lambda x: -aporte[x]):
        lineas.append(f"| {a} | {aporte[a]} |")
    for a in mudos:
        lineas.append(f"| {a} | 0 |")
    lineas += ["", "Un angulo en 0 no se borra por una corrida: puede ser el "
                   "unico que sirva para otro rubro, o haber caido en un "
                   "proveedor bloqueado. Se borra cuando varias mediciones lo "
                   "confirmen mudo.", ""]

    for f in ok:
        lineas.append(f"## {f['empresa']}")
        lineas.append("")
        for i, c in enumerate(f["cands"]):
            marca = "**ELEGIDO** " if i == 0 else ""
            from venara_discovery import cargos as _cg
            lineas.append(f"- {marca}**{c.nombre}** — {c.cargo or 'sin cargo'} "
                          f"(nivel {_cg.nivel(c.cargo)}) · score {c.score} · "
                          f"via {c.angulo} · {c.url}")
        lineas.append("")

    SALIDA.write_text("\n".join(lineas), encoding="utf-8")

    for f in filas:
        print(f"{f['veredicto']:<13} {f['empresa']:<11} n={len(f['cands'])} "
              f"linkedin={f['li']} ms={f['ms']}")
        from venara_discovery import cargos as _cg
        for i, c in enumerate(f["cands"][:3]):
            print(f"      {'*' if i == 0 else ' '} {c.nombre} | {c.cargo} | "
                  f"nivel={_cg.nivel(c.cargo)} | {c.score} | {c.angulo}")
    print(f"\nReporte: {SALIDA}")

    if errores:
        for n, e in errores:
            print(f"ERROR {n}: {e}", file=sys.stderr)
        return 1
    print("MEDICION REGISTRADA")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
