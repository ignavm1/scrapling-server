"""Medicion en vivo: cuantos decisores quedan con canal de contacto REAL.

Encontrar a la persona y no poder escribirle no sirve de nada, asi que lo que
se mide aca no es "cuantos decisores" sino "cuantos ALCANZABLES", y por que via.

No exige encontrar contacto: eso mediria el humor de los buscadores y de los
sitios ajenos. Exige que cada empresa tenga un veredicto y que quede escrito.
"""
from __future__ import annotations
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from venara_discovery import config, decisor  # noqa: E402

SALIDA = pathlib.Path(__file__).resolve().parent.parent / "MEDICION-contacto.md"

CASOS = [
    ("Fintual",   "fintual.cl",    "Santiago, Chile"),
    ("Buk",       "buk.cl",        "Santiago, Chile"),
    ("Xepelin",   "xepelin.com",   "Santiago, Chile"),
    ("Betterfly", "betterfly.com", "Santiago, Chile"),
    ("Toteat",    "toteat.com",    "Santiago, Chile"),
    ("Houm",      "houm.com",      "Santiago, Chile"),
]


def main() -> int:
    filas, errores = [], []
    for i, (nombre, dom, loc) in enumerate(CASOS):
        if i:
            time.sleep(4)
        try:
            r = decisor.resolver(nombre, dom, loc)
        except Exception as e:
            errores.append((nombre, str(e)[:200]))
            continue
        cands = r["candidatos"]
        filas.append({"empresa": nombre, "cands": cands,
                      "bloq": r["diagnostico"]["proveedores_bloqueados"],
                      "ms": r["diagnostico"]["ms"]})

    con_persona = [f for f in filas if f["cands"]]
    alcanzables, por_fuente = [], {}
    for f in con_persona:
        for c in f["cands"]:
            ct = c.contacto or {}
            if ct.get("email") or ct.get("phone"):
                alcanzables.append((f["empresa"], c))
                fuente = ct.get("email_source") or "solo_telefono"
                por_fuente[fuente] = por_fuente.get(fuente, 0) + 1

    lineas = [
        "# Medicion en vivo — contacto del decisor",
        "",
        f"Fecha: **{datetime.date.today().isoformat()}**. "
        f"Proxy configurado: **{'si' if config.PROXY_URL else 'NO'}**.",
        "Generado por `bench/medir_contacto.py`. No editar a mano.",
        "",
        "| Empresa | Decisores | Con contacto | Email | Fuente | Telefono | ms |",
        "|---|---|---|---|---|---|---|",
    ]
    for f in filas:
        if not f["cands"]:
            lineas.append(f"| {f['empresa']} | 0 | 0 | — | "
                          f"{'bloqueado' if f['bloq'] else 'no publicado'} | — | {f['ms']} |")
            continue
        for c in f["cands"]:
            ct = c.contacto or {}
            lineas.append(
                f"| {f['empresa']} | {c.nombre} | "
                f"{'si' if (ct.get('email') or ct.get('phone')) else 'no'} | "
                f"{ct.get('email') or '—'} | {ct.get('email_source') or '—'} | "
                f"{ct.get('phone') or '—'} | {f['ms']} |")
    lineas += ["", f"**Decisores alcanzables: {len(alcanzables)} "
                   f"de {sum(len(f['cands']) for f in filas)} encontrados, "
                   f"en {len(con_persona)} de {len(filas)} empresas.**",
               f"Por fuente del email: {por_fuente or '—'}", ""]

    for empresa, c in alcanzables:
        ct = c.contacto
        lineas.append(f"## {empresa} — {c.nombre}")
        lineas.append("")
        lineas.append(f"- cargo: {c.cargo} · score {c.score} · via {c.angulo}")
        lineas.append(f"- email: **{ct.get('email') or '—'}** "
                      f"({ct.get('email_source') or 'sin fuente'}, "
                      f"confianza {ct.get('email_confidence')})")
        lineas.append(f"- telefono: {ct.get('phone') or '—'} "
                      f"({ct.get('phone_kind') or '—'}) · "
                      f"whatsapp: {ct.get('whatsapp') or '—'}")
        for e in ct.get("evidence", []):
            lineas.append(f"  - {e}")
        lineas.append("")

    SALIDA.write_text("\n".join(lineas), encoding="utf-8")

    for f in filas:
        print(f"{f['empresa']:<11} decisores={len(f['cands'])} ms={f['ms']}")
        for c in f["cands"]:
            ct = c.contacto or {}
            print(f"    -> {c.nombre} | {c.cargo}")
            print(f"       email={ct.get('email') or '—'} ({ct.get('email_source') or '—'}"
                  f"/{ct.get('email_confidence')}) tel={ct.get('phone') or '—'} "
                  f"wa={ct.get('whatsapp') or '—'}")
    print(f"\nAlcanzables: {len(alcanzables)} · por fuente: {por_fuente}")
    print(f"Reporte: {SALIDA}")

    if errores:
        for n, e in errores:
            print(f"ERROR {n}: {e}", file=sys.stderr)
        return 1
    print("MEDICION REGISTRADA")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
