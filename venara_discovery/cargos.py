"""Niveles de cargo y eleccion del decisor.

QUE PROBLEMA RESUELVE

El score del resolutor mezcla dos cosas: que tan buen cargo tiene la persona y
que tan buena es la evidencia de que existe. Ordenar por ese numero hace que un
"Gerente de Marketing" bien documentado le gane a un "CEO" con evidencia
mediana -- y a quien hay que escribirle es al que FIRMA, no al que esta mejor
documentado.

Asi que la eleccion se hace en dos pasos, en este orden:

  1. NIVEL DE CARGO. Un fundador vale mas que un gerente de area, siempre.
  2. EVIDENCIA. Solo desempata dentro del mismo nivel.

EL PISO, QUE ES LO QUE EVITA EL DESASTRE

Sin un minimo de evidencia, la regla de arriba se vuelve peligrosa: cualquier
"CEO" recogido de un blog cualquiera desplazaria a un "Gerente General"
publicado en el sitio de la propia empresa. Por eso un cargo alto solo lidera si
ademas supera el piso; por debajo compite en el orden de siempre.
"""
from __future__ import annotations

from .linkedin import puntuar_cargo

# ─────────────────────────────────────────────────────────────────────────────
# Niveles
# ─────────────────────────────────────────────────────────────────────────────
# Las bandas salen de los pesos de `puntuar_cargo`, que ya estaban medidos. Se
# nombran para poder razonar sobre ellas: "fundador" y "0.95" dicen lo mismo,
# pero solo uno de los dos se puede discutir con el usuario.
NIVELES = [
    ("fundador",  0.90),   # founder, CEO, dueno: firma sin consultar a nadie
    ("ejecutivo", 0.80),   # gerente general, managing director, socio
    ("c_level",   0.70),   # CTO/CMO/COO/CFO, VP: decide en su area, con budget
    ("area",      0.60),   # head of, director de area
    ("mando",     0.40),   # gerente/manager a secas
]
NIVEL_MINIMO = "otro"

# Orden para comparar. Menor es mejor.
ORDEN = {n: i for i, (n, _) in enumerate(NIVELES)}
ORDEN[NIVEL_MINIMO] = len(NIVELES)

# Evidencia minima para que un cargo alto pueda LIDERAR el resultado.
#
# 0.55 no es arbitrario: con los pesos del resolutor, un candidato ligado a la
# empresa por el texto y con cargo de decisor llega a 0.65, y uno publicado en
# el sitio propio pasa de 0.75. Por debajo de 0.55 lo que hay es un nombre
# suelto que alguien menciono cerca de la empresa.
PISO_PARA_LIDERAR = 0.55


def nivel(cargo: str) -> str:
    """Nivel del cargo. `otro` cuando no se reconoce."""
    peso = puntuar_cargo(cargo or "")
    for nombre, minimo in NIVELES:
        if peso >= minimo:
            return nombre
    return NIVEL_MINIMO


def rango(cargo: str) -> int:
    """Posicion comparable del cargo. Menor es mejor decisor."""
    return ORDEN[nivel(cargo)]


def elegir_mejor(candidatos: list) -> list:
    """Ordena poniendo primero al MEJOR CARGO, no al mejor documentado.

    Devuelve la lista completa reordenada -- las alternativas siguen viajando,
    porque en una PyME el gerente general y el fundador suelen ser dos personas
    y a veces conviene escribirle a la otra.
    """
    def clave(c):
        # Un cargo alto sin respaldo no lidera: compite en el nivel de los que
        # no se reconocen, y ahi desempata la evidencia.
        alcanza = getattr(c, "score", 0.0) >= PISO_PARA_LIDERAR
        r = rango(getattr(c, "cargo", "")) if alcanza else ORDEN[NIVEL_MINIMO]
        return (r, -getattr(c, "score", 0.0))

    return sorted(candidatos, key=clave)
