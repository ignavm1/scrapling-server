"""Sacar el CONTACTO del decisor: por que canal se lo alcanza.

Confirmar quien decide no sirve de nada si no hay forma de escribirle. Este
modulo convierte "Omar Larre, Co-founder de Fintual" en un email y un telefono,
o dice honestamente que no los hay.

LA REGLA DE ORO, QUE ES DEL REPO Y NO SE NEGOCIA

De `lib/enrichment/pattern.ts` de Venara:

    NUNCA construir un email de envio sobre un patron que tambien se adivino.
    Un patron adivinado + un nombre = loteria, y cada fallo es un rebote que
    degrada el buzon del cliente.

Por eso hay tres fuentes, y cada dato dice de cual salio:

    publicado   el sitio publica el email de ESA persona. Es el dato, no una
                deduccion. Confianza maxima.
    patron      el sitio publica el email de OTRA persona del dominio; de ahi
                sale la convencion y se aplica a la nuestra. Se cita la muestra.
    generico    no hay nada personal. Se devuelve info@/contacto@ ETIQUETADO
                como generico -- va dirigido a la empresa, no a la persona.

Lo que no se hace, y es deliberado: no se construye un email cuando no hay
ninguna muestra del dominio. Sin muestra no hay convencion, hay una apuesta.

TAMPOCO SE VERIFICA EL BUZON, Y ESO ES A PROPOSITO

Probar si una casilla existe pide SMTP contra el servidor ajeno, que es
intrusivo, poco fiable y suele estar bloqueado. La verificacion vive del lado de
Venara (`lib/email/verify.ts`), que ya decide que se envia. Aca se entrega el
dato CON SU PROCEDENCIA para que esa decision sea posible.
"""
from __future__ import annotations
import re
import unicodedata

# ─────────────────────────────────────────────────────────────────────────────
# Nombres
# ─────────────────────────────────────────────────────────────────────────────
# Particulas que no forman parte del apellido usable en un correo.
_PARTICULAS = {"de", "del", "la", "las", "los", "da", "do", "dos", "van",
               "von", "di", "san", "y"}


def sin_tildes(s: str) -> str:
    """`Jose Munoz` desde `José Muñoz`.

    Ningun dominio hispano pone tildes en el buzon: el correo de Jose Munoz es
    `jose.munoz@`, nunca `josé.muñoz@`. Sin esto, todo nombre con tilde -- que
    en Chile es la mitad -- genera un candidato invalido.
    """
    s = (s or "").replace("ñ", "n").replace("Ñ", "N")
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def token(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", sin_tildes(s or "").lower())


def partir_nombre(nombre: str) -> tuple[str, str] | None:
    """"Juan Perez Gonzalez" -> ("juan", "perez").

    En espanol hay DOS apellidos y la convencion de correo casi siempre usa el
    primero: `juan.perez@`, no `juan.perezgonzalez@`. Una libreria anglosajona
    toma el ULTIMO token como apellido y produce `juan.gonzalez@`, que esta mal
    en la mayoria de los casos.
    """
    partes = [p for p in re.split(r"\s+", (nombre or "").strip()) if p]
    partes = [p for p in partes if token(p)]
    if len(partes) < 2:
        return None
    primero = token(partes[0])
    for p in partes[1:]:
        t = token(p)
        if t and t not in _PARTICULAS:
            return (primero, t) if primero else None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Emails
# ─────────────────────────────────────────────────────────────────────────────
RX_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# Buzones que son de la empresa, no de una persona.
LOCALES_GENERICOS = {
    "info", "contacto", "contact", "hola", "hello", "ventas", "sales",
    "comercial", "admin", "administracion", "soporte", "support", "ayuda",
    "help", "prensa", "press", "marketing", "rrhh", "hr", "trabaja",
    "postulaciones", "jobs", "reclutamiento", "facturacion", "cobranza",
    "noreply", "no-reply", "mail", "correo", "webmaster", "office", "team",
    "equipo", "clientes", "atencion", "servicioalcliente", "consultas",
}

# Convenciones de correo, de la mas frecuente a la menos. Mismo vocabulario que
# `lib/enrichment/pattern.ts` de Venara: si los dos lados nombran distinto el
# mismo patron, ningun dato se puede comparar entre sistemas.
PATRONES = {
    "first.last":  lambda f, l: f"{f}.{l}",
    "first":       lambda f, l: f,
    "flast":       lambda f, l: f"{f[0]}{l}",
    "f.last":      lambda f, l: f"{f[0]}.{l}",
    "firstlast":   lambda f, l: f"{f}{l}",
    "first_last":  lambda f, l: f"{f}_{l}",
    "firstl":      lambda f, l: f"{f}{l[0]}",
    "last.first":  lambda f, l: f"{l}.{f}",
}


def es_generico(email: str) -> bool:
    local = (email or "").split("@")[0].lower()
    return local in LOCALES_GENERICOS or any(
        local.startswith(g) for g in ("noreply", "no-reply"))


def emails_del_dominio(texto: str, dominio: str) -> list[str]:
    """Emails del texto que pertenecen al dominio de la empresa.

    Se filtra por dominio a proposito: una pagina de contacto suele traer
    tambien el correo de la agencia que le hizo el sitio, y atribuirselo al
    prospecto es empezar la conversacion con la empresa equivocada.
    """
    if not dominio:
        return []
    raiz = dominio.lower().split(".")[0]
    vistos, out = set(), []
    for e in RX_EMAIL.findall(texto or ""):
        e = e.lower().strip(".")
        if e in vistos:
            continue
        host = e.split("@")[-1]
        if raiz not in host:
            continue
        # Extensiones de imagen coladas por un `src` mal cerrado.
        if re.search(r"\.(png|jpg|jpeg|gif|svg|webp)$", e):
            continue
        vistos.add(e)
        out.append(e)
    return out


def detectar_patron(email: str, nombre: str) -> str | None:
    """Que convencion explica ESTE email para ESTA persona.

    Se pide el nombre del dueno del correo: sin el, `jperez@` podria ser
    `flast` o `first`, y elegir mal propaga el error a toda la empresa.
    """
    partes = partir_nombre(nombre)
    if not partes or not email:
        return None
    f, l = partes
    local = email.split("@")[0].lower()
    for nombre_patron, construir in PATRONES.items():
        try:
            if construir(f, l) == local:
                return nombre_patron
        except IndexError:
            continue
    return None


def patron_por_forma(email: str) -> tuple[str, int] | None:
    """Patron deducido de la FORMA del local, sin saber de quien es.

    Es mas debil que `detectar_patron` y por eso devuelve menos confianza: la
    forma `a.b` es casi siempre `first.last`, pero podria ser `last.first`.
    """
    local = (email or "").split("@")[0].lower()
    if es_generico(email):
        return None
    if re.fullmatch(r"[a-z]+\.[a-z]+", local):
        return "first.last", 60
    if re.fullmatch(r"[a-z]+_[a-z]+", local):
        return "first_last", 60
    if re.fullmatch(r"[a-z]\.[a-z]+", local):
        return "f.last", 60
    if re.fullmatch(r"[a-z][a-z]{2,}", local):
        # Un solo bloque: `first` o `flast`, y no se puede distinguir sin el
        # nombre del dueno. Se devuelve la mas conservadora.
        return "first", 45
    return None


def construir_email(nombre: str, dominio: str, patron: str) -> str | None:
    partes = partir_nombre(nombre)
    if not partes or not dominio or patron not in PATRONES:
        return None
    try:
        local = PATRONES[patron](*partes)
    except IndexError:
        return None
    return f"{local}@{dominio}" if local else None


# ─────────────────────────────────────────────────────────────────────────────
# Telefonos
# ─────────────────────────────────────────────────────────────────────────────
RX_TELEFONO = re.compile(r"(?:\+?\d[\d\s().\-]{7,20}\d)")

# Prefijo de pais y como se reconoce un movil. Un fijo NO tiene WhatsApp, y
# guardarlo como si lo tuviera genera una tarea que nunca va a llegar.
PAISES = {
    "CL": {"cc": "56", "largo_nacional": 9, "movil": lambda n: n.startswith("9") and len(n) == 9},
    "PE": {"cc": "51", "largo_nacional": 9, "movil": lambda n: n.startswith("9") and len(n) == 9},
    "MX": {"cc": "52", "largo_nacional": 10, "movil": lambda n: len(n) == 10},
    "CO": {"cc": "57", "largo_nacional": 10, "movil": lambda n: n.startswith("3") and len(n) == 10},
    "AR": {"cc": "54", "largo_nacional": 10, "movil": lambda n: len(n) == 10},
    "ES": {"cc": "34", "largo_nacional": 9, "movil": lambda n: n[:1] in "67" and len(n) == 9},
}


def normalizar_telefono(crudo: str, pais: str = "CL") -> dict | None:
    """Telefono -> E.164 + si es movil. None si no parece un numero real."""
    conf = PAISES.get((pais or "CL").upper())
    if not conf:
        return None
    d = re.sub(r"\D", "", crudo or "")
    if not d:
        return None
    cc = conf["cc"]
    if d.startswith("00" + cc):
        d = d[2 + len(cc):]
    elif d.startswith(cc) and len(d) > conf["largo_nacional"]:
        d = d[len(cc):]
    d = d.lstrip("0")
    if len(d) != conf["largo_nacional"]:
        return None
    return {"e164": f"+{cc}{d}", "movil": bool(conf["movil"](d)), "nacional": d}


def telefonos_del_texto(texto: str, pais: str = "CL") -> list[dict]:
    vistos, out = set(), []
    for crudo in RX_TELEFONO.findall(texto or ""):
        n = normalizar_telefono(crudo, pais)
        if not n or n["e164"] in vistos:
            continue
        vistos.add(n["e164"])
        out.append(n)
    return out


def emparejar(emails: list[str], nombres: list[str]) -> list[tuple[str, str]]:
    """Pares (nombre, email) DEMOSTRADOS: el email se explica por ese nombre.

    Un par probado vale mucho mas que deducir el patron de la forma del local:
    `jperez@` puede ser `flast` o `first`, y solo el nombre del dueno lo
    desambigua. Con un par, la convencion del dominio queda demostrada y toda
    persona futura de esa empresa sale gratis.
    """
    pares = []
    for e in emails:
        if es_generico(e):
            continue
        for n in nombres:
            if detectar_patron(e, n):
                pares.append((n, e))
                break
    return pares


# Palabras que delatan una pagina de contacto. Ahi es donde viven los emails.
_SENALES_CONTACTO = ("contacto", "contact", "contactanos", "escribenos",
                     "hablemos", "cotiza", "presupuesto")


def es_pagina_de_contacto(url: str, texto_ancla: str = "") -> bool:
    u = sin_tildes((url or "").lower())
    a = sin_tildes((texto_ancla or "").lower())
    return any(s in u or s in a for s in _SENALES_CONTACTO)


# ─────────────────────────────────────────────────────────────────────────────
# El contacto de una persona
# ─────────────────────────────────────────────────────────────────────────────
def contacto_de(nombre: str, dominio: str, texto: str, pais: str = "CL",
                nombres_del_equipo: list[tuple[str, str]] | None = None) -> dict:
    """Email y telefono para UNA persona, cada dato con su procedencia.

    `nombres_del_equipo` son pares (nombre, email) ya conocidos del sitio: son
    los que permiten deducir la convencion con certeza en vez de por la forma.
    """
    emails = emails_del_dominio(texto, dominio)
    personales = [e for e in emails if not es_generico(e)]
    genericos = [e for e in emails if es_generico(e)]
    partes = partir_nombre(nombre)

    resultado = {
        "email": None, "email_source": None, "email_confidence": 0,
        "phone": None, "whatsapp": None, "phone_kind": None,
        "evidence": [],
    }

    # ── 1) Publicado: el sitio trae el email de ESTA persona ─────────────────
    if partes:
        f, l = partes
        for e in personales:
            local = e.split("@")[0].lower()
            if any(construir(f, l) == local for construir in PATRONES.values()):
                resultado.update({"email": e, "email_source": "publicado",
                                  "email_confidence": 95})
                resultado["evidence"].append(f"el sitio publica {e}")
                break

    # ── 2) Patron: deducido de una muestra REAL del dominio ─────────────────
    if not resultado["email"] and partes:
        patron, conf, muestra = None, 0, None
        # Con nombre y email de un companero, la convencion queda demostrada.
        for otro_nombre, otro_email in (nombres_del_equipo or []):
            p = detectar_patron(otro_email, otro_nombre)
            if p:
                patron, conf, muestra = p, 80, f"{otro_email} ({otro_nombre})"
                break
        # Si no, se deduce de la forma del local. Vale menos y se dice.
        if not patron:
            for e in personales:
                por_forma = patron_por_forma(e)
                if por_forma:
                    patron, conf, muestra = por_forma[0], por_forma[1], e
                    break
        if patron:
            candidato = construir_email(nombre, dominio, patron)
            if candidato:
                resultado.update({"email": candidato, "email_source": "patron",
                                  "email_confidence": conf})
                resultado["evidence"].append(
                    f"patron {patron} deducido de {muestra}")

    # ── 3) Generico: de la empresa, NO de la persona ─────────────────────────
    if not resultado["email"] and genericos:
        resultado.update({"email": genericos[0], "email_source": "generico",
                          "email_confidence": 30})
        resultado["evidence"].append(
            f"{genericos[0]} es un buzon de la empresa, no de {nombre}")

    # Sin ninguna de las tres NO se construye nada. Un patron adivinado mas un
    # nombre es loteria, y cada rebote degrada el dominio del cliente.
    if not resultado["email"]:
        resultado["evidence"].append(
            "sin email: el sitio no publica ninguno del dominio")

    # ── Telefono ────────────────────────────────────────────────────────────
    tels = telefonos_del_texto(texto, pais)
    movil = next((t for t in tels if t["movil"]), None)
    elegido = movil or (tels[0] if tels else None)
    if elegido:
        resultado["phone"] = elegido["e164"]
        resultado["phone_kind"] = "mobile" if elegido["movil"] else "landline"
        # Solo un movil puede tener WhatsApp.
        resultado["whatsapp"] = elegido["e164"] if elegido["movil"] else None
        resultado["evidence"].append(
            f"telefono {elegido['e164']} publicado en el sitio "
            f"({'movil' if elegido['movil'] else 'fijo, sin WhatsApp'})")

    return resultado
