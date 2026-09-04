"""Sacar el contacto del decisor, y decir siempre de donde salio.

La regla que gobierna el archivo es del repo, no mia: nunca construir un email
sobre un patron adivinado. Un patron adivinado mas un nombre es loteria, y cada
rebote degrada el buzon del cliente.
"""
import pathlib

import pytest

from venara_discovery import contacto, website

FIX = pathlib.Path(__file__).parent / "fixtures"
PAGINA = (FIX / "sintetico_contacto.html").read_text(encoding="utf-8")
TEXTO = website.texto_por_bloques(PAGINA)
DOM = "onzamarketing.cl"


# ── C1: el email publicado ──────────────────────────────────────────────────

def test_devuelve_el_email_publicado_de_esa_persona():
    c = contacto.contacto_de("Matias Bravo", DOM, TEXTO)
    assert c["email"] == "matias.bravo@onzamarketing.cl"
    assert c["email_source"] == "publicado"
    assert c["email_confidence"] >= 90


def test_no_le_atribuye_a_uno_el_email_de_otro_empleado():
    # El fallo caro: colgarle a nuestra persona el correo de un companero
    # produce un dato que parece verificado y no lo es.
    c = contacto.contacto_de("Carolina Reyes", DOM, TEXTO)
    assert c["email"] != "matias.bravo@onzamarketing.cl"


def test_no_toma_el_email_de_otro_dominio():
    # La pagina trae el correo de la agencia que hizo el sitio. Atribuirselo al
    # prospecto es empezar la conversacion con la empresa equivocada.
    assert "otraagencia" not in str(contacto.contacto_de("Matias Bravo", DOM, TEXTO))


# ── C2: el patron, solo con muestra real ────────────────────────────────────

def test_deduce_el_patron_de_una_muestra_real_y_la_cita():
    c = contacto.contacto_de("Carolina Reyes", DOM, TEXTO)
    assert c["email"] == "carolina.reyes@onzamarketing.cl"
    assert c["email_source"] == "patron"
    assert any("matias.bravo" in e for e in c["evidence"]), c["evidence"]


def test_un_par_probado_da_mas_confianza_que_la_forma_del_local():
    # Con nombre y email de un companero la convencion queda DEMOSTRADA; por la
    # forma del local solo se infiere, y `jperez@` puede ser flast o first.
    por_forma = contacto.contacto_de("Carolina Reyes", DOM, TEXTO)
    probado = contacto.contacto_de(
        "Carolina Reyes", DOM, TEXTO,
        nombres_del_equipo=[("Matias Bravo", "matias.bravo@onzamarketing.cl")])
    assert probado["email_confidence"] > por_forma["email_confidence"]


def test_SIN_muestra_del_dominio_no_se_construye_ningun_email():
    # La regla de oro. Sin una muestra no hay convencion, hay una apuesta.
    c = contacto.contacto_de("Carolina Reyes", DOM, "Somos una agencia en Santiago.")
    assert c["email"] is None
    assert c["email_source"] is None
    assert any("no publica" in e for e in c["evidence"])


def test_emparejar_demuestra_la_convencion_del_dominio():
    pares = contacto.emparejar(
        ["matias.bravo@onzamarketing.cl", "info@onzamarketing.cl"],
        ["Matias Bravo", "Carolina Reyes"])
    assert pares == [("Matias Bravo", "matias.bravo@onzamarketing.cl")]


def test_el_apellido_usado_es_el_PRIMERO_como_manda_el_uso_hispano():
    # Una libreria anglosajona toma el ultimo token y produce juan.gonzalez@,
    # que esta mal en la mayoria de los casos.
    assert contacto.partir_nombre("Juan Perez Gonzalez") == ("juan", "perez")
    assert contacto.partir_nombre("Maria de los Angeles Rojas") == ("maria", "angeles")
    assert contacto.partir_nombre("Juan") is None


def test_las_tildes_no_llegan_al_buzon():
    # Ningun dominio hispano pone tildes en el local. Sin normalizar, la mitad
    # de los nombres chilenos genera un candidato invalido.
    assert contacto.construir_email("José Muñoz", "acme.cl", "first.last") == "jose.munoz@acme.cl"


# ── C3: el generico ─────────────────────────────────────────────────────────

def test_el_generico_se_devuelve_etiquetado_como_generico():
    texto = "Escribenos a info@acme.cl"
    c = contacto.contacto_de("Ana Soto", "acme.cl", texto)
    assert c["email"] == "info@acme.cl"
    assert c["email_source"] == "generico"
    # Y se dice explicitamente que NO es de la persona.
    assert any("no de Ana Soto" in e or "no de" in e for e in c["evidence"])


def test_el_generico_no_se_confunde_con_un_email_personal():
    assert contacto.es_generico("info@acme.cl") is True
    assert contacto.es_generico("ventas@acme.cl") is True
    assert contacto.es_generico("matias.bravo@acme.cl") is False


# ── C4: telefono y WhatsApp ─────────────────────────────────────────────────

def test_normaliza_el_telefono_a_e164():
    assert contacto.normalizar_telefono("+56 9 8765 4321", "CL")["e164"] == "+56987654321"
    assert contacto.normalizar_telefono("(2) 2345 6789", "CL")["e164"] == "+56223456789"


def test_distingue_movil_de_fijo():
    assert contacto.normalizar_telefono("+56 9 8765 4321", "CL")["movil"] is True
    assert contacto.normalizar_telefono("+56 2 2345 6789", "CL")["movil"] is False


def test_un_fijo_no_se_guarda_como_whatsapp():
    # Guardarlo como si lo tuviera genera una tarea que nunca va a llegar.
    c = contacto.contacto_de("Ana Soto", "acme.cl", "Oficina: +56 2 2345 6789")
    assert c["phone"] == "+56223456789"
    assert c["phone_kind"] == "landline"
    assert c["whatsapp"] is None


def test_prefiere_el_movil_cuando_hay_los_dos():
    c = contacto.contacto_de("Matias Bravo", DOM, TEXTO)
    assert c["phone_kind"] == "mobile"
    assert c["whatsapp"] == "+56987654321"


def test_no_inventa_un_telefono_desde_cualquier_numero():
    for basura in ["2026", "12", "123456789012345678", "Av. Providencia 1234"]:
        assert contacto.normalizar_telefono(basura, "CL") is None, basura


# ── C5: procedencia siempre ─────────────────────────────────────────────────

@pytest.mark.parametrize("nombre,texto", [
    ("Matias Bravo", TEXTO),                       # publicado
    ("Carolina Reyes", TEXTO),                     # patron
    ("Ana Soto", "Escribenos a info@acme.cl"),     # generico
    ("Ana Soto", "Somos una agencia."),            # nada
])
def test_todo_contacto_trae_su_procedencia_para_poder_auditar(nombre, texto):
    dom = DOM if texto is TEXTO else "acme.cl"
    c = contacto.contacto_de(nombre, dom, texto)
    assert c["evidence"], (nombre, c)
    if c["email"]:
        assert c["email_source"] in {"publicado", "patron", "generico"}
        assert c["email_confidence"] > 0


# ── C6: cuando el sitio no publica ningun email ─────────────────────────────
# MEDIDO (2026-09-04): fintual.cl y xepelin.com no tienen ni un mailto ni una
# direccion en todo el sitio. Usan formulario. Sin una muestra del dominio la
# regla del repo prohibe construir nada, asi que la muestra hay que buscarla.

def test_una_muestra_hallada_afuera_habilita_el_patron():
    # Una nota de prensa o un directorio publican "prensa@empresa.cl". Con esa
    # muestra la convencion queda deducida y la persona sale gratis.
    de_la_web = "Contacto de prensa: paula.torres@acme.cl para consultas."
    c = contacto.contacto_de("Ana Soto", "acme.cl", de_la_web)
    assert c["email"] == "ana.soto@acme.cl"
    assert c["email_source"] == "patron"
    assert any("paula.torres@acme.cl" in e for e in c["evidence"])


def test_una_muestra_generica_hallada_afuera_no_habilita_el_patron():
    # `info@acme.cl` no dice nada de la convencion para personas: no tiene
    # nombre ni apellido de donde deducirla.
    c = contacto.contacto_de("Ana Soto", "acme.cl", "Escribir a info@acme.cl")
    assert c["email_source"] == "generico"
    assert c["email"] == "info@acme.cl"
