"""Elegir al que FIRMA, no al que esta mejor documentado."""
import pytest

from venara_discovery import cargos, decisor


def _c(nombre, cargo, score):
    c = decisor.Candidato(nombre=nombre, cargo=cargo, url="https://x.cl",
                          angulo="a", proveedor="brave", origen="tercero",
                          donde="snippet", empresa_en_texto=True)
    c.score = score
    return c


# ── N1: la escala ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("cargo,esperado", [
    ("Co-founder & CEO", "fundador"),
    ("Fundador", "fundador"),
    ("Gerente General", "ejecutivo"),
    ("Managing Director", "ejecutivo"),
    ("CTO", "c_level"),
    ("Head of Growth", "area"),
    ("Gerente de Marketing", "mando"),
    ("Analista de datos", "otro"),
    ("", "otro"),
])
def test_el_nivel_del_cargo_es_el_esperado(cargo, esperado):
    assert cargos.nivel(cargo) == esperado, (cargo, cargos.nivel(cargo))


def test_la_escala_no_colapsa_todo_al_mismo_nivel():
    # Sin este control, una escala rota que devuelve siempre lo mismo pasaria
    # los tests de orden: si todos empatan, cualquier orden es "correcto".
    niveles = {cargos.nivel(c) for c in
               ["CEO", "Gerente General", "CTO", "Head of Sales",
                "Gerente de Marketing", "Practicante"]}
    assert len(niveles) >= 4, niveles


def test_un_fundador_vale_mas_que_un_gerente_de_area():
    assert cargos.rango("Fundador") < cargos.rango("Gerente General")
    assert cargos.rango("Gerente General") < cargos.rango("Head of Growth")
    assert cargos.rango("Head of Growth") < cargos.rango("Analista")


# ── N2: la eleccion ─────────────────────────────────────────────────────────

def test_elige_el_mejor_cargo_aunque_otro_tenga_mejor_evidencia():
    # El caso que motiva todo: el gerente de marketing esta publicado en el
    # sitio de la empresa (evidencia perfecta) y el CEO viene de un directorio.
    # A quien hay que escribirle es al CEO.
    marketing = _c("Ana Soto", "Gerente de Marketing", 1.0)
    ceo = _c("Matias Bravo", "CEO", 0.65)
    orden = cargos.elegir_mejor([marketing, ceo])
    assert orden[0].nombre == "Matias Bravo", [(c.nombre, c.cargo) for c in orden]


def test_dentro_del_mismo_nivel_desempata_la_evidencia():
    flojo = _c("Ana Soto", "CEO", 0.60)
    solido = _c("Matias Bravo", "Fundador", 0.95)
    assert cargos.elegir_mejor([flojo, solido])[0].nombre == "Matias Bravo"


def test_las_alternativas_siguen_viajando():
    # En una PyME el gerente general y el fundador suelen ser dos personas, y a
    # veces conviene escribirle a la otra.
    orden = cargos.elegir_mejor([_c("A", "CEO", 0.9), _c("B", "Gerente General", 0.9)])
    assert len(orden) == 2


# ── N3: el piso ─────────────────────────────────────────────────────────────

def test_un_cargo_alto_sin_evidencia_no_desplaza_a_uno_solido():
    # Sin piso, cualquier "CEO" recogido de un blog cualquiera le ganaria al
    # gerente general publicado en el sitio de la propia empresa.
    basura = _c("Quien Sabe", "CEO", 0.30)
    solido = _c("Matias Bravo", "Gerente General", 0.95)
    orden = cargos.elegir_mejor([basura, solido])
    assert orden[0].nombre == "Matias Bravo", [(c.nombre, c.score) for c in orden]


def test_control_positivo_sin_el_piso_la_eleccion_se_rompe():
    # Demuestra que el piso hace algo: con la MISMA entrada, ordenar solo por
    # cargo pone primero a la basura.
    basura = _c("Quien Sabe", "CEO", 0.30)
    solido = _c("Matias Bravo", "Gerente General", 0.95)
    solo_cargo = sorted([basura, solido], key=lambda c: cargos.rango(c.cargo))
    assert solo_cargo[0].nombre == "Quien Sabe"
    assert cargos.elegir_mejor([basura, solido])[0].nombre != "Quien Sabe"


def test_el_piso_deja_pasar_al_cargo_alto_bien_respaldado():
    # Y control de que el piso no bloquea todo: un CEO con evidencia suficiente
    # si lidera sobre un gerente general.
    ceo = _c("Matias Bravo", "CEO", 0.65)
    gg = _c("Ana Soto", "Gerente General", 0.95)
    assert cargos.elegir_mejor([ceo, gg])[0].nombre == "Matias Bravo"
