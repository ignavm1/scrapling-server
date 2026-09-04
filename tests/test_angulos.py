"""Siete angulos, y que los siete lleguen a ejecutarse.

El segundo punto no es un detalle: el angulo de LinkedIn existio una version
entera sin correr NUNCA porque el techo de fetches lo dejaba fuera (F24.3). Con
siete angulos ese mismo fallo habria matado a tres.
"""
from venara_discovery import config, decisor, providers
from venara_discovery.location import interpretar

EMPRESA, DOMINIO = "Onza Marketing", "onzamarketing.cl"
UBI = interpretar("Santiago, Chile")


def _plan():
    return decisor.construir_plan(EMPRESA, DOMINIO, UBI)


def _nombres():
    vistos = []
    for a in _plan():
        if a.nombre not in vistos:
            vistos.append(a.nombre)
    return vistos


def test_hay_siete_angulos_con_nombre_distinto():
    assert len(_nombres()) == 7, _nombres()


def test_los_siete_angulos_son_realmente_distintos_entre_si():
    # Siete nombres sobre la misma query no son siete angulos: el valor esta en
    # que cada uno alcance documentos que los otros no.
    queries = [a.query for a in _plan()]
    assert len(queries) == len(set(queries)), queries


def test_los_dos_angulos_nuevos_cubren_superficies_que_los_viejos_no():
    nombres = _nombres()
    assert "directorio_ejecutivo" in nombres
    assert "representante_legal" in nombres
    por_nombre = {a.nombre: a.query for a in _plan()}
    # El directorio busca la nomenclatura de los agregadores; ningun otro
    # angulo la usa.
    assert "leadership team" in por_nombre["directorio_ejecutivo"]
    # El representante legal busca lenguaje de registro publico, que no aparece
    # en ninguna pagina de equipo ni en una nota de prensa cualquiera.
    assert "representante legal" in por_nombre["representante_legal"]


def test_los_siete_angulos_llegan_a_ejecutarse_con_el_techo_real():
    # Existir en el plan no sirve de nada si el techo los recorta. Este test
    # reproduce el reparto real del resolutor.
    orden = sorted(_plan(), key=lambda a: a.prioridad)
    usables = providers.activos()[: config.DECISOR_PROVEEDORES_POR_ANGULO]
    trabajos = [(a, usables[r]) for r in range(len(usables)) for a in orden]
    ejecutados = {a.nombre for a, _ in trabajos[: config.DECISOR_MAX_FETCHES]}
    assert ejecutados == set(_nombres()), sorted(set(_nombres()) - ejecutados)


def test_el_reparto_por_rondas_le_da_un_proveedor_a_cada_angulo_antes_del_segundo():
    # Control de que el arreglo es el que se dice: la primera ronda cubre TODOS
    # los angulos. Con el reparto viejo, los primeros se llevaban dos
    # proveedores y los ultimos ninguno.
    orden = sorted(_plan(), key=lambda a: a.prioridad)
    usables = providers.activos()[: config.DECISOR_PROVEEDORES_POR_ANGULO]
    trabajos = [(a, usables[r]) for r in range(len(usables)) for a in orden]
    primera_ronda = trabajos[: len(orden)]
    assert {a.nombre for a, _ in primera_ronda} == set(_nombres())
    assert len({p.nombre for _, p in primera_ronda}) == 1


def test_el_techo_alcanza_para_la_primera_ronda_completa():
    # Si alguien baja DECISOR_MAX_FETCHES por debajo del numero de angulos,
    # vuelven los angulos que no corren nunca. Este test lo impide.
    assert config.DECISOR_MAX_FETCHES >= len(_plan()), (
        config.DECISOR_MAX_FETCHES, len(_plan()))
