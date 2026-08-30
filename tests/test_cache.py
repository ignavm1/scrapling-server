"""G9 -- la cache nunca guarda un bloqueo, ni crece sin limite."""
import time
from venara_discovery.cache import CacheBusqueda


def test_NO_guarda_resultados_incompletos():
    # El arreglo central (F9): una busqueda bloqueada guardaba [] con TTL 6h y
    # dejaba ese nicho muerto seis horas, con HTTP 200 para que nadie lo note.
    c = CacheBusqueda()
    k = c.clave("marketing", "Lima")
    c.guardar(k, [], completo=False)
    assert c.obtener(k) is None, "se cacheo un resultado bloqueado"
    assert len(c) == 0


def test_guarda_resultados_completos():
    c = CacheBusqueda()
    k = c.clave("marketing", "Lima")
    c.guardar(k, [{"name": "Acme"}], completo=True)
    assert c.obtener(k) == [{"name": "Acme"}]


def test_la_clave_ignora_max_results():
    # Pedir 20 y pedir 25 es el mismo trabajo de scraping. Incluirlo duplicaba
    # el gasto de proxy por nada.
    c = CacheBusqueda()
    assert c.clave("Marketing", "Lima") == c.clave("marketing", " lima ")


def test_expira():
    c = CacheBusqueda(ttl=1)
    k = c.clave("x", "y")
    c.guardar(k, [1], completo=True)
    assert c.obtener(k) == [1]
    time.sleep(1.05)
    assert c.obtener(k) is None


def test_no_crece_sin_limite():
    c = CacheBusqueda(maximo=10)
    for i in range(40):
        c.guardar(c.clave("n%d" % i, ""), [i], completo=True)
    assert len(c) <= 10, "la cache crecio a %d" % len(c)


def test_queries_distintas_no_se_pisan():
    c = CacheBusqueda()
    c.guardar(c.clave("marketing", "Lima"), ["a"], completo=True)
    c.guardar(c.clave("marketing", "Santiago"), ["b"], completo=True)
    assert c.obtener(c.clave("marketing", "Lima")) == ["a"]
    assert c.obtener(c.clave("marketing", "Santiago")) == ["b"]


def test_marcador_final():
    print("\nCACHE VERIFICADA")
