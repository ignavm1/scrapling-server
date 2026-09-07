"""Los tres hallazgos de la auditoria del 2026-09-07.

Los dos criticos se encadenan: la auth que fallaba abierta exponia el SSRF del
campo `domain` a cualquiera en internet. Se arreglan juntos porque por separado
no sirven.
"""
import pytest
from fastapi.testclient import TestClient

from venara_discovery import api, config, fetch, security
from venara_discovery.fetch import SaludProveedores

cliente = TestClient(api.app)


# ── H1: la auth falla CERRADA ───────────────────────────────────────────────

def test_fail_closed_sin_api_key_el_servidor_rechaza(monkeypatch):
    """MEDIDO en produccion el 2026-09-07: /scrape-website y
    /find-decision-maker devolvian 200 con datos reales SIN cabecera alguna.

    La causa era `if config.API_KEY and not compare_digest(...)`: sin la
    variable, el cuerpo entero se salteaba y el Depends no rechazaba nada.
    """
    monkeypatch.setattr(config, "API_KEY", None)
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    for ruta, cuerpo in [
        ("/scrape-website", {"url": "https://example.com"}),
        ("/find-decision-maker", {"company": "x"}),
        ("/search-people", {"query": "x"}),
        ("/search-linkedin-companies", {"query": "x", "location": "y"}),
    ]:
        r = cliente.post(ruta, json=cuerpo)
        assert r.status_code == 503, (ruta, r.status_code)


def test_sin_api_key_el_health_declara_que_esta_mal_configurado(monkeypatch):
    # Que se note al desplegar, y no meses despues en el log de un tercero.
    monkeypatch.setattr(config, "API_KEY", None)
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    d = cliente.get("/health").json()
    assert d["auth"] is False
    assert d["misconfigured"] is True


def test_key_correcta_pasa(monkeypatch):
    # El arreglo no puede romper al cliente legitimo.
    monkeypatch.setattr(config, "API_KEY", "secreto-de-prueba")
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    r = cliente.post("/find-decision-maker", json={"company": ""},
                     headers={"X-API-Key": "secreto-de-prueba"})
    assert r.status_code == 200


@pytest.mark.parametrize("cabeceras", [
    {}, {"X-API-Key": ""}, {"X-API-Key": "otra-cosa"},
])
def test_key_incorrecta_o_ausente_recibe_401(monkeypatch, cabeceras):
    monkeypatch.setattr(config, "API_KEY", "secreto-de-prueba")
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    r = cliente.post("/find-decision-maker", json={"company": "x"}, headers=cabeceras)
    assert r.status_code == 401, (cabeceras, r.status_code)


def test_key_con_caracteres_no_ascii_da_401_y_no_500(monkeypatch):
    """Se prueba la funcion directamente, no via el cliente HTTP.

    httpx rechaza un header no-ASCII antes de mandarlo, asi que por el cliente
    este caso no se puede reproducir -- pero por la red cruda si. Comparar str
    en vez de bytes lanzaba TypeError, y ese 500 convertia al propio chequeo de
    auth en un vector de denegacion de servicio.
    """
    from fastapi import HTTPException
    monkeypatch.setattr(config, "API_KEY", "secreto-de-prueba")
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    with pytest.raises(HTTPException) as e:
        api.require_api_key("caf\u00e9")
    assert e.value.status_code == 401


def test_escape_local_existe_pero_no_se_activa_solo(monkeypatch):
    # Un default que abre el servidor es exactamente como se llego al hallazgo
    # que esto cierra. El escape lo declara el operador.
    monkeypatch.setattr(config, "API_KEY", None)
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", False)
    assert cliente.post("/find-decision-maker", json={"company": "x"}).status_code == 503
    monkeypatch.setattr(config, "PERMITIR_SIN_AUTH", True)
    assert cliente.post("/find-decision-maker", json={"company": "x"}).status_code == 200


def test_escape_local_no_esta_encendido_por_defecto(monkeypatch):
    """Con el entorno limpio, el escape esta APAGADO.

    Se recarga el modulo en vez de leer `config.PERMITIR_SIN_AUTH`: el fixture
    autouse del conftest lo pone en True para toda la suite, asi que leerlo
    aca mediria el fixture y no el default -- un test que se prueba a si mismo.
    """
    import importlib
    monkeypatch.delenv("PERMITIR_SIN_AUTH", raising=False)
    limpio = importlib.reload(config)
    try:
        assert limpio.PERMITIR_SIN_AUTH is False
        monkeypatch.setenv("PERMITIR_SIN_AUTH", "true")   # cualquier otra cosa
        assert importlib.reload(config).PERMITIR_SIN_AUTH is False
        monkeypatch.setenv("PERMITIR_SIN_AUTH", "1")
        assert importlib.reload(config).PERMITIR_SIN_AUTH is True
    finally:
        monkeypatch.delenv("PERMITIR_SIN_AUTH", raising=False)
        importlib.reload(config)


# ── H2: el SSRF, cerrado en el cuello unico ─────────────────────────────────

@pytest.mark.parametrize("url", [
    "http://169.254.169.254/latest/meta-data/",   # metadata de la nube
    "http://127.0.0.1:8765/",
    "http://localhost/",
    "http://10.0.0.1/",
    "http://192.168.1.1/",
    "http://[::1]/",
    "file:///etc/passwd",
    "ftp://interno/",
])
def test_obtener_bloquea_las_urls_que_no_debe_pedir(url):
    """La validacion va en fetch.obtener y no en cada llamador.

    Por esta funcion salen las QUINCE llamadas de red del paquete. Validar en
    el llamador son quince lugares donde olvidarse, y la auditoria encontro
    exactamente eso: cero validaciones en fetch, decisor, website y personas.
    """
    r = fetch.obtener(url, "sitio", SaludProveedores(), timeout=2)
    assert r.sirve is False, url
    assert r.error == "url-no-permitida", (url, r.error)


def test_obtener_no_bloquea_un_host_publico_legitimo():
    # Control positivo: sin esto, un validador que rechaza TODO pasaria el test
    # de arriba y romperia el scraping entero en silencio.
    assert security.is_safe_public_url("https://example.com") is True


@pytest.mark.parametrize("entrada", [
    "169.254.169.254", "127.0.0.1", "localhost:8765", "10.0.0.1",
    "[::1]", "0.0.0.0", "metadata", "..", "https://169.254.169.254/x",
])
def test_domain_valida_rechaza_lo_que_no_es_un_dominio(entrada):
    # Segunda barrera, antes de que el valor llegue al resolutor. Es sintactica
    # a proposito: resolver DNS dentro del parseo del request lo vuelve lento y
    # regala un vector -- cien peticiones con dominios lentos cuelgan el server.
    assert api._dominio_valido(entrada) == "", entrada


@pytest.mark.parametrize("entrada,esperado", [
    ("fintual.cl", "fintual.cl"),
    ("https://www.fintual.cl/equipo", "fintual.cl"),
    ("ONZAMARKETING.CL", "onzamarketing.cl"),
])
def test_domain_valida_deja_pasar_un_dominio_legitimo(entrada, esperado):
    assert api._dominio_valido(entrada) == esperado


def test_metadata_de_la_nube_no_llega_al_resolutor(monkeypatch):
    """El exploit completo del hallazgo 2, de punta a punta.

    POST /find-decision-maker {"domain":"169.254.169.254"} hacia que el
    servidor pidiera la metadata de la nube desde dentro de Render.
    """
    pedidas = []

    def _espia(url, proveedor, salud, timeout=None):
        pedidas.append(url)
        from venara_discovery.fetch import Respuesta
        return Respuesta(proveedor, url, error="simulado")

    from venara_discovery import decisor
    monkeypatch.setattr(decisor, "obtener", _espia)
    cliente.post("/find-decision-maker",
                 json={"company": "Acme", "domain": "169.254.169.254"})
    assert not any("169.254" in u for u in pedidas), pedidas


# ── H3: el cierre del rebinding, ahora llamado ──────────────────────────────

def test_rebinding_la_defensa_ya_no_es_codigo_muerto():
    """`url_con_ip_fijada()` existia, su docstring decia ser "el cierre real"
    del rebinding, tenia CERO llamadas, y ademas devolvia la URL sin tocar --
    no hacia lo que su nombre prometia.

    Se reemplazo por `peer_es_publico()`, que mira la IP del socket ya
    conectado. Este test falla si alguien la vuelve a dejar sin llamar.
    """
    import pathlib
    raiz = pathlib.Path(__file__).parent.parent / "venara_discovery"
    llamadas = 0
    for f in raiz.glob("*.py"):
        if f.name == "security.py":
            continue
        llamadas += f.read_text(encoding="utf-8").count("peer_es_publico(")
    assert llamadas >= 1, "peer_es_publico volvio a quedar sin llamadas"
    # Y la funcion muerta y equivocada ya no existe.
    assert not hasattr(security, "url_con_ip_fijada")


class _RespFalsa:
    def __init__(self, ip):
        class _S:
            def getpeername(self_inner):
                return (ip, 443)
        class _Raw:
            _sock = _S()
        class _Fp:
            raw = _Raw()
        self.fp = _Fp()


def test_rebinding_una_ip_interna_en_el_socket_se_rechaza():
    # El caso que cierra: el DNS dijo publica al validar y el socket termino en
    # loopback. Es la unica fuente que no se puede envenenar.
    assert security.peer_es_publico(_RespFalsa("127.0.0.1")) is False
    assert security.peer_es_publico(_RespFalsa("169.254.169.254")) is False
    assert security.peer_es_publico(_RespFalsa("10.0.0.5")) is False


def test_rebinding_una_ip_publica_pasa():
    # Control positivo: sin esto, un chequeo que rechaza todo pasaria el test
    # de arriba y romperia el fallback de scraping.
    assert security.peer_es_publico(_RespFalsa("93.184.216.34")) is True


def test_rebinding_si_no_se_puede_ver_la_ip_no_se_lee_la_respuesta():
    # Si no sabemos adonde nos conectamos, no leemos el cuerpo.
    class _Opaca:
        pass
    assert security.peer_es_publico(_Opaca()) is False
