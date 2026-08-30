"""G10 -- SSRF cerrado. Con control positivo: un host publico real SI pasa."""
import pytest
from venara_discovery import security

INTERNOS = [
    "http://127.0.0.1/", "http://localhost/", "http://[::1]/",
    "http://169.254.169.254/latest/meta-data/",          # metadata AWS
    "http://metadata.google.internal/computeMetadata/",   # metadata GCP
    "http://10.0.0.5/", "http://192.168.1.1/", "http://172.16.0.1/",
    "http://0.0.0.0/", "file:///etc/passwd", "ftp://interno/",
    "gopher://127.0.0.1:11211/", "http://user:pass@127.0.0.1/",
]


@pytest.mark.parametrize("url", INTERNOS)
def test_bloquea_destinos_internos(url):
    assert security.is_safe_public_url(url) is False, url


@pytest.mark.parametrize("ip,esperado", [
    ("8.8.8.8", True), ("1.1.1.1", True),
    ("127.0.0.1", False), ("169.254.169.254", False),
    ("10.1.2.3", False), ("192.168.0.1", False), ("172.20.0.1", False),
    ("100.64.0.1", False),        # CGNAT -- no lo marca is_private
    ("::ffff:127.0.0.1", False),  # IPv4 mapeada en IPv6
    ("fd00::1", False),           # unique local IPv6
    ("64:ff9b::7f00:1", False),   # NAT64 hacia loopback
    ("0.0.0.0", False), ("224.0.0.1", False),
])
def test_clasificacion_de_ips(ip, esperado):
    assert security.ip_es_publica(ip) is esperado, ip


def test_CONTROL_un_host_publico_real_si_pasa():
    # Sin este control, una funcion que devuelva False siempre pasaria todos
    # los tests de arriba y bloquearia el producto entero.
    assert security.is_safe_public_url("https://example.com/") is True


def test_url_demasiado_larga_se_rechaza():
    assert security.is_safe_public_url("https://example.com/" + "a" * 5000) is False


def test_el_handler_de_redirect_corta_hacia_interno():
    h = security._RedirectSeguro()
    assert h.redirect_request(None, None, 302, "", {}, "http://169.254.169.254/") is None


def test_leer_acotado_trunca():
    # Sin techo, un servidor que nunca cierra llena la memoria del contenedor.
    class FalsaResp:
        def read(self, n=-1):
            return b"x" * (n if n and n > 0 else 10 ** 9)
    txt = security.leer_acotado(FalsaResp(), max_bytes=1000)
    assert len(txt) == 1000


def test_el_opener_no_maneja_esquemas_peligrosos():
    import urllib.request
    tipos = {type(h) for h in security.OPENER_SEGURO.handlers}
    assert urllib.request.FileHandler not in tipos
    assert urllib.request.FTPHandler not in tipos


def test_marcador_final():
    print("\nSEGURIDAD VERIFICADA")
