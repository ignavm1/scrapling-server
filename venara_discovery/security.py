"""Defensas de red: SSRF, rebinding DNS, redirects y agotamiento de recursos.

NADA de esto se relajo para conseguir mas resultados. Donde hubo que elegir
entre un lead mas y una puerta abierta, gano la puerta cerrada.
"""
from __future__ import annotations
import ipaddress
import socket
import urllib.request
import logging
from urllib.parse import urlparse

from . import config

log = logging.getLogger(__name__)

# Rangos que NUNCA se contactan. `ipaddress` cubre casi todo con sus flags, pero
# hay bloques que no marca y que son objetivos clasicos de SSRF.
_EXTRA_BLOQUEADOS = [
    ipaddress.ip_network("100.64.0.0/10"),    # CGNAT (RFC 6598)
    ipaddress.ip_network("192.0.0.0/24"),     # IETF protocol assignments
    ipaddress.ip_network("198.18.0.0/15"),    # benchmarking
    ipaddress.ip_network("::ffff:0:0/96"),    # IPv4 mapeada en IPv6
    ipaddress.ip_network("fc00::/7"),         # unique local IPv6
    ipaddress.ip_network("64:ff9b::/96"),     # NAT64 -- puede mapear a privadas
]


def ip_es_publica(ip_txt: str) -> bool:
    """True solo si la IP es enrutable en internet publico."""
    try:
        ip = ipaddress.ip_address(ip_txt)
    except ValueError:
        return False
    if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
            or ip.is_multicast or ip.is_unspecified):
        return False
    # 169.254.169.254 (metadata de cloud) ya cae por is_link_local; se deja el
    # comentario porque es el objetivo que mas se intenta.
    for red in _EXTRA_BLOQUEADOS:
        try:
            if ip in red:
                return False
        except TypeError:
            continue   # comparar IPv4 contra red IPv6 y viceversa
    return True


def resolver_ips(host: str) -> list[str]:
    """Todas las IPs a las que resuelve el host. Vacio si no resuelve."""
    try:
        return [i[4][0] for i in socket.getaddrinfo(host, None)]
    except Exception:
        return []


def is_safe_public_url(url: str) -> bool:
    """Solo http/https hacia hosts publicos.

    OJO CON EL LIMITE DE ESTA FUNCION: valida el nombre resolviendo DNS, pero
    quien conecta despues vuelve a resolver. Entre las dos resoluciones un
    atacante puede cambiar el registro y apuntar a 127.0.0.1 -- eso es DNS
    rebinding, y ninguna funcion de validacion por nombre lo puede cerrar sola.

    El cierre real esta en `peer_es_publico()`, que mira la IP del socket ya
    conectado -- la unica que no se puede envenenar entre la validacion y la
    conexion. Esta funcion queda como primer filtro barato: rechaza lo obvio
    sin gastar una conexion.
    """
    if not url or len(url) > config.MAX_URL_LEN:
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    # Un host con credenciales embebidas (user:pass@) suele ser ofuscacion.
    if p.username or p.password:
        return False
    ips = resolver_ips(p.hostname)
    if not ips:
        return False
    return all(ip_es_publica(ip) for ip in ips)


class _RedirectSeguro(urllib.request.HTTPRedirectHandler):
    """Corta redirects hacia hosts internos.

    Un servidor publico puede responder 302 hacia 169.254.169.254: sin esto la
    validacion inicial de la URL no sirve de nada.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not is_safe_public_url(newurl):
            log.warning("redirect bloqueado hacia host no permitido")
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def construir_opener() -> urllib.request.OpenerDirector:
    """Opener limitado a http/https. UnknownHandler convierte file:// y ftp://
    en error en vez de leerlos."""
    o = urllib.request.OpenerDirector()
    o.add_handler(urllib.request.HTTPHandler())
    o.add_handler(urllib.request.HTTPSHandler())
    o.add_handler(_RedirectSeguro())
    o.add_handler(urllib.request.HTTPErrorProcessor())
    o.add_handler(urllib.request.HTTPDefaultErrorHandler())
    o.add_handler(urllib.request.UnknownHandler())
    return o


OPENER_SEGURO = construir_opener()


def leer_acotado(resp, max_bytes: int = None) -> str:
    """Lee como mucho `max_bytes`.

    `resp.read()` sin argumento lee hasta que el otro lado cierre. Un servidor
    hostil que nunca cierra llena la memoria del contenedor: en Render eso es
    el OOM que ya tumbo el servicio una vez (F12 en FINDINGS.md).
    """
    limite = max_bytes or config.MAX_HTML_BYTES
    datos = resp.read(limite + 1)
    if len(datos) > limite:
        log.warning("respuesta truncada en %d bytes", limite)
        datos = datos[:limite]
    return datos.decode("utf-8", errors="ignore")


def ip_conectada(resp) -> str:
    """IP a la que de verdad se conecto esta respuesta, o "" si no se puede ver.

    Se lee del socket, no del DNS. Es la unica fuente que no se puede envenenar
    entre la validacion y la conexion.
    """
    try:
        return resp.fp.raw._sock.getpeername()[0]
    except Exception:
        return ""


def peer_es_publico(resp) -> bool:
    """La conexion termino en una IP publica?

    ESTE es el cierre real del DNS rebinding, y reemplaza a la
    `url_con_ip_fijada()` que vivia aca: aquella prometia "IP validada
    incrustada" en su nombre y su docstring, y devolvia la URL sin tocar. Era
    codigo muerto (cero llamadas) y ademas no hacia lo que decia -- la
    auditoria del 2026-09-07 encontro las dos cosas.

    Validar el socket funciona para http y para https por igual. Fijar la IP en
    la URL, en cambio, rompe la verificacion del certificado en https, que es
    justamente donde mas importa.

    Devuelve False cuando no se puede determinar la IP: si no sabemos adonde
    nos conectamos, no leemos la respuesta.
    """
    ip = ip_conectada(resp)
    return bool(ip) and ip_es_publica(ip)
