"""Entity resolution: decidir si dos resultados son la MISMA empresa.

El servidor viejo usaba `_domain(website)` como clave y eso fallaba en las dos
direcciones (F10 en FINDINGS.md):

  - Unia de mas: dos negocios distintos en `algo.wixsite.com` colapsaban en uno,
    porque el dominio pertenece al proveedor de hosting, no a la empresa.
  - Unia de menos: sin website usaba la `linkedin_url` completa, asi que la
    misma empresa con y sin `?trk=` quedaba duplicada.

Aca las senales estan ordenadas por cuanto prueban. Un dominio registrable
compartido es prueba fuerte de identidad; un nombre parecido, no.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from .normalize import clave_nombre, dominio_registrable, normalizar_url


@dataclass
class Empresa:
    """Una empresa resuelta, con la evidencia de donde salio."""
    nombre: str = ""
    website: str = ""
    linkedin_url: str = ""
    descripcion: str = ""
    ubicacion_texto: str = ""
    # Titulo y snippet TAL COMO vinieron del buscador. Se separa de `nombre`
    # porque son cosas distintas: `nombre` es lo que ve el cliente y se limpia
    # hasta quedar en "Limadigital"; `texto_fuente` conserva "Agencia de
    # Marketing Digital en Lima" que es LA evidencia para medir relevancia y
    # ubicacion. Puntuar sobre el nombre limpio da relevancia 0 a una empresa
    # perfectamente relevante.
    texto_fuente: str = ""
    # De cuantas fuentes distintas vino. Aparecer en Bing Y en DuckDuckGo es
    # evidencia real de que la empresa existe; una sola fuente puede ser ruido.
    fuentes: set[str] = field(default_factory=set)
    queries: set[str] = field(default_factory=set)
    senales: dict = field(default_factory=dict)

    @property
    def dominio(self) -> str:
        return dominio_registrable(self.website) if self.website else ""

    @property
    def clave_linkedin(self) -> str:
        """Slug de la pagina de empresa, sin querystring."""
        u = (self.linkedin_url or "").lower()
        if "linkedin.com/company/" not in u:
            return ""
        resto = u.split("linkedin.com/company/", 1)[1]
        return resto.split("?")[0].split("#")[0].strip("/")

    def claves(self) -> list[str]:
        """Identificadores por los que esta empresa puede reconocerse.

        El orden refleja cuanto prueba cada senal. `nom:` va ultimo y solo se
        usa cuando no hay dominio ni LinkedIn: unir por nombre es lo que mas
        falsos positivos genera ("Agencia Digital" hay una por ciudad).
        """
        ks = []
        if self.dominio:
            ks.append("dom:" + self.dominio)
        if self.clave_linkedin:
            ks.append("li:" + self.clave_linkedin)
        if not ks:
            k = clave_nombre(self.nombre)
            if len(k) >= 4:      # "acme" si, "sa" no
                ks.append("nom:" + k)
        return ks

    def absorber(self, otra: "Empresa") -> None:
        """Funde otra observacion de la misma empresa.

        Se completa lo que falta y no se pisa lo que ya hay: el primer valor
        vino de la fuente que mas alto rankeo, y sobreescribirlo con el de una
        fuente peor degrada el dato.
        """
        if not self.website and otra.website:
            self.website = otra.website
        if not self.linkedin_url and otra.linkedin_url:
            self.linkedin_url = otra.linkedin_url
        # Entre dos nombres se queda el mas informativo, no el mas largo: un
        # titulo largo suele ser una frase de marketing.
        if otra.nombre and (not self.nombre or
                            (len(self.nombre) < 3 and len(otra.nombre) >= 3)):
            self.nombre = otra.nombre
        if not self.descripcion and otra.descripcion:
            self.descripcion = otra.descripcion
        if not self.ubicacion_texto and otra.ubicacion_texto:
            self.ubicacion_texto = otra.ubicacion_texto
        if otra.texto_fuente and otra.texto_fuente not in self.texto_fuente:
            self.texto_fuente = (self.texto_fuente + " " + otra.texto_fuente).strip()[:600]
        self.fuentes |= otra.fuentes
        self.queries |= otra.queries
        for k, v in otra.senales.items():
            self.senales.setdefault(k, v)


class Resolutor:
    """Acumula observaciones y las funde en empresas unicas.

    Union-find sobre las claves: si A comparte dominio con B y B comparte
    LinkedIn con C, las tres son la misma empresa aunque A y C no compartan
    ninguna senal directa. Un dict simple por clave no captura eso.
    """

    def __init__(self) -> None:
        self._por_clave: dict[str, int] = {}
        self._empresas: dict[int, Empresa] = {}
        self._siguiente = 0

    def agregar(self, e: Empresa) -> None:
        ks = e.claves()
        if not ks:
            return
        existentes = {self._por_clave[k] for k in ks if k in self._por_clave}

        if not existentes:
            eid = self._siguiente
            self._siguiente += 1
            self._empresas[eid] = e
            for k in ks:
                self._por_clave[k] = eid
            return

        destino = min(existentes)
        # Fusion transitiva: dos grupos que hasta ahora eran distintos resultan
        # ser el mismo porque esta observacion los conecta.
        for otro in existentes - {destino}:
            self._empresas[destino].absorber(self._empresas.pop(otro))
            for k, v in list(self._por_clave.items()):
                if v == otro:
                    self._por_clave[k] = destino
        self._empresas[destino].absorber(e)
        for k in ks:
            self._por_clave[k] = destino

    def _puede_unirse_por_nombre(self, a: Empresa, b: Empresa) -> bool:
        """Reconciliacion final por nombre, con guardas.

        Hace falta porque una empresa vista SOLO en LinkedIn y la MISMA vista
        solo por su website no comparten ninguna clave: una tiene
        `li:acme-digital`, la otra `dom:acmedigital.com`. Sin esta pasada
        quedan como dos empresas, que es justo el duplicado que mas se ve en
        produccion.

        Las guardas son lo que impide el desastre opuesto: unir por nombre a
        secas juntaria las veinte "Agencia Digital" de veinte ciudades.
        """
        ka, kb = clave_nombre(a.nombre), clave_nombre(b.nombre)
        if not ka or ka != kb or len(ka) < 4:
            return False
        # Dos dominios propios distintos = dos empresas distintas, por mucho
        # que se llamen igual.
        if a.dominio and b.dominio and a.dominio != b.dominio:
            return False
        # Idem con dos paginas de LinkedIn distintas.
        if a.clave_linkedin and b.clave_linkedin and a.clave_linkedin != b.clave_linkedin:
            return False
        # Solo se unen si son COMPLEMENTARIAS: una aporta lo que a la otra le
        # falta. Si las dos tienen exactamente lo mismo, no hay evidencia de
        # que sean la misma y si de que son homonimas.
        return bool(a.dominio) != bool(b.dominio) or bool(a.clave_linkedin) != bool(b.clave_linkedin)

    def empresas(self) -> list[Empresa]:
        items = list(self._empresas.values())
        fusionadas: list[Empresa] = []
        for e in items:
            destino = next((f for f in fusionadas if self._puede_unirse_por_nombre(f, e)), None)
            if destino is not None:
                destino.absorber(e)
            else:
                fusionadas.append(e)
        return fusionadas
