"""Resolutor de decisor: de un nombre de empresa a la persona que decide.

Lo que estos tests protegen, en orden de dano si se rompe:

  1. Que no se devuelva a la persona equivocada. Un decision maker de otra
     empresa produce un correo real, a nombre del cliente, a quien no
     corresponde. Es peor que no encontrar a nadie.
  2. Que el gasto siga acotado. La version anterior tardaba 4m43s por consulta
     medidos contra produccion, y se llama UNA VEZ POR EMPRESA.
  3. Que un vacio diga por que.
"""
import pathlib

import pytest
from fastapi.testclient import TestClient

from venara_discovery import api, config, decisor, extraction, personas, providers, website
from venara_discovery.location import interpretar

FIX = pathlib.Path(__file__).parent / "fixtures"
leer = lambda n: (FIX / n).read_text(encoding="utf-8")

# Fixtures SINTETICOS (prefijo declarado): estan escritos a mano para probar el
# parseo. La medicion F7 de test_linkedin.py los excluye a proposito -- un
# archivo hecho a medida no es evidencia sobre el indice publico.
SERP = leer("sintetico_serp_decisor.html")
PAGINA = leer("sintetico_equipo_pagina.html")

EMPRESA = "Onza Marketing"
DOMINIO = "onzamarketing.cl"

cliente = TestClient(api.app)


# ── D1: los angulos ─────────────────────────────────────────────────────────

def test_el_plan_ataca_desde_angulos_distintos():
    plan = decisor.construir_plan(EMPRESA, DOMINIO, interpretar("Santiago, Chile"))
    nombres = {a.nombre for a in plan}
    # Una sola query encuentra siempre lo mismo. El valor del sistema es que
    # cada angulo apunta a una superficie distinta del indice.
    assert {"sitio_equipo", "cargo_directo", "prensa", "entrevista"} <= nombres
    assert len({a.query for a in plan}) == len(plan), "hay queries repetidas"


def test_la_query_directa_es_empresa_mas_cargo():
    # Es literalmente lo pedido: "(nombre de la empresa) ceo".
    plan = decisor.construir_plan(EMPRESA, "", interpretar("Chile"))
    directas = [a.query for a in plan if a.nombre == "cargo_directo"]
    assert any(q == '"Onza Marketing" CEO' for q in directas), directas
    assert any("gerente general" in q for q in directas), directas


def test_el_angulo_del_sitio_propio_usa_el_dominio_cuando_se_conoce():
    con = decisor.construir_plan(EMPRESA, DOMINIO, interpretar("Chile"))[0]
    sin = decisor.construir_plan(EMPRESA, "", interpretar("Chile"))[0]
    assert con.query.startswith("site:onzamarketing.cl"), con.query
    # Sin dominio no se puede usar `site:`, pero el angulo no desaparece: se
    # pregunta por el nombre. Perder la pagina de equipo seria perder la fuente
    # con menos ruido que existe.
    assert "site:" not in sin.query and "nuestro equipo" in sin.query


def test_el_dominio_llega_sucio_y_se_normaliza():
    a = decisor.construir_plan(EMPRESA, "https://www.onzamarketing.cl/equipo", interpretar("Chile"))[0]
    assert a.query.startswith("site:onzamarketing.cl ")


def test_los_cargos_se_pueden_pedir_y_estan_topeados():
    plan = decisor.construir_plan(EMPRESA, "", interpretar("Chile"),
                                  ["CEO", "socio", "gerente general", "director", "jefe"])
    directas = [a for a in plan if a.nombre == "cargo_directo"]
    assert len(directas) <= decisor.MAX_CARGOS_DIRECTOS
    assert '"Onza Marketing" socio' in {a.query for a in directas}


def test_ninguna_query_del_resolutor_usa_linkedin():
    # F7: `site:linkedin.com/in` devuelve cero perfiles con control positivo.
    # Un angulo que no puede traer nada es un angulo menos para los que si.
    for dom in ("", DOMINIO):
        for a in decisor.construir_plan(EMPRESA, dom, interpretar("Chile")):
            assert "linkedin" not in a.query.lower(), a.query


def test_control_positivo_una_query_prohibida_plantada_si_se_detecta():
    # El test de arriba afirma una AUSENCIA, y una ausencia pasa tambien cuando
    # el detector esta roto.
    plantada = decisor.Angulo("plantada", 'site:linkedin.com/in "Onza"', 99)
    assert "linkedin" in plantada.query.lower()


# ── D2: leer la PAGINA, no solo el snippet ──────────────────────────────────

def test_extrae_del_texto_de_la_pagina_de_equipo():
    # F21: 67 resultados crudos dieron UN candidato de snippet, y era falso.
    # Los nombres estan dentro de la pagina.
    texto = website.texto_por_bloques(PAGINA)
    hallados = {p["person_name"]: p["person_title"]
                for p in personas.extraer_de_texto(texto, "https://onzamarketing.cl/nuestro-equipo")}
    assert hallados.get("Matias Bravo") == "Gerente General"
    assert hallados.get("Carolina Reyes") == "Gerente Comercial"


def test_el_texto_por_bloques_conserva_la_frontera_que_el_plano_destruye():
    # "<h3>Matias Bravo</h3><p>Gerente General</p>" aplanado es "Matias Bravo
    # Gerente General", que no se distingue de una frase cualquiera. La
    # estructura ES el dato.
    plano = website.limpiar_html(PAGINA)
    bloques = website.texto_por_bloques(PAGINA)
    assert "\n" not in plano
    assert "Matias Bravo\nGerente General" in bloques
    assert personas.extraer_de_texto(plano, "https://onzamarketing.cl/x") == []


def test_la_pagina_no_inventa_a_quien_no_tiene_cargo_de_decisor():
    # "Tomas Alarcon - Disenador Senior" esta en la pagina y NO debe entrar:
    # no decide, y gastar un toque en el cuesta el toque.
    texto = website.texto_por_bloques(PAGINA)
    nombres = {p["person_name"] for p in personas.extraer_de_texto(texto, "https://x.cl/e")}
    assert "Tomas Alarcon" not in nombres


def test_la_pagina_no_toma_nombres_de_un_script():
    # El fixture esconde "Pedro Falso - Gerente General" dentro de un <script>.
    texto = website.texto_por_bloques(PAGINA)
    assert "Pedro Falso" not in texto


def test_una_pagina_sin_personas_no_produce_candidatos():
    html = "<html><body><h1>Servicios</h1><p>SEO, Ads y redes sociales.</p></body></html>"
    assert personas.extraer_de_texto(website.texto_por_bloques(html), "https://x.cl/s") == []


# ── D3: el ranking ──────────────────────────────────────────────────────────

def _cand(**kw):
    base = dict(nombre="Matias Bravo", cargo="Gerente General", url="https://x.cl/e",
                angulo="pagina", proveedor="sitio", origen="tercero", donde="snippet",
                empresa_en_texto=False)
    base.update(kw)
    return decisor.puntuar(decisor.Candidato(**base))


def test_el_ranking_prefiere_el_sitio_propio_sobre_un_tercero():
    propio = _cand(origen="sitio_propio")
    tercero = _cand(origen="tercero", empresa_en_texto=True)
    assert propio.score > tercero.score
    assert any("sitio de la propia empresa" in e for e in propio.evidencia)


def test_el_ranking_prefiere_al_cargo_que_decide():
    ceo = _cand(cargo="CEO", origen="sitio_propio")
    jefe = _cand(cargo="jefe de ventas", origen="sitio_propio")
    assert ceo.score > jefe.score


def test_el_ranking_prefiere_lo_leido_de_la_pagina_sobre_el_snippet():
    # El snippet pudo mezclar dos resultados; el cuerpo de la pagina no.
    de_pagina = _cand(donde="pagina", origen="sitio_propio")
    de_snippet = _cand(donde="snippet", origen="sitio_propio")
    assert de_pagina.score > de_snippet.score


def test_la_evidencia_explica_el_score():
    # Sin evidencia legible, un falso positivo no se puede auditar despues.
    c = _cand(origen="sitio_propio", empresa_en_texto=True, donde="pagina")
    assert len(c.evidencia) >= 3
    assert any("cargo" in e for e in c.evidencia)


# ── D4: el resolutor completo, sin red ──────────────────────────────────────

class _Rta:
    veredicto = None

    def __init__(self, html="", bloqueado=False, motivo=""):
        self.html = html
        self.page = None
        self.error = motivo if bloqueado else ""
        self._sirve = bool(html) and not bloqueado

    @property
    def sirve(self):
        return self._sirve


def _sin_red(monkeypatch, serp=SERP, pagina=PAGINA, bloqueados=None, contador=None):
    def _obtener(url, proveedor, salud, timeout=None):
        if contador is not None:
            contador.append((proveedor, url))
        if bloqueados and proveedor in bloqueados:
            salud.marcar_bloqueado(proveedor, bloqueados[proveedor])
            return _Rta(bloqueado=True, motivo=bloqueados[proveedor])
        if proveedor == "sitio":
            return _Rta(html=pagina)
        return _Rta(html=serp)
    monkeypatch.setattr(decisor, "obtener", _obtener)


def test_resolver_encuentra_al_decisor_de_la_empresa(monkeypatch):
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Santiago, Chile")
    nombres = [c.nombre for c in r["candidatos"]]
    assert "Matias Bravo" in nombres
    assert r["candidatos"][0].score >= 0.9
    assert r["diagnostico"]["paginas_visitadas"] >= 1


def test_resolver_descarta_a_la_persona_de_OTRA_empresa(monkeypatch):
    # El fixture trae "Rodrigo Fuentes - Gerente General - Otra Agencia" en un
    # blog que no nombra a Onza. Es el falso positivo caro: cargo perfecto,
    # empresa equivocada.
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert "Rodrigo Fuentes" not in [c.nombre for c in r["candidatos"]]


def test_resolver_ignora_los_perfiles_de_linkedin_del_serp(monkeypatch):
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert not any("linkedin.com/in" in c.url for c in r["candidatos"])


def test_resolver_acepta_a_la_persona_nombrada_por_la_prensa(monkeypatch):
    # "Paula Guzman asume como gerente comercial de Onza Marketing" en df.cl:
    # es un tercero, pero el texto nombra a la empresa, asi que liga.
    #
    # Se mide SIN dominio a proposito: con dominio el resolutor encuentra al
    # decisor en el sitio y no llega a buscar, que es justo lo que debe hacer.
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, "", "Chile")
    assert "Paula Guzman" in [c.nombre for c in r["candidatos"]]
    assert r["diagnostico"]["busco_en_internet"] is True


def test_el_motivo_distingue_bloqueo_de_no_publicado(monkeypatch):
    # Se bloquea TAMBIEN el fetch al sitio: si la pagina de equipo responde, el
    # resolutor encuentra al decisor sin buscar y no hay bloqueo que reportar.
    todos = {p.nombre: "captcha" for p in providers.activos()}
    todos["sitio"] = "captcha"
    _sin_red(monkeypatch, bloqueados=todos)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert r["candidatos"] == []
    assert r["diagnostico"]["motivo_vacio"] == "providers_blocked"
    assert r["completo"] is False


def test_control_positivo_sin_bloqueo_el_motivo_es_otro(monkeypatch):
    # Si los dos casos dijeran lo mismo, la distincion no existiria.
    vacio = "<html><body><div class='results'></div></body></html>"
    _sin_red(monkeypatch, serp=vacio, pagina="<html><body><p>Servicios</p></body></html>")
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert r["candidatos"] == []
    assert r["diagnostico"]["motivo_vacio"] != "providers_blocked"


def test_una_empresa_vacia_no_dispara_ni_un_fetch(monkeypatch):
    llamadas = []
    _sin_red(monkeypatch, contador=llamadas)
    r = decisor.resolver("", "", "Chile")
    assert r["diagnostico"]["motivo_vacio"] == "no_company"
    assert llamadas == [], "se gastaron fetches buscando al decisor de nadie"


# ── D5: contrato HTTP ───────────────────────────────────────────────────────

def test_el_endpoint_devuelve_al_decisor_con_su_evidencia(monkeypatch):
    _sin_red(monkeypatch)
    r = cliente.post("/find-decision-maker", json={
        "company": EMPRESA, "domain": DOMINIO, "location": "Santiago, Chile"})
    assert r.status_code == 200
    d = r.json()
    assert d["found"] is True
    for campo in ("person_name", "person_title", "confidence", "evidence", "url", "angle"):
        assert campo in d["person"], campo
    assert isinstance(d["alternatives"], list)


def test_el_endpoint_dice_por_que_cuando_no_encuentra(monkeypatch):
    todos = {p.nombre: "captcha" for p in providers.activos()}
    _sin_red(monkeypatch, bloqueados=todos)
    d = cliente.post("/find-decision-maker", json={"company": EMPRESA}).json()
    assert d["found"] is False and d["person"] is None
    assert d["reason"] == "providers_blocked"
    assert d["error"] == "providers_blocked"


def test_search_linkedin_conserva_su_forma_historica_y_ahora_encuentra(monkeypatch):
    # El cliente de Venara lee estos cuatro campos por nombre. Cambiar la forma
    # lo rompe en silencio; mejorar el contenido no.
    _sin_red(monkeypatch)
    d = cliente.post("/search-linkedin", json={"company": EMPRESA, "location": "Chile"}).json()
    assert set(d) >= {"person_name", "person_title", "linkedin_url", "source"}
    assert d["person_name"] != "NOT_FOUND"
    # `linkedin_url` sigue existiendo y sigue vacio: los perfiles personales no
    # estan en el indice (F7). Prometerlo seria vender lo que no hay.
    assert d["linkedin_url"] == ""


def test_search_linkedin_sigue_devolviendo_NOT_FOUND_con_la_forma_vieja(monkeypatch):
    todos = {p.nombre: "captcha" for p in providers.activos()}
    _sin_red(monkeypatch, bloqueados=todos)
    d = cliente.post("/search-linkedin", json={"company": EMPRESA}).json()
    assert d["person_name"] == "NOT_FOUND"
    assert set(d) >= {"person_name", "person_title", "linkedin_url", "source"}


# ── D6: el gasto ────────────────────────────────────────────────────────────

def test_el_presupuesto_de_fetches_esta_acotado(monkeypatch):
    llamadas = []
    _sin_red(monkeypatch, contador=llamadas)
    decisor.resolver(EMPRESA, DOMINIO, "Santiago, Chile")
    de_busqueda = [c for c in llamadas if c[0] != "sitio"]
    de_pagina = [c for c in llamadas if c[0] == "sitio"]
    assert len(de_busqueda) <= config.DECISOR_MAX_FETCHES, len(de_busqueda)
    assert len(de_pagina) <= config.DECISOR_MAX_PAGINAS, len(de_pagina)


def test_el_techo_de_fetches_de_verdad_recorta_algo():
    # Sin este control, el test de arriba pasaria tambien con un techo enorme
    # que nunca corta -- y el techo existe justo para el caso en que corta.
    plan = decisor.construir_plan(EMPRESA, DOMINIO, interpretar("Chile"))
    assert len(plan) * len(providers.activos()) > config.DECISOR_MAX_FETCHES


def test_los_topes_de_gasto_existen_y_son_finitos():
    # La version anterior no tenia ninguno y una consulta tardo 4m43s medidos
    # contra produccion. Este test falla si alguien los borra.
    for nombre in ("DECISOR_MAX_FETCHES", "DECISOR_MAX_PAGINAS", "DECISOR_BUDGET_S"):
        valor = getattr(config, nombre)
        assert isinstance(valor, int) and 0 < valor < 1000, (nombre, valor)


# ── D9: "buscar en Google" ──────────────────────────────────────────────────

def test_google_no_sirve_resultados_en_el_html_y_el_sistema_no_depende_de_el():
    # El pedido fue "buscar en Google". Medido (F3/F17) y verificable sobre el
    # fixture capturado: Google devuelve un bootstrap de JavaScript, no
    # resultados. Ningun selector arregla eso porque el contenido no esta.
    items, _ = extraction.extraer(None, leer("google_jsshell.html"), "google")
    utiles = [i for i in items if i["url"].startswith("http")]
    assert len(utiles) <= 1, "Google empezo a servir resultados: revisar F3/F17"
    # Y el resolutor no lo tiene entre sus fuentes, asi que no depende de el.
    assert "google" not in {p.nombre for p in providers.activos()}
    assert config.ENABLE_GOOGLE is False


def test_control_positivo_los_buscadores_que_si_responden_dan_resultados():
    # Sin este control, el test de arriba pasaria con un extractor roto que
    # devuelve cero para todo, y se concluiria lo contrario de lo que pasa.
    items, _ = extraction.extraer(None, leer("bing_ok.html"), "bing")
    assert len([i for i in items if i["url"].startswith("http")]) >= 5


def test_el_ranking_no_satura_y_conserva_el_orden_entre_los_fuertes():
    # La primera version sumaba el peso del cargo (hasta 1.0) mas los bonus y
    # topeaba en 1.0: dos candidatos fuertes quedaban empatados en 1.0 y el
    # orden se perdia justo donde importa.
    maximo = _cand(cargo="fundador", origen="sitio_propio",
                   empresa_en_texto=True, donde="pagina")
    assert maximo.score == 1.0
    casi = _cand(cargo="fundador", origen="sitio_propio",
                 empresa_en_texto=True, donde="snippet")
    assert casi.score < maximo.score
    assert (decisor.PESO_CARGO + decisor.PESO_SITIO_PROPIO
            + decisor.PESO_EMPRESA_EN_TEXTO + decisor.PESO_LEIDO_DE_PAGINA) == 1.0


def test_la_liga_con_la_empresa_funciona_sobre_el_texto_de_una_pagina_entera():
    # `clave_nombre()` esta hecha para un NOMBRE corto: sobre el texto de una
    # pagina devolvia "nuestroequipo" y TODA liga daba False, asi que ningun
    # candidato de pagina sumaba esa senal.
    texto = website.texto_por_bloques(PAGINA)
    assert decisor._liga_con_la_empresa(texto, EMPRESA) is True


def test_la_liga_no_se_conforma_con_una_palabra_generica():
    # "Marketing" aparece en toda pagina del rubro. Si bastara, cada empresa
    # quedaria ligada a su competencia entera.
    ajeno = "Otra Agencia es una empresa de marketing digital en Santiago."
    assert decisor._liga_con_la_empresa(ajeno, "Onza Marketing") is False
    assert decisor._liga_con_la_empresa(ajeno, "Otra Agencia") is True


def test_el_presupuesto_acota_el_reloj_aunque_los_fetches_se_cuelguen(monkeypatch):
    # MEDIDO EN VIVO (2026-09-03): con los buscadores colgados, una consulta
    # tardaba 48s con DECISOR_BUDGET_S en 25. Dos causas: `as_completed` sin
    # timeout esperaba para siempre, y salir del `with` del executor esperaba a
    # los hilos vivos. El presupuesto existia y no acotaba nada.
    import time as _t

    llamadas = []

    def _lento(url, proveedor, salud, timeout=None):
        llamadas.append(proveedor)
        _t.sleep(2)
        return _Rta(html="<html><body><p>nada</p></body></html>")

    monkeypatch.setattr(decisor, "obtener", _lento)
    monkeypatch.setattr(config, "DECISOR_BUDGET_S", 1)
    t0 = _t.monotonic()
    decisor.resolver(EMPRESA, DOMINIO, "Chile")
    transcurrido = _t.monotonic() - t0
    # El tope no puede ser menor que UN fetch: uno ya en vuelo no se interrumpe.
    # Lo que el presupuesto garantiza es el RELOJ -- que las fases no se
    # encadenen. Sin el, la corrida medida en vivo tardo 48s con el presupuesto
    # puesto en 25.
    assert transcurrido < 6, "el presupuesto no acoto el reloj: %.1fs" % transcurrido
    # La CANTIDAD la acota otra cosa, y contarla como si la acotara el
    # presupuesto seria medir el oraculo equivocado: las busquedas salen en
    # paralelo, asi que las ya despachadas se pagan aunque el reloj corte.
    techo = config.DECISOR_MAX_FETCHES + config.DECISOR_MAX_PAGINAS + 1
    assert len(llamadas) <= techo, "%d fetches, techo %d" % (len(llamadas), techo)


def test_control_positivo_sin_presupuesto_apretado_si_termina_el_trabajo(monkeypatch):
    # Sin este control, el test de arriba pasaria con un resolutor que no hace
    # nada nunca.
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert r["candidatos"], "el resolutor no encontro a nadie con red simulada sana"


def test_el_timeout_por_fetch_es_menor_que_el_presupuesto():
    # Scrapling reintenta 3 veces por su cuenta. Con FETCH_TIMEOUT (15s) un
    # solo fetch colgado cuesta 45s, mas que el presupuesto entero.
    assert config.DECISOR_FETCH_TIMEOUT < config.DECISOR_BUDGET_S
    assert config.DECISOR_FETCH_TIMEOUT * 3 <= config.DECISOR_BUDGET_S + 5


def test_resolver_entra_al_sitio_sin_gastar_una_sola_busqueda(monkeypatch):
    # MEDIDO EL 2026-09-03: `site:fintual.cl equipo` en Bing devolvio foros
    # franceses sobre Instagram (F6) y los demas proveedores estaban en captcha
    # (F1). Un resolutor que solo sabe buscar no encuentra nada. El sitio del
    # propio prospecto es la unica fuente que siempre atiende.
    llamadas = []
    _sin_red(monkeypatch, contador=llamadas)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert "Matias Bravo" in [c.nombre for c in r["candidatos"]]
    assert r["diagnostico"]["busco_en_internet"] is False
    assert all(p == "sitio" for p, _ in llamadas), \
        "se gastaron busquedas teniendo al decisor en el sitio"


def test_la_url_de_la_pagina_de_equipo_se_arma_bien(monkeypatch):
    # La primera version concatenaba a mano y `lstrip("./")` se comia la barra:
    # "/nuestro-equipo" producia "https://onzamarketing.clnuestro-equipo". El
    # fetch fallaba y la pagina no se leia nunca, sin ningun error visible.
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    for c in r["candidatos"]:
        assert c.url.startswith("https://onzamarketing.cl/"), c.url


def test_no_sigue_enlaces_a_otro_dominio_desde_la_home(monkeypatch):
    # Seguir un enlace externo desde la home lleva al equipo de otra empresa.
    home = ('<html><body>'
            '<a href="https://otraagencia.cl/nuestro-equipo">Nuestro equipo</a>'
            '<a href="/nuestro-equipo">Nuestro equipo</a>'
            '</body></html>')
    salud_llamadas = []

    def _obtener(url, proveedor, salud, timeout=None):
        salud_llamadas.append(url)
        if url.rstrip("/") == "https://" + DOMINIO:
            return _Rta(html=home)
        return _Rta(html=PAGINA)

    monkeypatch.setattr(decisor, "obtener", _obtener)
    decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert not any("otraagencia.cl" in u for u in salud_llamadas), salud_llamadas


def test_bing_envenenado_no_produce_ni_un_candidato(monkeypatch):
    # Captura REAL de Bing devolviendo resultados sin relacion con la query
    # (F6). El resolutor tiene que no atribuirle ninguna de esas personas a la
    # empresa buscada: la regla de liga con la empresa es lo que lo impide.
    veneno = leer("bing_poisoned_recipes.html")
    _sin_red(monkeypatch, serp=veneno, pagina="<html><body><p>Servicios</p></body></html>")
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert r["candidatos"] == [], [c.nombre for c in r["candidatos"]]


def _c(nombre, score, cargo="CEO"):
    c = decisor.Candidato(nombre=nombre, cargo=cargo, url="https://x.cl",
                          angulo="a", proveedor="brave", origen="tercero",
                          donde="snippet", empresa_en_texto=True)
    c.score = score
    return c


def test_fusiona_las_dos_formas_del_mismo_nombre():
    # MEDIDO EN VIVO (2026-09-03): la corrida sobre Buk devolvio "Jaime Arrieta"
    # y "Jaime Arrieta Boetsch" como DOS decisores. Son la misma persona, y dos
    # leads de la misma persona son dos mensajes al mismo humano.
    from venara_discovery.normalize import clave_nombre
    entrada = {clave_nombre("Jaime Arrieta"): _c("Jaime Arrieta", 0.65),
               clave_nombre("Jaime Arrieta Boetsch"): _c("Jaime Arrieta Boetsch", 0.625)}
    salida = decisor.fusionar_mismo_humano(entrada)
    assert len(salida) == 1
    unico = list(salida.values())[0]
    # Gana el de mayor score, y la otra forma queda como evidencia en vez de
    # desaparecer sin dejar rastro.
    assert unico.nombre == "Jaime Arrieta"
    assert any("Boetsch" in e for e in unico.evidencia)


def test_no_fusiona_a_dos_personas_distintas():
    # Sin este control, la fusion podria estar colapsando el lote entero.
    from venara_discovery.normalize import clave_nombre
    entrada = {clave_nombre("Omar Larre"): _c("Omar Larre", 0.9),
               clave_nombre("Ricardo Sateler"): _c("Ricardo Sateler", 0.8)}
    assert len(decisor.fusionar_mismo_humano(entrada)) == 2


def test_no_fusiona_por_un_nombre_de_pila_corto():
    # "Ana" contenido en "Ana Maria Rojas" tambien esta contenido en "Anabel
    # Soto", que es otra persona.
    from venara_discovery.normalize import clave_nombre
    entrada = {clave_nombre("Ana Diaz"): _c("Ana Diaz", 0.7),
               clave_nombre("Anabel Soto"): _c("Anabel Soto", 0.6)}
    assert len(decisor.fusionar_mismo_humano(entrada)) == 2


def test_search_linkedin_usa_el_sitio_cuando_le_pasan_el_dominio(monkeypatch):
    # MEDIDO EN PRODUCCION (2026-09-03): sin dominio, `/search-linkedin`
    # devolvia NOT_FOUND por captcha de los proveedores; con dominio, la misma
    # empresa se resuelve entrando al sitio, sin una sola busqueda. El campo es
    # el que separa esos dos resultados.
    todos = {p.nombre: "captcha" for p in providers.activos()}
    _sin_red(monkeypatch, bloqueados=todos)
    d = cliente.post("/search-linkedin", json={
        "company": EMPRESA, "domain": DOMINIO, "location": "Chile"}).json()
    assert d["person_name"] == "Matias Bravo", d
    # La forma historica no se toca: el cliente lee estos campos por nombre.
    assert set(d) >= {"person_name", "person_title", "linkedin_url", "source"}


def test_search_linkedin_sin_dominio_sigue_comportandose_como_siempre(monkeypatch):
    # `domain` es opcional: un cliente viejo que no lo manda no puede romperse.
    todos = {p.nombre: "captcha" for p in providers.activos()}
    todos["sitio"] = "captcha"
    _sin_red(monkeypatch, bloqueados=todos)
    d = cliente.post("/search-linkedin", json={"company": EMPRESA}).json()
    assert d["person_name"] == "NOT_FOUND"
    assert set(d) >= {"person_name", "person_title", "linkedin_url", "source"}


# ── D7: defectos vistos buscando 10 decisores reales (2026-09-03) ───────────

def test_una_red_social_no_es_fuente_de_decisor(monkeypatch):
    # MEDIDO: un post de Instagram y un video de Facebook entraron como fuente.
    # Una red social no publica el organigrama de nadie: lo que hay es texto
    # suelto que casualmente junta un nombre y una palabra que parece cargo.
    serp = ('<html><body><div class="results">'
            '<div class="result"><a class="result__a" href="https://www.instagram.com/p/ABC123">'
            'Benjamin Labra - Co-Founder - Onza Marketing</a>'
            '<a class="result__snippet" href="https://www.instagram.com/p/ABC123">'
            'Onza Marketing con su fundador.</a></div>'
            '</div></body></html>')
    _sin_red(monkeypatch, serp=serp, pagina="<html><body><p>Servicios</p></body></html>")
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert not any("instagram" in c.url for c in r["candidatos"]), \
        [c.url for c in r["candidatos"]]


def test_control_positivo_un_medio_de_prensa_SI_sigue_siendo_fuente(monkeypatch):
    # Sin este control, el filtro podria estar descartando tambien la prensa,
    # que es uno de los angulos que mas rinde para nombramientos.
    _sin_red(monkeypatch, pagina="<html><body><p>Servicios</p></body></html>")
    r = decisor.resolver(EMPRESA, "", "Chile")
    assert any("df.cl" in c.url for c in r["candidatos"]), [c.url for c in r["candidatos"]]


def test_una_palabra_de_contexto_no_se_pega_al_nombre():
    # MEDIDO: "Invitado Ian Lee" entro como persona desde el titulo de un video.
    assert personas.es_nombre_de_persona("Invitado Ian Lee") is False
    assert personas.es_nombre_de_persona("Live Ian Lee") is False
    # Control positivo: el nombre limpio sigue pasando.
    assert personas.es_nombre_de_persona("Ian Lee") is True


def test_fusiona_dos_lecturas_del_mismo_nombre_en_la_misma_pagina():
    # MEDIDO: "Karim Pichara" y "Kim Pichara" salieron los dos de notco.ai/about
    # como si fueran dos CTO. "Kim" no es prefijo de "Karim", asi que la regla
    # de contencion no las une; dentro de UNA pagina, mismo apellido y misma
    # inicial es el parser leyendo dos veces.
    from venara_discovery.normalize import clave_nombre
    a = _c("Karim Pichara", 1.0, cargo="Co-Founder & CTO")
    b = _c("Kim Pichara", 0.89, cargo="CTO")
    a.url = b.url = "https://notco.ai/about"
    salida = decisor.fusionar_mismo_humano({clave_nombre("Karim Pichara"): a,
                                            clave_nombre("Kim Pichara"): b})
    assert len(salida) == 1
    unico = list(salida.values())[0]
    assert unico.nombre == "Karim Pichara"
    assert any("Kim Pichara" in e for e in unico.evidencia)


def test_no_fusiona_a_dos_personas_del_mismo_apellido_en_paginas_distintas():
    # Hermanos o familia duena de la empresa existen. Fusionarlos entre fuentes
    # distintas borraria a una persona real.
    from venara_discovery.normalize import clave_nombre
    a = _c("Cristobal Della Maggiora", 0.65)
    b = _c("Carlos Della Maggiora", 0.65)
    a.url = "https://craft.co/x"
    b.url = "https://otra.cl/y"
    salida = decisor.fusionar_mismo_humano({clave_nombre(a.nombre): a,
                                            clave_nombre(b.nombre): b})
    assert len(salida) == 2


def test_distingue_la_pagina_hallada_buscando_de_la_hallada_por_la_home(monkeypatch):
    # MEDIDO: los candidatos de notco.ai/about salian etiquetados
    # "sitio_directo" aunque esa pagina se encontro BUSCANDO. La etiqueta hacia
    # leer la evidencia como si el sistema hubiera entrado solo por la home, y
    # ocultaba que ese candidato desaparece cuando los buscadores bloquean.
    home_sin_equipo = "<html><body><a href='/servicios'>Servicios</a></body></html>"

    def _obtener(url, proveedor, salud, timeout=None):
        if url.rstrip("/") == "https://" + DOMINIO:
            return _Rta(html=home_sin_equipo)
        if proveedor == "sitio":
            return _Rta(html=PAGINA)
        return _Rta(html=SERP)

    monkeypatch.setattr(decisor, "obtener", _obtener)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    desde_pagina = [c for c in r["candidatos"] if c.proveedor == "sitio"]
    assert desde_pagina, "no se leyo ninguna pagina"
    assert all(c.angulo == "pagina_desde_busqueda" for c in desde_pagina), \
        [(c.nombre, c.angulo) for c in desde_pagina]


def test_control_positivo_la_home_si_etiqueta_sitio_directo(monkeypatch):
    # Sin este control, el test de arriba pasaria con un resolutor que etiqueta
    # todo igual, en la direccion contraria.
    _sin_red(monkeypatch)
    r = decisor.resolver(EMPRESA, DOMINIO, "Chile")
    assert any(c.angulo == "sitio_directo" for c in r["candidatos"])
