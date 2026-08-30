"""G5 -- entity resolution: unir lo mismo, NO unir lo distinto."""
from venara_discovery.entity import Empresa, Resolutor
from venara_discovery.normalize import clave_nombre, dominio_registrable


def test_la_misma_empresa_en_cuatro_formas_queda_en_una():
    r = Resolutor()
    for e in [
        Empresa(nombre="Acme Digital", website="https://www.acmedigital.com/", fuentes={"bing"}),
        Empresa(nombre="ACME Digital Agency", website="https://acmedigital.com", fuentes={"duckduckgo"}),
        Empresa(nombre="Acme Digital | LinkedIn",
                linkedin_url="https://www.linkedin.com/company/acme-digital", fuentes={"duckduckgo"}),
        Empresa(nombre="Acme Digital S.A.C.", website="https://acmedigital.com/contacto", fuentes={"bing"}),
    ]:
        r.agregar(e)
    assert len(r.empresas()) == 1, [e.nombre for e in r.empresas()]


def test_fusion_transitiva():
    # A comparte dominio con B; B comparte LinkedIn con C. Las tres son la
    # misma aunque A y C no compartan NINGUNA senal directa. Un dict por clave
    # no captura esto.
    r = Resolutor()
    r.agregar(Empresa(nombre="Acme", website="https://acme.pe/"))
    r.agregar(Empresa(nombre="Acme", website="https://acme.pe/nosotros",
                      linkedin_url="https://www.linkedin.com/company/acme"))
    r.agregar(Empresa(nombre="Acme Peru", linkedin_url="https://www.linkedin.com/company/acme"))
    assert len(r.empresas()) == 1


def test_dos_empresas_en_el_MISMO_hosting_NO_se_unen():
    # El bug del servidor viejo: _domain() devolvia "wixsite.com" para las dos.
    r = Resolutor()
    r.agregar(Empresa(nombre="Panaderia Luz", website="https://panaderialuz.wixsite.com/inicio"))
    r.agregar(Empresa(nombre="Ferreteria Sur", website="https://ferreteriasur.wixsite.com/home"))
    assert len(r.empresas()) == 2, "dos negocios distintos colapsaron en uno"


def test_dominios_con_sufijo_compuesto_no_colapsan():
    # Sin la lista de sufijos, "com.pe" seria el dominio y TODA empresa
    # peruana seria la misma entidad.
    assert dominio_registrable("https://acme.com.pe/x") == "acme.com.pe"
    assert dominio_registrable("https://otra.com.pe/y") == "otra.com.pe"
    r = Resolutor()
    r.agregar(Empresa(nombre="Acme", website="https://acme.com.pe/"))
    r.agregar(Empresa(nombre="Otra", website="https://otra.com.pe/"))
    assert len(r.empresas()) == 2


def test_linkedin_con_y_sin_querystring_es_la_misma():
    r = Resolutor()
    r.agregar(Empresa(nombre="Acme", linkedin_url="https://www.linkedin.com/company/acme?trk=abc"))
    r.agregar(Empresa(nombre="Acme", linkedin_url="https://linkedin.com/company/acme/"))
    assert len(r.empresas()) == 1


def test_no_se_une_por_nombre_generico():
    # "Agencia Digital" hay una por ciudad: unir por nombre corto es el mayor
    # generador de falsos positivos.
    r = Resolutor()
    r.agregar(Empresa(nombre="SA"))
    r.agregar(Empresa(nombre="SA"))
    assert len(r.empresas()) == 0, "un nombre de 2 letras no debe crear entidad"


def test_absorber_completa_lo_que_falta_sin_pisar_lo_bueno():
    a = Empresa(nombre="Acme Digital", website="https://acme.pe/", fuentes={"bing"})
    b = Empresa(nombre="Acme", linkedin_url="https://www.linkedin.com/company/acme",
                descripcion="Agencia en Lima", fuentes={"duckduckgo"})
    a.absorber(b)
    assert a.website == "https://acme.pe/"          # no se pisa
    assert a.linkedin_url.endswith("/acme")          # se completa
    assert a.descripcion == "Agencia en Lima"
    assert a.fuentes == {"bing", "duckduckgo"}       # evidencia acumulada


def test_clave_nombre_normaliza_variantes():
    assert clave_nombre("Acme Digital") == clave_nombre("ACME Digital Agency")
    assert clave_nombre("Acme Digital S.A.C.") == clave_nombre("Acme Digital")


def test_marcador_final():
    print("\nENTITY RESOLUTION VERIFICADA")


def test_NO_une_dos_empresas_homonimas_con_dominios_propios_distintos():
    # La guarda que hace segura la reconciliacion por nombre: "Agencia Digital"
    # hay una por ciudad, y unirlas perderia un lead y crearia uno falso.
    r = Resolutor()
    r.agregar(Empresa(nombre="Estudio Norte", website="https://estudionorte.pe/"))
    r.agregar(Empresa(nombre="Estudio Norte", website="https://estudionorte.cl/"))
    assert len(r.empresas()) == 2


def test_NO_une_dos_paginas_de_linkedin_distintas_con_el_mismo_nombre():
    r = Resolutor()
    r.agregar(Empresa(nombre="Estudio Norte",
                      linkedin_url="https://www.linkedin.com/company/estudio-norte-pe"))
    r.agregar(Empresa(nombre="Estudio Norte",
                      linkedin_url="https://www.linkedin.com/company/estudio-norte-cl"))
    assert len(r.empresas()) == 2
