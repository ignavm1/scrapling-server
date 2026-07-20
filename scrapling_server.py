"""
scrapling_server.py - Venara AI Lead Scraping Server v4.0

Sin browsers - FetcherSession HTTP only
CAMBIOS v4:
- /search-linkedin-companies devuelve [] en vez de 404 cuando no hay resultados
- Mejor manejo de ubicaciones (Buenos Aires, Lima, etc.)
- Fallback: si LinkedIn no da resultados, busca negocios directamente
"""
from __future__ import annotations
import re, os, logging, ipaddress, socket, secrets, urllib.request, time, concurrent.futures
from contextlib import asynccontextmanager
from urllib.parse import quote, urlparse
import anyio
import uvicorn
from fastapi import FastAPI, Depends, Header, HTTPException
from pydantic import BaseModel
from scrapling.fetchers import FetcherSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cada request hace I/O de red bloqueante (hasta 3 motores) en el threadpool
    # de FastAPI. Subimos el limite para tolerar mas requests concurrentes.
    try:
        anyio.to_thread.current_default_thread_limiter().total_tokens = 100
    except Exception as e:
        log.warning("No se pudo ajustar el threadpool: " + str(e))
    yield

app = FastAPI(title="Venara Scrapling Server", version="4.0.0", lifespan=lifespan)

# Proxy residencial (opcional). Sin esto, las búsquedas salen desde la IP del
# servidor (Render/datacenter), que Google/Bing/LinkedIn bloquean (429/403) →
# pocas o cero empresas. Con un proxy residencial las requests parecen tráfico
# real y no se bloquean. Formato: "http://usuario:password@host:puerto".
# Si PROXY_URL no está definido, el server funciona igual (sin proxy, como antes).
PROXY_URL = os.environ.get("PROXY_URL") or None
if PROXY_URL:
    log.info("Proxy residencial ACTIVO")
else:
    log.warning("PROXY_URL no configurado - scraping desde IP directa (riesgo de bloqueo)")

# Autenticacion por API key (recomendada). Si API_KEY esta definida, los endpoints
# de datos exigen el header "X-API-Key". Si no, el server funciona sin auth
# (compatibilidad) pero queda expuesto: configurala en produccion.
API_KEY = os.environ.get("API_KEY") or None
if API_KEY:
    log.info("Autenticacion por API key ACTIVA")
else:
    log.warning("API_KEY no configurado - endpoints SIN autenticacion (configurar en produccion)")

def require_api_key(x_api_key: str = Header(default="")):
    if API_KEY and not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="invalid or missing api key")

def make_session():
    """FetcherSession con proxy si está configurado. Centraliza la config para
    no repetir el kwarg en cada llamada."""
    if PROXY_URL:
        return FetcherSession(impersonate="chrome", proxy=PROXY_URL)
    return FetcherSession(impersonate="chrome")

def is_safe_public_url(url):
    """Solo http/https hacia hosts publicos. Bloquea SSRF: file://, localhost,
    IPs privadas/loopback/link-local (incl. 169.254.169.254 metadata)."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    try:
        infos = socket.getaddrinfo(p.hostname, None)
    except Exception:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
            return False
    return True

class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Bloquea redirects hacia hosts internos/privados en el fallback urllib."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not is_safe_public_url(newurl):
            log.warning("Redirect bloqueado (host no permitido): " + newurl)
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)

def _build_safe_opener():
    # Opener limitado a http/https con redirects validados: sin file:// ni ftp://.
    o = urllib.request.OpenerDirector()
    o.add_handler(urllib.request.HTTPHandler())
    o.add_handler(urllib.request.HTTPSHandler())
    o.add_handler(_SafeRedirectHandler())
    o.add_handler(urllib.request.HTTPErrorProcessor())
    o.add_handler(urllib.request.HTTPDefaultErrorHandler())
    o.add_handler(urllib.request.UnknownHandler())  # file://, ftp://... -> URLError
    return o

_SAFE_OPENER = _build_safe_opener()

class CompanySearchRequest(BaseModel):
    query: str
    location: str = ""
    max_results: int = 25

class WebsiteRequest(BaseModel):
    url: str

class LinkedInRequest(BaseModel):
    company: str
    location: str = ""
    fallback_name: str = ""

def looks_like_name(s):
    w = s.split()
    return 2 <= len(w) <= 6 and len(s) <= 60 and not any(c.isdigit() for c in s)

def extract_title(txt):
    clean = re.sub(r"\s*\|\s*LinkedIn\s*$", "", txt, flags=re.I).strip()
    m = re.match(r"^([^|\-\u2013\u2014]+)", clean)
    if not m: return None, None
    name = m.group(1).strip()
    if not looks_like_name(name): return None, None
    tm = re.search(r"[-\u2013\u2014]\s*(.+)$", clean)
    return name, (tm.group(1).strip() if tm else "")

def extract_snippet(snip):
    parts = re.split(r"\s*[·•]\s*", snip)
    if len(parts) >= 2 and looks_like_name(parts[0].strip()):
        return parts[0].strip(), parts[1].strip()
    return None, None

def clean_html(html, max_chars=12000):
    t = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    t = re.sub(r"<style[\s\S]*?</style>", " ", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("&nbsp;"," ").replace("&amp;","&")
    return re.sub(r"\s+", " ", t).strip()[:max_chars]

def fix_href(href):
    if "/url?q=" in href:
        import urllib.parse
        p = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
        href = p.get("q",[href])[0]
    if "uddg=" in href:
        from urllib.parse import parse_qs, urlparse, unquote
        try:
            ps = parse_qs(urlparse(href).query)
            href = unquote(ps.get("uddg",[href])[0])
        except Exception: pass
    return href

def get_urls(query):
    return [
        ("https://www.google.com/search?q=" + quote(query) + "&num=20&hl=es&gl=pe", "google"),
        ("https://html.duckduckgo.com/html/?q=" + quote(query) + "&kl=es-es", "duckduckgo"),
        ("https://www.bing.com/search?q=" + quote(query) + "&count=20&setlang=es", "bing"),
    ]

BASE_SELS = [("div.g a","h3"),("div.tF2Cxc a","h3"),("li.b_algo h2 a",None),(".result__title a",None)]
COMPANY_SELS = BASE_SELS + [("a[href*='linkedin.com/company']",None)]
PERSON_SELS = BASE_SELS
GENERAL_SELS = BASE_SELS

# ------------------------------------------------------------------
# v5: extracción + merge para /search-linkedin-companies
# El objetivo real es el WEBSITE de la empresa (de ahí se extrae el email
# aguas abajo). LinkedIn aporta nombre/URL pero casi nunca el website, y el
# cliente descarta toda empresa sin website — por eso antes se perdían casi
# todas. v5 corre LinkedIn + búsqueda web directa en paralelo y las une por
# dominio, así una empresa vista en LinkedIn recupera su website de la web.
# ------------------------------------------------------------------

# Hosts que NO son el sitio de una empresa (motores, redes, agregadores).
JUNK_HOST_SUBSTR = (
    "google.", "bing.", "duckduckgo.", "facebook.", "instagram.",
    "twitter.", "x.com", "youtube.", "wikipedia.", "pinterest.",
    "tiktok.", "maps.", "translate.", "webcache.", "linkedin.",
)

def _domain(url):
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host

def _is_business_site(url):
    host = _domain(url)
    if not host or "." not in host:
        return False
    return not any(j in host for j in JUNK_HOST_SUBSTR)

def _name_from_domain(url):
    base = _domain(url).split(".")[0]
    return base[:1].upper() + base[1:] if base else ""

def _anchor_text(anchor, tsel):
    if tsel:
        tel = anchor.css(tsel)
        return tel.css("::text").get() if tel else (anchor.css("::text").get() or "")
    return anchor.css("::text").get() or ""

def _website_near(anchor):
    """Busca un http(s) no-linkedin en los ancestros del ancla (snippet)."""
    try:
        par = anchor
        for _ in range(5):
            par = par.parent
            txt = " ".join(par.css("::text").getall())
            um = re.search(r"https?://[^\s]+", txt)
            if um:
                cand = um.group(0).rstrip(".,)")
                if "linkedin" not in cand:
                    return cand
    except Exception:
        pass
    return ""

def _extract_linkedin(page, sname):
    out = []
    for asel, tsel in COMPANY_SELS:
        for anchor in page.css(asel):
            href = fix_href(anchor.attrib.get("href", ""))
            if "linkedin.com/company/" not in href:
                continue
            tt = _anchor_text(anchor, tsel)
            name = re.sub(r"\s*[|\-]\s*(LinkedIn|Company).*$", "", tt, flags=re.I).strip()
            if not name or len(name) < 2:
                continue
            out.append({"name": name, "linkedin_url": href,
                        "website": _website_near(anchor), "source": sname})
    return out

def _extract_direct(page, sname):
    out = []
    for asel, tsel in GENERAL_SELS:
        for anchor in page.css(asel):
            href = fix_href(anchor.attrib.get("href", "")).rstrip(".,)")
            if not href.startswith("http") or not _is_business_site(href):
                continue
            tt = _anchor_text(anchor, tsel)
            name = re.sub(r"\s*[|\-].*$", "", tt, flags=re.I).strip()
            if not name or len(name) < 3:
                name = _name_from_domain(href)
            if not name:
                continue
            out.append({"name": name, "linkedin_url": "",
                        "website": href, "source": "web_" + sname})
    return out

def _fetch_search_page(url_f, sname):
    try:
        with make_session() as s:
            return s.get(url_f, stealthy_headers=True, timeout=15)
    except Exception as e:
        log.warning("search [" + sname + "] failed: " + str(e))
        return None

# Cache TTL en memoria. Las mismas (nicho, ubicación) se repiten entre campañas;
# cachear ahorra CPU de Render y ancho de banda del proxy residencial.
_SEARCH_CACHE = {}
_SEARCH_TTL = 6 * 60 * 60
_SEARCH_CACHE_MAX = 500

def _cache_get(key):
    v = _SEARCH_CACHE.get(key)
    if not v:
        return None
    ts, data = v
    if time.time() - ts > _SEARCH_TTL:
        _SEARCH_CACHE.pop(key, None)
        return None
    return data

def _cache_set(key, data):
    if len(_SEARCH_CACHE) >= _SEARCH_CACHE_MAX:
        for k in sorted(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])[:100]:
            _SEARCH_CACHE.pop(k, None)
    _SEARCH_CACHE[key] = (time.time(), data)

@app.get("/health")
def health():
    return {"status": "ok", "version": "4.0.0"}

@app.post("/search-linkedin-companies", dependencies=[Depends(require_api_key)])
def search_linkedin_companies(req: CompanySearchRequest):
    niche = req.query.strip()
    loc = req.location.strip()

    key = (niche.lower(), loc.lower(), req.max_results)
    cached = _cache_get(key)
    if cached is not None:
        log.info("cache hit: " + niche + " " + loc)
        return {"results": cached, "total": len(cached), "cached": True}

    loc_q = '"' + loc + '"' if loc else ""
    # 3 estrategias, cada una en los 3 motores = 9 fetches en paralelo.
    # LinkedIn -> nombre/URL. Las dos directas -> el website real del negocio.
    jobs = []
    for u, s in get_urls("site:linkedin.com/company " + niche + " " + loc_q):
        jobs.append((u, s, "linkedin"))
    for u, s in get_urls(niche + " " + loc + " sitio web contacto"):
        jobs.append((u, s, "direct"))
    for u, s in get_urls(niche + " empresa " + loc):
        jobs.append((u, s, "direct"))

    # Merge por dominio: una empresa vista en LinkedIn (sin web) recupera su
    # website de la búsqueda directa, y viceversa recupera su linkedin_url.
    merged = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(_fetch_search_page, u, s): (s, m) for (u, s, m) in jobs}
        for fut in concurrent.futures.as_completed(futs):
            sname, mode = futs[fut]
            page = fut.result()
            if page is None:
                continue
            try:
                items = _extract_linkedin(page, sname) if mode == "linkedin" else _extract_direct(page, sname)
            except Exception as e:
                log.warning("extract [" + sname + "] failed: " + str(e))
                continue
            for it in items:
                k = _domain(it["website"]) or it["linkedin_url"]
                if not k:
                    continue
                cur = merged.get(k)
                if cur is None:
                    merged[k] = it
                else:
                    if not cur["website"] and it["website"]:
                        cur["website"] = it["website"]
                    if not cur["linkedin_url"] and it["linkedin_url"]:
                        cur["linkedin_url"] = it["linkedin_url"]

    companies = list(merged.values())
    # Priorizar las que tienen website (las sin website se descartan aguas abajo).
    companies.sort(key=lambda c: 0 if c["website"] else 1)
    capped = companies[:req.max_results]
    _cache_set(key, capped)
    with_web = sum(1 for c in capped if c["website"])
    log.info("v5 total: " + str(len(capped)) + " (" + str(with_web) + " con web) para " + niche)
    return {"results": capped, "total": len(capped)}

@app.post("/scrape-website", dependencies=[Depends(require_api_key)])
def scrape_website(req: WebsiteRequest):
    url = req.url.strip()
    if not url: return {"clean_text": "NO_CONTENT", "url": url}
    if not is_safe_public_url(url):
        log.warning("Scraping rechazado (URL no permitida): " + url)
        return {"clean_text": "NO_CONTENT", "url": url, "method": "blocked"}
    log.info("Scraping: " + url)
    try:
        with make_session() as s:
            page = s.get(url, stealthy_headers=True, follow_redirects="safe", timeout=15)
        html = page.html_content or ""
        if html and len(html) > 100:
            return {"clean_text": clean_html(html), "url": url, "method": "fetcher_session"}
    except Exception as e:
        log.warning("FetcherSession failed: " + str(e))
    try:
        r2 = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _SAFE_OPENER.open(r2, timeout=15) as resp:
            if not is_safe_public_url(resp.geturl()):
                raise ValueError("final url not allowed: " + resp.geturl())
            html = resp.read().decode("utf-8", errors="ignore")
        if html and len(html) > 100:
            return {"clean_text": clean_html(html), "url": url, "method": "urllib"}
    except Exception as e:
        log.warning("urllib failed: " + str(e))
    return {"clean_text": "NO_CONTENT", "url": url, "method": "failed"}

@app.post("/search-linkedin", dependencies=[Depends(require_api_key)])
def search_linkedin(req: LinkedInRequest):
    company = req.company.strip()
    if not company or company in {"NO_COMPANY_FOUND","NOT_FOUND",""}:
        return {"person_name":"NOT_FOUND","person_title":"","linkedin_url":"","source":"no_company"}
    loc = req.location.strip()
    loc_p = ' "' + loc + '"' if loc else ""
    role_q = '"CEO" OR "Founder" OR "Co-Founder" OR "Director General" OR "Director" OR "Gerente" OR "Owner" OR "Presidente" OR "CTO"'
    q1 = 'site:linkedin.com/in "' + company + '" (' + role_q + ')' + loc_p
    q2 = 'site:linkedin.com/in "' + company + '" CEO OR Director' + loc_p
    queries = [
        ("https://www.google.com/search?q=" + quote(q1) + "&num=10&hl=es", "google"),
        ("https://html.duckduckgo.com/html/?q=" + quote(q2) + "&kl=es-es", "duckduckgo"),
        ("https://www.bing.com/search?q=" + quote(q1) + "&count=10&setlang=es", "bing"),
    ]
    for url_f, sname in queries:
        try:
            with make_session() as s:
                page = s.get(url_f, stealthy_headers=True, timeout=15)
            for asel, tsel in PERSON_SELS:
                for anchor in page.css(asel):
                    href = fix_href(anchor.attrib.get("href",""))
                    if "linkedin.com/in/" not in href: continue
                    if tsel:
                        tel = anchor.css(tsel)
                        tt = tel.css("::text").get() if tel else (anchor.css("::text").get() or "")
                    else:
                        tt = anchor.css("::text").get() or ""
                    n, t = extract_title(tt)
                    if n:
                        log.info("LinkedIn " + sname + ": " + n)
                        return {"person_name":n,"person_title":t,"linkedin_url":href,"source":sname}
                    par = anchor
                    for _ in range(4):
                        try:
                            par = par.parent
                            ft = " ".join(par.css("::text").getall())
                            n, t = extract_snippet(ft)
                            if n:
                                return {"person_name":n,"person_title":t,"linkedin_url":href,"source":sname+"_snippet"}
                        except Exception: break
        except Exception as e:
            log.warning(sname + " failed: " + str(e))
    return {"person_name":"NOT_FOUND","person_title":"","linkedin_url":"","source":"not_found"}

if __name__ == "__main__":
    log.info("Venara Scrapling Server v4.0.0 - port 8765")
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
