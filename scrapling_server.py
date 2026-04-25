"""
scrapling_server.py  —  Venara AI Lead Scraping Server  v2.0
════════════════════════════════════════════════════════════════
Servidor FastAPI que expone 3 endpoints para el workflow n8n v6.0

  POST /search-google-maps  →  busca negocios en Google Maps
  POST /scrape-website       →  visita un sitio y retorna texto limpio
  POST /search-linkedin      →  busca el decision-maker en LinkedIn

INSTALACIÓN (https://github.com/D4Vinci/Scrapling):
  pip install "scrapling[fetchers]"
  scrapling install
  pip install fastapi uvicorn

EJECUCIÓN:
  python scrapling_server.py
"""

from __future__ import annotations
import re, logging
from urllib.parse import quote

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scrapling.fetchers import Fetcher, FetcherSession, StealthyFetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
app = FastAPI(title="Venara Scrapling Server", version="2.0.0")

DECISION_MAKER_ROLES = [
    "CEO","Co-Founder","Founder","CTO","COO","CMO",
    "Director General","Director","Gerente General","Gerente",
    "Owner","Propietario","Dueño","Socio","President","VP",
]

class MapsRequest(BaseModel):
    query: str
    location: str = ""
    max_results: int = 25

class WebsiteRequest(BaseModel):
    url: str

class LinkedInRequest(BaseModel):
    company: str
    location: str = ""
    fallback_name: str = ""

def looks_like_name(s: str) -> bool:
    words = s.split()
    return 2 <= len(words) <= 6 and len(s) <= 60 and not any(c.isdigit() for c in s)

def extract_from_title(title: str):
    clean = re.sub(r"\s*\|\s*LinkedIn\s*$", "", title, flags=re.I).strip()
    m = re.match(r"^([^|\-–—]+)", clean)
    if not m: return None, None
    name = m.group(1).strip()
    if not looks_like_name(name): return None, None
    tm = re.search(r"[-–—]\s*(.+)$", clean)
    return name, (tm.group(1).strip() if tm else "")

def extract_from_snippet(snippet: str):
    parts = re.split(r"\s*·\s*", snippet)
    if len(parts) >= 2 and looks_like_name(parts[0].strip()):
        return parts[0].strip(), parts[1].strip()
    return None, None

def clean_html(html: str, max_chars=12000) -> str:
    t = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    t = re.sub(r"<style[\s\S]*?</style>",   " ", t, flags=re.I)
    t = re.sub(r"<svg[\s\S]*?</svg>",        " ", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("&nbsp;"," ").replace("&amp;","&").replace("&lt;","<").replace("&gt;",">")
    return re.sub(r"\s+", " ", t).strip()[:max_chars]

@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}

@app.post("/search-linkedin-companies")
def search_linkedin_companies(req: MapsRequest):
    niche    = req.query.strip()
    location = req.location.strip()
    max_r    = req.max_results
    location_part = f'"{location}"' if location else ""
    query_google  = f'site:linkedin.com/company {niche} {location_part}'
    query_ddg     = f'site:linkedin.com/company "{niche}" {location_part}'
    results = []

    def parse_company_results(page, source):
        found = []
        selectors = [
            ("div.g a", "h3"), ("div.tF2Cxc a", "h3"),
            ("li.b_algo h2 a", None), (".result__title a", None),
            ("a[href*='linkedin.com/company']", None),
        ]
        for anchor_sel, title_sel in selectors:
            try:
                anchors = page.css(anchor_sel)
                if not anchors: continue
                for anchor in anchors:
                    href = anchor.attrib.get("href", "")
                    if "/url?q=" in href:
                        import urllib.parse
                        parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                        href = parsed.get("q", [href])[0]
                    if "uddg=" in href:
                        from urllib.parse import parse_qs, urlparse, unquote
                        try:
                            params = parse_qs(urlparse(href).query)
                            href = unquote(params.get("uddg", [href])[0])
                        except: pass
                    if "linkedin.com/company/" not in href: continue
                    if title_sel:
                        title_el = anchor.css(title_sel)
                        name = title_el.css("::text").get() if title_el else (anchor.css("::text").get() or "")
                    else:
                        name = anchor.css("::text").get() or ""
                    import re as _re
                    name = _re.sub(r"\s*[|·\-]s\*(LinkedIn|Company).*$", "", name, flags=_re.I).strip()
                    if not name or len(name) < 2: continue
                    website = ""
                    try:
                        parent = anchor
                        for _ in range(5):
                            parent = parent.parent
                            texts = " ".join(parent.css("::text").getall())
                            url_m = _re.search(r"https?://(?!linkedin)[^\s]+", texts)
                            if url_m:
                                website = url_m.group(0).rstrip(".,)")
                                break
                    except: pass
                    found.append({"name": name, "linkedin_url": href, "website": website, "source": source})
            if found: break
            except Exception as e:
                log.debug(f"Selector {anchor_sel} falló: {e}")
        return found

    for (url_f, source_name) in [
        (f"https://www.google.com/search?q={quote(query_google)}&num=20&hl=es&gl=pe", "google"),
        (f"https://html.duckduckgo.com/html/?q={quote(query_ddg)}&kl=es-es", "duckduckgo"),
        (f"https://www.bing.com/search?q={quote(query_ddg)}&count=20&setlang=es", "bing"),
    ]:
        if len(results) >= max_r: break
        try:
            log.info(f"LinkedIn Companies [{source_name}]: '{niche}' en '{location}'")
            with FetcherSession(impersonate="chrome") as session:
                page = session.get(url_f, stealthy_headers=True)
            results += parse_company_results(page, source_name)
        except Exception as e:
            log.warning(f"{source_name} falló: {e}")

    seen, clean = set(), []
    for r in results:
        key = r.get("linkedin_url") or r.get("name")
        if key and key not in seen and r.get("name"):
            seen.add(key); clean.append(r)

    log.info(f"LinkedIn Companies total: {len(clean)} para '{niche}'")
    if not clean:
        raise HTTPException(status_code=404, detail=f"No se encontraron empresas en LinkedIn para: {niche} {location}")
    return {"results": clean[:max_r], "total": len(clean)}

@app.post("/search-google-maps")
def search_google_maps(req: MapsRequest):
    query = req.query if req.location in req.query else f"{req.query} {req.location}".strip()
    log.info(f"Google Maps search: '{query}' | max={req.max_results}")
    results = []
    try:
        bing_url = f"https://www.bing.com/search?q={quote(query + ' contacto sitio web')}&count=50&setlang=es"
        with FetcherSession(impersonate="chrome") as session:
            page = session.get(bing_url, stealthy_headers=True)
        for card in page.css("li.b_algo"):
            name = card.css("h2 a::text").get() or ""
            href = (card.css("h2 a").get() or card.css("cite::text").get() or "")
            website = ""
            if href and href.startswith("http") and "bing.com" not in href:
                website = href
            if name and website:
                results.append({"name": name.strip(), "website": website.strip(), "address": "", "phone": "", "maps_url": bing_url})
    except Exception as e:
        log.warning(f"Bing search falló: {e}")
    if len(results) < 5:
        try:
            ddg_url = f"https://duckduckgo.com/html/?q={quote(query + ' email web')}&kl=es-es"
            page = Fetcher.get(ddg_url, stealthy_headers=True)
            for div in page.css(".result"):
                name = div.css(".result__title a::text").get() or ""
                website = (div.css(".result__url::text").get() or "").strip()
                if not website.startswith("http"): website = f"https://{website}"
                if name and website and "duckduckgo" not in website:
                    results.append({"name": name.strip(), "website": website, "address": "", "phone": "", "maps_url": ddg_url})
        except Exception as e:
            log.warning(f"DuckDuckGo falló: {e}")
    seen, clean = set(), []
    for r in results:
        key = r.get("website") or r.get("name")
        if key and key not in seen and r.get("name"):
            seen.add(key); clean.append(r)
    if not clean: raise HTTPException(status_code=404, detail=f"No negocios para: {query}")
    return {"results": clean[:req.max_results], "total": len(clean)}

@app.post("/scrape-website")
def scrape_website(req: WebsiteRequest):
    url = req.url.strip()
    if not url: return {"clean_text": "NO_CONTENT", "url": url}
    log.info(f"Scraping website: {url}")
    try:
        with FetcherSession(impersonate="chrome") as session:
            page = session.get(url, stealthy_headers=True, follow_redirects=True)
        html = page.html_content or ""
        if html and len(html) > 100:
            ct = clean_html(html)
            return {"clean_text": ct, "url": url, "method": "fetcher_session"}
    except Exception as e:
        log.warning(f"FetcherSession failed: {e}")
    try:
        page = StealthyFetcher.fetch(url, headless=True, network_idle=True)
        html = page.html_content or ""
        if html and len(html) > 100:
            ct = clean_html(html)
            return {"clean_text": ct, "url": url, "method": "stealthy_fetcher"}
    except Exception as e:
        log.warning(f"StealthyFetcher failed: {e}")
    return {"clean_text": "NO_CONTENT", "url": url, "method": "failed"}

@app.post("/search-linkedin")
def search_linkedin(req: LinkedInRequest):
    company = req.company.strip()
    if not company or company in {"NO_COMPANY_FOUND", "NOT_FOUND", ""}:
        return {"person_name": "NOT_FOUND", "person_title": "", "linkedin_url": "", "source": "no_company"}
    location = req.location.strip()
    role_q = '"CEO" OR "Founder" OR "Co-Founder" OR "Director General" OR "Director" OR "Gerente General" OR "Gerente" OR "Owner" OR "Presidente" OR "CTO" OR "COO"'
    location_part = f' "{location}"' if location else ""
    query_google = f'site:linkedin.com/in "{company}" ({role_q}){location_part}'
    query_ddg = f'site:linkedin.com/in "{company}" CEO OR Director OR Founder{location_part}'

    def parse_results(page, source_name):
        selectors = [("div.g a", "h3"),("div.tF2Cxc a","h3"),("li.b_algo h2 a",None),(".result__title a",None),("a[href*='linkedin.com/in']",None)]
        for anchor_sel,title_sel in selectors:
            try:
                anchors=page.css(anchor_sel)
                if not anchors: continue
                for anchor in anchors:
                    href=anchor.attrib.get("href","")
                    if "/url?q=" in href:
                        import urllib.parse; parsed=urllib.parse.parse_qs(urllib.parse.urlparse(href).query); href=parsed.get("q",[href])[0]
                    if "uddg=" in href:
                        from urllib.parse import parse_qs,urlparse,unquote
                        try: params=parse_qs(urlparse(href).query); href=unquote(params.get("uddg",[href])[0])
                        except: pass
                    if "linkedin.com/in/" not in href: continue
                    if title_sel:
                        title_el=anchor.css(title_sel); title_text=title_el.css("::text").get() if title_el else(anchor.css("::text").get() or"")
                    else: title_text=anchor.css("::text").get() or""
                    name,title=extract_from_title(title_text)
                    if name:
                        log.info(f"✅ LinkedIn ({source_name}): {name} | {title}")
                        return {"person_name": name,"person_title": title,"linkedin_url": href,"source": source_name}
                    parent=anchor
                    for _ in range(4):
                        try: parent=parent.parent; full_text=" ".join(parent.css("::text").getall()); name,title=extract_from_snippet(full_text)
                        if name:
                            log.info(f"✅ LinkedIn snippet ({source_name}): {name} | {title}")
                            return {"person_name": name,"person_title": title,"linkedin_url": href,"source": f"{source_name}_snippet"}
                        except: break
            except Exception as e: log.debug(f"Selector {anchor_sel} falló: {e}")
        return None

    for(url_f,source_n) in [(f"https://www.google.com/search?q={quote(query_google)}&num=10&hl=es&gl=pe","google"),(f"https://html.duckduckgo.com/html/?q={quote(query_ddg)}&kl=es-es","duckduckgo"),(f"https://www.bing.com/search?q={quote(' '.join(f'\"{rs}\"' for rs in DECISION_MAKER_ROLESY��]))}&count=10&setlang=es","bing")]:
        try:
            with FetcherSession(impersonate="chrome") as session: page=session.get(url_f,stealthy_headers=True)
            result=parse_results(page,source_n)
            if result: return result
        except Exception as e: log.warning(f"{source_n} falló: {e}")
    log.info(f"❌ No encontrado para '{company}'")
    return {"person_name":"NOT_FOUND","person_title":"","linkedin_url":"","source":"not_found"}

if __name__ == "__main__":
    uvicorn.run(app,host="0.0.0.0",port=8765,log_level="info")
