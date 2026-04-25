"""
scrapling_server.py - Venara AI Lead Scraping Server v3.1
Sin browsers - usa solo FetcherSession (HTTP sin Playwright)
"""
from __future__ import annotations
import re, logging
from urllib.parse import quote
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scrapling.fetchers import FetcherSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s?")
log = logging.getLogger(__name__)
app = FastAPI(title="Venara Scrapling Server", version="3.1.0")

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
    parts = re.split(r"\s*\\u00b7\s*", snip)
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
        except: pass
    return href

def get_search_urls(query):
    return [
        ("https://www.google.com/search?q=" + quote(query) + "&num=20&hl=es&gl?pe", "google"),
        ("https://html.duckduckgo.com/html/?q=" + quote(query) + "&kl=es-es", "duckduckgo"),
        ("https://www.bing.com/search?q=" + quote(query) + "&count=20&setlang=es", "bing"),
    ]

COMPANY_SELS = [
    ("div.g a", "h3"),
    ("div.tF2Cxc a", "h3"),
    ("li.b_algo h2 a", None),
    (".result__title a", None),
    ("a[href*='linkedin.com/company']", None),
]

PERSON_SELS = [
    ("div.g a", "h3"),
    ("div.tF2Cxc a", "h3"),
    ("li.b_algo h2 a", None),
    (".result__title a", None),
]

@app.get("/health")
def health():
    return {"status": "ok", "version": "3.1.0"}

@app.post("/search-linkedin-companies")
def search_linkedin_companies(req: MapsRequest):
    niche = req.query.strip()
    loc = req.location.strip()
    loc_p = '"' + loc + '"' if loc else ""
    query = "site:linkedin.com/company " + niche + " " + loc_p
    results = []
    seen = set()
    for url_f, sname in get_search_urls(query):
        if len(results) >= req.max_results: break
        try:
            log.info("LinkedIn Companies [" + sname + "]: " + niche)
            with FetcherSession(impersonate="chrome") as s:
                page = s.get(url_f, stealthy_headers=True)
            for asel, tsel in COMPANY_SELS:
                anchors = page.css(asel)
                if not anchors: continue
                found_here = False
                for anchor in anchors:
                    href = fix_href(anchor.attrib.get("href",""))
                    if "linkedin.com/company/" not in href or href in seen: continue
                    if tsel:
                        tel = anchor.css(tsel)
                        tt = tel.css("::text").get() if tel else (anchor.css("::text").get() or "")
                    else:
                        tt = anchor.css("::text").get() or ""
                    name = re.sub(r"\s*[|\-]\s*(LinkedIn|Company).*$","",tt,flags=re.I).strip()
                    if not name or len(name) < 2: continue
                    web = ""
                    try:
                        par = anchor
                        for _ in range(5):
                            par = par.parent
                            txt = " ".join(par.css("::text").getall())
                            um = re.search(r"https?://(?!linkedin)[^\s]+", txt)
                            if um: web = um.group(0).rstrip(".,)"); break
                    except: pass
                    results.append({"name": name, "linkedin_url": href, "website": web, "source": sname})
                    seen.add(href)
                    found_here = True
                if found_here: break
        except Exception as e:
            log.warning(sname + " failed: " + str(e))
    if not results:
        raise HTTPException(404, "No empresas LinkedIn para: " + niche + " " + loc)
    return {"results": results[:req.max_results], "total": len(results)}

@app.post("/scrape-website")
def scrape_website(req: WebsiteRequest):
    url = req.url.strip()
    if not url: return {"clean_text": "NO_CONTENT", "url": url}
    log.info("Scraping: " + url)
    try:
        with FetcherSession(impersonate="chrome") as s:
            page = s.get(url, stealthy_headers=True, follow_redirects=True)
        html = page.html_content or ""
        if html and len(html) > 100:
            return {"clean_text": clean_html(html), "url": url, "method": "fetcher_session"}
    except Exception as e:
        log.warning("FetcherSession failed: " + str(e))
    try:
        import urllib.request
        r2 = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(r2, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        if html and len(html) > 100:
            return {"clean_text": clean_html(html), "url": url, "method": "urllib"}
    except Exception as e:
        log.warning("urllib failed: " + str(e))
    return {"clean_text": "NO_CONTENT", "url": url, "method": "failed"}

@app.post("/search-linkedin")
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
            with FetcherSession(impersonate="chrome") as s:
                page = s.get(url_f, stealthy_headers=True)
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
                        log.info("LinkedIn " + sname + ": " + n + " | " + t)
                        return {"person_name":n,"person_title":t,"linkedin_url":href,"source":sname}
                    par = anchor
                    for _ in range(4):
                        try:
                            par = par.parent
                            ft = " ".join(par.css("::text").getall())
                            n, t = extract_snippet(ft)
                            if n:
                                return {"person_name":n,"person_title":t,"linkedin_url":href,"source":sname+"_snippet"}
                        except: break
        except Exception as e:
            log.warning(sname + " failed: " + str(e))
    return {"person_name":"NOT_FOUND","person_title":"","linkedin_url":"","source":"not_found"}

if __name__ == "__main__":
    log.info("Venara Scrapling Server v3.1.0 - port 8765")
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
