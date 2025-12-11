import logging
import os
import time
import re
import hashlib
import datetime
import requests
from urllib.parse import urljoin, urlparse
from collections import deque
from bs4 import BeautifulSoup

import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "KennisgroepenDB")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "Articles")

BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "kennisgroepen-text")

START_URLS = [
    "https://kennisgroepen.belastingdienst.nl/",
    "https://kennisgroepen.belastingdienst.nl/publicaties/",
    "https://kennisgroepen.belastingdienst.nl/standpunten/",
    "https://kennisgroepen.belastingdienst.nl/nieuws/"
]

ALLOWED_PATH_PREFIXES = ("/", "/nieuws", "/publicaties", "/standpunten", "/categorie")
BASE_DOMAIN = "kennisgroepen.belastingdienst.nl"

MAX_PAGES_PER_RUN = 200  # Safety limit for a single function execution (prevents timeouts)
REQUEST_TIMEOUT = 25
SLEEP_SECONDS = 1.5

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# HTTP HELPERS
# ---------------------------------------------------------------------------
def get_session():
    session = requests.Session()
    session.headers.update({"User-Agent": "AzureFunction/1.0 (+https://rsm.nl)"})
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

session = get_session()

def clean_url(u):
    return urlparse(u)._replace(fragment="").geturl().strip()

def same_site(u):
    return urlparse(u).netloc == BASE_DOMAIN

def allowed_path(u):
    p = urlparse(u).path or "/"
    return any(p.startswith(pref) for pref in ALLOWED_PATH_PREFIXES)

def url_to_id(u):
    """Generates deterministic ID (hash) from URL."""
    return hashlib.md5(u.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# PARSING HELPERS (Ported from your script)
# ---------------------------------------------------------------------------
def strip_boilerplate(soup):
    for sel in ["header","nav","footer",".footer",".header",".sidebar",
                ".cookie",".breadcrumb",".pagination","ul[role='navigation']"]:
        for el in soup.select(sel): el.decompose()
    return soup

def is_listing_page(soup):
    txt = soup.get_text(" ", strip=True)
    many_dates = len(re.findall(r"\b\d{2}-\d{2}-\d{4}\b", txt)) >= 3
    has_pagination = "Pagina" in txt or "Volgende" in txt or "page" in txt
    return many_dates or has_pagination

DUTCH_MONTHS = {
    "januari":1,"februari":2,"maart":3,"april":4,"mei":5,"juni":6,
    "juli":7,"augustus":8,"september":9,"oktober":10,"november":11,"december":12
}

def to_iso_date(s):
    s = s.strip().lower()
    m = re.match(r"(\d{2})-(\d{2})-(\d{4})", s)
    if m: d,mo,y=m.groups(); return f"{y}-{mo}-{d}"
    m = re.match(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", s)
    if m and m.group(2) in DUTCH_MONTHS:
        d,mon,y=m.groups()
        return f"{y}-{DUTCH_MONTHS[mon]:02d}-{int(d):02d}"
    return s

def extract_main_text(soup):
    parts=[]
    for tag in soup.find_all(["h1","h2","h3","p","li"]):
        txt = tag.get_text(" ", strip=True)
        if txt:
            if tag.name in ("h1","h2","h3"): parts.append("\n"+txt+"\n")
            elif tag.name=="li": parts.append("- "+txt)
            else: parts.append(txt)
    return "\n".join(parts).strip()

def extract_listing_items(soup, base_url):
    items = []
    for a in soup.select("a[href]"):
        t = a.get_text(" ", strip=True)
        if not t or len(t) < 10: continue
        href = a["href"]
        url = urljoin(base_url, href)
        if not same_site(url) or not allowed_path(url): continue
        
        # Heuristic to find metadata near the link
        block = a.find_parent(["li","article","div"]) or a
        block_txt = block.get_text(" ", strip=True)
        if any(x in block_txt for x in ["Algemene informatie","Handige links","Cookies"]): continue
        
        items.append({"url": url})
    return items

# ---------------------------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------------------------
@app.schedule(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def kennisgroepen_scraper(myTimer: func.TimerRequest) -> None:
    logging.info('🚀 Kennisgroepen Scraper started.')

    # 1. Init Azure Clients
    try:
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
        database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
        container = database.get_container_client(COSMOS_CONTAINER_NAME)

        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        if not blob_container_client.exists():
            blob_container_client.create_container()
    except Exception as e:
        logging.error(f"❌ Client Init Error: {e}")
        return

    # 2. Setup Crawl
    queue = deque(START_URLS)
    visited = set() # Track visited in this run
    pages_processed = 0

    while queue and pages_processed < MAX_PAGES_PER_RUN:
        current_url = clean_url(queue.popleft())
        
        if current_url in visited:
            continue
        visited.add(current_url)

        try:
            r = session.get(current_url, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type", ""):
                continue
            
            soup = BeautifulSoup(r.text, "html.parser")
            soup = strip_boilerplate(soup)

            # --- CASE A: Listing Page (Find more links) ---
            if is_listing_page(soup):
                # 1. Articles in list
                items = extract_listing_items(soup, current_url)
                for it in items:
                    if it["url"] not in visited:
                        queue.append(it["url"])
                
                # 2. Pagination links
                for a in soup.select("a[href]"):
                    href = a["href"]
                    if re.search(r"(page|sf_paged)=\d+", href):
                        nxt = urljoin(current_url, href)
                        if nxt not in visited:
                            queue.append(nxt)
                
                logging.info(f"📂 Listing scanned: {current_url} -> Found {len(items)} items")
                continue

            # --- CASE B: Article Page (Process & Save) ---
            title = soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "Untitled"
            text_content = extract_main_text(soup)

            if not text_content or len(text_content) < 50:
                logging.info(f"⏩ Skipped short content: {current_url}")
                continue

            # Check Idempotency (Have we scraped this exact URL before?)
            doc_id = url_to_id(current_url)
            try:
                # If you want to force-update content, comment out this check
                container.read_item(item=doc_id, partition_key=doc_id)
                logging.info(f"⏭️  Already exists (skipping): {title[:30]}...")
                continue 
            except Exception:
                pass # Item not found, proceed to save

            # Extract Metadata
            mdate = re.search(r"\b(\d{1,2}\s+[a-z]+\s+\d{4}|\d{2}-\d{2}-\d{4})\b", soup.get_text(" ", strip=True).lower())
            date = to_iso_date(mdate.group(1)) if mdate else datetime.datetime.utcnow().date().isoformat()
            
            mcat = re.search(r"Categorie\s+([A-Za-z\-\s]+)", soup.get_text(" ", strip=True))
            category = mcat.group(1).strip() if mcat else "Unknown"

            # 1. Upload Text to Blob
            safe_filename = f"{doc_id}.txt"
            blob_url = None
            try:
                blob_client = blob_container_client.get_blob_client(safe_filename)
                blob_client.upload_blob(text_content, overwrite=True)
                blob_url = blob_client.url
            except Exception as e:
                logging.error(f"Blob upload failed: {e}")

            # 2. Upload Metadata to Cosmos
            item = {
                "id": doc_id,
                "url": current_url,
                "title": title,
                "category": category,
                "publication_date": date,
                "blob_url": blob_url,
                "scraped_at": datetime.datetime.utcnow().isoformat()
            }
            container.upsert_item(item)
            
            pages_processed += 1
            logging.info(f"✅ Saved: {title[:50]}...")
            
            # Enqueue deeper links from article just in case
            for a in soup.find_all("a", href=True):
                nxt = urljoin(current_url, a["href"])
                if same_site(nxt) and allowed_path(nxt) and nxt not in visited:
                    if not re.search(r"\.(pdf|jpg|png|gif|zip|xml|mp4)(\?|$)", nxt, re.I):
                        queue.append(nxt)

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            logging.error(f"⚠️ Error processing {current_url}: {e}")
            continue

    logging.info(f"🏁 Run complete. Processed {pages_processed} articles.")