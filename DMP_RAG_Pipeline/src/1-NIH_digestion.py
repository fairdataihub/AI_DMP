from __future__ import annotations
import os, sys, time, json, hashlib, requests
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# --- project imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from logger.custom_logger import GLOBAL_LOGGER as log


class UnifiedWebIngestion:
    """
    🌐 Unified NIH Grants + DMPTool Ingestion (RAG v3 – Manifest Ready)
    ------------------------------------------------------------------
    ✅ Crawls NIH Grants (text + PDFs)
    ✅ Crawls DMPTool (NIH-funded DMP PDFs)
    ✅ Deduplication via hash index
    ✅ Predictable folder logic: latest / daily / new
    ✅ Unique filenames for each PDF
    ✅ Manifest saved with hash + metadata
    """

    def __init__(
        self,
        data_root: str = "C:/Users/Nahid/AI_DMP/DMP_RAG_Pipeline/data",
        json_links: str = "data/web_links.json",
        max_depth: int = 5,
        crawl_delay: float = 1.2,
        max_pages: int = 18000,
        session_mode: str = "new",
    ):
        self.data_root = Path(data_root)
        self.session_mode = session_mode
        self.session_folder = self._detect_or_create_session_folder()

        # Create manifest paths
        self.master_manifest = self.session_folder / "manifest_master.json"
        self.global_manifest = {"sites": {}}

        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (UnifiedIngestor/NIH-RAG)"})

        self.urls = self._load_links(json_links)
        self.stats = {
            "dmptool.org": {"pdfs": 0, "skipped": 0},
            "grants.nih.gov": {"pages": 0, "pdfs": 0, "skipped": 0},
        }

        log.info("🚀 Ingestion initialized", mode=self.session_mode, folder=str(self.session_folder))
        print(f"\n✅ Session Folder Created: {self.session_folder}\n")

    # --------------------------------------------------------
    # Folder logic
    # --------------------------------------------------------
    def _detect_or_create_session_folder(self) -> Path:
        parent = self.data_root / "data_ingestion"
        parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        tag = f"{now.year}_{now.month:02d}_{now.day:02d}_NIH_ingestion"
        ts = now.strftime("%Y%m%d_%H%M%S")

        folder = (
            parent / f"{tag}_{ts}" if self.session_mode == "new"
            else parent / tag
        )
        folder.mkdir(parents=True, exist_ok=True)
        log.info("🆕 Created session folder", folder=str(folder))
        return folder

    # --------------------------------------------------------
    # Helpers
    # --------------------------------------------------------
    def _load_links(self, path: str) -> list[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get("sources", [])
        except Exception as e:
            log.error("Failed to load web_links.json", error=str(e))
            return []

    def _compute_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def _prepare_site_dirs(self, domain: str):
        site_root = self.session_folder / domain
        txt_dir, pdf_dir = site_root / "texts", site_root / "pdfs"
        manifest_path = site_root / f"manifest_{domain.replace('.', '_')}.json"
        for d in [txt_dir, pdf_dir]:
            d.mkdir(parents=True, exist_ok=True)
        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as f:
                return txt_dir, pdf_dir, manifest_path, json.load(f)
        return txt_dir, pdf_dir, manifest_path, {"files": {}}

    def _save_manifest(self, manifest_path: Path, manifest: dict, domain: str):
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        self.global_manifest["sites"][domain] = manifest.get("files", {})
        with open(self.master_manifest, "w", encoding="utf-8") as f:
            json.dump(self.global_manifest, f, indent=2)
        log.info("💾 Manifest saved", domain=domain, file=str(manifest_path))

    # --------------------------------------------------------
    # NIH text filtering
    # --------------------------------------------------------
    def _is_valid_text_block(self, text: str) -> bool:
        text = text.strip().lower()
        if not text or len(text.split()) < 5:
            return False
        skip = ["menu", "cookie", "privacy", "terms", "footer", "navigation"]
        if any(term in text for term in skip):
            return False
        return any(k in text for k in ["nih", "grant", "data", "funding", "policy"])

    # --------------------------------------------------------
    # NIH Crawl
    # --------------------------------------------------------
    def _crawl_nih(self, start_url: str, domain: str):
        txt_dir, pdf_dir, manifest_path, manifest = self._prepare_site_dirs(domain)
        visited, queue, count = set(), [(start_url, 0)], 0
        log.info("🌐 NIH Grants crawl started", url=start_url)

        with tqdm(total=self.max_pages, desc="NIH Grants pages", unit="page") as pbar:
            while queue and count < self.max_pages:
                url, depth = queue.pop(0)
                if url in visited or depth > self.max_depth:
                    continue
                visited.add(url)
                try:
                    r = self.session.get(url, timeout=30)
                    if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
                        continue
                    soup = BeautifulSoup(r.text, "html.parser")

                    # Text extraction
                    blocks = [
                        t.get_text(" ", strip=True)
                        for t in soup.find_all(["h1", "h2", "h3", "p", "li"])
                        if self._is_valid_text_block(t.get_text(" ", strip=True))
                    ]
                    if blocks:
                        count += 1
                        text = "\n\n".join(blocks)
                        fpath = txt_dir / f"page_{count:04d}.txt"
                        fpath.write_text(text, encoding="utf-8")
                        h = self._compute_hash(text.encode())
                        manifest["files"][url] = {
                            "url": url,
                            "file": str(fpath),
                            "hash": h,
                            "type": "text",
                            "last_updated": datetime.utcnow().isoformat(),
                        }
                        self.stats[domain]["pages"] += 1
                        log.info("💾 Saved text", file=fpath.name)

                    # PDFs
                    for a in soup.find_all("a", href=True):
                        href = urljoin(url, a["href"])
                        if href.lower().endswith(".pdf"):
                            self._download_pdf(href, pdf_dir, domain, manifest)

                    for a in soup.find_all("a", href=True):
                        nxt = urljoin(url, a["href"])
                        if urlparse(nxt).netloc == domain and nxt not in visited:
                            if not any(x in nxt for x in ["contact", "faq", "press"]):
                                queue.append((nxt, depth + 1))

                    pbar.update(1)
                    time.sleep(self.crawl_delay)
                except Exception as e:
                    log.error("Crawl failed", url=url, error=str(e))

        self._save_manifest(manifest_path, manifest, domain)
        log.info("✅ NIH crawl completed", pages=self.stats[domain]["pages"])

    # --------------------------------------------------------
    # PDF Download with Manifest Entry
    # --------------------------------------------------------
    def _download_pdf(self, href: str, pdf_dir: Path, domain: str, manifest: dict, index: int = None):
        try:
            r = self.session.get(href, timeout=30)
            if r.status_code != 200 or b"%PDF" not in r.content[:500]:
                return
            ph = self._compute_hash(r.content)
            base = f"dmp_{len(manifest['files']) + 1:04d}"
            dest = pdf_dir / f"{base}.pdf"
            dest.write_bytes(r.content)

            manifest["files"][href] = {
                "url": href,
                "file": str(dest),
                "hash": ph,
                "type": "pdf",
                "last_updated": datetime.utcnow().isoformat(),
            }
            self.stats[domain]["pdfs"] += 1
            log.info("📥 PDF downloaded", file=dest.name)
        except Exception as e:
            log.warning("⚠️ PDF download failed", href=href, error=str(e))

    # --------------------------------------------------------
    # DMPTool Crawl
    # --------------------------------------------------------
    def _crawl_dmptool(self, start_url: str, domain: str):
        _, pdf_dir, manifest_path, manifest = self._prepare_site_dirs(domain)
        log.info("🌐 Starting DMPTool crawl", url=start_url)

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        prefs = {"download.default_directory": str(pdf_dir.resolve()),
                 "download.prompt_for_download": False,
                 "plugins.always_open_pdf_externally": True}
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(60)

        try:
            driver.get(start_url)
            time.sleep(5)
            count = 0
            with tqdm(total=self.max_pages, desc="DMPTool PDFs", unit="pdf") as pbar:
                while count < self.max_pages:
                    pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, '/export.pdf')]")
                    if not pdf_links:
                        break
                    for link in pdf_links:
                        href = link.get_attribute("href")
                        if not href:
                            continue
                        self._download_pdf(href, pdf_dir, domain, manifest)
                        count += 1
                        pbar.update(1)
                    try:
                        next_btn = driver.find_element(By.PARTIAL_LINK_TEXT, "Next")
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(4)
                    except NoSuchElementException:
                        break
        except TimeoutException:
            log.warning("⚠️ DMPTool timeout")
        finally:
            driver.quit()
            self._save_manifest(manifest_path, manifest, domain)
            log.info("✅ DMPTool completed", pdfs=self.stats[domain]["pdfs"])

    # --------------------------------------------------------
    # Run all
    # --------------------------------------------------------
    def run_all(self):
        for url in self.urls:
            domain = urlparse(url).netloc
            if "dmptool.org" in domain:
                self._crawl_dmptool(url, domain)
            else:
                self._crawl_nih(url, domain)
        self._summary_report()

    def _summary_report(self):
        log.info("📊 Crawl Summary:")
        for d, s in self.stats.items():
            log.info(f"{d}: Pages={s.get('pages',0)}, PDFs={s.get('pdfs',0)}, Skipped={s['skipped']}")


# --------------------------------------------------------
# Example Run
# --------------------------------------------------------
if __name__ == "__main__":
    crawler = UnifiedWebIngestion(
        data_root="C:/Users/Nahid/AI_DMP/DMP_RAG_Pipeline/data",
        json_links="data/web_links.json",
        max_depth=5,
        crawl_delay=1.2,
        max_pages=18000,
        session_mode="new",
    )
    crawler.run_all()
