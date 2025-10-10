from __future__ import annotations
import os, sys, time, json, hashlib, requests
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

# --- project imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


class GenericWebIngestion:
    """
    🌐 Generic Web Ingestion for DMP-RAG (Multi-Seed Edition)
    ----------------------------------------------------------
    ✅ Accepts multiple seed URLs from file (.json)
    ✅ Crawls internal links recursively (depth-limited)
    ✅ Extracts readable text (headings, paragraphs, lists)
    ✅ Downloads linked PDFs
    ✅ Skips duplicates via manifest.json (hash-based)
    ✅ Handles multiple domains in one run
    """

    def __init__(
        self,
        data_root: str = "data",
        max_depth: int = 2,
        crawl_delay: float = 1.5,
        max_pages: int = 200,
    ):
        self.data_root = Path(data_root)
        self.web_dir = self.data_root / "web_sources"
        self.manifest_path = self.web_dir / "manifest.json"
        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()

        self.web_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self._load_manifest()
        log.info("GenericWebIngestion initialized", web_dir=str(self.web_dir))

    # --------------------------------------------------------
    # Manifest Handling
    # --------------------------------------------------------
    def _load_manifest(self):
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_manifest(self):
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2)

    # --------------------------------------------------------
    # Hash Utility
    # --------------------------------------------------------
    def _compute_hash(self, content: bytes) -> str:
        h = hashlib.sha256()
        h.update(content)
        return h.hexdigest()

    # --------------------------------------------------------
    # PDF Downloader
    # --------------------------------------------------------
    def _download_pdf(self, pdf_url: str):
        """Downloads a PDF file and records it in the manifest."""
        try:
            resp = self.session.get(pdf_url, timeout=30)
            if resp.status_code != 200 or b"%PDF" not in resp.content[:500]:
                return

            file_name = Path(urlparse(pdf_url).path).name or f"file_{hashlib.md5(pdf_url.encode()).hexdigest()}.pdf"
            domain_folder = self.web_dir / urlparse(pdf_url).netloc
            domain_folder.mkdir(parents=True, exist_ok=True)
            dest = domain_folder / file_name

            file_hash = self._compute_hash(resp.content)
            for _, v in self.manifest.items():
                if v.get("hash") == file_hash:
                    log.info("⏩ Skipped existing PDF", file=file_name)
                    return

            with open(dest, "wb") as f:
                f.write(resp.content)

            self.manifest[pdf_url] = {
                "file": str(dest),
                "hash": file_hash,
                "last_updated": datetime.utcnow().isoformat(),
                "type": "pdf",
            }
            log.info("📥 PDF downloaded", file=file_name)
            self._save_manifest()

        except Exception as e:
            log.error("❌ PDF download failed", url=pdf_url, error=str(e))

    # --------------------------------------------------------
    # Text Extractor
    # --------------------------------------------------------
    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        sections = []
        for elem in soup.find_all(["h1", "h2", "h3", "p", "li"]):
            text = elem.get_text(" ", strip=True)
            if text:
                sections.append(text)
        return "\n".join(sections)

    # --------------------------------------------------------
    # Recursive Crawl
    # --------------------------------------------------------
    def crawl_site(self, start_url: str):
        visited = set()
        to_visit = [(start_url, 0)]
        domain = urlparse(start_url).netloc
        domain_folder = self.web_dir / domain
        domain_folder.mkdir(parents=True, exist_ok=True)
        page_count = 0

        log.info("🌐 Starting crawl", start=start_url, domain=domain)

        while to_visit and page_count < self.max_pages:
            url, depth = to_visit.pop(0)
            if url in visited or depth > self.max_depth:
                continue
            visited.add(url)
            page_count += 1

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    log.warning("⚠️ Skipping (HTTP error)", url=url, status=resp.status_code)
                    continue

                html = resp.text
                text = self._extract_text(html)
                if text:
                    file_name = f"{urlparse(url).path.strip('/').replace('/', '_') or 'index'}_{page_count}.txt"
                    dest = domain_folder / file_name
                    with open(dest, "w", encoding="utf-8") as f:
                        f.write(text)
                    self.manifest[url] = {
                        "file": str(dest),
                        "hash": self._compute_hash(text.encode()),
                        "last_updated": datetime.utcnow().isoformat(),
                        "type": "text",
                    }
                    log.info("📝 Saved page", file=file_name)

                # Find and download PDFs
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.lower().endswith(".pdf"):
                        pdf_url = urljoin(url, href)
                        self._download_pdf(pdf_url)

                # Queue new internal links
                for a in soup.find_all("a", href=True):
                    next_url = urljoin(url, a["href"])
                    if (
                        urlparse(next_url).netloc == domain
                        and next_url not in visited
                        and "#" not in next_url
                    ):
                        to_visit.append((next_url, depth + 1))

                time.sleep(self.crawl_delay)

            except Exception as e:
                log.error("❌ Crawl failed for page", url=url, error=str(e))

        log.info("✅ Crawl completed", total_pages=page_count)
        self._save_manifest()

    # --------------------------------------------------------
    # Multi-Seed Crawl Entry Point
    # --------------------------------------------------------
    def crawl_multiple_sites(self, urls: list[str]):
        for url in urls:
            try:
                log.info("🚀 Starting crawl for site", site=url)
                self.crawl_site(url)
            except Exception as e:
                log.error("❌ Failed to crawl site", site=url, error=str(e))
        log.info("🏁 Multi-site crawl complete", total_entries=len(self.manifest))


# --------------------------------------------------------
# Helper to load URLs from JSON file
# --------------------------------------------------------
def load_links(file_path: str = "data/web_links.json") -> list[str]:
    """Load URLs from .json file."""
    p = Path(file_path)
    if not p.exists():
        log.error("Link file not found", file=file_path)
        return []
    try:
        data = json.load(open(p, "r", encoding="utf-8"))
        return data.get("sources", [])
    except Exception as e:
        log.error("Failed to load link file", file=file_path, error=str(e))
        return []


# --------------------------------------------------------
# Example Run
# --------------------------------------------------------
if __name__ == "__main__":
    crawler = GenericWebIngestion(max_depth=2, crawl_delay=1.5, max_pages=100)

    links = load_links("data/web_links.json")
    if not links:
        log.error("No links found to crawl.")
    else:
        crawler.crawl_multiple_sites(links)
