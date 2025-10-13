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
    🌐 Generic Web Ingestion (RAG-Optimized Edition)
    ------------------------------------------------
    ✅ Collects only meaningful content (text + PDFs)
    ✅ Removes logos, headers, menus, ads, etc.
    ✅ Saves each run separately under data/general_web_ingestion/<session_id>/
    ✅ Ideal for building clean retrieval datasets
    """

    def __init__(
        self,
        data_root: str = "data",
        max_depth: int = 2,
        crawl_delay: float = 1.5,
        max_pages: int = 200,
        session_id: str | None = None,
    ):
        self.data_root = Path(data_root)
        self.session_id = session_id or datetime.now().strftime("session_%Y%m%d_%H%M%S")

        # Directory isolation
        self.base_dir = self.data_root / "general_web_ingestion" / self.session_id
        self.txt_dir = self.base_dir / "texts"
        self.pdf_dir = self.base_dir / "pdfs"
        self.manifest_path = self.base_dir / "manifest.json"

        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()

        self.txt_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = {}

        log.info("🆕 RAG Ingestion started", session=self.session_id, folder=str(self.base_dir))

    # --------------------------------------------------------
    # Manifest Handling
    # --------------------------------------------------------
    def _save_manifest(self):
        manifest_data = {
            "session_id": self.session_id,
            "created_at": datetime.utcnow().isoformat(),
            "file_count": len(self.manifest),
            "files": list(self.manifest.values()),
        }
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
        log.info("💾 Manifest saved", file=self.manifest_path)

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
        """Downloads PDF content suitable for RAG use."""
        try:
            resp = self.session.get(pdf_url, timeout=30)
            if resp.status_code != 200 or b"%PDF" not in resp.content[:500]:
                return

            file_name = Path(urlparse(pdf_url).path).name or f"file_{hashlib.md5(pdf_url.encode()).hexdigest()}.pdf"
            dest = self.pdf_dir / file_name
            file_hash = self._compute_hash(resp.content)

            if any(file_hash == item.get("hash") for item in self.manifest.values()):
                log.info("⏩ Skipped duplicate PDF", file=file_name)
                return

            with open(dest, "wb") as f:
                f.write(resp.content)

            self.manifest[pdf_url] = {
                "file": str(dest),
                "hash": file_hash,
                "type": "pdf",
                "last_updated": datetime.utcnow().isoformat(),
            }
            log.info("📥 PDF downloaded", file=file_name)

        except Exception as e:
            log.error("❌ PDF download failed", url=pdf_url, error=str(e))

    # --------------------------------------------------------
    # Text Extractor (content-focused)
    # --------------------------------------------------------
    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        # Remove non-content tags
        for tag in soup(["script", "style", "noscript", "footer", "header", "nav", "form", "aside", "img", "svg"]):
            tag.extract()

        skip_phrases = [
            "cookie", "privacy", "terms", "subscribe", "newsletter",
            "login", "sign in", "menu", "share", "back to top", "copyright"
        ]

        sections = []
        for elem in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "article", "section", "div"]):
            text = elem.get_text(" ", strip=True)
            if not text:
                continue
            if any(word in text.lower() for word in skip_phrases):
                continue
            if len(text.split()) < 5:  # skip very short non-content lines
                continue
            sections.append(text)

        return "\n\n".join(sections)

    # --------------------------------------------------------
    # Recursive Crawl
    # --------------------------------------------------------
    def crawl_site(self, start_url: str):
        visited = set()
        to_visit = [(start_url, 0)]
        page_count = 0
        domain = urlparse(start_url).netloc

        log.info("🌐 Crawling", domain=domain, session=self.session_id)

        while to_visit and page_count < self.max_pages:
            url, depth = to_visit.pop(0)
            if url in visited or depth > self.max_depth:
                continue
            visited.add(url)
            page_count += 1

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    continue

                html = resp.text
                text = self._extract_text(html)
                if text:
                    file_name = f"page_{page_count}.txt"
                    dest = self.txt_dir / file_name
                    with open(dest, "w", encoding="utf-8") as f:
                        f.write(text)

                    self.manifest[url] = {
                        "file": str(dest),
                        "hash": self._compute_hash(text.encode()),
                        "type": "text",
                        "last_updated": datetime.utcnow().isoformat(),
                    }
                    log.info("📝 Saved page", file=file_name)

                soup = BeautifulSoup(html, "html.parser")

                # Download PDFs only (ignore images)
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if ".pdf" in href.lower():
                        pdf_url = urljoin(url, href)
                        self._download_pdf(pdf_url)

                # Follow internal links
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
                log.error("❌ Crawl failed", url=url, error=str(e))

        log.info("✅ Crawl completed", total_pages=page_count)
        self._save_manifest()

    # --------------------------------------------------------
    # Multi-Site Entry
    # --------------------------------------------------------
    def crawl_multiple_sites(self, urls: list[str]):
        for url in urls:
            log.info("🚀 Crawling site", url=url)
            self.crawl_site(url)
        log.info("🏁 All crawls complete", total_files=len(self.manifest))


# --------------------------------------------------------
# Helper to load URLs
# --------------------------------------------------------
def load_links(file_path: str = "data/web_links.json") -> list[str]:
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
