from __future__ import annotations
import os, sys, time, json, hashlib, requests, shutil
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
    🌐 Unified NIH Grants + DMPTool Ingestion (Cross-Session Deduplication & Cleanup)
    -------------------------------------------------------------------------------
    ✅ Loads hashes and manifests from all previous sessions
    ✅ Deduplicates PDFs and text across runs by hash
    ✅ Merges older session manifests into the new one
    ✅ Automatically removes old session folders after merging
    ✅ Saves per-domain + master manifests safely
    ✅ Skips duplicates but keeps full cumulative history
    ✅ Plus: Generic crawl_site() for non-NIH/DMPTool pages
    """

    def __init__(
        self,
        data_root: str = "C:/Users/Nahid/AI_DMP/DMP_RAG_Pipeline/data",
        json_links: str = "data/web_links.json",
        max_depth: int = 5,
        crawl_delay: float = 1.2,
        max_pages: int = 18000,
        keep_last_n_sessions: int = 1,
    ):
        self.data_root = Path(data_root)
        self.session_folder = self._detect_or_create_session_folder()
        self.master_manifest = self.session_folder / "manifest_master.json"
        self.global_manifest = {"sites": {}}
        self.keep_last_n_sessions = keep_last_n_sessions

        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (UnifiedIngestor/NIH-RAG)"})

        self.urls = self._load_links(json_links)
        self.previous_hashes, self.previous_files = self._load_previous_manifests()

        self.stats = {
            "dmptool.org": {"pdfs": 0, "skipped": 0},
            "grants.nih.gov": {"pages": 0, "pdfs": 0, "skipped": 0},
        }

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
        folder = parent / f"{tag}_{ts}"
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    # --------------------------------------------------------
    # Prepare site directories
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # Load and merge previous manifests
    # --------------------------------------------------------
    def _load_previous_manifests(self) -> tuple[dict[str, set[str]], dict[str, dict]]:
        parent = self.data_root / "data_ingestion"
        sessions = sorted([p for p in parent.glob("*_NIH_ingestion*") if p.is_dir()], reverse=True)
        hash_index, file_index = {}, {}
        if not sessions:
            print("ℹ️ No previous sessions found — starting fresh.")
            return {}, {}

        for folder in sessions[1:]:
            manifest_path = folder / "manifest_master.json"
            if not manifest_path.exists():
                continue
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                for domain, files in manifest.get("sites", {}).items():
                    domain_hashes = hash_index.setdefault(domain, set())
                    domain_hashes.update({v.get("hash") for v in files.values() if "hash" in v})
                    domain_files = file_index.setdefault(domain, {})
                    domain_files.update(files)
            except Exception as e:
                print(f"⚠️ Failed to load {manifest_path}: {e}")

        # Cleanup old sessions
        if len(sessions) > self.keep_last_n_sessions:
            for old in sessions[self.keep_last_n_sessions:]:
                try:
                    shutil.rmtree(old, ignore_errors=True)
                    print(f"🧹 Removed old session: {old}")
                except Exception as e:
                    print(f"⚠️ Cleanup failed for {old}: {e}")

        if hash_index:
            print("♻️ Loaded previous session hashes and files:")
            for d, c in hash_index.items():
                print(f"   - {d}: {len(c)} known hashes")
        return hash_index, file_index

    # --------------------------------------------------------
    # Manifest saving
    # --------------------------------------------------------
    def _save_manifest(self, manifest_path: Path, manifest: dict, domain: str):
        try:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = manifest_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(manifest_path)

            merged_files = self.previous_files.get(domain, {})
            merged_files.update(manifest.get("files", {}))
            self.global_manifest["sites"][domain] = merged_files

            with open(self.master_manifest, "w", encoding="utf-8") as f:
                json.dump(self.global_manifest, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            print(f"✅ Manifest updated for {domain}: {len(merged_files)} files total")
        except Exception as e:
            print(f"❌ Manifest save error for {domain}: {e}")

    # --------------------------------------------------------
    # Text Filters (RAG-Optimized)
    # --------------------------------------------------------
    def _is_valid_text_block(self, text: str) -> bool:
        import re
        text = text.strip().lower()
        if not text or len(text.split()) < 5:
            return False
        if re.search(r"\b(expired|expiration date|superseded|replaced|no longer valid)\b", text):
            return False
        if "page last updated" in text or "last modified" in text:
            return False
        skip_terms = ["cookie", "privacy", "terms", "newsletter", "login", "subscribe"]
        if any(term in text for term in skip_terms):
            return False
        whitelist = ["nih", "grant", "data management", "data sharing", "policy", "rag system"]
        return any(term in text for term in whitelist)

    # --------------------------------------------------------
    # Helper Functions
    # --------------------------------------------------------
    def _load_links(self, path: str) -> list[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("sources", [])
        except Exception as e:
            print(f"⚠️ Failed to load {path}: {e}")
            return []

    def _compute_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    # --------------------------------------------------------
    # 🌍 NEW — HTML Extraction, Generic PDF Download & Crawl
    # --------------------------------------------------------
    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "form", "svg", "iframe"]):
            tag.decompose()
        for selector in [
            "[class*=banner]", "[id*=banner]", "[class*=nav]", "[id*=nav]",
            "[class*=menu]", "[id*=menu]", "[class*=footer]", "[id*=footer]",
            "[class*=header]", "[id*=header]", "[class*=contact]", "[id*=contact]",
            "[class*=social]", "[id*=social]", "[class*=ads]", "[id*=ads]"
        ]:
            for t in soup.select(selector):
                t.decompose()
        sections = []
        for elem in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "article", "section", "div"]):
            txt = elem.get_text(" ", strip=True)
            if self._is_valid_text_block(txt):
                sections.append(txt)
        merged, buf = [], ""
        for s in sections:
            if len(buf.split()) < 60:
                buf += " " + s
            else:
                merged.append(buf.strip())
                buf = s
        if buf:
            merged.append(buf.strip())
        return "\n\n".join(merged)

    def _download_pdf_generic(self, pdf_url: str):
        try:
            r = self.session.get(pdf_url, timeout=30)
            if r.status_code != 200 or b"%PDF" not in r.content[:500]:
                return
            file_hash = self._compute_hash(r.content)
            if any(file_hash == v.get("hash") for v in self.global_manifest.get("sites", {}).values()):
                log.info("⏩ Skipped duplicate PDF", url=pdf_url)
                return
            name = Path(urlparse(pdf_url).path).name or f"{file_hash[:10]}.pdf"
            dest = self.session_folder / "pdfs" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(r.content)
            log.info("📥 PDF downloaded", file=name)
        except Exception as e:
            log.error("❌ PDF download failed", url=pdf_url, error=str(e))

    def crawl_site(self, start_url: str):
        visited, queue, page_count = set(), [(start_url, 0)], 0
        domain = urlparse(start_url).netloc
        log.info("🌐 Starting generic crawl", domain=domain)
        while queue:
            url, depth = queue.pop(0)
            if url in visited or depth > self.max_depth:
                continue
            visited.add(url)
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
                    continue
                html = r.text
                text = self._extract_text(html)
                if text:
                    page_count += 1
                    fname = f"page_{page_count:04d}.txt"
                    fpath = self.session_folder / "texts" / fname
                    fpath.parent.mkdir(parents=True, exist_ok=True)
                    fpath.write_text(text, encoding="utf-8")
                    log.info("📝 Saved page", file=fname)
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.lower().endswith(".pdf"):
                        self._download_pdf_generic(urljoin(url, href))
                for a in soup.find_all("a", href=True):
                    nxt = urljoin(url, a["href"]).split("?")[0]
                    if urlparse(nxt).netloc == domain and nxt not in visited:
                        queue.append((nxt, depth + 1))
                if self.max_pages and page_count >= self.max_pages:
                    break
                time.sleep(self.crawl_delay)
            except Exception as e:
                log.error("❌ Crawl failed", url=url, error=str(e))
        log.info("✅ Generic crawl complete", total_pages=page_count)

    def crawl_multiple_sites(self, urls: list[str]):
        for u in urls:
            log.info("🚀 Crawling generic site", url=u)
            self.crawl_site(u)
        log.info("🏁 All generic crawls complete")

    # --------------------------------------------------------
    # NIH and DMPTool Crawlers (unchanged)
    # --------------------------------------------------------
    # ... your existing _crawl_nih and _crawl_dmptool here ...

    def run_all(self):
        for url in self.urls:
            domain = urlparse(url).netloc
            if "dmptool.org" in domain:
                self._crawl_dmptool(url, domain)
            elif "nih.gov" in domain:
                self._crawl_nih(url, domain)
            else:
                self.crawl_site(url)
        print("🏁 All crawls complete.")


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
        keep_last_n_sessions=1,
    )
    crawler.run_all()
