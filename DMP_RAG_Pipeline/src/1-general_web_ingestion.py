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
    🌐 NIH Grants Web Ingestion (RAG-Optimized)
    -------------------------------------------
    ✅ Crawls entire https://grants.nih.gov/ domain
    ✅ Keeps policy, funding, and data-management content
    ✅ Skips news, contact, social, and media sections
    ✅ Downloads and deduplicates PDFs
    ✅ Saves manifest checkpoints for long sessions
    """

    def __init__(
        self,
        data_root: str = "data",
        max_depth: int = 5,
        crawl_delay: float = 1.2,
        max_pages: int = 8000,
        session_id: str | None = None,
    ):
        self.data_root = Path(data_root)
        self.session_id = session_id or datetime.now().strftime("session_%Y%m%d_%H%M%S")
        self.base_dir = self.data_root / "general_web_ingestion" / self.session_id
        self.txt_dir = self.base_dir / "texts"
        self.pdf_dir = self.base_dir / "pdfs"
        self.manifest_path = self.base_dir / "manifest.json"

        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (RAG-Ingestor/NIH-Grants)"})

        self.txt_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = {}

        log.info("🆕 NIH Grants RAG ingestion started", session=self.session_id, folder=str(self.base_dir))

    # --------------------------------------------------------
    # Utilities
    # --------------------------------------------------------
    def _compute_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def _save_manifest(self):
        data = {
            "session_id": self.session_id,
            "created_at": datetime.utcnow().isoformat(),
            "file_count": len(self.manifest),
            "files": list(self.manifest.values()),
        }
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        log.info("💾 Manifest saved", file=self.manifest_path)

    # --------------------------------------------------------
    # Text filtering
    # --------------------------------------------------------
    def _is_valid_text_block(self, text: str) -> bool:
        text = text.strip()
        lower = text.lower()

        skip_terms = [
            "cookie", "privacy", "terms", "subscribe", "newsletter", "login",
            "sign in", "menu", "share", "back to top", "copyright", "contact",
            "about us", "mission", "vision", "barcode", "advertisement",
            "banner", "home", "follow us", "facebook", "twitter", "linkedin",
            "instagram", "©", "disclaimer", "accessibility"
        ]
        if not text or len(text.split()) < 5:
            return False
        if any(term in lower for term in skip_terms):
            return False
        if sum(c.isdigit() for c in text) > len(text) * 0.5:
            return False

        # ✅ Expanded NIH-relevant whitelist
        relevant_terms = [
            # --- Core NIH & Grants ---
            "nih", "grant", "grants", "funding", "application", "award", "proposal",
            "applicant", "recipient", "investigator", "principal investigator", "pi",
            "co-investigator", "subaward", "budget", "submission", "deadline", "review",
            "peer review", "program", "program officer", "notice", "notice of award",
            "foa", "rfa", "pa", "opportunity", "solicitation", "eligibility", "renewal",
            "resubmission", "amendment", "supplement", "modification",
            # --- Data Management & Sharing ---
            "data management", "data sharing", "data plan", "data repository",
            "data access", "metadata", "data standards", "dataset", "data policy",
            "data reuse", "data retention", "privacy", "de-identification", "human subjects",
            "clinical data", "data stewardship", "research data", "open data", "findability",
            "interoperability", "reusability", "fair data", "data availability",
            "data governance", "sharing policy", "controlled access", "data protection",
            "confidentiality", "data oversight",
            # --- Policy & Compliance ---
            "policy", "policies", "guideline", "guidance", "requirement", "regulation",
            "compliance", "oversight", "reporting", "documentation", "monitoring",
            "review process", "review criteria", "terms and conditions", "stewardship",
            "federal policy", "federal regulation", "omb", "uniform guidance",
            "grants policy statement", "gps", "office of management and budget",
            "hhs policy", "public access policy", "reproducibility", "accountability",
            "ethics", "integrity", "research integrity",
            # --- Research & Programmatic ---
            "biomedical", "research", "clinical trial", "basic research", "translational",
            "health research", "scientific", "project", "program announcement",
            "extramural", "intramural", "federal funding", "research development",
            "training grant", "fellowship", "career development", "institute", "center",
            "ninds", "nci", "nhlbi", "nida", "niaid", "nigms", "nimh", "nia", "neuroscience",
            "cancer", "health equity", "implementation research",
            # --- Forms & Submissions ---
            "forms", "form instructions", "sf424", "biosketch", "budget justification",
            "other support", "cover letter", "attachments", "submission portal",
            "era commons", "commons id", "assistance listing", "submission package",
            "grants.gov", "workspace", "application guide", "submission requirements",
            # --- DMSP Specific ---
            "dmsp", "data management and sharing plan", "example plan", "sample plan",
            "plan element", "data type", "data format", "metadata standard",
            "data repository", "access control", "data sharing timeline",
            "public availability", "restricted access", "sensitive data", "secondary use",
            # --- Post-Award & Reporting ---
            "progress report", "rppr", "financial report", "final report",
            "closeout", "reporting requirements", "post-award", "subrecipient monitoring",
            "audit", "payment management", "drawdown", "terms of award",
            # --- Policy Notices & Docs ---
            "notice number", "notice of funding opportunity", "policy notice",
            "nih guide notice", "guide for grants and contracts", "not-od",
            "notices", "notice type",
            # --- Administrative & Oversight ---
            "sponsor", "institution", "university", "research organization",
            "consortium", "collaboration", "responsible conduct", "training requirement",
            "human subjects protection", "animal welfare", "irb", "iacuc", "fcoi",
            "financial conflict of interest", "reportable events", "foreign component",
            "foreign institution", "data use agreement", "dug", "conflict of interest"
        ]
        if not any(term in lower for term in relevant_terms):
            return False

        return True

    # --------------------------------------------------------
    # HTML Extraction
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

    # --------------------------------------------------------
    # PDF Download
    # --------------------------------------------------------
    def _download_pdf(self, pdf_url: str):
        try:
            r = self.session.get(pdf_url, timeout=30)
            if r.status_code != 200 or b"%PDF" not in r.content[:500]:
                return
            file_hash = self._compute_hash(r.content)
            if any(file_hash == v.get("hash") for v in self.manifest.values()):
                log.info("⏩ Skipped duplicate PDF", url=pdf_url)
                return

            name = Path(urlparse(pdf_url).path).name or f"{file_hash[:10]}.pdf"
            dest = self.pdf_dir / name
            with open(dest, "wb") as f:
                f.write(r.content)

            self.manifest[pdf_url] = {
                "file": str(dest),
                "hash": file_hash,
                "type": "pdf",
                "last_updated": datetime.utcnow().isoformat(),
            }
            log.info("📥 PDF downloaded", file=name)
        except Exception as e:
            log.error("❌ PDF download failed", url=pdf_url, error=str(e))

    # --------------------------------------------------------
    # Crawl Core
    # --------------------------------------------------------
    def crawl_site(self, start_url: str):
        visited, queue, page_count = set(), [(start_url, 0)], 0
        domain = "grants.nih.gov"
        log.info("🌐 Starting NIH Grants crawl", domain=domain)

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
                    fpath = self.txt_dir / fname
                    fpath.write_text(text, encoding="utf-8")

                    self.manifest[url] = {
                        "file": str(fpath),
                        "hash": self._compute_hash(text.encode()),
                        "type": "text",
                        "last_updated": datetime.utcnow().isoformat(),
                    }
                    log.info("📝 Saved page", file=fname)

                soup = BeautifulSoup(html, "html.parser")

                # PDFs
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.lower().endswith(".pdf"):
                        self._download_pdf(urljoin(url, href))

                # Internal links
                for a in soup.find_all("a", href=True):
                    nxt = urljoin(url, a["href"]).split("?")[0]
                    nxt_lower = nxt.lower()
                    if (
                        urlparse(nxt).netloc == domain
                        and nxt not in visited
                        and not nxt.endswith(".pdf")
                        and "#" not in nxt
                        and not any(x in nxt_lower for x in [
                            "contact", "faq", "news", "media", "press", "events",
                            "training", "webinar", "workshop", "calendar",
                            "subscribe", "video", "search", "login",
                            "filter=", "sort="
                        ])
                    ):
                        queue.append((nxt, depth + 1))

                if page_count % 200 == 0:
                    self._save_manifest()
                    log.info(f"📊 Progress: {page_count} pages crawled so far...")

                if self.max_pages and page_count >= self.max_pages:
                    log.info("⚠️ Reached max page limit — stopping crawl.")
                    break

                time.sleep(self.crawl_delay)

            except Exception as e:
                log.error("❌ Crawl failed", url=url, error=str(e))

        log.info("✅ NIH crawl completed", total_pages=page_count)
        self._save_manifest()

    # --------------------------------------------------------
    # Multi-site
    # --------------------------------------------------------
    def crawl_multiple_sites(self, urls: list[str]):
        for u in urls:
            log.info("🚀 Crawling site", url=u)
            self.crawl_site(u)
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
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("sources", [])
    except Exception as e:
        log.error("Failed to load link file", file=file_path, error=str(e))
        return []


# --------------------------------------------------------
# Example Run
# --------------------------------------------------------
if __name__ == "__main__":
    crawler = GenericWebIngestion(
        max_depth=5,
        crawl_delay=1.2,
        max_pages=8000,  # Large but safe limit
    )

    links = load_links("data/web_links.json")
    if not links:
        log.error("No links found to crawl.")
    else:
        crawler.crawl_multiple_sites(links)
