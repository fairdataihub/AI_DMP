from __future__ import annotations
import os, sys, time, json, hashlib, requests
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

# --- Optional project imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
try:
    from logger.custom_logger import GLOBAL_LOGGER as log
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("crawler")


# --------------------------------------------------------
# 🌐 NIH Grants Web Ingestion (Full Whitelist + Monthly Resume)
# --------------------------------------------------------
class GenericWebIngestion:
    def __init__(
        self,
        data_root: str = "data",
        last_session_id: str = "session_20251024_092159",  # ✅ your last crawl session
        max_depth: int = 5,
        crawl_delay: float = 1.2,
        max_pages: int | None = None,  # None = unlimited
    ):
        self.data_root = Path(data_root)
        now = datetime.now()

        # 🗂️ Organize by year/month
        self.base_dir = self.data_root / "general_web_ingestion" / f"{now:%Y}" / f"{now:%m}"
        self.txt_dir = self.base_dir / "texts"
        self.pdf_dir = self.base_dir / "pdfs"
        self.manifest_path = self.base_dir / "manifest.json"

        # 🔄 Link to last session for deduplication
        self.last_session_id = last_session_id
        self.last_session_path = self.data_root / "general_web_ingestion" / self.last_session_id
        self.last_manifest_path = self.last_session_path / "manifest.json"

        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (RAG-Ingestor/NIH-Grants)"})

        self.txt_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

        # Load manifest from previous session
        self.manifest = {}
        if self.last_manifest_path.exists():
            try:
                with open(self.last_manifest_path, "r", encoding="utf-8") as f:
                    old = json.load(f)
                self.manifest = {v.get("url", v.get("file")): v for v in old.get("files", [])}
                log.info(f"♻️ Loaded previous manifest ({self.last_session_id}) with {len(self.manifest)} entries.")
            except Exception as e:
                log.error(f"Failed to load previous manifest: {e}")
        else:
            log.warning("⚠️ No previous manifest found — starting fresh.")

        log.info(f"🆕 Starting new monthly crawl in {self.base_dir}")

    # --------------------------------------------------------
    def _compute_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def _save_manifest(self):
        data = {
            "created_at": datetime.utcnow().isoformat(),
            "file_count": len(self.manifest),
            "files": list(self.manifest.values()),
        }
        self.manifest_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        log.info(f"💾 Manifest saved ({len(self.manifest)} files)")

    # --------------------------------------------------------
    def _is_valid_text_block(self, text: str) -> bool:
        text = text.strip()
        lower = text.lower()
        if len(lower.split()) < 5:
            return False

        # 🚫 Skip irrelevant blocks
        skip_terms = [
            "cookie", "privacy", "terms", "subscribe", "newsletter", "login",
            "sign in", "menu", "share", "back to top", "copyright", "contact",
            "about us", "mission", "vision", "advertisement", "banner", "home",
            "follow us", "facebook", "twitter", "linkedin", "instagram", "©",
            "disclaimer", "accessibility"
        ]
        if any(term in lower for term in skip_terms):
            return False
        if sum(c.isdigit() for c in lower) > len(lower) * 0.5:
            return False

        # ✅ Full NIH whitelist — expanded for grants, policy, DMSP, compliance, etc.
        relevant_terms = [
            # --- NIH Core Grants ---
            "nih", "grant", "grants", "funding", "application", "award", "proposal",
            "program officer", "review process", "review criteria", "program announcement",
            "notice of funding opportunity", "nofo", "rfa", "pa", "foa", "eligibility",
            "principal investigator", "pi", "co-investigator", "recipient", "subaward",
            "notice of award", "budget", "submission", "deadline", "renewal", "amendment",
            "modification", "extension", "supplement", "terms and conditions", "payment management",
            "drawdown", "audit", "rppr", "progress report", "final report", "closeout",

            # --- Data Management & Sharing (DMSP) ---
            "data management", "data sharing", "data management and sharing plan", "dmsp",
            "metadata", "data standard", "data repository", "data plan", "data access",
            "data policy", "data retention", "data reuse", "data release", "privacy",
            "de-identification", "anonymization", "clinical data", "dataset", "data storage",
            "research data", "data governance", "fair data", "findability", "interoperability",
            "reusability", "data availability", "open data", "controlled access", "restricted data",
            "public access", "secondary use", "data sharing policy", "data oversight", "data control",

            # --- Policy & Compliance ---
            "policy", "policies", "guideline", "guidance", "regulation", "requirement",
            "compliance", "oversight", "monitoring", "stewardship", "omb", "uniform guidance",
            "office of management and budget", "grants policy statement", "gps", "federal regulation",
            "federal policy", "hhs policy", "reporting compliance", "research integrity",
            "accountability", "ethics", "integrity", "public access policy", "open science",
            "transparency", "reproducibility", "terms and conditions", "not-od", "nih guide notice",
            "notice type", "nih guide for grants and contracts", "federal register",

            # --- Research Programmatic & Institutes ---
            "biomedical research", "clinical trial", "health research", "scientific program",
            "extramural research", "intramural program", "translational research",
            "implementation research", "health equity", "center grant", "institute", "nci",
            "niaid", "nigms", "ninds", "nhlbi", "nia", "nimh", "nida", "neuroscience",
            "cancer", "aging", "precision medicine", "training grant", "fellowship",
            "career development", "k award", "t32", "f31", "r01", "r03", "r21", "r35",

            # --- Forms & Submissions ---
            "forms", "sf424", "biosketch", "budget justification", "other support", "cover letter",
            "era commons", "commons id", "grants.gov", "submission guide", "workspace", "attachments",
            "submission requirements", "subrecipient monitoring", "financial report",

            # --- Post-Award Reporting ---
            "progress report", "financial report", "final report", "closeout", "stewardship",
            "reporting requirement", "subrecipient monitoring", "drawdown", "audit", "payment management",

            # --- Administrative & Oversight ---
            "sponsor", "institution", "university", "organization", "collaboration", "consortium",
            "responsible conduct", "human subjects protection", "animal welfare", "irb",
            "iacuc", "fcoi", "financial conflict of interest", "foreign component",
            "foreign institution", "data use agreement", "dua", "dug", "conflict of interest",
            "foreign disclosure", "training requirement", "responsible conduct of research",

            # --- Misc Research Terms ---
            "extramural", "federal funding", "project period", "budget period",
            "supplemental funding", "program income", "cost sharing", "match requirement",
            "fellowship program", "mentored research", "postdoctoral", "graduate trainee",
            "scholar", "career transition", "infrastructure award", "core grant"
        ]

        return any(term in lower for term in relevant_terms)

    # --------------------------------------------------------
    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "form", "svg", "iframe"]):
            tag.decompose()
        text_blocks = []
        for e in soup.find_all(["h1", "h2", "h3", "p", "li", "section", "div"]):
            t = e.get_text(" ", strip=True)
            if self._is_valid_text_block(t):
                text_blocks.append(t)
        return "\n\n".join(text_blocks)

    # --------------------------------------------------------
    def crawl_site(self, start_url: str):
        visited, queue = set(), [(start_url, 0)]
        domain = "grants.nih.gov"
        page_count = len(list(self.txt_dir.glob("page_*.txt")))

        log.info(f"🌐 Resuming crawl from page {page_count + 1}")

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
                if not text:
                    continue

                file_hash = self._compute_hash(text.encode())
                if any(file_hash == v.get("hash") for v in self.manifest.values()):
                    continue  # skip duplicate

                page_count += 1
                fname = f"page_{page_count:04d}.txt"
                fpath = self.txt_dir / fname
                fpath.write_text(text, encoding="utf-8")

                self.manifest[url] = {
                    "url": url,
                    "file": str(fpath),
                    "hash": file_hash,
                    "type": "text",
                    "last_updated": datetime.utcnow().isoformat(),
                }
                log.info(f"📝 Saved page {fname}")

                # Internal links
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    nxt = urljoin(url, a["href"]).split("?")[0]
                    nxt_lower = nxt.lower()
                    if (
                        urlparse(nxt).netloc == domain
                        and nxt not in visited
                        and not nxt.endswith(".pdf")
                        and "#" not in nxt
                        and not any(x in nxt_lower for x in ["contact", "news", "faq", "search", "events"])
                    ):
                        queue.append((nxt, depth + 1))

                if page_count % 200 == 0:
                    self._save_manifest()
                    log.info(f"📊 Progress: {page_count} pages crawled...")

                if self.max_pages and page_count >= self.max_pages:
                    log.info("⚠️ Reached max page limit — stopping crawl.")
                    break

                time.sleep(self.crawl_delay)

            except Exception as e:
                log.error(f"❌ Crawl failed for {url}: {e}")

        log.info(f"✅ Crawl completed — {page_count} total pages")
        self._save_manifest()

    # --------------------------------------------------------
    def crawl_multiple_sites(self, urls: list[str]):
        for u in urls:
            log.info(f"🚀 Crawling site: {u}")
            self.crawl_site(u)
        log.info(f"🏁 All crawls complete ({len(self.manifest)} files)")


# --------------------------------------------------------
# 🧩 Merge manifests across all months and sessions
# --------------------------------------------------------
def merge_all_manifests(data_root: str = "data/general_web_ingestion"):
    root = Path(data_root)
    merged_manifest = root / "merged_manifest.json"
    all_files = {}

    for manifest_path in root.rglob("manifest.json"):
        try:
            data = json.load(open(manifest_path, "r", encoding="utf-8"))
            for f in data.get("files", []):
                all_files[f["hash"]] = f
        except Exception:
            continue

    merged_data = {
        "merged_at": datetime.utcnow().isoformat(),
        "file_count": len(all_files),
        "files": list(all_files.values()),
    }
    merged_manifest.write_text(json.dumps(merged_data, indent=2), encoding="utf-8")
    log.info(f"✅ Global merged manifest saved ({len(all_files)} unique files)")


# --------------------------------------------------------
def load_links(file_path: str = "data/web_links.json") -> list[str]:
    p = Path(file_path)
    if not p.exists():
        log.error(f"Link file not found: {file_path}")
        return []
    try:
        return json.load(open(p, "r", encoding="utf-8")).get("sources", [])
    except Exception as e:
        log.error(f"Failed to load link file: {e}")
        return []


# --------------------------------------------------------
if __name__ == "__main__":
    crawler = GenericWebIngestion(
        last_session_id="session_20251024_092159",  # ✅ your last crawl session
        max_depth=5,
        crawl_delay=1.2,
        max_pages=None,  # unlimited
    )

    links = load_links("data/web_links.json")
    if not links:
        log.error("No links found to crawl.")
    else:
        crawler.crawl_multiple_sites(links)
        merge_all_manifests()
