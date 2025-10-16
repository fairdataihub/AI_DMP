from __future__ import annotations
import os, sys, time, json, hashlib
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# --- project imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


class WebIngestion:
    """
    🌐 DMP-RAG Web Ingestion (NIH-only Edition)
    ----------------------------------------------------------
    ✅ Crawls the NIH-only page on DMPTool
    ✅ Downloads all NIH-funded DMP PDFs
    ✅ Uses SHA-256 manifest deduplication
    ✅ Saves to data/web_sources/nih_only_downloads/
    """

    def __init__(
        self,
        data_root: str = "data",
        save_subfolder: str = "nih_only_downloads",
        wait_time: int = 5,
        scroll_pause: int = 2,
        max_pages: int = 100,
    ):
        self.data_root = Path(data_root)
        self.web_dir = self.data_root / "NIH_sources"

        # --- create NIH-only save folder ---
        self.save_dir = self.web_dir / save_subfolder
        self.save_dir.mkdir(parents=True, exist_ok=True)

        # --- session folder inside NIH folder ---
        self.session_id = f"session_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        self.session_dir = self.save_dir / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # --- manifest paths ---
        self.manifest_path = self.save_dir / "manifest.json"
        self.session_manifest_path = self.session_dir / "manifest_session.json"

        self.wait_time = wait_time
        self.scroll_pause = scroll_pause
        self.max_pages = max_pages
        self.manifest = self._load_manifest()

        log.info("WebIngestion initialized", session=self.session_id, save_dir=str(self.session_dir))

    # --------------------------------------------------------
    # Manifest handling
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

        session_data = {
            "session_id": self.session_id,
            "created_at": datetime.utcnow().isoformat(),
            "file_count": len(self.manifest),
            "files": list(self.manifest.keys()),
        }
        with open(self.session_manifest_path, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=2)

    # --------------------------------------------------------
    # Selenium setup
    # --------------------------------------------------------
    def _init_driver(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        prefs = {
            "download.default_directory": str(self.session_dir.resolve()),
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
        }
        options.add_experimental_option("prefs", prefs)
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    # --------------------------------------------------------
    # Utility
    # --------------------------------------------------------
    def _compute_hash(self, file_path: Path) -> str:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    # --------------------------------------------------------
    # Crawl NIH-only pages
    # --------------------------------------------------------
    def crawl_dmptool_full(self, start_url: str):
        driver = self._init_driver()
        page_num = 1
        nih_count = 0
        try:
            driver.get(start_url)
            time.sleep(self.wait_time)
            log.info("🧭 Starting NIH-only crawl", url=start_url)

            while page_num <= self.max_pages:
                log.info("📄 Crawling page", page=page_num, url=driver.current_url)

                # scroll for full load
                last_height = 0
                for _ in range(5):
                    driver.execute_script("window.scrollBy(0, 3000);")
                    time.sleep(self.scroll_pause)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                # download visible PDFs (all are NIH)
                count_on_page = self._download_visible_pdfs(driver, page_num)
                nih_count += count_on_page
                page_num += 1

                # next page
                try:
                    next_btn = driver.find_element(By.PARTIAL_LINK_TEXT, "Next")
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                    time.sleep(1)
                    next_btn.click()
                    time.sleep(self.wait_time)
                except NoSuchElementException:
                    log.info("✅ No further pages detected — stopping crawl.")
                    break
                except WebDriverException as e:
                    log.warning("⚠️ Could not click Next", error=str(e))
                    break

            log.info("✅ Crawl finished", total_nih_pdfs=nih_count)

        except Exception as e:
            log.error("Crawl failed", error=str(e))
        finally:
            driver.quit()
            self._save_manifest()

    # --------------------------------------------------------
    # Download all visible PDFs (already NIH only)
    # --------------------------------------------------------
    def _download_visible_pdfs(self, driver, page_num: int) -> int:
        pdf_buttons = driver.find_elements(By.XPATH, "//a[contains(@href, '/export.pdf')]")
        titles = driver.find_elements(By.CSS_SELECTOR, "tr td:first-child")
        log.info("🔎 Found PDF buttons", count=len(pdf_buttons), page=page_num)

        nih_downloads = 0

        for i, button in enumerate(pdf_buttons):
            try:
                title_text = titles[i].text.strip() if i < len(titles) else f"plan_{page_num}_{i}"
                safe_name = "".join(c for c in title_text if c.isalnum() or c in (" ", "_", "-")).strip()
                fname = f"{safe_name or 'plan'}_{page_num}_{i}.pdf"
                dest = self.session_dir / fname

                # Skip duplicates
                if dest.exists():
                    file_hash = self._compute_hash(dest)
                    for k, v in self.manifest.items():
                        if v.get("hash") == file_hash:
                            log.info("⏩ Skipped (unchanged)", file=fname)
                            raise StopIteration

                # Click & wait for file
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                ActionChains(driver).move_to_element(button).click(button).perform()
                time.sleep(4)

                downloaded = None
                for _ in range(15):
                    candidates = list(self.session_dir.glob("*.pdf"))
                    if candidates:
                        downloaded = max(candidates, key=os.path.getctime)
                        break
                    time.sleep(1)

                if not downloaded:
                    log.warning("⚠️ No PDF downloaded", title=title_text)
                    continue

                downloaded.rename(dest)
                file_hash = self._compute_hash(dest)
                self.manifest[str(dest)] = {
                    "file": str(dest),
                    "hash": file_hash,
                    "session": self.session_id,
                    "title": title_text,
                    "funder": "NIH",
                    "last_updated": datetime.utcnow().isoformat(),
                }
                log.info("✅ Downloaded (NIH)", file=dest.name)
                nih_downloads += 1
                self._save_manifest()

            except StopIteration:
                continue
            except Exception as e:
                log.error("❌ Download failed", page=page_num, index=i, error=str(e))

        return nih_downloads


# --------------------------------------------------------
# Example Run (NIH-only)
# --------------------------------------------------------
if __name__ == "__main__":
    crawler = WebIngestion(
        data_root="C:/Users/Nahid/DMP-RAG/data",
        save_subfolder="nih_only_downloads",
        wait_time=6,
        scroll_pause=3,
        max_pages=100,
    )
    # NIH-only DMPTool link (funder_id=123)
    crawler.crawl_dmptool_full(
        "https://dmptool.org/public_plans?search=&facet%5Bfunder_ids%5D%5B%5D=123&sort_by=featured"
    )
