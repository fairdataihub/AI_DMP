from __future__ import annotations
import os, sys, time, json, hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

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
    🌐 DMP-RAG Web Ingestion (Session-Aware Edition)
    ----------------------------------------------------------
    ✅ Scrolls each page to ensure full table load
    ✅ Clicks “Next” to go through all public plan pages
    ✅ Downloads all visible DMP PDFs
    ✅ Skips duplicates using manifest.json
    ✅ Creates a session folder for each run
    ✅ Safe for large datasets (≈1700+ PDFs)
    """

    def __init__(
        self,
        data_root: str = "data",
        wait_time: int = 5,
        scroll_pause: int = 2,
        max_pages: int = 1000,
    ):
        self.data_root = Path(data_root)
        self.web_dir = self.data_root / "web_sources"

        # --- create session folder ---
        self.session_id = f"session_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        self.session_dir = self.web_dir / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # --- manifest paths ---
        self.manifest_path = self.web_dir / "manifest.json"
        self.session_manifest_path = self.session_dir / "manifest_session.json"

        self.wait_time = wait_time
        self.scroll_pause = scroll_pause
        self.max_pages = max_pages
        self.manifest = self._load_manifest()

        log.info("WebIngestion initialized", session=self.session_id, web_dir=str(self.session_dir))

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
        """Save both global and session manifests."""
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2)

        # also save session-specific manifest
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
    # Crawl all pages (scroll + next)
    # --------------------------------------------------------
    def crawl_dmptool_full(self, start_url: str):
        driver = self._init_driver()
        page_num = 1  # starting page
        try:
            driver.get(start_url)
            time.sleep(self.wait_time)
            log.info("🧭 Starting full crawl (scroll + pagination)", url=start_url)

            while page_num <= self.max_pages:
                log.info("📄 Crawling page", page=page_num, url=driver.current_url)

                # --- Scroll to ensure full content loaded ---
                last_height = 0
                for _ in range(5):
                    driver.execute_script("window.scrollBy(0, 3000);")
                    time.sleep(self.scroll_pause)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                # --- Extract & download PDFs on this page ---
                self._download_visible_pdfs(driver, page_num)
                page_num += 1

                # --- Try next page ---
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

            log.info("✅ Crawl finished", total_pdfs=len(self.manifest))

        except Exception as e:
            log.error("Crawl failed", error=str(e))
        finally:
            driver.quit()
            self._save_manifest()

    # --------------------------------------------------------
    # Extract visible PDFs
    # --------------------------------------------------------
    def _download_visible_pdfs(self, driver, page_num: int):
        pdf_buttons = driver.find_elements(By.XPATH, "//a[contains(@href, '/export.pdf')]")
        titles = driver.find_elements(By.CSS_SELECTOR, "tr td:first-child")
        log.info("🔎 Found PDF buttons", count=len(pdf_buttons), page=page_num)

        for i, button in enumerate(pdf_buttons):
            try:
                title_text = titles[i].text.strip() if i < len(titles) else f"plan_{page_num}_{i}"
                safe_name = "".join(c for c in title_text if c.isalnum() or c in (" ", "_", "-")).strip()
                fname = f"{safe_name or 'plan'}_{page_num}_{i}.pdf"
                dest = self.session_dir / fname  # save inside session folder

                # --- Skip duplicates ---
                if dest.exists():
                    file_hash = self._compute_hash(dest)
                    for k, v in self.manifest.items():
                        if v.get("hash") == file_hash:
                            log.info("⏩ Skipped (unchanged)", file=fname)
                            raise StopIteration

                # --- Click and wait for file ---
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

                # rename and move into session folder
                downloaded.rename(dest)
                file_hash = self._compute_hash(dest)
                self.manifest[str(dest)] = {
                    "file": str(dest),
                    "hash": file_hash,
                    "session": self.session_id,
                    "last_updated": datetime.utcnow().isoformat(),
                }
                log.info("✅ Downloaded", file=dest.name)
                self._save_manifest()

            except StopIteration:
                continue
            except Exception as e:
                log.error("❌ Download failed", page=page_num, index=i, error=str(e))


# --------------------------------------------------------
# Example Run
# --------------------------------------------------------
if __name__ == "__main__":
    crawler = WebIngestion(wait_time=6, scroll_pause=3, max_pages=500)
    crawler.crawl_dmptool_full("https://dmptool.org/public_plans")
