from __future__ import annotations
import os, json, re
from pathlib import Path
import fitz  # PyMuPDF

# --- Core project imports ---
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


class PDFTextProcessor:
    """
    Step 2–3 of the DMP-RAG pipeline.
    --------------------------------------------------
    ✅ Extract text from PDFs in data/all_pdfs/
    ✅ Clean and normalize extracted text
    ✅ Save final text to data/text_final_cleaned/
    ✅ Log progress and save manifest files
    """

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.pdf_folder = self.base_dir / "all_pdfs"
        self.raw_text_folder = self.base_dir / "text_cleaned"
        self.clean_text_folder = self.base_dir / "text_final_cleaned"

        self.raw_text_folder.mkdir(exist_ok=True)
        self.clean_text_folder.mkdir(exist_ok=True)

        self.manifest_raw = self.base_dir / "manifest_text.json"
        self.manifest_clean = self.base_dir / "manifest_cleaned.json"

        self.raw_manifest_data = []
        self.clean_manifest_data = []

    # ---------------------------------------------------------
    # 1️⃣ PDF → Raw text
    # ---------------------------------------------------------
    def extract_text(self, pdf_path: Path) -> str:
        text = ""
        try:
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    text += page.get_text("text") + "\n"
            log.info(f"Extracted: {pdf_path.name}")
            return text.strip()
        except Exception as e:
            msg = f"Error reading {pdf_path.name}: {e}"
            log.error(msg)
            raise DocumentPortalException(msg)

    # ---------------------------------------------------------
    # 2️⃣ Cleaning
    # ---------------------------------------------------------
    def clean_text(self, text: str) -> str:
        try:
            # Remove URLs
            text = re.sub(r'https?://\S+|www\.\S+', '', text)
            # Remove "Page x of y"
            text = re.sub(r'Page\s+\d+(\s+of\s+\d+)?', '', text, flags=re.IGNORECASE)
            # Fix hyphenation across lines
            text = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
            # Merge broken lines into paragraphs
            text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
            # Normalize spaces
            text = re.sub(r'[ \t]+', ' ', text)
            # Replace multiple blank lines with one
            text = re.sub(r'\n{2,}', '\n', text)
            # Normalize numbered headings to Markdown
            text = re.sub(r'(\n?\d\.\s+[A-Z][^\n]+)', r'\n## \1\n', text)
            return text.strip()
        except Exception as e:
            log.error(f"Cleaning failed: {e}")
            raise DocumentPortalException(f"Cleaning failed: {e}")

    # ---------------------------------------------------------
    # 3️⃣ Run full pipeline
    # ---------------------------------------------------------
    def run(self):
        if not self.pdf_folder.exists():
            raise DocumentPortalException(f"PDF folder not found: {self.pdf_folder}")

        log.info("🚀 Starting extraction + cleaning pipeline...")
        pdf_files = list(self.pdf_folder.glob("*.pdf"))

        if not pdf_files:
            log.warning("No PDF files found in all_pdfs/")
            return

        for pdf_path in pdf_files:
            try:
                # ---- Extract ----
                raw_text = self.extract_text(pdf_path)
                raw_path = self.raw_text_folder / (pdf_path.stem + ".txt")
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(raw_text)
                self.raw_manifest_data.append({"pdf": pdf_path.name, "text_file": raw_path.name})

                # ---- Clean ----
                clean_text = self.clean_text(raw_text)
                clean_path = self.clean_text_folder / (pdf_path.stem + ".txt")
                with open(clean_path, "w", encoding="utf-8") as f:
                    f.write(clean_text)
                self.clean_manifest_data.append({"raw": raw_path.name, "clean": clean_path.name})

                log.info(f"✅ Processed & cleaned: {pdf_path.name}")

            except DocumentPortalException:
                continue
            except Exception as e:
                log.error(f"Unexpected error for {pdf_path.name}: {e}")

        # ---- Save manifests ----
        with open(self.manifest_raw, "w", encoding="utf-8") as f:
            json.dump(self.raw_manifest_data, f, indent=2)
        with open(self.manifest_clean, "w", encoding="utf-8") as f:
            json.dump(self.clean_manifest_data, f, indent=2)

        log.info("✨ All PDFs extracted and cleaned successfully!")
        log.info(f"Cleaned text files saved in: {self.clean_text_folder}")


if __name__ == "__main__":
    processor = PDFTextProcessor(base_dir="C:/Users/Nahid/DMP-RAG/data")
    processor.run()
