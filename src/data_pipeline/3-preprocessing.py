from __future__ import annotations
import os, sys, re, json, shutil
from pathlib import Path
from typing import List
import fitz  # PyMuPDF

# --- Project imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


# =========================================================
# 1️⃣ Collect PDFs from all subfolders
# =========================================================
class PDFCollector:
    """
    Step 1: Collect all PDFs into data/all_pdfs/
    --------------------------------------------------
    ✅ Recursively finds all PDFs
    ✅ Copies them to a single central folder
    ✅ Avoids overwriting by appending counters
    """

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.ultimate_folder = self.base_dir / "all_pdfs"
        self.ultimate_folder.mkdir(exist_ok=True)

    def run(self):
        log.info("🚀 Starting PDF collection...")
        for pdf_path in self.base_dir.rglob("*.pdf"):
            if pdf_path.parent == self.ultimate_folder:
                continue
            try:
                dest_path = self.ultimate_folder / pdf_path.name
                if dest_path.exists():
                    counter = 1
                    while True:
                        new_name = f"{dest_path.stem}_{counter}{dest_path.suffix}"
                        new_dest = self.ultimate_folder / new_name
                        if not new_dest.exists():
                            dest_path = new_dest
                            break
                        counter += 1
                shutil.copy2(pdf_path, dest_path)
                log.info(f"Copied: {pdf_path} → {dest_path}")
            except Exception as e:
                log.error(f"❌ Error copying {pdf_path.name}: {e}")
        log.info(f"✅ All PDFs collected into: {self.ultimate_folder}")


# =========================================================
# 2️⃣ Extract + Clean text from PDFs
# =========================================================
class PDFTextProcessor:
    """
    Step 2–3: Extract & clean text from PDFs
    --------------------------------------------------
    ✅ Extracts text from PDFs
    ✅ Cleans and normalizes text
    ✅ Saves raw & cleaned text with manifests
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

    def clean_text(self, text: str) -> str:
        try:
            text = re.sub(r'https?://\S+|www\.\S+', '', text)
            text = re.sub(r'Page\s+\d+(\s+of\s+\d+)?', '', text, flags=re.IGNORECASE)
            text = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
            text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
            text = re.sub(r'[ \t]+', ' ', text)
            text = re.sub(r'\n{2,}', '\n', text)
            text = re.sub(r'(\n?\d\.\s+[A-Z][^\n]+)', r'\n## \1\n', text)
            return text.strip()
        except Exception as e:
            log.error(f"Cleaning failed: {e}")
            raise DocumentPortalException(f"Cleaning failed: {e}")

    def run(self):
        if not self.pdf_folder.exists():
            raise DocumentPortalException(f"PDF folder not found: {self.pdf_folder}")

        pdf_files = list(self.pdf_folder.glob("*.pdf"))
        if not pdf_files:
            log.warning("No PDF files found in all_pdfs/")
            return

        for pdf_path in pdf_files:
            try:
                raw_text = self.extract_text(pdf_path)
                raw_path = self.raw_text_folder / f"{pdf_path.stem}.txt"
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(raw_text)
                self.raw_manifest_data.append({"pdf": pdf_path.name, "text_file": raw_path.name})

                clean_text = self.clean_text(raw_text)
                clean_path = self.clean_text_folder / f"{pdf_path.stem}.txt"
                with open(clean_path, "w", encoding="utf-8") as f:
                    f.write(clean_text)
                self.clean_manifest_data.append({"raw": raw_path.name, "clean": clean_path.name})

                log.info(f"✅ Processed & cleaned: {pdf_path.name}")
            except Exception as e:
                log.error(f"Unexpected error for {pdf_path.name}: {e}")

        with open(self.manifest_raw, "w", encoding="utf-8") as f:
            json.dump(self.raw_manifest_data, f, indent=2)
        with open(self.manifest_clean, "w", encoding="utf-8") as f:
            json.dump(self.clean_manifest_data, f, indent=2)
        log.info("✨ All PDFs extracted and cleaned successfully!")


# =========================================================
# 3️⃣ Chunk cleaned text
# =========================================================
class TextChunker:
    """
    Step 4: Chunk cleaned text files
    --------------------------------------------------
    ✅ Loads cleaned text files
    ✅ Performs semantic + size-based chunking
    ✅ Saves JSON chunks for embedding
    """

    def __init__(self, base_dir: str | Path, chunk_size: int = 800, overlap: int = 100):
        self.base_dir = Path(base_dir)
        self.input_dir = self.base_dir / "text_final_cleaned"
        self.output_dir = self.base_dir / "chunks"
        self.output_dir.mkdir(exist_ok=True)
        self.manifest_path = self.base_dir / "manifest_chunks.json"
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.manifest = []

    def split_into_paragraphs(self, text: str) -> List[str]:
        sections = re.split(r"\n##\s*", text)
        return [s.strip() for s in sections if s.strip()]

    def chunk_text(self, text: str) -> List[str]:
        words = text.split()
        chunks = []
        start = 0
        while start < len(words):
            end = min(start + self.chunk_size, len(words))
            chunks.append(" ".join(words[start:end]))
            start += self.chunk_size - self.overlap
        return chunks

    def process_file(self, txt_path: Path):
        try:
            text = txt_path.read_text(encoding="utf-8")
            sections = self.split_into_paragraphs(text)
            all_chunks = []
            for sec in sections:
                all_chunks.extend(self.chunk_text(sec))
            out_path = self.output_dir / f"{txt_path.stem}_chunks.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(all_chunks, f, indent=2, ensure_ascii=False)
            self.manifest.append({
                "source": txt_path.name,
                "chunks_file": out_path.name,
                "num_chunks": len(all_chunks)
            })
            log.info(f"✅ Chunked: {txt_path.name} → {len(all_chunks)} chunks")
        except Exception as e:
            msg = f"Chunking failed for {txt_path.name}: {e}"
            log.error(msg)
            raise DocumentPortalException(msg)

    def run(self):
        log.info("🚀 Starting text chunking pipeline...")
        txt_files = list(self.input_dir.glob("*.txt"))
        if not txt_files:
            log.warning("No cleaned text files found in text_final_cleaned/")
            return
        for txt_path in txt_files:
            self.process_file(txt_path)
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2)
        log.info("✨ All cleaned text files chunked successfully!")


# =========================================================
# 4️⃣ Unified Runner
# =========================================================
class DMPPipeline:
    """
    Unified DMP-RAG pipeline runner.
    --------------------------------------------------
    ✅ Step 1: Collect PDFs
    ✅ Step 2–3: Extract + clean text
    ✅ Step 4: Chunk cleaned text
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.collector = PDFCollector(base_dir)
        self.processor = PDFTextProcessor(base_dir)
        self.chunker = TextChunker(base_dir)

    def run_all(self):
        log.info("🚀 Starting full DMP-RAG pipeline...")
        self.collector.run()
        self.processor.run()
        self.chunker.run()
        log.info("🏁 DMP-RAG pipeline completed successfully!")


if __name__ == "__main__":
    base_dir = "C:/Users/Nahid/DMP-RAG/data"
    pipeline = DMPPipeline(base_dir)
    pipeline.run_all()
