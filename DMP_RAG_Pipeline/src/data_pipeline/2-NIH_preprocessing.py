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
# 1️⃣ Collect NIH PDFs
# =========================================================
class PDFCollector:
    """
    Step 1: Collect NIH PDFs into data/NIH_all_pdfs/
    --------------------------------------------------
    ✅ Collects PDFs only from NIH_sources/NIH_only_downloads/
    ✅ Copies them into a single central folder
    ✅ Avoids overwriting by appending counters
    """

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.nih_source_root = self.base_dir / "NIH_sources" / "NIH_only_downloads"
        self.nih_pdfs_folder = self.base_dir / "NIH_all_pdfs"
        self.nih_pdfs_folder.mkdir(parents=True, exist_ok=True)

    def run(self):
        log.info("🚀 Starting NIH PDF collection...")

        if not self.nih_source_root.exists():
            raise DocumentPortalException(f"NIH crawler folder not found: {self.nih_source_root}")

        for pdf_path in self.nih_source_root.rglob("*.pdf"):
            try:
                dest_path = self.nih_pdfs_folder / pdf_path.name
                if dest_path.exists():
                    counter = 1
                    while True:
                        new_name = f"{dest_path.stem}_{counter}{dest_path.suffix}"
                        new_dest = self.nih_pdfs_folder / new_name
                        if not new_dest.exists():
                            dest_path = new_dest
                            break
                        counter += 1
                shutil.copy2(pdf_path, dest_path)
                log.info(f"📥 Copied NIH PDF: {pdf_path} → {dest_path}")
            except Exception as e:
                log.error(f"❌ Error copying {pdf_path.name}: {e}")

        log.info(f"✅ All NIH PDFs collected into: {self.nih_pdfs_folder}")


# =========================================================
# 2️⃣ Extract + Clean text from NIH PDFs
# =========================================================
class PDFTextProcessor:
    """
    Step 2–3: Extract & clean text from NIH PDFs
    --------------------------------------------------
    ✅ Extracts text from NIH PDFs
    ✅ Cleans and normalizes text
    ✅ Saves raw & cleaned text into NIH-specific folders
    """

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.pdf_folder = self.base_dir / "NIH_all_pdfs"
        self.raw_text_folder = self.base_dir / "NIH_text_raw"
        self.clean_text_folder = self.base_dir / "NIH_text_cleaned"
        self.raw_text_folder.mkdir(exist_ok=True)
        self.clean_text_folder.mkdir(exist_ok=True)

        self.manifest_raw = self.base_dir / "NIH_manifest_text_raw.json"
        self.manifest_clean = self.base_dir / "NIH_manifest_text_cleaned.json"
        self.raw_manifest_data = []
        self.clean_manifest_data = []

    def extract_text(self, pdf_path: Path) -> str:
        text = ""
        try:
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    text += page.get_text("text") + "\n"
            log.info(f"📄 Extracted text: {pdf_path.name}")
            return text.strip()
        except Exception as e:
            msg = f"Error reading {pdf_path.name}: {e}"
            log.error(msg)
            raise DocumentPortalException(msg)

    def clean_text(self, text: str) -> str:
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        text = re.sub(r'Page\s+\d+(\s+of\s+\d+)?', '', text, flags=re.IGNORECASE)
        text = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
        text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{2,}', '\n', text)
        text = re.sub(r'(\n?\d\.\s+[A-Z][^\n]+)', r'\n## \1\n', text)
        return text.strip()

    def run(self):
        pdf_files = list(self.pdf_folder.glob("*.pdf"))
        if not pdf_files:
            log.warning("No NIH PDF files found in NIH_all_pdfs/")
            return

        for pdf_path in pdf_files:
            try:
                raw_text = self.extract_text(pdf_path)
                raw_path = self.raw_text_folder / f"{pdf_path.stem}_NIH_raw.txt"
                raw_path.write_text(raw_text, encoding="utf-8")
                self.raw_manifest_data.append({"pdf": pdf_path.name, "text_file": raw_path.name})

                clean_text = self.clean_text(raw_text)
                clean_path = self.clean_text_folder / f"{pdf_path.stem}_NIH_clean.txt"
                clean_path.write_text(clean_text, encoding="utf-8")
                self.clean_manifest_data.append({"raw": raw_path.name, "clean": clean_path.name})

                log.info(f"✅ Cleaned NIH PDF: {pdf_path.name}")
            except Exception as e:
                log.error(f"Unexpected error for {pdf_path.name}: {e}")

        self.manifest_raw.write_text(json.dumps(self.raw_manifest_data, indent=2, ensure_ascii=False), encoding="utf-8")
        self.manifest_clean.write_text(json.dumps(self.clean_manifest_data, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("✨ All NIH PDFs extracted and cleaned successfully!")


# =========================================================
# 3️⃣ Chunk cleaned NIH text
# =========================================================
class TextChunker:
    """
    Step 4: Chunk NIH cleaned text files
    --------------------------------------------------
    ✅ Performs semantic + size-based chunking
    ✅ Saves into NIH_chunks/
    """

    def __init__(self, base_dir: str | Path, chunk_size: int = 800, overlap: int = 100):
        self.base_dir = Path(base_dir)
        self.input_dir = self.base_dir / "NIH_text_cleaned"
        self.output_dir = self.base_dir / "NIH_chunks"
        self.output_dir.mkdir(exist_ok=True)
        self.manifest_path = self.base_dir / "NIH_manifest_chunks.json"
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

            out_path = self.output_dir / f"{txt_path.stem}_NIH_chunks.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(all_chunks, f, indent=2, ensure_ascii=False)

            self.manifest.append({
                "source": txt_path.name,
                "chunks_file": out_path.name,
                "num_chunks": len(all_chunks)
            })
            log.info(f"✅ Chunked NIH text: {txt_path.name} → {len(all_chunks)} chunks")
        except Exception as e:
            raise DocumentPortalException(f"Chunking failed for {txt_path.name}: {e}")

    def run(self):
        txt_files = list(self.input_dir.glob("*.txt"))
        if not txt_files:
            log.warning("No cleaned NIH text files found in NIH_text_cleaned/")
            return
        for txt_path in txt_files:
            self.process_file(txt_path)
        self.manifest_path.write_text(json.dumps(self.manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("✨ All NIH cleaned text files chunked successfully!")


# =========================================================
# 4️⃣ Unified NIH Pipeline Runner
# =========================================================
class DMPPipelineNIH:
    """
    Unified DMP-RAG pipeline (NIH-only)
    --------------------------------------------------
    ✅ Step 1: Collect NIH PDFs
    ✅ Step 2–3: Extract + clean text
    ✅ Step 4: Chunk cleaned text
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.collector = PDFCollector(base_dir)
        self.processor = PDFTextProcessor(base_dir)
        self.chunker = TextChunker(base_dir)

    def run_all(self):
        log.info("🚀 Starting NIH DMP pipeline...")
        self.collector.run()
        self.processor.run()
        self.chunker.run()
        log.info("🏁 NIH DMP pipeline completed successfully!")


# =========================================================
# Example Run
# =========================================================
if __name__ == "__main__":
    base_dir = "C:/Users/Nahid/DMP-RAG/data"
    pipeline = DMPPipelineNIH(base_dir)
    pipeline.run_all()
