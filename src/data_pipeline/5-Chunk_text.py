from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import List, Dict

# --- Core project imports ---
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


class TextChunker:
    """
    Step 4 of the DMP-RAG pipeline.
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

    # ---------------------------------------------------------
    # 1️⃣ Sentence or paragraph splitting
    # ---------------------------------------------------------
    def split_into_paragraphs(self, text: str) -> List[str]:
        # split by markdown section headers first
        sections = re.split(r"\n##\s*", text)
        cleaned_sections = [s.strip() for s in sections if s.strip()]
        return cleaned_sections

    # ---------------------------------------------------------
    # 2️⃣ Chunking logic
    # ---------------------------------------------------------
    def chunk_text(self, text: str) -> List[str]:
        """Chunk text into overlapping pieces of ~chunk_size words."""
        words = text.split()
        chunks = []
        start = 0

        while start < len(words):
            end = min(start + self.chunk_size, len(words))
            chunk = " ".join(words[start:end])
            chunks.append(chunk)
            start += self.chunk_size - self.overlap  # overlap between chunks

        return chunks

    # ---------------------------------------------------------
    # 3️⃣ Process each file
    # ---------------------------------------------------------
    def process_file(self, txt_path: Path):
        try:
            with open(txt_path, "r", encoding="utf-8") as f:
                text = f.read()

            sections = self.split_into_paragraphs(text)
            all_chunks = []
            for sec in sections:
                sec_chunks = self.chunk_text(sec)
                all_chunks.extend(sec_chunks)

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

    # ---------------------------------------------------------
    # 4️⃣ Run full chunking process
    # ---------------------------------------------------------
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
        log.info(f"Chunks saved in: {self.output_dir}")


if __name__ == "__main__":
    chunker = TextChunker(base_dir="C:/Users/Nahid/DMP-RAG/data", chunk_size=800, overlap=100)
    chunker.run()
