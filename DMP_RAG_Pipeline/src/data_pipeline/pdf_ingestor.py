from __future__ import annotations
from pathlib import Path
from typing import List, Any

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException

from langchain_community.document_loaders import PyPDFLoader
from tqdm import tqdm


class PDFIngestor:
    """
    Loads PDFs from a folder into LangChain Document objects.
    """

    def __init__(self, pdf_dir: Path):
        self.pdf_dir = Path(pdf_dir)

    def run(self) -> List[Any]:
        if not self.pdf_dir.exists():
            raise DocumentPortalException(f"PDF folder not found: {self.pdf_dir}")

        pdf_files = sorted(self.pdf_dir.glob("*.pdf"))
        if not pdf_files:
            raise DocumentPortalException(f"No PDF files in: {self.pdf_dir}")

        docs = []
        for pdf in tqdm(pdf_files, desc="📥 Loading PDFs"):
            try:
                loader = PyPDFLoader(str(pdf))
                docs.extend(loader.load())
            except Exception as e:
                log.error(f"❌ Error loading {pdf.name}: {e}")
        log.info(f"✅ Loaded {len(docs)} documents from {self.pdf_dir}")
        return docs
