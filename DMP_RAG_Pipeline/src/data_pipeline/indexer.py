from __future__ import annotations
from pathlib import Path
from typing import List, Any

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException

from langchain_community.vectorstores import FAISS


class Indexer:
    """
    Builds/loads a FAISS index.
    """

    def __init__(self, index_dir: Path, embeddings):
        self.index_dir = Path(index_dir)
        self.embeddings = embeddings
        self.index_dir.mkdir(parents=True, exist_ok=True)

    def build(self, chunks: List[Any]):
        try:
            vs = FAISS.from_documents(chunks, self.embeddings)
            vs.save_local(str(self.index_dir))
            log.info(f"🎯 FAISS index built and saved to {self.index_dir}")
            return vs
        except Exception as e:
            log.exception("Failed to build FAISS index.")
            raise DocumentPortalException(f"Index build failed: {e}")

    def load(self):
        try:
            vs = FAISS.load_local(
                str(self.index_dir),
                self.embeddings,
                allow_dangerous_deserialization=True,
            )
            log.info(f"🔄 FAISS index loaded from {self.index_dir}")
            return vs
        except Exception as e:
            log.exception("Failed to load FAISS index.")
            raise DocumentPortalException(f"Index load failed: {e}")
