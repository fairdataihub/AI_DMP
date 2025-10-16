from __future__ import annotations
from typing import List, Any

from logger.custom_logger import GLOBAL_LOGGER as log
from langchain.text_splitter import RecursiveCharacterTextSplitter


class TextChunker:
    """
    Splits Documents into manageable chunks for retrieval.
    """

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 150):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap
        )

    def run(self, docs: List[Any]) -> List[Any]:
        chunks = self.splitter.split_documents(docs)
        log.info(f"✅ Split into {len(chunks)} chunks.")
        return chunks
