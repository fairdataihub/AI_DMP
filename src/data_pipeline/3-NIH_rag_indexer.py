from __future__ import annotations
import sys
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from utils.model_loader import ModelLoader
from exception.custom_exception import DocumentPortalException
from logger.custom_logger import GLOBAL_LOGGER as log


# =========================================================
# NIH DMP Configuration
# =========================================================
@dataclass
class DMPConfigNIH:
    """
    NIH-specific directory configuration for DMP-RAG indexing.
    -----------------------------------------------------------
    ✅ Points to NIH-specific chunk, index, and output folders
    ✅ Automatically creates directories if missing
    """

    base_dir: Path
    chunks_dir: Path = field(init=False)
    index_dir: Path = field(init=False)
    outputs_dir: Path = field(init=False)
    index_name: str = "NIH_dmp_index"
    top_k: int = 6

    def __post_init__(self):
        self.base_dir = Path(self.base_dir)
        self.chunks_dir = self.base_dir / "NIH_chunks"
        self.index_dir = self.base_dir / "NIH_faiss_index"
        self.outputs_dir = self.base_dir / "NIH_outputs"

        # Create folders if missing
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

        log.info("📁 NIH Config initialized", base=str(self.base_dir))


# =========================================================
# NIH DMP Indexer
# =========================================================
class DMPIndexNIH:
    """
    Handles FAISS index creation, loading, and retrieval for NIH DMPs.
    -----------------------------------------------------------
    ✅ Uses embeddings strictly from config.yaml (via ModelLoader)
    ✅ Supports auto-build and load modes
    ✅ Logs each operation clearly for audit and debugging
    """

    def __init__(self, config: DMPConfigNIH):
        self.cfg = config
        self.embeddings = None
        self.vstore: Optional[FAISS] = None

    # --------------------------------------------------------
    # Load embeddings
    # --------------------------------------------------------
    def _load_embeddings(self):
        """Load embeddings from ModelLoader (YAML-based)."""
        try:
            if not self.embeddings:
                loader = ModelLoader()
                self.embeddings = loader.load_embeddings()
                log.info("✅ NIH embeddings loaded for DMPIndexNIH")
        except Exception as e:
            raise DocumentPortalException(f"Embedding loading failed: {e}", sys)

    # --------------------------------------------------------
    # Check if FAISS index exists
    # --------------------------------------------------------
    def _index_exists(self) -> bool:
        faiss_file = self.cfg.index_dir / f"{self.cfg.index_name}.faiss"
        pkl_file = self.cfg.index_dir / f"{self.cfg.index_name}.pkl"
        exists = faiss_file.exists() and pkl_file.exists()
        if exists:
            log.info("🔍 NIH FAISS index found", path=str(self.cfg.index_dir))
        else:
            log.info("⚙️ No NIH FAISS index detected — will build new one")
        return exists

    # --------------------------------------------------------
    # Build FAISS index from NIH chunks
    # --------------------------------------------------------
    def build_from_chunks(self):
        """Build a FAISS index using NIH chunked JSON files."""
        try:
            self._load_embeddings()

            chunk_files = list(self.cfg.chunks_dir.glob("*_NIH_chunks.json"))
            if not chunk_files:
                raise FileNotFoundError(f"No NIH chunk files found in {self.cfg.chunks_dir}")

            docs: List[Document] = []
            for file in chunk_files:
                data = json.loads(file.read_text(encoding="utf-8"))
                src = file.stem.replace("_NIH_chunks", "")
                for i, chunk_text in enumerate(data):
                    docs.append(Document(page_content=chunk_text, metadata={"source": src, "id": i}))

            log.info(f"📄 Preparing {len(docs)} NIH chunk documents for FAISS index...")

            # Build FAISS index
            self.vstore = FAISS.from_documents(docs, self.embeddings)
            self.vstore.save_local(str(self.cfg.index_dir), index_name=self.cfg.index_name)
            log.info(f"✅ NIH FAISS index built and saved at: {self.cfg.index_dir}")
            return self.vstore

        except Exception as e:
            raise DocumentPortalException(f"NIH FAISS index build failed: {e}", sys)

    # --------------------------------------------------------
    # Load existing FAISS index
    # --------------------------------------------------------
    def load_local(self):
        """Load a previously built NIH FAISS index."""
        try:
            self._load_embeddings()
            self.vstore = FAISS.load_local(
                str(self.cfg.index_dir),
                self.embeddings,
                index_name=self.cfg.index_name,
                allow_dangerous_deserialization=True,
            )
            log.info("✅ NIH FAISS index loaded successfully")
            return self.vstore
        except Exception as e:
            raise DocumentPortalException(f"Failed to load NIH FAISS index: {e}", sys)

    # --------------------------------------------------------
    # Auto-detect or build index
    # --------------------------------------------------------
    def load_or_build(self):
        """Automatically load existing NIH index or build a new one if missing."""
        try:
            if self._index_exists():
                return self.load_local()
            else:
                return self.build_from_chunks()
        except Exception as e:
            raise DocumentPortalException(f"NIH index auto-load/build failed: {e}", sys)

    # --------------------------------------------------------
    # Return retriever
    # --------------------------------------------------------
    def get_retriever(self, k: Optional[int] = None):
        """Return a retriever interface for FAISS-based similarity search."""
        if self.vstore is None:
            raise DocumentPortalException("NIH vector store not initialized.", sys)
        log.info("🔎 NIH retriever initialized", top_k=k or self.cfg.top_k)
        return self.vstore.as_retriever(search_kwargs={"k": k or self.cfg.top_k})


# =========================================================
# Example Run
# =========================================================
if __name__ == "__main__":
    try:
        base_dir = Path("C:/Users/Nahid/DMP-RAG/data")
        cfg = DMPConfigNIH(base_dir)
        indexer = DMPIndexNIH(cfg)
        indexer.load_or_build()  # Auto-detects or builds index
        log.info("🏁 NIH DMP Indexing completed successfully.")
    except Exception as e:
        log.error("❌ NIH DMP Indexing pipeline failed", error=str(e))
