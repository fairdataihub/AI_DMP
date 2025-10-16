# ===============================================================
# core_pipeline.py — Experimental RAG Core
# ===============================================================

import sys
import re
import pandas as pd
import pypandoc
from pathlib import Path
from tqdm import tqdm
import yaml

from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser

# ---- Internal project imports ----
from utils.model_loader import ModelLoader
from exception.custom_exception import DocumentPortalException
from logger.custom_logger import GLOBAL_LOGGER as log
from prompt.prompt_library import PROMPT_REGISTRY, PromptType
from config.config_schema import ExperimentConfig


# ===============================================================
# CONFIGURATION MANAGER (with Pydantic)
# ===============================================================
class ConfigManager:
    """Loads and validates experiment configuration via Pydantic schema."""

    def __init__(self, config_path: str = "config/config.yaml"):
        try:
            path = Path(config_path)
            if not path.exists():
                raise FileNotFoundError(f"YAML config not found: {path}")

            with open(path, "r", encoding="utf-8") as f:
                cfg_dict = yaml.safe_load(f)

            # --- Validate using Pydantic schema ---
            self.cfg = ExperimentConfig(**cfg_dict)

            log.info(
                "✅ YAML configuration validated successfully",
                config_path=str(path),
                experiment=self.cfg.experiment_name,
            )

        except Exception as e:
            log.error("❌ Configuration validation failed", error=str(e))
            raise DocumentPortalException("Configuration validation error", e)

    @property
    def paths(self):
        return self.cfg.paths

    @property
    def rag(self):
        return self.cfg.rag


# ===============================================================
# PDF INGESTION
# ===============================================================
class PDFProcessor:
    """Loads PDFs and splits them into text chunks."""

    def __init__(self, data_pdfs: Path):
        self.data_pdfs = data_pdfs

    def load_pdfs(self):
        try:
            pdf_files = sorted(self.data_pdfs.glob("*.pdf"))
            if not pdf_files:
                raise FileNotFoundError(f"No PDFs found in {self.data_pdfs}")
            docs = []
            for pdf_path in tqdm(pdf_files, desc="📥 Loading PDFs"):
                loader = PyPDFLoader(str(pdf_path))
                docs.extend(loader.load())
            log.info("PDFs loaded successfully", count=len(docs))
            return docs
        except Exception as e:
            raise DocumentPortalException("PDF loading error", e)

    def split_chunks(self, docs, chunk_size=800, chunk_overlap=120):
        try:
            splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size, chunk_overlap=chunk_overlap
            )
            chunks = splitter.split_documents(docs)
            log.info("Chunks created", count=len(chunks))
            return chunks
        except Exception as e:
            raise DocumentPortalException("Chunk splitting error", e)


# ===============================================================
# VECTOR INDEXING
# ===============================================================
class FAISSIndexer:
    """Creates or loads FAISS vector index using HuggingFace embeddings."""

    def __init__(self, index_dir: Path):
        self.index_dir = index_dir
        try:
            self.embeddings = ModelLoader().load_embeddings()
        except Exception as e:
            raise DocumentPortalException("Embedding model load failed", e)

    def build_or_load(self, chunks):
        try:
            faiss_path = self.index_dir / "index.faiss"
            if faiss_path.exists():
                log.info("📦 Loading existing FAISS index", path=str(faiss_path))
                return FAISS.load_local(
                    str(self.index_dir),
                    self.embeddings,
                    allow_dangerous_deserialization=True,
                )
            log.info("🔨 Building new FAISS index", index_dir=str(self.index_dir))
            vectorstore = FAISS.from_documents(chunks, self.embeddings)
            vectorstore.save_local(str(self.index_dir))
            log.info("✅ FAISS index saved successfully", index_dir=str(self.index_dir))
            return vectorstore
        except Exception as e:
            raise DocumentPortalException("FAISS indexing error", e)


# ===============================================================
# RAG BUILDER
# ===============================================================
class RAGBuilder:
    """Builds LCEL-based RAG chain (Retriever + Prompt + LLM)."""

    def __init__(self):
        try:
            self.llm = ModelLoader().load_llm()
            self.qa_prompt = PROMPT_REGISTRY[PromptType.CONTEXT_QA.value]
            log.info("RAGBuilder initialized successfully", model=str(self.llm))
        except Exception as e:
            raise DocumentPortalException("RAGBuilder initialization error", e)

    def build(self, retriever):
        try:
            if retriever is None:
                raise ValueError("Retriever object is None")

            chain = (
                {"context": retriever, "input": lambda x: x}
                | self.qa_prompt
                | self.llm
                | StrOutputParser()
            )
            log.info("RAG chain built successfully")
            return chain
        except Exception as e:
            raise DocumentPortalException("RAG chain build error", e)


# ===============================================================
# DMP GENERATOR
# ===============================================================
class DMPGenerator:
    """Generates DMP Markdown + DOCX outputs from Excel project titles."""

    def __init__(self, excel_path: Path, output_md: Path, output_docx: Path):
        self.excel_path = excel_path
        self.output_md = output_md
        self.output_docx = output_docx

    def run_generation(self, rag_chain):
        try:
            df = pd.read_excel(self.excel_path)
            if "title" not in df.columns:
                raise ValueError("Excel file must contain a 'title' column")

            for _, row in df.iterrows():
                title = re.sub(r'[\\/*?:"<>|]', "_", row["title"])
                md_path = self.output_md / f"{title}.md"
                docx_path = self.output_docx / f"{title}.docx"

                log.info("🧠 Generating DMP", title=title)
                result = rag_chain.invoke({"input": row["title"]})
                if not result:
                    log.warning("⚠️ No response generated", title=title)
                    continue

                md_path.write_text(result, encoding="utf-8")
                pypandoc.convert_text(result, "docx", format="md", outputfile=docx_path)
                log.info("✅ DMP saved", markdown=str(md_path), docx=str(docx_path))

            log.info("🎯 All DMPs generated successfully")
        except Exception as e:
            raise DocumentPortalException("DMP generation error", e)
