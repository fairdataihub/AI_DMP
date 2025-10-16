# src/utils/model_loader.py
import sys
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.llms import Ollama
from exception.custom_exception import DocumentPortalException
from logger.custom_logger import GLOBAL_LOGGER as log
import yaml
from pathlib import Path

class ModelLoader:
    """Unified model loader for embeddings and LLMs."""

    def __init__(self, config_path: str = "config/config.yaml"):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self.cfg = yaml.safe_load(f)

            models = self.cfg.get("models", {})
            self.llm_name = models.get("llm_name", "llama3")
            self.embedding_model = models.get(
                "embedding_model", "sentence-transformers/all-MiniLM-L6-v2"
            )

            log.info("ModelLoader initialized",
                     llm=self.llm_name,
                     embed=self.embedding_model)
        except Exception as e:
            log.error("Failed to load model config", error=str(e))
            raise DocumentPortalException("ModelLoader initialization error", e)

    def load_llm(self):
        try:
            llm = Ollama(model=self.llm_name)
            log.info("LLM loaded successfully", model=self.llm_name)
            return llm
        except Exception as e:
            log.error("Failed to load LLM", error=str(e))
            raise DocumentPortalException("LLM loading error", e)

    def load_embeddings(self):
        try:
            emb = HuggingFaceEmbeddings(model_name=self.embedding_model)
            log.info("Embeddings loaded successfully", model=self.embedding_model)
            return emb
        except Exception as e:
            log.error("Failed to load embeddings", error=str(e))
            raise DocumentPortalException("Embedding loading error", e)
