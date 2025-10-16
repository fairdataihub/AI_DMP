from __future__ import annotations
from typing import Optional, Tuple

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException

# Embeddings / LLMs
from langchain.embeddings import HuggingFaceEmbeddings
from langchain_community.llms import Ollama
# Add other providers if you need (OpenAI, Groq, etc.)


class ModelLoader:
    """
    Central place to instantiate embeddings + LLM.
    You can swap providers by changing names in pipeline config.
    """

    def __init__(
        self,
        embed_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        llm_model: str = "llama3.3",
    ):
        self.embed_model = embed_model
        self.llm_model = llm_model
        log.info(f"ModelLoader configured | embeddings={embed_model} | llm={llm_model}")

    def load_embeddings(self):
        try:
            emb = HuggingFaceEmbeddings(model_name=self.embed_model)
            log.info("✅ Embeddings loaded.")
            return emb
        except Exception as e:
            log.exception("Failed to load embeddings.")
            raise DocumentPortalException(f"Embeddings init failed: {e}")

    def load_llm(self):
        try:
            llm = Ollama(model=self.llm_model)
            log.info("✅ LLM loaded.")
            return llm
        except Exception as e:
            log.exception("Failed to load LLM.")
            raise DocumentPortalException(f"LLM init failed: {e}")

    def load_all(self) -> Tuple[object, object]:
        return self.load_embeddings(), self.load_llm()
