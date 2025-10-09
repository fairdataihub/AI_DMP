import os
import sys
from dotenv import load_dotenv
from utils.config_loader import load_config
from langchain_community.chat_models import ChatOllama
from langchain_community.embeddings import OllamaEmbeddings
from logger.custom_logger import CustomLogger
from exception.custom_exception import DocumentPortalException


# Initialize logger
# ✅ Create the actual structlog logger
log = CustomLogger().get_logger(__file__)

class ModelLoader:
    """
    Loads only LLaMA-based LLMs and Ollama embeddings (local or remote).
    Supports multiple versions: LLaMA 3.1, 3.3, 4, etc.
    """

    def __init__(self):
        if os.getenv("ENV", "local").lower() != "production":
            load_dotenv()
            log.info("Running in LOCAL mode: .env loaded")
        else:
            log.info("Running in PRODUCTION mode")

        self.config = load_config()
        log.info("YAML config loaded", config_keys=list(self.config.keys()))

    def load_embeddings(self):
        """
        Load and return embedding model via Ollama.
        """
        try:
            embed_block = self.config["embedding_model"]
            provider = embed_block.get("provider", "ollama")
            model_name = embed_block.get("model_name", "nomic-embed-text")

            if provider != "ollama":
                raise ValueError(f"Unsupported embedding provider: {provider}")

            log.info("Loading embedding model", model=model_name)
            return OllamaEmbeddings(model=model_name)

        except Exception as e:
            log.error("Error loading embedding model", error=str(e))
            raise DocumentPortalException("Failed to load embedding model", sys)

    def load_llm(self):
        """
        Load and return a LLaMA model (local via Ollama).
        """
        llm_block = self.config["llm"]
        llama_variant = os.getenv("LLAMA_VARIANT", "llama3.3")  # Default variant

        if llama_variant not in llm_block:
            log.error("Requested LLaMA variant not found", variant=llama_variant)
            raise ValueError(f"LLaMA variant '{llama_variant}' not found in config")

        llm_config = llm_block[llama_variant]
        provider = llm_config.get("provider", "ollama")
        model_name = llm_config.get("model_name")
        base_url = llm_config.get("api_base_url", "http://localhost:11434")
        temperature = llm_config.get("temperature", 0.1)
        max_tokens = llm_config.get("max_output_tokens", 2048)

        log.info("Loading LLaMA model", variant=llama_variant, model=model_name)

        if provider == "ollama":
            return ChatOllama(
                model=model_name,
                base_url=base_url,
                temperature=temperature,
                num_predict=max_tokens
            )

        else:
            log.error("Unsupported provider", provider=provider)
            raise ValueError(f"Unsupported provider: {provider}")


if __name__ == "__main__":
    loader = ModelLoader()

    # Test Embedding
    embeddings = loader.load_embeddings()
    print(f"Embedding Model Loaded: {embeddings}")
    result = embeddings.embed_query("Hello, how are you?")
    print(f"Embedding Result: {result}")

    # Test LLM
    llm = loader.load_llm()
    print(f"LLM Loaded: {llm}")
    result = llm.invoke("Hello, how are you?")
    print(f"LLM Result: {result.content}")
