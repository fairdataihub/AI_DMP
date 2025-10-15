import os
import sys
import yaml
from dotenv import load_dotenv
from pathlib import Path
from langchain_community.chat_models import ChatOllama
from langchain_community.embeddings import OllamaEmbeddings
from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


class ModelLoader:
    """
    Strict + robust version for DMP-RAG.
    ----------------------------------------------------------
    ✅ Automatically finds config/config.yaml from any depth
    ✅ Requires all parameters to come from YAML (no defaults)
    ✅ Works both locally and in production environments
    """

    def __init__(self):
        # --- Detect environment mode ---
        env_mode = os.getenv("ENV", "local").lower()
        if env_mode != "production":
            load_dotenv()
            log.info("🌱 Running in LOCAL mode: .env loaded")
        else:
            log.info("🏭 Running in PRODUCTION mode")

        # --- Locate config/config.yaml automatically ---
        try:
            current_path = Path(__file__).resolve()
            config_path = None
            for parent in current_path.parents:
                potential_path = parent / "config" / "config.yaml"
                if potential_path.exists():
                    config_path = potential_path
                    break

            if not config_path:
                raise FileNotFoundError("Could not locate config/config.yaml in parent directories.")

            with open(config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)

            log.info("✅ YAML config loaded", config_path=str(config_path))

        except Exception as e:
            log.error("❌ Failed to load config.yaml", error=str(e))
            raise DocumentPortalException(f"Failed to load config.yaml: {e}", sys)

    # ----------------------------------------------------------
    # Embedding Loader (strict)
    # ----------------------------------------------------------
    def load_embeddings(self):
        """Load embedding model from config.yaml (no hardcoded defaults)."""
        try:
            embed_block = self.config.get("embedding_model")
            if not embed_block:
                raise ValueError("Missing 'embedding_model' block in config.yaml")

            provider = embed_block.get("provider")
            model_name = embed_block.get("model_name")

            if not provider or not model_name:
                raise ValueError("Both 'provider' and 'model_name' are required in embedding_model")

            if provider.lower() != "ollama":
                raise ValueError(f"Unsupported embedding provider: {provider}")

            log.info(f"🔤 Loading embedding model from config.yaml → {model_name}")
            return OllamaEmbeddings(model=model_name)

        except Exception as e:
            log.error("❌ Embedding loading failed", error=str(e))
            raise DocumentPortalException(f"Embedding loading failed: {e}", sys)

    # ----------------------------------------------------------
    # LLaMA 3.3 Loader (strict)
    # ----------------------------------------------------------
    def load_llm(self):
        """Load LLaMA 3.3 (or variant) strictly from config.yaml."""
        try:
            llm_block = self.config.get("llm")
            if not llm_block:
                raise ValueError("Missing 'llm' section in config.yaml")

            llama_variant = os.getenv("LLAMA_VARIANT", "llama3.3")
            if llama_variant not in llm_block:
                raise ValueError(f"LLaMA variant '{llama_variant}' not found in config.yaml")

            llm_config = llm_block[llama_variant]

            # Required fields
            required_keys = ["provider", "model_name", "api_base_url", "temperature", "max_output_tokens"]
            for key in required_keys:
                if key not in llm_config:
                    raise ValueError(f"Missing required key '{key}' under llm.{llama_variant} in config.yaml")

            provider = llm_config["provider"]
            model_name = llm_config["model_name"]
            base_url = llm_config["api_base_url"]
            temperature = llm_config["temperature"]
            max_tokens = llm_config["max_output_tokens"]

            log.info(f"🦙 Loading LLaMA model from config.yaml → {llama_variant} ({model_name})")

            if provider.lower() == "ollama":
                return ChatOllama(
                    model=model_name,
                    base_url=base_url,
                    temperature=temperature,
                    num_predict=max_tokens
                )
            else:
                raise ValueError(f"Unsupported LLM provider: {provider}")

        except Exception as e:
            log.error("❌ LLaMA loading failed", error=str(e))
            raise DocumentPortalException(f"LLaMA loading failed: {e}", sys)


# ----------------------------------------------------------
# Local Test (Optional)
# ----------------------------------------------------------
if __name__ == "__main__":
    loader = ModelLoader()

    # --- Test Embedding ---
    embeddings = loader.load_embeddings()
    print(f"✅ Embedding model loaded: {embeddings}")
    test_vec = embeddings.embed_query("Hello NIH DMP!")
    print(f"🔢 Embedding vector length: {len(test_vec)}")

    # --- Test LLaMA ---
    llm = loader.load_llm()
    print(f"✅ LLaMA model loaded: {llm}")
    response = llm.invoke("Summarize the purpose of NIH Data Management Plans.")
    print("\n🧠 Model Output:\n", response.content)
