# validate_config.py
import yaml
from config.config_schema import ExperimentConfig
from pathlib import Path

CONFIG_PATH = Path("config/config.yaml")

try:
    print("🔍 Loading YAML config from:", CONFIG_PATH.resolve())
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg_dict = yaml.safe_load(f)

    # Validate with Pydantic model
    cfg = ExperimentConfig(**cfg_dict)

    print("\n✅ Configuration validated successfully!\n")
    print("Experiment name:", cfg.experiment_name)
    print("Root directory:", cfg.root_dir)
    print("PDF folder:", cfg.paths.data_pdfs)
    print("Index directory:", cfg.paths.index_dir)
    print("Excel file:", cfg.paths.excel_path)
    print("Output (Markdown):", cfg.paths.output_md)
    print("Output (Docx):", cfg.paths.output_docx)
    print("\nChunk size:", cfg.rag.chunk_size)
    print("Retriever top-k:", cfg.rag.retriever_top_k)
    print("LLM:", cfg.models.llm_name)
    print("Embedding model:", cfg.models.embedding_model)
    print("Logging level:", cfg.logging.level)

except Exception as e:
    print("\n❌ Configuration validation failed!")
    print("Error:", str(e))
