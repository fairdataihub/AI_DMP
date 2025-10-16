from pathlib import Path
from pydantic import BaseModel, Field, field_validator

class PathsConfig(BaseModel):
    data_pdfs: Path = Field(..., description="Path to directory with PDFs")
    index_dir: Path = Field(..., description="Directory for FAISS index")
    excel_path: Path = Field(..., description="Path to Excel file with titles")
    output_md: Path = Field(..., description="Output directory for Markdown DMPs")
    output_docx: Path = Field(..., description="Output directory for DOCX DMPs")

class RAGConfig(BaseModel):
    chunk_size: int = Field(800, description="Chunk size for text splitting")
    chunk_overlap: int = Field(120, description="Overlap between chunks")
    retriever_top_k: int = Field(3, description="Top-K retrieved chunks")

class ModelConfig(BaseModel):
    llm_name: str = Field("llama3", description="LLM name for Ollama or other backend")
    embedding_model: str = Field(
        "sentence-transformers/all-MiniLM-L6-v2",
        description="Embedding model name"
    )

class LoggingConfig(BaseModel):
    log_dir: Path = Field("logs", description="Directory for logs")
    level: str = Field("INFO", description="Logging level")
    format: str = Field("json", description="Log format (json/text)")

class ExperimentConfig(BaseModel):
    """Root configuration class for the entire pipeline."""
    experiment_name: str = Field("DefaultExp", description="Experiment name")
    root_dir: Path = Field(Path.cwd(), description="Root project directory")
    paths: PathsConfig
    rag: RAGConfig
    models: ModelConfig
    logging: LoggingConfig

    @field_validator("root_dir")
    @classmethod
    def expand_root(cls, v: Path) -> Path:
        """Ensure root_dir is expanded and absolute."""
        return v.expanduser().resolve()
