import yaml
from pathlib import Path

# ...
class DMPRAGPipeline:
    @classmethod
    def from_config(cls, config_path: str | Path):
        """Create pipeline instance from YAML config."""
        config = yaml.safe_load(open(config_path, "r", encoding="utf-8"))
        proj = config.get("project", {})
        paths = config.get("paths", {})
        models = config.get("models", {})
        gen = config.get("generation", {})

        return cls(
            root_dir=proj.get("root_dir"),
            pdf_subdir=paths.get("pdf_subdir"),
            index_subdir=paths.get("index_subdir"),
            excel_relpath=paths.get("excel_relpath"),
            output_md_relpath=paths.get("output_md_relpath"),
            output_docx_relpath=paths.get("output_docx_relpath"),
            embed_model=models.get("embed_model"),
            llm_model=models.get("llm_model"),
            top_k=gen.get("top_k", 6),
            build_index_if_missing=gen.get("build_index_if_missing", True),
        )


if __name__ == "__main__":
    try:
        pipeline = DMPRAGPipeline.from_config("config.yaml")
        pipeline.run()
    except DocumentPortalException as e:
        log.error(f"Pipeline failed: {e}")
    except Exception as ex:
        log.exception(f"Unexpected failure: {ex}")
