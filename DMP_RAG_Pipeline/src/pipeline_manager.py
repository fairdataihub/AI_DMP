# ===============================================================
# pipeline_manager.py — Complete RAG + Generation Pipeline
# ===============================================================
from src.core_pipeline import ConfigManager, PDFProcessor, FAISSIndexer, RAGBuilder, DMPGenerator
from logger.custom_logger import GLOBAL_LOGGER as log


class PipelineManager:
    """Controls the full RAG + DMP generation pipeline."""

    def __init__(self, config_path="config/config.yaml"):
        self.config = ConfigManager(config_path)
        self.pdf_proc = PDFProcessor(self.config.paths.data_pdfs)
        self.indexer = FAISSIndexer(self.config.paths.index_dir)
        self.rag_builder = RAGBuilder(self.config.models.llm_name)

        # ✅ FIXED: Added template_md parameter here
        self.generator = DMPGenerator(
            excel_path=self.config.paths.excel_path,
            template_md="data/inputs/dmp-template.md",  # <—— add your template path here
            output_md=self.config.paths.output_md,
            output_docx=self.config.paths.output_docx,
        )

        log.info("PipelineManager initialized successfully")

    def run_generation(self, force_rebuild=False):
        """Runs the full RAG + DMP generation workflow."""
        docs = self.pdf_proc.load_pdfs()
        chunks = self.pdf_proc.split_chunks(
            docs,
            self.config.rag.chunk_size,
            self.config.rag.chunk_overlap,
        )
        vectorstore = self.indexer.build_or_load(chunks, force_rebuild)
        retriever = vectorstore.as_retriever(search_kwargs={"k": self.config.rag.retriever_top_k})
        rag_chain = self.rag_builder.build(retriever)
        self.generator.run_generation(rag_chain, retriever, self.config.rag.retriever_top_k)
        log.info("🎯 Pipeline generation completed successfully")
