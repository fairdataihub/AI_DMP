# src/pipeline_manager.py
from src.core_pipeline import ConfigManager, PDFProcessor, FAISSIndexer, RAGBuilder, DMPGenerator
from src.evaluation import DMPEvaluator
from logger.custom_logger import GLOBAL_LOGGER as log



class PipelineManager:
    """Controls the full end-to-end RAG + Evaluation pipeline."""

    def __init__(self, config_path="config/config.yaml"):
        self.config = ConfigManager(config_path)
        self.pdf_proc = PDFProcessor(self.config.paths.data_pdfs)
        self.indexer = FAISSIndexer(self.config.paths.index_dir)
        self.rag_builder = RAGBuilder()
        self.generator = DMPGenerator(
            self.config.paths.excel_path,
            self.config.paths.output_md,
            self.config.paths.output_docx,
        )
        self.evaluator = DMPEvaluator()
        log.info("PipelineManager initialized")

    def run_generation(self):
        docs = self.pdf_proc.load_pdfs()
        chunks = self.pdf_proc.split_chunks(
            docs,
            self.config.rag.chunk_size,
            self.config.rag.chunk_overlap,
        )
        vectorstore = self.indexer.build_or_load(chunks)
        retriever = vectorstore.as_retriever(search_kwargs={"k": self.config.rag.retriever_top_k})
        rag_chain = self.rag_builder.build(retriever)
        self.generator.run_generation(rag_chain)
        log.info("Pipeline generation completed successfully")

    def run_evaluation(self, gen_texts, gold_texts):
        results = [self.evaluator.compare(g, t) for g, t in zip(gen_texts, gold_texts)]
        summary = self.evaluator.summarize(results)
        log.info("Pipeline evaluation completed", summary=summary)
        return summary
