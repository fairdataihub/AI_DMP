# rag_dmp_runner.py
# ---------------------------------------------------------
# Main orchestrator for the RAG-based DMP Generator pipeline
# ---------------------------------------------------------
# Responsibilities:
#   1. Build or load FAISS vector store (rebuild only if missing or forced)
#   2. Initialize retriever + DMPGeneratorRAG
#   3. Generate all DMP sections from prompt templates
#   4. Assemble and export DMP in Markdown / Docx
#   5. Provide visual progress (tqdm)
# ---------------------------------------------------------

import sys
from pathlib import Path
from typing import Dict, Any, Optional
from tqdm import tqdm  # progress bar for section generation

# --- Internal imports ---
from logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException
from model.models import PromptType
from data_pipeline.rag_indexer import DMPConfig, DMPIndex
from data_pipeline.rag_generator import DMPGeneratorRAG
from data_pipeline.rag_assembler import DMPAssembler


# =========================================================
# Default Section Map
# =========================================================
def build_default_section_map() -> Dict[str, PromptType]:
    """
    Maps DMP section keys to PromptType enums in PROMPT_REGISTRY.
    Modify this mapping if you add or rename sections.
    """
    return {
        "data_types": PromptType.DMP_DATA_TYPES,
        "metadata": PromptType.DMP_METADATA_STANDARDS,
        "access": PromptType.DMP_ACCESS_SHARING,
        "preservation": PromptType.DMP_PRESERVATION,
        "oversight": PromptType.DMP_OVERSIGHT_QA,
    }


# =========================================================
# Core Pipeline Runner
# =========================================================
def run_rag_dmp(
    base_dir: str,
    project_info: Dict[str, Any],
    section_prompt_map: Optional[Dict[str, PromptType]] = None,
    rebuild_index: bool = False,
    top_k: int = 6,
    out_name: str = "generated_dmp",
) -> Dict[str, Any]:
    """
    End-to-end runner for the DMP-RAG pipeline.

    Args:
        base_dir: Root data folder containing chunks/, faiss_index/, outputs/
        project_info: dict with project metadata (title, PI, institution, etc.)
        section_prompt_map: mapping of section_key -> PromptType
        rebuild_index: if True, rebuild embeddings even if FAISS index exists
        top_k: number of context chunks to retrieve per section
        out_name: output filename (without extension)

    Returns:
        dict: containing generated sections, file paths, and metadata
    """
    try:
        log.info("🚀 Starting DMP-RAG generation pipeline...")
        cfg = DMPConfig(base_dir=Path(base_dir), top_k=top_k)
        indexer = DMPIndex(cfg)

        # ---------------------------------------------------------
        # Step 1️⃣: Build or Load FAISS Index
        # ---------------------------------------------------------
        faiss_file = cfg.index_dir / f"{cfg.index_name}.faiss"
        if rebuild_index or not faiss_file.exists():
            log.info("🔨 Building FAISS index from chunks (new or missing index)...")
            indexer.build_from_chunks()
        else:
            log.info("📦 Loading existing FAISS index...")
            indexer.load_local()

        retriever = indexer.get_retriever(k=top_k)
        log.info("✅ Retriever initialized successfully")

        # ---------------------------------------------------------
        # Step 2️⃣: Initialize Generator
        # ---------------------------------------------------------
        section_map = section_prompt_map or build_default_section_map()
        generator = DMPGeneratorRAG(retriever=retriever, section_prompt_map=section_map)
        log.info("🧠 DMPGeneratorRAG initialized")

        # ---------------------------------------------------------
        # Step 3️⃣: Generate DMP Sections (with tqdm progress bar)
        # ---------------------------------------------------------
        sections = {}
        ordered_sections = list(section_map.keys())
        log.info("📄 Generating DMP sections...")
        for sec in tqdm(ordered_sections, desc="Generating DMP Sections", ncols=90):
            sections[sec] = generator.generate_section(sec, project_info, top_k=top_k)

        log.info("✅ All DMP sections generated successfully")

        # ---------------------------------------------------------
        # Step 4️⃣: Assemble and Export DMP
        # ---------------------------------------------------------
        assembler = DMPAssembler()
        md_text = assembler.to_markdown(sections, project_info)

        md_path = cfg.outputs_dir / f"{out_name}.md"
        assembler.save_markdown(md_text, md_path)
        log.info(f"💾 Markdown saved: {md_path}")

        docx_path = cfg.outputs_dir / f"{out_name}.docx"
        docx_saved = assembler.save_docx(md_text, docx_path)
        if docx_saved:
            log.info(f"💾 DOCX saved: {docx_path}")
        else:
            log.warning("DOCX export skipped (python-docx not installed)")

        # ---------------------------------------------------------
        # Step 5️⃣: Final Summary
        # ---------------------------------------------------------
        result = {
            "sections": sections,
            "markdown_path": str(md_path),
            "docx_path": str(docx_path) if docx_saved else None,
            "index_dir": str(cfg.index_dir),
        }

        log.info("🎉 DMP generation pipeline completed successfully", output=result)
        return result

    except DocumentPortalException:
        raise
    except Exception as e:
        log.error("❌ RAG DMP pipeline failed", error=str(e))
        raise DocumentPortalException(f"RAG DMP run error: {e}", sys)


# =========================================================
# CLI Usage Example
# =========================================================
if __name__ == "__main__":
    project_info = {
        "project_title": "Predictive Modeling of Cancer Symptoms from EHR Notes",
        "pi_name": "Dr. Jane Doe",
        "institution": "University of Iowa",
        "grant_id": "R01-CA-XXXXXX",
        "repository_preferences": "dbGaP for controlled access; Zenodo for derived data",
        "privacy_constraints": "HIPAA-compliant de-identification of clinical text",
    }

    BASE_DIR = "C:/Users/Nahid/DMP-RAG/data"

    result = run_rag_dmp(
        base_dir=BASE_DIR,
        project_info=project_info,
        rebuild_index=False,   # Automatically rebuild if index missing
        top_k=6,
        out_name="dmp_generated_example",
    )

    print("\n✅ DMP Generation Complete!")
    print("Markdown:", result["markdown_path"])
    print("DOCX    :", result["docx_path"])
