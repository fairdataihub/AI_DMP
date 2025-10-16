# ---------------------------------------------------------
# NIH-RAG Section Generator
# ---------------------------------------------------------
# Responsibilities:
#   - Retrieve context from NIH FAISS retriever
#   - Select section-specific prompt from PROMPT_REGISTRY
#   - Generate NIH-style DMP section text using LLaMA 3.3
#   - Return structured section outputs
# ---------------------------------------------------------

import sys
from typing import Dict, Any, List, Optional
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from utils.model_loader import ModelLoader
from prompt.prompt_library import PROMPT_REGISTRY
from model.models import PromptType
from exception.custom_exception import DocumentPortalException
from logger.custom_logger import GLOBAL_LOGGER as log


class NIH_DMPGeneratorRAG:
    """
    Retrieval-Augmented Generator for NIH-style DMP sections.
    --------------------------------------------------------------
    Combines:
      - project_info (user-supplied metadata)
      - retrieved NIH corpus context (FAISS retriever)
      - section-specific prompt (PROMPT_REGISTRY)
    --------------------------------------------------------------
    """

    def __init__(self, retriever, section_prompt_map: Dict[str, PromptType]):
        try:
            self.retriever = retriever
            # LLM loaded strictly from config/config.yaml
            self.llm = ModelLoader().load_llm()
            self.section_prompt_map = section_prompt_map
            self._validate_prompts()
            log.info("✅ NIH_DMPGeneratorRAG initialized successfully")
        except Exception as e:
            log.error("❌ Initialization failed", error=str(e))
            raise DocumentPortalException(f"Initialization error in NIH_DMPGeneratorRAG: {e}", sys)

    # ---------------------------------------------------------
    # Validate prompt registry completeness
    # ---------------------------------------------------------
    def _validate_prompts(self):
        missing = []
        for section, prompt_enum in self.section_prompt_map.items():
            if prompt_enum.value not in PROMPT_REGISTRY:
                missing.append((section, prompt_enum.value))
        if missing:
            msg = f"Missing prompt templates in PROMPT_REGISTRY: {missing}"
            log.error(msg)
            raise DocumentPortalException(msg, sys)

    # ---------------------------------------------------------
    # Utility: join retrieved document contents
    # ---------------------------------------------------------
    @staticmethod
    def _join_docs(docs: List[Document]) -> str:
        """Combine retrieved NIH chunks into one context string."""
        return "\n\n".join([d.page_content for d in docs])

    # ---------------------------------------------------------
    # Section generation
    # ---------------------------------------------------------
    def generate_section(
        self,
        section_key: str,
        project_info: Dict[str, Any],
        top_k: int = 6,
    ) -> str:
        """
        Generate a single NIH DMP section via retrieval + generation.
        """
        try:
            # ---------- 1️⃣ Retrieve context ----------
            query = self._make_query(section_key, project_info)
            docs = self.retriever.get_relevant_documents(query)
            if not docs:
                log.warning(f"⚠️ No relevant context found for '{section_key}'")
                context = ""
            else:
                context = self._join_docs(docs[:top_k])

            # ---------- 2️⃣ Load section prompt ----------
            prompt_type = self.section_prompt_map.get(section_key)
            if not prompt_type:
                raise DocumentPortalException(f"No prompt mapping for {section_key}", sys)
            if prompt_type.value not in PROMPT_REGISTRY:
                raise DocumentPortalException(f"Prompt '{prompt_type.value}' not found in registry", sys)

            prompt_tpl: ChatPromptTemplate = PROMPT_REGISTRY[prompt_type.value]

            # ---------- 3️⃣ Fill template variables ----------
            variables = {
                "project_info": project_info,
                "context": context,
                "section_name": section_key.replace("_", " ").title(),
            }

            # ---------- 4️⃣ Generate text ----------
            response = self.llm.invoke(prompt_tpl.format_messages(**variables))
            text = getattr(response, "content", None) if hasattr(response, "content") else str(response)

            # ---------- 5️⃣ Postprocess ----------
            if not text or not text.strip():
                log.warning("⚠️ Empty output", section=section_key)
                return f"[No content generated for {section_key}]"

            text = text.strip()
            log.info(f"✅ Generated NIH section: {section_key}", preview=text[:150])
            return text

        except Exception as e:
            log.error(f"❌ Generation failed for section '{section_key}'", error=str(e))
            raise DocumentPortalException(f"Section generation error: {section_key}: {e}", sys)

    # ---------------------------------------------------------
    # Query builder (customized for NIH sections)
    # ---------------------------------------------------------
    def _make_query(self, section_key: str, project_info: Dict[str, Any]) -> str:
        """Build a semantic retrieval query using project title."""
        base = project_info.get("project_title") or project_info.get("title") or "research project"
        templates = {
            "data_types": f"{base}: describe NIH-supported data types, formats, modalities, and standards.",
            "metadata": f"{base}: NIH metadata requirements, FAIR compliance, controlled vocabularies, documentation.",
            "access": f"{base}: NIH data sharing, repositories, access control, consent, HIPAA, dbGaP.",
            "preservation": f"{base}: NIH long-term storage, archiving, versioning, persistent identifiers.",
            "oversight": f"{base}: data quality assurance, management roles, oversight, NIH policy compliance.",
        }
        query = templates.get(section_key, f"{base}: information relevant to {section_key} in NIH context")
        log.debug("Constructed NIH retrieval query", section=section_key, query=query)
        return query

    # ---------------------------------------------------------
    # Generate full NIH DMP
    # ---------------------------------------------------------
    def generate_dmp(
        self,
        project_info: Dict[str, Any],
        section_order: Optional[List[str]] = None,
        top_k: int = 6,
    ) -> Dict[str, str]:
        """
        Generate all NIH DMP sections sequentially.
        Returns a dict: {section_name: generated_text}.
        """
        try:
            ordered_keys = section_order or list(self.section_prompt_map.keys())
            output: Dict[str, str] = {}
            log.info("🚀 Starting NIH DMP generation across sections...")
            for sec in ordered_keys:
                output[sec] = self.generate_section(sec, project_info, top_k=top_k)
            log.info("🏁 NIH DMP generation completed successfully.")
            return output
        except Exception as e:
            log.error("❌ Failed to generate full NIH DMP", error=str(e))
            raise DocumentPortalException(f"Full NIH DMP generation error: {e}", sys)
