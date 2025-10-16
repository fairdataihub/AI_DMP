from __future__ import annotations
import time, re
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

from prompt.prompt_library import PROMPT_REGISTRY
from model.models import PromptType


def _clean_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", str(name)).strip()


class DMPGenerator:
    """
    Generates NIH-style DMPs using:
      - a FAISS retriever (vectorstore.as_retriever)
      - a PromptTemplate pulled from PROMPT_REGISTRY
      - an LLM (provided by ModelLoader)
    """

    def __init__(
        self,
        excel_input: Path,
        output_md_dir: Path,
        output_docx_dir: Path,
        template_type: PromptType,
        vectorstore,
        llm,
        top_k: int = 6,
    ):
        self.excel_input = Path(excel_input)
        self.output_md_dir = Path(output_md_dir); self.output_md_dir.mkdir(parents=True, exist_ok=True)
        self.output_docx_dir = Path(output_docx_dir); self.output_docx_dir.mkdir(parents=True, exist_ok=True)
        self.prompt_template = PROMPT_REGISTRY[template_type]
        self.retriever = vectorstore.as_retriever(search_kwargs={"k": top_k})
        self.llm = llm

    def _save_md(self, filepath: Path, text: str):
        filepath.write_text(text, encoding="utf-8")
        log.info(f"💾 Saved {filepath}")

    def _md_to_docx(self, md_path: Path, docx_path: Path):
        # Pandoc is required on system PATH.
        import pypandoc
        pypandoc.convert_file(str(md_path), "docx", outputfile=str(docx_path))
        log.info(f"📄 Converted to {docx_path}")

    def run(self):
        if not self.excel_input.exists():
            raise DocumentPortalException(f"Excel file not found: {self.excel_input}")

        df = pd.read_excel(self.excel_input)
        if df.empty:
            raise DocumentPortalException("Input Excel is empty.")

        prompt = PromptTemplate(
            template=self.prompt_template,
            input_variables=["project_info", "context"],
        )
        chain = (
            {"project_info": RunnablePassthrough(), "context": self.retriever}
            | prompt
            | self.llm
            | StrOutputParser()
        )

        for i, (_, row) in enumerate(df.iterrows()):
            project_info: Dict[str, Any] = row.to_dict()
            start = time.time()
            try:
                text = chain.invoke(project_info)
            except Exception as e:
                log.error(f"Generation failed at row {i}: {e}")
                continue

            fname = _clean_filename(project_info.get("Project_Title", f"dmp_{i}"))
            md_path = self.output_md_dir / f"{fname}.md"
            self._save_md(md_path, text)
            self._md_to_docx(md_path, self.output_docx_dir / f"{fname}.docx")
            log.info(f"✅ Completed {fname} in {round(time.time()-start, 2)}s")
