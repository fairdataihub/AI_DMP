from __future__ import annotations
from typing import Dict
from model.models import PromptType

# You can store multiple prompts keyed by PromptType.
# Template expects: {project_info} and {context}
PROMPT_REGISTRY: Dict[PromptType, str] = {
    PromptType.NIH_DMP: (
        "You are an assistant generating NIH-style Data Management Plans.\n\n"
        "PROJECT INFO:\n{project_info}\n\n"
        "RETRIEVED CONTEXT:\n{context}\n\n"
        "TASK: Draft a complete NIH-compliant DMP that is specific, concise, and consistent.\n"
        "Organize by the 6 NIH elements. Avoid generic filler. Cite policies when present in context."
    )
}
