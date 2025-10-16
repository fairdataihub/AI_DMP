# src/prompt/prompt_library.py
from langchain_core.prompts import ChatPromptTemplate
from enum import Enum

class PromptType(Enum):
    CONTEXT_QA = "context_qa"
    CONTEXTUALIZE_QUESTION = "contextualize_question"

PROMPT_REGISTRY = {
    PromptType.CONTEXTUALIZE_QUESTION.value: ChatPromptTemplate.from_template(
        "Rephrase the question to include relevant context from the chat history:\n\nChat: {chat_history}\n\nUser: {input}"
    ),
    PromptType.CONTEXT_QA.value: ChatPromptTemplate.from_template(
        "You are an NIH data steward assisting in creating a Data Management Plan.\n"
        "Context:\n{context}\n\nQuestion:\n{input}\n\nAnswer concisely and accurately."
    )
}
