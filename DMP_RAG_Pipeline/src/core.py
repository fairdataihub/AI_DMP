# ===============================================================
# core_pipeline.py — NIH DMP RAG Pipeline (Full Cleaning Version)
# ===============================================================

import re, json, yaml, pandas as pd, pypandoc
from pathlib import Path
from tqdm import tqdm
from types import SimpleNamespace

# ---- LangChain imports ----
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# ---- Internal imports ----
from utils.model_loader import ModelLoader
from exception.custom_exception import DocumentPortalException
from logger.custom_logger import GLOBAL_LOGGER as log
from prompt.prompt_library import PROMPT_REGISTRY, PromptType


# ===============================================================
# CONFIGURATION MANAGER (YAML + dot-access)
# ===============================================================
class ConfigManager:
    """Loads YAML config and allows dot-access (e.g., cfg.paths.data_pdfs)."""

    def __init__(self, config_path="config/config.yaml"):
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        def to_namespace(obj):
            if isinstance(obj, dict):
                return SimpleNamespace(**{k: to_namespace(v) for k, v in obj.items()})
            elif isinstance(obj, list):
                return [to_namespace(i) for i in obj]
            else:
                return obj

        self.cfg = to_namespace(cfg)
        log.info("✅ Config loaded successfully (dot-access enabled)")

    @property
    def paths(self): return self.cfg.paths
    @property
    def rag(self): return self.cfg.rag
    @property
    def models(self): return self.cfg.models


# ===============================================================
# CLEANING MODULE (PDFs, Figures, Logos, Non-Text Filtering)
# ===============================================================
import fitz  # PyMuPDF
from sentence_transformers import SentenceTransformer, util

class Cleaner:
    """Handles advanced PDF cleanup and semantic filtering."""

    def __init__(self, cfg: ConfigManager):
        self.cfg = cfg
        model_name = cfg.models.embedding_model
        log.info(f"🧠 Loading embedding model for cleaning: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.keywords = [
            "data management plan", "data sharing plan", "FAIR data principles",
            "data stewardship", "NIH data sharing policy", "research integrity",
        ]
        self.kw_emb = self.model.encode(self.keywords, convert_to_tensor=True)

    # -----------------------------------------------------------
    # FULL PDF STRUCTURAL CLEANING
    # -----------------------------------------------------------
    def extract_clean_text(self, pdf_path: Path) -> str:
        """
        Extract only meaningful text from a PDF:
          - Removes logos, images, figures, captions, watermarks
          - Skips numeric-heavy, OCR-like, or blank pages
          - Keeps body text paragraphs for RAG
        """
        doc = fitz.open(pdf_path)
        text_chunks = []

        for page in doc:
            # Remove images (logos, figures, etc.)
            for img in page.get_images(full=True):
                xref = img[0]
                try:
                    doc._deleteObject(xref)
                except Exception:
                    pass

            blocks = page.get_text("blocks")
            if not blocks or len(blocks) < 2:
                continue

            if page.widgets() or len(page.get_text("text").strip()) == 0:
                continue

            text = page.get_text("text")

            num_ratio = sum(c.isdigit() for c in text) / max(len(text), 1)
            if num_ratio > 0.25:
                continue

            # Remove figure/table captions, logos, watermarks
            text = re.sub(r"(Figure|Fig\.|Table)\s*\d+[:\.\-]?\s*.*", "", text, flags=re.I)
            text = re.sub(r"(NIH|Page\s*\d+\s*of\s*\d+|Confidential|Draft)", "", text, flags=re.I)
            text = re.sub(r"[\u2022■□▪●◆▶►•◦▪▫]", "", text)
            text = re.sub(r"(\bAuthorized\b.*\bUse\b)|(\bCopyright\b.*\d{4})", "", text, flags=re.I)

            if len(text.strip()) < 50:
                continue

            text_chunks.append(text)

        return "\n".join(text_chunks)

    # -----------------------------------------------------------
    # TEXT & PARAGRAPH FILTERS
    # -----------------------------------------------------------
    def advanced_text_cleanup(self, text: str) -> str:
        """Remove NIH-specific identifiers and boilerplate + structural sections."""
        rules = [
            # --- Structural / Navigational ---
            r"Table of Contents.*", r"List of Tables.*", r"List of Figures.*",
            r"Appendix(\s+[A-Z0-9]+)?[:\-]?.*", r"References.*", r"Bibliography.*",
            r"Acknowledg(e)?ments.*", r"Revision History.*", r"Version\s*Notes.*",
            r"Glossary.*", r"Index.*",

            # --- Administrative / Boilerplate ---
            r"PHS\s*\d+", r"OMB\s*No\.\s*\d+", r"Expiration\s*Date.*",
            r"(Principal Investigator|Institution Name).*",
            r"NIH Public Access Policy.*", r"^Page\s*\d+\s*of\s*\d+",
            r"\b\d{6,}\b",

            # --- Metadata & Disclaimers ---
            r"(Disclaimer|Copyright|Confidential).*", r"^Draft.*",
            r"Contact Information.*", r"Address:.*", r"Phone:.*", r"Email:.*",
            r"Revision Date.*", r"Update\s*History.*",
        ]
        for r_ in rules:
            text = re.sub(r_, "", text, flags=re.I | re.M)
        text = re.sub(r"\s{2,}", " ", text)
        return text.strip()

    def remove_banned_sections(self, text: str):
        """Remove large sections like TOC, References, Appendix, etc."""
        BANNED_HEADINGS = [
            "table of contents", "references", "appendix", "bibliography",
            "acknowledgments", "revision history", "contact information",
            "list of figures", "list of tables", "index", "glossary"
        ]
        paras = text.split("\n")
        cleaned = []
        skip = False
        for p in paras:
            low = p.strip().lower()
            if any(h in low for h in BANNED_HEADINGS):
                skip = True
                continue
            if skip and (len(low.strip()) < 10 or re.match(r"^[0-9A-Z\. ]+$", low)):
                continue
            else:
                skip = False
                cleaned.append(p)
        return "\n".join(cleaned)

    def clean_paragraphs(self, text: str):
        """Remove low-quality or irrelevant paragraphs."""
        text = self.remove_banned_sections(text)
        paras = [p.strip() for p in text.split("\n") if len(p.strip()) > 40]
        clean = []
        for p in paras:
            low = p.lower()
            if re.search(r"\btable\s*\d+|^\s*\d+\s+\w+", low):
                continue
            if len(re.findall(r"\d", p)) > len(p) * 0.3:
                continue
            if re.search(
                r"(click here|footer|subscribe|copyright|faq|press release|menu)",
                low,
            ):
                continue
            if len(low.split()) < 8:
                continue
            clean.append(p)
        return clean

    def semantic_filter(self, paragraphs, threshold=0.45):
        """Keep only paragraphs relevant to NIH DMP/FAIR domain."""
        kept = []
        for p in paragraphs:
            emb = self.model.encode(p, convert_to_tensor=True)
            score = float(util.cos_sim(self.kw_emb, emb).max())
            if score >= threshold:
                kept.append(p)
        return kept


# ===============================================================
# PDF PROCESSOR
# ===============================================================
class PDFProcessor:
    """Loads PDFs, applies cleaning, and splits into chunks."""

    def __init__(self, data_pdfs: Path, cfg: ConfigManager):
        self.data_pdfs = Path(data_pdfs)
        self.cleaner = Cleaner(cfg)

    def load_pdfs(self):
        pdf_files = sorted(self.data_pdfs.glob("*.pdf"))
        if not pdf_files:
            raise FileNotFoundError(f"No PDFs found in {self.data_pdfs}")

        docs = []
        for p in tqdm(pdf_files, desc="📥 Loading + Cleaning PDFs"):
            try:
                clean_text = self.cleaner.extract_clean_text(p)
                clean_text = self.cleaner.advanced_text_cleanup(clean_text)
                paragraphs = self.cleaner.clean_paragraphs(clean_text)
                filtered_paras = self.cleaner.semantic_filter(paragraphs)

                if not filtered_paras:
                    log.warning(f"⚠️ {p.name}: No relevant text after full cleaning.")
                    continue

                tmp_path = p.with_suffix(".clean.txt")
                tmp_path.write_text("\n\n".join(filtered_paras), encoding="utf-8")

                loader = PyPDFLoader(str(p))
                docs.extend(loader.load())

            except Exception as e:
                log.warning(f"⚠️ Cleaning failed for {p.name}: {e}")
                loader = PyPDFLoader(str(p))
                docs.extend(loader.load())

        log.info("📚 PDFs loaded", count=len(docs))
        return docs

    def split_chunks(self, docs, chunk_size=800, chunk_overlap=120):
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        chunks = splitter.split_documents(docs)
        log.info("✂️ Chunks created", count=len(chunks))
        return chunks


# ===============================================================
# FAISS INDEXER
# ===============================================================
class FAISSIndexer:
    """Builds or loads FAISS vector index."""
    def __init__(self, index_dir: Path):
        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.embeddings = ModelLoader().load_embeddings()

    def build_or_load(self, chunks, force_rebuild=False):
        faiss_path = self.index_dir / "index.faiss"
        if faiss_path.exists() and not force_rebuild:
            log.info("📦 Loading existing FAISS index", path=str(faiss_path))
            return FAISS.load_local(
                str(self.index_dir),
                self.embeddings,
                allow_dangerous_deserialization=True,
            )
        log.info("🧱 Building new FAISS index ...")
        store = FAISS.from_documents(chunks, self.embeddings)
        store.save_local(str(self.index_dir))
        log.info("✅ FAISS index saved", path=str(self.index_dir))
        return store


# ===============================================================
# RAG BUILDER
# ===============================================================
class RAGBuilder:
    """Builds the same RAG chain as in your Jupyter notebook."""

    def __init__(self, llm_model="llama3.3"):
        self.llm_model = llm_model
        self.llm = Ollama(model=self.llm_model)
        self.prompt = PROMPT_REGISTRY[PromptType.NIH_DMP.value]
        self.parser = StrOutputParser()

    def build(self, retriever):
        """Exact replica of notebook build_rag_chain()."""
        def format_docs(docs):
            if not docs:
                return ""
            formatted = []
            for d in docs:
                page = d.metadata.get("page", "")
                title = d.metadata.get("source", "")
                formatted.append(f"[Page {page}] {title}\n{d.page_content.strip()}")
            return "\n\n".join(formatted)

        rag_chain = (
            {
                "context": retriever | format_docs,
                "question": RunnablePassthrough()
            }
            | self.prompt
            | self.llm
            | self.parser
        )
        log.info(f"🔗 RAG chain initialized with model: {self.llm_model}")
        return rag_chain


# ===============================================================
# DMP GENERATOR
# ===============================================================
class DMPGenerator:
    """Generates NIH DMPs as Markdown, DOCX, and JSON."""

    def __init__(self, excel_path, template_md, output_md, output_docx):
        self.excel_path = Path(excel_path)
        self.template_md = Path(template_md)
        self.output_md = Path(output_md)
        self.output_docx = Path(output_docx)
        self.output_json = self.output_md.parent / "json"
        for p in [self.output_md, self.output_docx, self.output_json]:
            p.mkdir(parents=True, exist_ok=True)

    def _sanitize_filename(self, name): 
        return re.sub(r'[\\/*?:"<>|]', "_", str(name)).strip()

    def run_generation(self, rag_chain, retriever, top_k=6):
        try:
            df = pd.read_excel(self.excel_path)
            df.columns = df.columns.str.strip().str.lower()
            df = df.fillna("")
            log.info(f"🧾 Loaded Excel with {len(df)} rows")

            if not self.template_md.exists():
                raise FileNotFoundError(f"Template not found: {self.template_md}")
            dmp_template_text = self.template_md.read_text(encoding="utf-8")
            log.info(f"📄 NIH DMP Markdown template loaded from {self.template_md}")

            records = []
            for _, row in tqdm(df.iterrows(), total=len(df), desc="🧠 Generating NIH DMPs"):
                title = str(row.get("title", "")).strip()
                if not title:
                    continue

                log.info(f"🧩 Generating DMP for: {title}")
                element_texts = []
                for col in [c for c in df.columns if c.startswith("element")]:
                    val = str(row[col]).strip()
                    if val:
                        element_texts.append(f"{col.upper()}: {val}")
                query_data = "\n".join(element_texts)
                query = (
                    f"You are an expert biomedical data steward and grant writer. "
                    f"Create a complete NIH Data Management and Sharing Plan (DMSP) "
                    f"for the project titled '{title}'. Use retrieved context from "
                    f"the NIH corpus to fill in all template sections accurately.\n\n"
                    f"Here is background information from the proposal:\n{query_data}\n"
                )

                try:
                    retrieved_docs = retriever.get_relevant_documents(query)
                    context_text = "\n\n".join(doc.page_content for doc in retrieved_docs[:top_k])
                except Exception as e:
                    log.warning(f"⚠️ Retrieval failed for {title}: {e}")
                    context_text = ""

                full_prompt = f"""
You are an expert biomedical data steward and grant writer.
Use the retrieved NIH context and the provided template to generate a complete Data Management and Sharing Plan.

----
Context:
{context_text}

----
Project Query:
{query}

Use the following NIH DMSP Markdown template. Do not alter section titles:
{dmp_template_text}
"""
                try:
                    response = rag_chain.invoke(full_prompt)
                    safe = self._sanitize_filename(title)
                    md_path = self.output_md / f"{safe}.md"
                    docx_path = self.output_docx / f"{safe}.docx"
                    json_path = self.output_json / f"{safe}.json"

                    md_path.write_text(response, encoding="utf-8")
                    pypandoc.convert_text(response, "docx", format="md", outputfile=str(docx_path))
                    json.dump({
                        "title": title,
                        "query": query,
                        "retrieved_context": context_text,
                        "generated_markdown": response
                    }, open(json_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

                    records.append({
                        "Title": title,
                        "Query": query,
                        "Retrieved_Context": context_text[:1000],
                        "Generated_DMP_Preview": response[:1000],
                        "Error": ""
                    })
                    log.info("✅ DMP saved", markdown=str(md_path), docx=str(docx_path), json=str(json_path))

                except Exception as e:
                    log.error("❌ Generation failed", title=title, error=str(e))
                    records.append({
                        "Title": title,
                        "Query": query,
                        "Retrieved_Context": context_text[:1000],
                        "Generated_DMP_Preview": "",
                        "Error": str(e)
                    })

            out_log = self.output_md.parent / "rag_generated_dmp_log.csv"
            pd.DataFrame(records).to_csv(out_log, index=False, encoding="utf-8")
            log.info(f"📊 Log saved to: {out_log}")

        except Exception as e:
            raise DocumentPortalException("DMP generation error", e)
