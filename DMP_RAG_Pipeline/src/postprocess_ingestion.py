from pathlib import Path
import re, json
import pandas as pd
from sentence_transformers import SentenceTransformer, util


# --------------------------------------------------------
# Locate latest ingestion session
# --------------------------------------------------------
DATA_ROOT = Path("C:/Users/Nahid/AI_DMP/DMP_RAG_Pipeline/data")
INGESTION_DIR = DATA_ROOT / "data_ingestion"
sessions = sorted([p for p in INGESTION_DIR.glob("*_NIH_ingestion*") if p.is_dir()], reverse=True)

if not sessions:
    raise RuntimeError("❌ No ingestion sessions found. Run data_ingestion.py first.")

latest_session = sessions[0]
print(f"📂 Using latest ingestion session: {latest_session.name}")

text_files = list(latest_session.rglob("*.txt"))
print(f"🔍 Found {len(text_files)} text files.")


# --------------------------------------------------------
# Text cleanup filters
# --------------------------------------------------------
def clean_paragraphs(text: str):
    """Remove low-quality, numeric-heavy, or irrelevant paragraphs."""
    paras = [p.strip() for p in text.split("\n") if len(p.strip()) > 40]
    clean = []
    for p in paras:
        low = p.lower()

        # skip tables, menus, or metadata lines
        if re.search(r"\btable\s*\d+|^\s*\d+\s+\w+", low):
            continue
        if len(re.findall(r"\d", p)) > len(p) * 0.3:
            continue
        if re.search(r"(click here|menu|footer|subscribe|share this|home page|copyright|faq|press release)", low):
            continue
        if "last updated" in low or "disclaimer" in low or "terms of use" in low:
            continue
        if len(low.split()) < 8:
            continue

        clean.append(p)
    return clean


# --------------------------------------------------------
# Load model for semantic filtering
# --------------------------------------------------------
print("🧠 Loading semantic model...")
model = SentenceTransformer("all-MiniLM-L6-v2")

# --------------------------------------------------------
# Expanded domain-specific keywords
# --------------------------------------------------------
domain_keywords = [
    # --- NIH Policy & Grant Management ---
    "NIH data sharing policy", "NIH grants policy statement", "NIH DMSP",
    "NIH extramural research", "NIH intramural research", "grant compliance",
    "data management plan", "DMS policy implementation", "funding opportunity announcement",
    "research proposal", "post-award reporting", "progress report", "final report", "RPPR",
    "financial conflict of interest", "research integrity", "peer review process",
    "federal funding compliance", "uniform guidance", "HHS policy", "OMB guidance",
    "Office of Data Science Strategy", "Office of Extramural Research", "data sharing requirements",

    # --- Data Management & Sharing (DMP) ---
    "data management plan", "data sharing plan", "data documentation", "metadata standards",
    "data dictionary", "data retention", "data curation", "data storage", "data stewardship",
    "data lifecycle management", "data access policy", "data repository", "controlled access",
    "open access repository", "data harmonization", "data interoperability",
    "machine-readable metadata", "persistent identifier", "data reuse policy",
    "data publication", "dataset citation", "data preservation", "data quality standards",

    # --- FAIR Data & Open Science ---
    "FAIR data principles", "findable accessible interoperable reusable",
    "open science", "open data", "research transparency", "data reproducibility",
    "data provenance", "metadata schema", "linked data", "semantic annotation",
    "data versioning", "data validation", "data quality assurance",
    "data standardization", "federated repositories", "data federation",
    "data discovery portal", "FAIR-compliant repository", "data stewardship framework",

    # --- Research Data & Clinical Context ---
    "clinical research data", "human subjects data", "de-identified data",
    "protected health information", "PHI", "electronic health record", "EHR data",
    "clinical trial data sharing", "data governance policy", "health informatics",
    "biomedical research data", "multi-omics data", "genomics data repository",
    "imaging data sharing", "biospecimen repository", "longitudinal dataset",
    "patient privacy", "data security", "HIPAA compliance", "data access request process",

    # --- Compliance, Oversight & Regulation ---
    "regulatory compliance", "data oversight", "audit trail", "federal data strategy",
    "policy guidance", "data retention requirements", "ethical data use",
    "research data policy", "federal regulation", "federal register notice",
    "data reporting requirement", "OMB circular", "HHS guidance document",
    "post-award monitoring", "data use agreement", "DUA", "memorandum of understanding",
    "interagency data sharing policy", "data breach reporting",

    # --- AI, ML, and Data Science Relevance ---
    "artificial intelligence", "machine learning", "deep learning",
    "data-driven research", "computational biology", "bioinformatics",
    "natural language processing", "retrieval-augmented generation", "RAG system",
    "semantic search", "knowledge graph", "metadata extraction",
    "data embeddings", "large language models", "LLM applications in science",
    "data annotation", "data labeling", "AI ethics", "algorithmic fairness",
    "data bias detection", "transformer models", "open models", "AI reproducibility",

    # --- Research Lifecycle & Sustainability ---
    "research data lifecycle", "project data management", "data sustainability",
    "long-term data preservation", "repository sustainability", "data reuse metrics",
    "reproducible workflows", "data pipeline documentation", "workflow automation",
    "data stewardship principles", "digital curation", "FAIR maturity", "dataset registration",
    "data accessibility indicators", "community data standards", "open source data tools"
]

kw_emb = model.encode(domain_keywords, convert_to_tensor=True)


# --------------------------------------------------------
# Semantic relevance scoring
# --------------------------------------------------------
records = []
for f in text_files:
    try:
        raw = Path(f).read_text(encoding="utf-8", errors="ignore")
        for para in clean_paragraphs(raw):
            emb = model.encode(para, convert_to_tensor=True)
            score = float(util.cos_sim(kw_emb, emb).max())

            if score >= 0.45:  # relevance threshold
                records.append({
                    "file": str(f),
                    "paragraph": para.strip(),
                    "relevance_score": round(score, 3)
                })
    except Exception as e:
        print(f"⚠️ Failed to process {f}: {e}")

print(f"✅ Kept {len(records)} relevant paragraphs out of {len(text_files)} files.")


# --------------------------------------------------------
# Save filtered relevant texts
# --------------------------------------------------------
if not records:
    print("⚠️ No relevant content found. Try lowering threshold or expanding keywords.")
else:
    df = pd.DataFrame(records)
    df = df.sort_values("relevance_score", ascending=False)
    out_path = DATA_ROOT / "filtered_relevant_texts.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"💾 Saved filtered relevant texts → {out_path}")
    print(f"📊 Average relevance score: {df['relevance_score'].mean():.3f}")

print("\n🏁 Content filtering complete.")
