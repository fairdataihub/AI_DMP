# prompt_library.py
# ---------------------------------------------------------
# Central registry of prompt templates for the DMP-RAG system.
# Each prompt corresponds to one DMP section or task type.
# Accessible via PROMPT_REGISTRY[PromptType.<ENUM>.value]
# ---------------------------------------------------------

from langchain_core.prompts import ChatPromptTemplate

# =========================================================
# NIH DMP SECTION PROMPTS
# =========================================================

# --- 1️⃣ Data Types & Formats ---
DMP_DATA_TYPES_PROMPT = ChatPromptTemplate.from_template("""
You are an expert data management planner.
Your task is to write the **"Data Types and Formats"** section of an NIH-style
Data Management and Sharing Plan (DMP).

### Project information
{project_info}

### Reference context from other NIH DMPs and FAIR data policies
{context}

Using the above, write a concise, professional paragraph describing:
- Types of data generated or collected (e.g., images, code, clinical records)
- File formats and any transformations
- Expected size or scale of the dataset
- Data quality and standardization

### Output
Write in formal academic tone (approx. 150-200 words).
Do not include section headings or markdown syntax.
""")

# --- 2️⃣ Metadata & Documentation ---
DMP_METADATA_PROMPT = ChatPromptTemplate.from_template("""
You are writing the **"Metadata and Documentation"** section of a Data Management Plan.

### Project info
{project_info}

### Context
{context}

Describe:
- Metadata standards and controlled vocabularies that will be used
- How metadata supports FAIR data principles
- Tools or systems used to capture metadata (e.g., REDCap, DataCite schema)
- File-level documentation practices

Keep the tone formal, concise, and NIH-compliant (≈150-200 words).
""")

# --- 3️⃣ Data Access & Sharing ---
DMP_ACCESS_PROMPT = ChatPromptTemplate.from_template("""
You are writing the **"Data Access and Sharing"** section of a Data Management Plan.

### Project info
{project_info}

### Context
{context}

Describe:
- How and when data will be shared
- Data repositories or archives that will host the data
- Access levels (open, restricted, controlled)
- Privacy, consent, and HIPAA considerations

Keep the answer clear, structured, and policy-oriented (≈150-200 words).
""")

# --- 4️⃣ Preservation & Storage ---
DMP_PRESERVATION_PROMPT = ChatPromptTemplate.from_template("""
Write the **"Data Preservation, Archiving, and Storage"** section of an NIH DMP.

### Project info
{project_info}

### Context
{context}

Discuss:
- Long-term repositories and archiving plans
- Retention duration
- Version control and backup strategies
- Persistent identifiers (DOIs, handles)
- Any associated costs or responsibilities

Formal, factual tone (≈150-200 words).
""")

# --- 5️⃣ Oversight & Quality Assurance ---
DMP_OVERSIGHT_PROMPT = ChatPromptTemplate.from_template("""
You are writing the **"Oversight and Data Quality Assurance"** section of a Data Management Plan.

### Project info
{project_info}

### Context
{context}

Explain:
- Roles and responsibilities in data management
- QA/QC procedures
- Compliance monitoring
- Review frequency and governance

Be concise and professional (≈150-200 words).
""")

# =========================================================
# Registry Mapping
# =========================================================
PROMPT_REGISTRY = {
    "dmp_data_types": DMP_DATA_TYPES_PROMPT,
    "dmp_metadata": DMP_METADATA_PROMPT,
    "dmp_access": DMP_ACCESS_PROMPT,
    "dmp_preservation": DMP_PRESERVATION_PROMPT,
    "dmp_oversight": DMP_OVERSIGHT_PROMPT,
}
