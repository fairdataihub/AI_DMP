# ===============================================================
# web_app.py — Inline NIH DMP Generator (FastAPI)
# ===============================================================
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
import sys
from pathlib import Path

# Ensure src is importable from project root
sys.path.append(str(Path(__file__).parent / "src"))

from core_pipeline_web import DMPPipeline

app = FastAPI()
pipeline = DMPPipeline()


# ---------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def form_page():
    """Render the input form (empty results initially)."""
    return render_form()


# ---------------------------------------------------------------
@app.post("/", response_class=HTMLResponse)
async def generate_dmp(
    request: Request,
    title: str = Form(...),
    research_context: str = Form(""),
    data_types: str = Form(""),
    data_source: str = Form(""),
    human_subjects: str = Form(""),
    consent_status: str = Form(""),
    data_volume: str = Form(""),
):
    """Generate NIH DMP and show results below the form."""
    form_inputs = {
        "research_context": research_context,
        "data_types": data_types,
        "data_source": data_source,
        "human_subjects": human_subjects,
        "consent_status": consent_status,
        "data_volume": data_volume,
    }

    # Run generation
    md_text = pipeline.generate_dmp(title, form_inputs)

    # Return the same form with the generated result appended below
    return render_form(result=md_text, title=title)


# ---------------------------------------------------------------
def render_form(result: str = "", title: str = ""):
    """Helper to render the HTML form + result (if any)."""
    result_html = ""
    if result:
        result_html = f"""
        <hr style="margin: 40px 0;">
        <h2>✅ NIH DMP Generated for: <i>{title}</i></h2>
        <pre style="white-space: pre-wrap; background:#f8f8f8; padding:15px; border-radius:8px;">{result}</pre>
        """

    return f"""
    <html>
    <head>
        <title>NIH DMP Generator</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; max-width: 900px; }}
            textarea, input {{ width: 100%; padding: 8px; margin-top: 4px; margin-bottom: 16px; }}
            textarea {{ height: 100px; }}
            button {{ padding: 10px 20px; background-color: #0078d7; color: white; border: none; border-radius: 4px; cursor: pointer; }}
            button:hover {{ background-color: #005fa3; }}
            pre {{ font-family: Consolas, monospace; font-size: 14px; line-height: 1.5; }}
        </style>
    </head>
    <body>
        <h1>🧠 NIH Data Management Plan Generator</h1>
        <form action="/" method="post">
            <label><b>Project Title:</b></label>
            <input type="text" name="title" required><br>

            <label><b>Brief summary of the research context:</b></label>
            <textarea name="research_context" placeholder="Describe the scientific goals and objectives..."></textarea><br>

            <label><b>Types of data to be collected:</b></label>
            <textarea name="data_types" placeholder="Genomic data, survey responses, imaging, etc."></textarea><br>

            <label><b>Source of data:</b></label>
            <textarea name="data_source" placeholder="Human participants, sensors, clinical instruments..."></textarea><br>

            <label><b>Human subjects involvement:</b></label>
            <textarea name="human_subjects" placeholder="Does the study involve human participants?"></textarea><br>

            <label><b>Data sharing consent status (if applicable):</b></label>
            <textarea name="consent_status" placeholder="Broad sharing approved? Any restrictions?"></textarea><br>

            <label><b>Estimated data volume, modality, and format:</b></label>
            <textarea name="data_volume" placeholder="Structured/unstructured data, formats (CSV, DICOM, etc.)"></textarea><br>

            <button type="submit">Generate DMP</button>
        </form>

        {result_html}
    </body>
    </html>
    """
