# ==============================================================
# view_dmp_web.py — Simple Web Viewer for Generated NIH DMP JSONs
# ==============================================================
import json
import re
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# -----------------------------
# Configuration
# -----------------------------
DMP_JSON_DIR = Path("data/outputs/json/Secondary data analysis")  # adjust path if different
app = FastAPI(title="NIH DMP Web Viewer")

# Allow local frontend connections (optional)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static assets (optional CSS)
app.mount("/static", StaticFiles(directory="static"), name="static")


# -----------------------------
# Utility functions
# -----------------------------
def extract_elements_from_markdown(markdown_text: str):
    """
    Parse the 'generated_markdown' field into structured NIH elements.
    Looks for headings like: **Element 1: Data Type**
    """
    pattern = re.compile(r"\*\*Element (\d+): (.*?)\*\*(.*?)(?=\*\*Element|\Z)", re.S)
    elements = []
    for match in pattern.findall(markdown_text):
        num, title, content = match
        elements.append({
            "number": num.strip(),
            "title": title.strip(),
            "content": content.strip()
        })
    return elements


def load_dmp(filename: str):
    """Load a single DMP JSON file and parse its elements."""
    file_path = DMP_JSON_DIR / f"{filename}.json"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"DMP not found: {filename}")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["elements"] = extract_elements_from_markdown(data["generated_markdown"])
    return data


# -----------------------------
# Routes
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def list_dmps():
    """Homepage: list all available DMP files."""
    files = sorted(DMP_JSON_DIR.glob("*.json"))
    if not files:
        return "<h3>No DMP JSON files found.</h3>"

    links = "".join([
        f"<li><a href='/dmp/{f.stem}'>{f.stem}</a></li>"
        for f in files
    ])
    html = f"""
    <html>
    <head>
        <title>NIH DMP Viewer</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #1b4d89; }}
            a {{ text-decoration: none; color: #007bff; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <h1>📑 Available NIH DMP Files</h1>
        <ul>{links}</ul>
    </body>
    </html>
    """
    return html


@app.get("/dmp/{filename}", response_class=HTMLResponse)
def show_dmp(filename: str):
    """Display one DMP file with all NIH elements."""
    data = load_dmp(filename)

    # Build HTML dynamically
    element_html = ""
    for el in data["elements"]:
        element_html += f"""
        <div style='margin-bottom: 30px;'>
            <h3>Element {el["number"]}: {el["title"]}</h3>
            <pre style='white-space: pre-wrap; background: #f7f7f7; padding: 10px; border-radius: 8px;'>{el["content"]}</pre>
        </div>
        """

    html = f"""
    <html>
    <head>
        <title>{data['title']}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #1b4d89; }}
            pre {{ font-size: 15px; }}
        </style>
    </head>
    <body>
        <a href='/'>← Back to list</a>
        <h1>{data['title']}</h1>
        <p><b>Query:</b> {data['query']}</p>
        <hr>
        {element_html}
    </body>
    </html>
    """
    return html


# -----------------------------
# Run locally
# -----------------------------
if __name__ == "__main__":
    if not DMP_JSON_DIR.exists():
        raise FileNotFoundError(f"❌ DMP JSON directory not found: {DMP_JSON_DIR}")
    print(f"🚀 Starting DMP Viewer — open http://127.0.0.1:8000/")
    uvicorn.run(app, host="127.0.0.1", port=8000)
