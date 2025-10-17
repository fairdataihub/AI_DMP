from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from src.core_pipeline_web import DMPPipeline

app = FastAPI()
pipeline = DMPPipeline()

@app.post("/generate", response_class=HTMLResponse)
async def generate_dmp(title: str = Form(...), **form_inputs):
    md_text = pipeline.generate_dmp(title, form_inputs)
    return f"""
    <html>
    <body>
      <h2>✅ NIH DMP Generated for: {title}</h2>
      <pre style="white-space: pre-wrap;">{md_text}</pre>
      <a href="/">← Back</a>
    </body>
    </html>
    """
