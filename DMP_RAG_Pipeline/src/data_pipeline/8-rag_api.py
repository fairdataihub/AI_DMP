# rag_api.py
# ---------------------------------------------------------
# FastAPI service for the RAG-based DMP Generator
# ---------------------------------------------------------
# Endpoints:
#   POST /generate_dmp      → run RAG pipeline and return results
#   GET  /health            → quick health check
# ---------------------------------------------------------

import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional

# --- Internal imports ---
from data_pipeline.rag_dmp_runner import run_rag_dmp
from exception.custom_exception import DocumentPortalException
from logger import GLOBAL_LOGGER as log


# =========================================================
# FastAPI App Initialization
# =========================================================
app = FastAPI(
    title="DMP-RAG API",
    description="An AI-driven API for generating NIH-style Data Management Plans using Retrieval-Augmented Generation.",
    version="1.0.0",
)


# =========================================================
# Request / Response Models
# =========================================================
class ProjectInfo(BaseModel):
    project_title: str
    pi_name: Optional[str] = None
    institution: Optional[str] = None
    grant_id: Optional[str] = None
    repository_preferences: Optional[str] = None
    privacy_constraints: Optional[str] = None
    additional_fields: Optional[Dict[str, Any]] = None


class DMPRequest(BaseModel):
    project_info: ProjectInfo
    rebuild_index: bool = False
    top_k: int = 6
    out_name: str = "generated_dmp"


class DMPResponse(BaseModel):
    sections: Dict[str, str]
    markdown_path: str
    docx_path: Optional[str] = None
    index_dir: str


# =========================================================
# Health Check Endpoint
# =========================================================
@app.get("/health")
async def health_check():
    """Simple health endpoint to verify API is running."""
    return {"status": "ok", "message": "DMP-RAG API is running"}


# =========================================================
# Main DMP Generation Endpoint
# =========================================================
@app.post("/generate_dmp", response_model=DMPResponse)
async def generate_dmp(request: DMPRequest):
    """
    Run the full RAG DMP generation pipeline.
    Returns generated sections and output file paths.
    """
    try:
        log.info("📩 Received DMP generation request")

        # Base data directory (adjust path if needed)
        base_dir = str(Path("C:/Users/Nahid/DMP-RAG/data").resolve())

        # Convert Pydantic model to plain dict
        project_info = request.project_info.dict(exclude_none=True)

        result = run_rag_dmp(
            base_dir=base_dir,
            project_info=project_info,
            rebuild_index=request.rebuild_index,
            top_k=request.top_k,
            out_name=request.out_name,
        )

        log.info("✅ DMP generation completed successfully")
        return DMPResponse(**result)

    except DocumentPortalException as e:
        log.error("❌ DocumentPortalException during DMP generation", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.error("❌ Unexpected error in DMP generation", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")


# =========================================================
# CLI Launch (for local testing)
# =========================================================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "rag_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
