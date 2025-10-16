from __future__ import annotations

import os
import uuid
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Any

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException


# =========================================================
#                    Session ID Utility
# =========================================================

def generate_session_id(prefix: str = "session") -> str:
    """
    Generate a unique session ID for ingestion or other pipeline stages.

    Example:
        >>> generate_session_id("ingest")
        'ingest_20251010_183040_87a2f1'
    """
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_uid = uuid.uuid4().hex[:6]
    session_id = f"{prefix}_{now}_{short_uid}"
    return session_id


# =========================================================
#                 File Save / Upload Utility
# =========================================================

def save_uploaded_files(
    uploaded_files: List[Any],
    dest_dir: str,
    allowed_exts: Optional[Set[str]] = None,
    keep_original_names: bool = True,
) -> List[str]:
    """
    Save uploaded files (e.g., from FastAPI, Streamlit, or Flask) to dest_dir.

    Args:
        uploaded_files: list of uploaded file-like objects.
        dest_dir: directory to save the uploaded files.
        allowed_exts: optional set of allowed extensions (e.g., {'.pdf'}).
        keep_original_names: if False, generate a unique name.

    Returns:
        List of saved file paths.

    Raises:
        DocumentPortalException: if saving fails or file type invalid.
    """
    saved_paths: List[str] = []
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    if not uploaded_files:
        log.warning("No uploaded files provided")
        return saved_paths

    for f in uploaded_files:
        try:
            # Determine file name
            if hasattr(f, "name"):
                original_name = os.path.basename(f.name)
            elif hasattr(f, "filename"):
                original_name = os.path.basename(f.filename)
            else:
                original_name = f"file_{uuid.uuid4().hex[:6]}"

            ext = Path(original_name).suffix.lower()
            if allowed_exts and ext not in allowed_exts:
                log.warning("Skipping disallowed file type", file=original_name)
                continue

            filename = original_name if keep_original_names else f"{uuid.uuid4().hex}{ext}"
            save_path = dest_path / filename

            # Read and write content
            if hasattr(f, "read"):
                with open(save_path, "wb") as out:
                    out.write(f.read())
            elif hasattr(f, "getbuffer"):
                with open(save_path, "wb") as out:
                    out.write(f.getbuffer())
            elif isinstance(f, (str, Path)) and Path(f).exists():
                shutil.copy2(f, save_path)
            else:
                raise ValueError("Unsupported file object type")

            log.info("Uploaded file saved", file=filename, dest=str(save_path))
            saved_paths.append(str(save_path))

        except Exception as e:
            log.error("Failed to save uploaded file", error=str(e))
            raise DocumentPortalException(f"Failed to save file {getattr(f, 'name', '?')}", e) from e

    return saved_paths
