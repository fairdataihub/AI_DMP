from __future__ import annotations

import os
import shutil
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

# --- Core project imports ---
# add top-level path in case script is run directly
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from logger.custom_logger import GLOBAL_LOGGER as log
from exception.custom_exception import DocumentPortalException
from utils.file_io import generate_session_id, save_uploaded_files


class DataIngestion:
    """
    Step 1 of the DMP-RAG data pipeline.
    --------------------------------------------------
    • Accepts local or uploaded PDF files.
    • Computes SHA-256 hash for deduplication.
    • Stores PDFs under a session directory:
          data/ingested/<session_id>/raw_pdfs/
    • Updates manifest.json with file metadata.
    """

    def __init__(self, data_root: Optional[str] = None, session_id: Optional[str] = None):
        # --- Paths ---
        self.data_root = Path(data_root or os.getenv("DATA_STORAGE_PATH", Path.cwd() / "data"))
        self.ingest_root = self.data_root / "ingested"
        self.manifest_path = self.data_root / "manifest.json"

        # --- Session setup ---
        self.session_id = session_id or generate_session_id("session")
        self.session_dir = self.ingest_root / self.session_id
        self.raw_dir = self.session_dir / "raw_pdfs"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # --- Load or init manifest ---
        self.manifest = self._load_manifest()

        log.info(
            "DataIngestion initialized",
            session_id=self.session_id,
            session_path=str(self.session_dir),
        )

    # =========================================================
    #                Manifest & Utility Methods
    # =========================================================

    def _load_manifest(self) -> Dict[str, Any]:
        """Load manifest.json or start new one."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log.error("Failed to read manifest.json", error=str(e))
                raise DocumentPortalException("Could not load manifest.json", e)
        return {"files": {}, "sessions": {}}

    def _save_manifest(self) -> None:
        """Persist manifest.json safely."""
        try:
            self.data_root.mkdir(parents=True, exist_ok=True)
            with open(self.manifest_path, "w", encoding="utf-8") as f:
                json.dump(self.manifest, f, indent=2)
        except Exception as e:
            log.error("Failed to save manifest.json", error=str(e))
            raise DocumentPortalException("Manifest save failed", e)

    @staticmethod
    def _compute_hash(path: Path) -> str:
        """Return SHA-256 checksum of file content."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    # =========================================================
    #                   Public Ingestion APIs
    # =========================================================

    def ingest_local_paths(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Ingest PDFs from local directories or files.
        Skips non-existent or duplicate files.
        """
        added_files: List[str] = []
        for path_str in file_paths:
            path = Path(path_str)
            if not path.exists():
                log.warning("Path does not exist; skipping", path=path_str)
                continue

            if path.is_dir():
                for pdf in path.rglob("*.pdf"):
                    added = self._copy_if_new(pdf)
                    if added:
                        added_files.append(added.name)
            elif path.suffix.lower() == ".pdf":
                added = self._copy_if_new(path)
                if added:
                    added_files.append(added.name)
            else:
                log.warning("Skipping non-PDF file", file=path_str)

        return self._finalize_session(added_files, source="local")

    def ingest_uploaded(self, uploaded_files: List[Any]) -> Dict[str, Any]:
        """
        Ingest PDFs uploaded via a web interface (Streamlit / FastAPI).
        Uses utils.save_uploaded_files() and applies same dedup logic.
        """
        try:
            saved_files = save_uploaded_files(
                uploaded_files,
                dest_dir=str(self.raw_dir),
                allowed_exts={".pdf"},
                keep_original_names=True,
            )
        except Exception as e:
            log.error("Upload ingestion failed", error=str(e))
            raise DocumentPortalException("Upload ingestion failed", e)

        added_files: List[str] = []
        for saved_path in saved_files:
            p = Path(saved_path)
            added = self._copy_if_new(p, already_in_raw=True)
            if added:
                added_files.append(added.name)

        return self._finalize_session(added_files, source="upload")

    # =========================================================
    #                      Core Logic
    # =========================================================

    def _copy_if_new(self, src: Path, already_in_raw: bool = False) -> Optional[Path]:
        """
        Copy file into session/raw_pdfs if hash not already seen.
        If already_in_raw=True, skip copying but still register in manifest.
        """
        try:
            file_hash = self._compute_hash(src)
        except Exception as e:
            log.error("Hashing failed", file=str(src), error=str(e))
            return None

        if file_hash in self.manifest["files"]:
            log.info("Duplicate detected; skipping", file=str(src))
            return None

        dest = self.raw_dir / src.name
        if not already_in_raw:
            try:
                shutil.copy2(src, dest)
            except Exception as e:
                log.error("File copy failed", src=str(src), dest=str(dest), error=str(e))
                return None
        else:
            dest = src  # file already present

        # Record in manifest
        stat = dest.stat()
        self.manifest["files"][file_hash] = {
            "filename": dest.name,
            "size": stat.st_size,
            "source": "upload" if already_in_raw else "local",
            "session": self.session_id,
            "added_at": datetime.utcnow().isoformat(),
        }

        log.info("File added", file=str(dest), size=stat.st_size)
        return dest

    def _finalize_session(self, added_files: List[str], source: str) -> Dict[str, Any]:
        """Update manifest with session info and return structured summary."""
        self.manifest["sessions"][self.session_id] = {
            "created_at": datetime.utcnow().isoformat(),
            "source": source,
            "files": added_files,
        }

        try:
            self._save_manifest()
        except DocumentPortalException as e:
            raise e
        except Exception as e:
            raise DocumentPortalException("Session finalize failed", e)

        summary = {
            "session_id": self.session_id,
            "session_dir": str(self.session_dir),
            "raw_dir": str(self.raw_dir),
            "file_count": len(added_files),
            "added_files": added_files,
        }

        log.info("Ingestion completed", **summary)
        return summary


# =========================================================
#                     Example Execution
# =========================================================
if __name__ == "__main__":
    ingestion = DataIngestion()
    result = ingestion.ingest_local_paths(["data/raw"])  # adjust your folder
    print(json.dumps(result, indent=2))
