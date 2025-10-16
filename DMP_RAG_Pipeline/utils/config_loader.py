# ===============================
# config_loader.py
# Utility functions to reliably locate and load configuration files
# ===============================

from pathlib import Path
import os
import yaml

def _project_root() -> Path:
    """
    Return the absolute path to the project root directory.
    Logic:
      - __file__ gives the current file path (e.g., .../utils/config_loader.py)
      - .resolve() gives the absolute resolved path
      - .parents[1] goes two levels up to reach the project root
    """
    return Path(__file__).resolve().parents[1]


def load_config(config_path: str | None = None) -> dict:
    """
    Load configuration from a YAML file.

    Priority for resolving the config path:
      1. Explicit argument (config_path)
      2. Environment variable CONFIG_PATH
      3. Default path: <project_root>/config/config.yaml

    The function ensures:
      - Works even if the script is called from any current working directory (CWD)
      - Falls back to a safe default config path if not specified
      - Raises FileNotFoundError if no valid config file is found
    """

    # --- Step 1: Check environment variable for config path override ---
    env_path = os.getenv("CONFIG_PATH")

    # --- Step 2: Determine which path to use (priority order as above) ---
    if config_path is None:
        config_path = env_path or str(_project_root() / "config" / "config.yaml")

    # --- Step 3: Convert to Path object for path manipulations ---
    path = Path(config_path)

    # --- Step 4: Make relative paths absolute (based on project root) ---
    if not path.is_absolute():
        path = _project_root() / path

    # --- Step 5: Verify file existence ---
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    # --- Step 6: Open and load YAML configuration safely ---
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}  # Return an empty dict if file is empty


if __name__ == "__main__":
    config = load_config()
    print(config)
