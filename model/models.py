# models.py
from enum import Enum

class PromptType(Enum):
    """Enum for all supported prompt templates."""
    DMP_DATA_TYPES = "dmp_data_types"
    DMP_METADATA_STANDARDS = "dmp_metadata"
    DMP_ACCESS_SHARING = "dmp_access"
    DMP_PRESERVATION = "dmp_preservation"
    DMP_OVERSIGHT_QA = "dmp_oversight"
