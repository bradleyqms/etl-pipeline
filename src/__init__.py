# QMS ETL Pipeline
# Load .env at package import time so env vars are always available
from pathlib import Path as _Path
from dotenv import load_dotenv as _load_dotenv

_load_dotenv(_Path(__file__).parent.parent / ".env")
