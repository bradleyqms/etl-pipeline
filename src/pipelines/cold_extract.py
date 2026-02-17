"""
pipelines/cold_extract.py — Cold_Extract email → Bronze blob

Thin wrapper: loads config + delegates to pipeline_runner.
"""

import logging

from ..core.pipeline_runner import process_emails
from .config import get_pipeline

log = logging.getLogger(__name__)


def run(dry_run: bool = False, include_processed: bool = False, flat: bool = False) -> dict:
    """Run the Cold_Extract email → blob pipeline."""
    cfg = get_pipeline("cold_extract")
    log.info("cold_extract — subject='%s', dest=bronze/%s/", cfg["subject_filter"], cfg["blob_prefix"])
    return process_emails(cfg, dry_run=dry_run, include_processed=include_processed, flat=flat)
