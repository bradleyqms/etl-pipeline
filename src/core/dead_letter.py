from __future__ import annotations

import json
from pathlib import PurePosixPath


DEAD_LETTER_PREFIX = "dead_letter"


def quarantine_blob(
    container_client,
    *,
    pipeline: str,
    source_blob_name: str,
    raw_bytes: bytes,
    run_date: str,
    reason: str,
    details: list[dict] | dict,
) -> dict:
    """Persist a failed blob and its metadata under the dead-letter prefix."""
    file_name = PurePosixPath(source_blob_name).name
    blob_path = f"{DEAD_LETTER_PREFIX}/{run_date}/{file_name}"
    metadata_path = f"{blob_path}.error.json"
    payload = {
        "pipeline": pipeline,
        "source_blob": source_blob_name,
        "dead_letter_blob": blob_path,
        "reason": reason,
        "details": details,
        "run_date": run_date,
    }

    container_client.upload_blob(name=blob_path, data=raw_bytes, overwrite=True)
    container_client.upload_blob(
        name=metadata_path,
        data=json.dumps(payload, indent=2).encode("utf-8"),
        overwrite=True,
    )
    payload["metadata_blob"] = metadata_path
    return payload
