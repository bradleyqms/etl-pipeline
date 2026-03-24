from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import requests

from .graph_client import GRAPH_BASE, get_graph_token

log = logging.getLogger(__name__)

# Blob path template for per-pipeline dedupe state
ALERT_DEDUPE_BLOB = "state/{pipeline_name}_alert_dedupe.json"


def alerts_enabled() -> bool:
    return os.getenv("ALERT_ENABLED", "true").strip().lower() in {"1", "true", "yes"}


def configured_alert_recipients() -> list[str]:
    raw = os.getenv("ALERT_EMAIL_TO", "")
    if not raw.strip():
        return []
    parts = raw.replace(";", ",").split(",")
    return [value.strip() for value in parts if value.strip()]


# ─────────────────────────────────────────────
# Deduplification helpers
# ─────────────────────────────────────────────

def _compute_alert_key(pipeline_name: str, run_date: str | None, result: dict) -> str:
    """Derive a stable fingerprint for a failure event.

    Key is: ``{pipeline}:{run_date}:{sha1[:12]}``
    The SHA-1 input is the sorted union of error strings and dead-letter
    source-blob paths, so re-running the same broken file produces the
    same key while a *new* file failing on the same date produces a
    different key (and a new alert).
    """
    error_parts = sorted(str(e) for e in (result.get("errors") or []))
    dl_parts = sorted(
        entry.get("source_blob", "")
        for entry in (result.get("dead_letter_files") or [])
    )
    transform = result.get("transform") or {}
    if transform.get("status") == "error":
        error_parts.append(f"transform:{transform.get('error', '')}")
    fingerprint = "|".join(error_parts + dl_parts) or result.get("status", "error")
    short = hashlib.sha1(fingerprint.encode()).hexdigest()[:12]
    return f"{pipeline_name}:{run_date or 'unknown'}:{short}"


def _load_alert_dedupe(cfg: dict, pipeline_name: str) -> dict:
    """Load the last alert key for this pipeline from blob state.

    Returns ``{}`` on any failure so the caller always gets a safe default.
    """
    from .blob_client import _get_blob_client_for_state

    blob_path = ALERT_DEDUPE_BLOB.format(pipeline_name=pipeline_name)
    try:
        blob = _get_blob_client_for_state(cfg, blob_path)
        data = blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        return {}


def _save_alert_dedupe(cfg: dict, pipeline_name: str, key: str) -> None:
    """Persist the last alert key so identical future failures are suppressed."""
    from .blob_client import _get_blob_client_for_state

    blob_path = ALERT_DEDUPE_BLOB.format(pipeline_name=pipeline_name)
    payload = {"last_key": key, "sent_at": datetime.now(timezone.utc).isoformat()}
    try:
        blob = _get_blob_client_for_state(cfg, blob_path)
        blob.upload_blob(json.dumps(payload, indent=2), overwrite=True)
    except Exception as exc:
        log.warning("Could not save alert dedupe state for %s: %s", pipeline_name, exc)

def should_send_failure_alert(result: dict | None) -> bool:
    if not result:
        return False
    if result.get("errors"):
        return True
    if result.get("status") == "error":
        return True
    if result.get("dead_letter_files"):
        return True

    transform = result.get("transform") or {}
    if transform.get("status") == "error":
        return True
    if transform.get("dead_letter_files"):
        return True
    return False


def build_failure_alert(
    *,
    pipeline_name: str,
    environment: str,
    result: dict,
    file_name: str | None = None,
    run_date: str | None = None,
) -> tuple[str, str]:
    recipients = ", ".join(configured_alert_recipients()) or "unconfigured"
    error_lines = result.get("errors") or []
    transform = result.get("transform") or {}
    dead_letter_files = result.get("dead_letter_files") or []

    subject = f"[ETL {environment}] {pipeline_name} failure"
    body_lines = [
        f"Pipeline: {pipeline_name}",
        f"Environment: {environment}",
        f"Run date: {run_date or result.get('date') or 'unknown'}",
        f"File: {file_name or 'n/a'}",
        f"Recipients: {recipients}",
        "",
    ]
    if error_lines:
        body_lines.append("Errors:")
        body_lines.extend(f"- {entry}" for entry in error_lines)
        body_lines.append("")
    if transform.get("status") == "error":
        body_lines.extend([
            "Transform:",
            f"- status: {transform.get('status')}",
            f"- error: {transform.get('error')}",
            "",
        ])
    if dead_letter_files:
        body_lines.append("Dead-lettered files:")
        for entry in dead_letter_files:
            body_lines.append(
                f"- {entry.get('source_blob', entry.get('dead_letter_blob', 'unknown'))}: {entry.get('reason', 'validation_failed')}"
            )
    return subject, "\n".join(body_lines)


def send_failure_alert(
    cfg: dict,
    *,
    pipeline_name: str,
    result: dict,
    environment: str = "prod",
    file_name: str | None = None,
    run_date: str | None = None,
) -> bool:
    recipients = configured_alert_recipients()
    if not alerts_enabled() or not recipients:
        return False

    # Resolve run date once so it is consistent in both the key and the email body
    effective_run_date = run_date or result.get("date")

    # Deduplicate: suppress if this exact failure was already alerted
    alert_key = _compute_alert_key(pipeline_name, effective_run_date, result)
    dedupe_state = _load_alert_dedupe(cfg, pipeline_name)
    if dedupe_state.get("last_key") == alert_key:
        log.info("%s: alert suppressed (duplicate key %s)", pipeline_name, alert_key)
        return False

    subject, body = build_failure_alert(
        pipeline_name=pipeline_name,
        environment=environment,
        result=result,
        file_name=file_name,
        run_date=effective_run_date,
    )

    token = get_graph_token(cfg)
    url = f"{GRAPH_BASE}/users/{cfg['mailbox']}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [
                {"emailAddress": {"address": address}}
                for address in recipients
            ],
        },
        "saveToSentItems": "false",
    }
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    _save_alert_dedupe(cfg, pipeline_name, alert_key)
    return True
