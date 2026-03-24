from __future__ import annotations

import os
from typing import Iterable

import requests

from .graph_client import GRAPH_BASE, get_graph_token


def alerts_enabled() -> bool:
    return os.getenv("ALERT_ENABLED", "true").strip().lower() in {"1", "true", "yes"}


def configured_alert_recipients() -> list[str]:
    raw = os.getenv("ALERT_EMAIL_TO", "")
    if not raw.strip():
        return []
    parts = raw.replace(";", ",").split(",")
    return [value.strip() for value in parts if value.strip()]


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

    subject, body = build_failure_alert(
        pipeline_name=pipeline_name,
        environment=environment,
        result=result,
        file_name=file_name,
        run_date=run_date,
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
    return True
