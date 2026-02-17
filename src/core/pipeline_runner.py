"""
core/pipeline_runner.py — Generic email → blob pipeline engine

Takes a fully-merged pipeline config dict and orchestrates:
  1. Graph API auth
  2. Find mail folder
  3. Fetch matching emails (state-aware)
  4. Download attachments → upload to blob
  5. Mark as read + update state
"""

import base64
import logging

from .graph_client import (
    get_graph_token,
    find_mail_folder,
    get_matching_emails,
    get_attachments,
    mark_as_read,
)
from .blob_client import (
    get_container_client,
    upload_to_blob,
    load_state,
    save_state,
)

log = logging.getLogger(__name__)


def process_emails(
    cfg: dict,
    dry_run: bool = False,
    include_processed: bool = False,
    flat: bool = False,
) -> dict:
    """
    Main email → blob pipeline.

    ``cfg`` must contain all keys (merged by get_pipeline()):
      tenant_id, client_id, client_secret, mailbox,
      subject_filter, mail_folder, storage_account, storage_key,
      container, blob_prefix, state_blob_path
    """
    pipeline_name = cfg.get("subject_filter", "?")

    stats = {
        "pipeline":         f"{pipeline_name} → Data Lake",
        "storage":          f"{cfg['storage_account']}/{cfg['container']}",
        "emails_found":     0,
        "emails_processed": 0,
        "files_uploaded":   0,
        "errors":           [],
        "files":            [],
    }

    # 1. Authenticate
    token = get_graph_token(cfg)

    # 2. Find mail folder
    folder_id = find_mail_folder(token, cfg["mailbox"], cfg["mail_folder"])

    # 3. Load state + fetch matching emails
    state = load_state(cfg)
    processed_ids = set(state.get("processed_ids", []))

    emails = get_matching_emails(
        token,
        cfg["mailbox"],
        folder_id,
        cfg["subject_filter"],
        processed_ids=processed_ids,
        include_processed=include_processed,
    )
    stats["emails_found"] = len(emails)

    if not emails:
        log.info("Nothing to process")
        return stats

    # 4. Connect to Blob Storage
    container_client = None
    if not dry_run:
        container_client = get_container_client(
            account=cfg.get("storage_account"),
            key=cfg.get("storage_key"),
            container=cfg.get("container", "bronze"),
        )
        log.info(
            "Connected to Blob Storage: %s/%s",
            cfg["storage_account"], cfg["container"],
        )

    # 5. Process each email
    for email in emails:
        msg_id = email["id"]
        subject = email.get("subject", "?")
        received = email.get("receivedDateTime", "?")
        sender = (
            email.get("from", {}).get("emailAddress", {}).get("address", "?")
        )
        email_date = received[:10] if len(received) >= 10 else "unknown"

        log.info("─" * 50)
        log.info("Processing: '%s' from %s (%s)", subject, sender, received)

        try:
            attachments = get_attachments(token, cfg["mailbox"], msg_id)
            log.info("  %d file attachment(s)", len(attachments))

            for att in attachments:
                filename = att["name"].replace("/", "_").replace("\\", "_")
                content = base64.b64decode(att["contentBytes"])

                if flat:
                    blob_path = f"{cfg['blob_prefix']}/{filename}"
                else:
                    blob_path = f"{cfg['blob_prefix']}/{email_date}/{filename}"

                if dry_run:
                    log.info(
                        "  [DRY RUN] Would upload: %s → bronze/%s (%s bytes)",
                        filename, blob_path, f"{len(content):,}",
                    )
                else:
                    upload_to_blob(container_client, blob_path, content)

                stats["files_uploaded"] += 1
                stats["files"].append(blob_path)

            # Mark read + update state
            if not dry_run:
                mark_as_read(token, cfg["mailbox"], msg_id)
                if msg_id not in state["processed_ids"]:
                    state["processed_ids"].append(msg_id)
                save_state(state, cfg)
                log.info("  Marked as read + recorded in state")

            stats["emails_processed"] += 1

        except Exception as e:
            error = f"Error on '{subject}': {e}"
            log.error("  ❌ %s", error)
            stats["errors"].append(error)

    return stats
