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


def run_transform(transform_name: str, date: str | None = None) -> dict:
    """Run a Bronze → Silver transform by name. Returns result dict."""
    from ..transforms import cold_extract_to_parquet
    from ..transforms import dim_customer_to_parquet
    from ..transforms import dim_product_to_parquet
    from ..transforms import fact_sales_daily_to_parquet

    transform_map = {
        "cold_extract":     cold_extract_to_parquet.transform,
        "dim_customer":     dim_customer_to_parquet.transform,
        "dim_product":      dim_product_to_parquet.transform,
        "fact_sales_daily": fact_sales_daily_to_parquet.transform,
    }
    if transform_name not in transform_map:
        raise KeyError(f"Unknown transform '{transform_name}'. "
                       f"Available: {', '.join(transform_map)}")
    return transform_map[transform_name](date=date)


def process_emails(
    cfg: dict,
    dry_run: bool = False,
    include_processed: bool = False,
    flat: bool = False,
    auto_transform: bool = True,
) -> dict:
    """
    Main email → blob pipeline.

    ``cfg`` must contain all keys (merged by get_pipeline()):
      tenant_id, client_id, client_secret, mailbox,
      subject_filter, mail_folder, storage_account, storage_key,
      container, blob_prefix, state_blob_path

    If ``auto_transform`` is True and the pipeline has a ``transform_name``,
    the corresponding Bronze → Silver transform runs automatically after
    new files are uploaded.
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

    # ── Auto-transform: Bronze CSV → Silver Parquet ──
    transform_name = cfg.get("transform_name")
    if (
        auto_transform
        and not dry_run
        and transform_name
        and stats["files_uploaded"] > 0
        and not stats["errors"]
    ):
        log.info("─" * 50)
        log.info("Auto-transform: %s — Bronze CSV → Silver Parquet", transform_name)
        try:
            result = run_transform(transform_name)
            stats["transform"] = result
            log.info(
                "Auto-transform complete: status=%s, files=%d, rows=%s",
                result.get("status"),
                result.get("files_converted", 0),
                result.get("total_rows", 0),
            )
        except Exception as e:
            error = f"Auto-transform '{transform_name}' failed: {e}"
            log.error("  ❌ %s", error)
            stats["errors"].append(error)
            stats["transform"] = {"status": "error", "error": str(e)}
    elif auto_transform and transform_name and stats["files_uploaded"] == 0:
        log.info("Auto-transform skipped: no new files uploaded")

    return stats
