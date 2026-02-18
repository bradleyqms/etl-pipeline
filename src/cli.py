#!/usr/bin/env python3
"""
cli.py — Unified CLI entry point for QMS ETL Pipeline

Usage:
  python -m src.cli ingest cold_extract              # Email → Bronze (state-tracked)
  python -m src.cli ingest dim_customer --dry-run     # Preview dim_customer ingest
  python -m src.cli ingest dim_product --all          # Re-process all dim_product emails

  python -m src.cli transform cold_extract            # Bronze CSV → Silver Parquet
  python -m src.cli transform dim_customer            # Customer master → Parquet
  python -m src.cli transform dim_product             # Product master → Parquet
  python -m src.cli transform all                     # Run all transforms

  python -m src.cli list                              # List all pipelines
  python -m src.cli test cold_extract                 # Connection test

Requires .env with Graph + Azure credentials.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Suppress verbose Azure SDK HTTP noise
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
log = logging.getLogger("qms-etl")


def cmd_ingest(args):
    """Run an email → blob ingest pipeline."""
    from .pipelines import config as cfg_mod
    from .core.pipeline_runner import process_emails

    pipeline_cfg = cfg_mod.get_pipeline(args.pipeline)

    # Validate credentials
    required = ("tenant_id", "client_id", "client_secret", "mailbox")
    missing = [k for k in required if not pipeline_cfg.get(k)]
    if missing:
        log.error("Missing .env variables: %s", ", ".join(missing))
        sys.exit(1)
    if not pipeline_cfg.get("storage_key") and not os.getenv("DATALAKE_CONNECTION_STRING"):
        log.error("Missing blob credentials (DATALAKE_ACCOUNT_KEY or DATALAKE_CONNECTION_STRING)")
        sys.exit(1)

    mode = ("DRY RUN" if args.dry_run
            else "ALL (ignore state)" if args.all
            else "New only (state-tracked)")

    log.info("=" * 55)
    log.info("%s → Data Lake — Starting", pipeline_cfg["subject_filter"])
    log.info("  Pipeline:  %s", args.pipeline)
    log.info("  Filter:    subject contains '%s'", pipeline_cfg["subject_filter"])
    log.info("  Upload to: %s/%s/%s/",
             pipeline_cfg["storage_account"], pipeline_cfg["container"], pipeline_cfg["blob_prefix"])
    log.info("  State:     %s", pipeline_cfg.get("state_blob_path", "?"))
    log.info("  Mode:      %s", mode)
    log.info("=" * 55)

    stats = process_emails(
        pipeline_cfg,
        dry_run=args.dry_run,
        include_processed=args.all,
        flat=args.flat,
        auto_transform=not args.no_transform,
    )

    # Summary
    log.info("=" * 55)
    log.info("DONE")
    log.info("  Emails found:     %d", stats["emails_found"])
    log.info("  Emails processed: %d", stats["emails_processed"])
    log.info("  Files uploaded:   %d", stats["files_uploaded"])
    if stats["files"]:
        log.info("  Blob paths:")
        for f in stats["files"]:
            log.info("    → bronze/%s", f)
    if stats.get("transform"):
        t = stats["transform"]
        log.info("  Transform:    %s (%d files, %s rows)",
                 t.get("status", "?"),
                 t.get("files_converted", 0),
                 t.get("total_rows", 0))
    if stats["errors"]:
        log.error("  Errors: %d", len(stats["errors"]))
        for err in stats["errors"]:
            log.error("    ❌ %s", err)
    log.info("=" * 55)

    sys.exit(1 if stats["errors"] else 0)


def cmd_transform(args):
    """Run a Bronze → Silver parquet transform."""
    from .transforms import cold_extract_to_parquet
    from .transforms import dim_customer_to_parquet
    from .transforms import dim_product_to_parquet
    from .transforms import fact_sales_daily_to_parquet

    transform_map = {
        "cold_extract":     cold_extract_to_parquet.transform,
        "fact_sales_daily": fact_sales_daily_to_parquet.transform,
        "dim_customer":     dim_customer_to_parquet.transform,
        "dim_product":      dim_product_to_parquet.transform,
    }

    if args.pipeline == "all":
        targets = list(transform_map.keys())
    else:
        targets = [args.pipeline]

    for name in targets:
        if name not in transform_map:
            log.error("Unknown transform: %s (available: %s)", name, ", ".join(transform_map))
            sys.exit(1)

        log.info("=" * 55)
        log.info("Transform: %s — Bronze CSV → Silver Parquet", name)
        if args.date:
            log.info("  Date: %s", args.date)
        log.info("  Dry run: %s", args.dry_run)
        log.info("=" * 55)

        result = transform_map[name](date=args.date, dry_run=args.dry_run)

        log.info("Result: status=%s, files=%s, rows=%s",
                 result.get("status"),
                 result.get("files_converted", 0),
                 result.get("total_rows", 0))

        if result.get("compression_ratio"):
            log.info("  Compression: %s", result["compression_ratio"])
        if result.get("latest"):
            latest = result["latest"]
            log.info("  Latest parquet: %s (%d rows)",
                     latest.get("path", "?"), latest.get("rows", 0))


def cmd_list(args):
    """List all available pipelines."""
    from .pipelines.config import list_pipelines
    pipelines = list_pipelines()

    print("\n  QMS ETL Pipelines")
    print("  " + "─" * 55)
    for name, desc in sorted(pipelines.items()):
        planned = " [PLANNED]" if "[PLANNED]" in desc else ""
        print(f"  {name:<20s}  {desc}")
    print()


def cmd_test(args):
    """Connection test for a pipeline."""
    from .pipelines.config import get_pipeline
    from .core.graph_client import get_graph_token, find_mail_folder, get_matching_emails
    from .core.blob_client import get_container_client, load_state, list_recent_blobs

    cfg = get_pipeline(args.pipeline)

    print("\n" + "=" * 60)
    print(f"  {args.pipeline} — Connection Test")
    print("=" * 60)

    # 1. Auth
    print("\n[1/4] Graph API Authentication")
    try:
        token = get_graph_token(cfg)
        print("  ✅ Token acquired")
    except Exception as e:
        print(f"  ❌ Auth failed: {e}")
        return

    # 2. Mail folder
    print(f"\n[2/4] Mail folder '{cfg['mail_folder']}'")
    try:
        folder_id = find_mail_folder(token, cfg["mailbox"], cfg["mail_folder"])
        print(f"  ✅ Folder found")
    except Exception as e:
        print(f"  ❌ {e}")
        return

    # 3. Emails
    print(f"\n[3/4] Emails matching '{cfg['subject_filter']}'")
    try:
        emails = get_matching_emails(
            token, cfg["mailbox"], folder_id,
            cfg["subject_filter"], include_processed=True,
        )
        state = load_state(cfg)
        seen = set(state.get("processed_ids", []))
        new_emails = [e for e in emails if e["id"] not in seen]
        print(f"  ✅ {len(emails)} matching ({len(new_emails)} new, "
              f"{len(emails) - len(new_emails)} processed)")
        if state.get("last_run"):
            print(f"  📋 State: {len(seen)} IDs (last run: {state['last_run'][:16]})")
        for e in emails[:3]:
            icon = "✅" if e["id"] in seen else "🆕"
            print(f"      {icon} '{e['subject']}' — {e['receivedDateTime'][:16]}")
    except Exception as e:
        print(f"  ❌ {e}")

    # 4. Blob Storage
    print(f"\n[4/4] Blob Storage ({cfg['storage_account']}/{cfg['container']})")
    try:
        client = get_container_client(
            account=cfg.get("storage_account"),
            key=cfg.get("storage_key"),
            container=cfg.get("container", "bronze"),
        )
        blobs = list_recent_blobs(client, cfg["blob_prefix"], limit=5)
        print(f"  ✅ Container accessible")
        if blobs:
            print(f"  Recent files in bronze/{cfg['blob_prefix']}/:")
            for b in blobs:
                size = b.size or 0
                mod = b.last_modified.strftime("%Y-%m-%d %H:%M") if b.last_modified else "?"
                print(f"      📄 {b.name} ({size:,} B, {mod})")
        else:
            print(f"  ⚠️  No files yet in bronze/{cfg['blob_prefix']}/")
    except Exception as e:
        print(f"  ❌ Blob Storage failed: {e}")
        return

    print("\n" + "=" * 60)
    print("  All checks passed ✅")
    print("=" * 60 + "\n")


# ═════════════════════════════════════════════
# Argument parser
# ═════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="QMS ETL Pipeline — Email → Data Lake → Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── ingest ──
    ingest_p = sub.add_parser("ingest", help="Email → Bronze blob ingest")
    ingest_p.add_argument("pipeline",
                          choices=["cold_extract", "fact_sales_daily",
                                   "dim_customer", "dim_product", "dim_salesperson"],
                          help="Pipeline to run")
    ingest_p.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    ingest_p.add_argument("--all", action="store_true", help="Reprocess all (ignore state)")
    ingest_p.add_argument("--flat", action="store_true", help="No date subfolder")
    ingest_p.add_argument("--no-transform", action="store_true",
                          help="Skip auto-transform after ingest")
    ingest_p.set_defaults(func=cmd_ingest)

    # ── transform ──
    transform_p = sub.add_parser("transform", help="Bronze CSV → Silver Parquet")
    transform_p.add_argument("pipeline",
                             choices=["cold_extract", "fact_sales_daily",
                                      "dim_customer", "dim_product", "all"],
                             help="Transform to run (or 'all')")
    transform_p.add_argument("--date", help="Specific date folder (YYYY-MM-DD)")
    transform_p.add_argument("--dry-run", action="store_true", help="Preview without writing")
    transform_p.set_defaults(func=cmd_transform)

    # ── list ──
    list_p = sub.add_parser("list", help="List all pipelines")
    list_p.set_defaults(func=cmd_list)

    # ── test ──
    test_p = sub.add_parser("test", help="Connection test")
    test_p.add_argument("pipeline",
                        choices=["cold_extract", "fact_sales_daily",
                                 "dim_customer", "dim_product"],
                        help="Pipeline to test")
    test_p.set_defaults(func=cmd_test)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
