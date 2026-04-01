"""Manual trigger: run dim_tables ingest + transform, then rebuild gold."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.pipelines.config import get_pipeline
from src.core.pipeline_runner import process_emails
from src.transforms import dim_tables_to_parquet
from src.transforms import build_gold

print("=== Step 1: Ingest dim_tables email attachments → blob ===")
cfg = get_pipeline("dim_tables")
result = process_emails(cfg, dry_run=False, include_processed=False, auto_transform=False)
print("emails_processed:", result.get("emails_processed", 0))
print("files_uploaded:  ", result.get("files_uploaded", 0))
print("errors:          ", result.get("errors", []))
print()

if result.get("files_uploaded", 0) == 0:
    print("No new files uploaded — checking if already processed...")
    result2 = process_emails(cfg, dry_run=False, include_processed=True, auto_transform=False)
    print("With include_processed=True:")
    print("emails_processed:", result2.get("emails_processed", 0))
    print("files_uploaded:  ", result2.get("files_uploaded", 0))
    print()

print("=== Step 2: dim_tables CSV → Silver Parquet ===")
r2 = dim_tables_to_parquet.transform()
print("status:", r2.get("status"))
print("files_converted:", r2.get("files_converted"))
print("total_rows:", r2.get("total_rows"))
print()

print("=== Step 3: Rebuild gold ===")
r3 = build_gold.transform()
print("status:", r3.get("status"))
print("fact_sales:", r3.get("fact_sales"))
print("fact_budget:", r3.get("fact_budget"))
print("dim_customer:", r3.get("dim_customer"))
