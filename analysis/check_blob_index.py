import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from src.core.blob_client import get_container_client

c = get_container_client()
blobs = list(c.list_blobs())

prefixes = sorted(set(b["name"].split("/")[0] for b in blobs))
print("Top-level prefixes:", prefixes)
print()

cold_blobs = sorted([b for b in blobs if "cold_extract" in b["name"]],
                    key=lambda b: b["last_modified"], reverse=True)
print("All cold_extract files (most recent first):")
for b in cold_blobs[:25]:
    ts = b["last_modified"].strftime("%Y-%m-%d %H:%M")
    name = b["name"]
    kb = b["size"] / 1024
    print(f"  {ts}  {name}  ({kb:.0f} KB)")

print()
# Also look for any blob modified today (April 1)
print("All blobs modified on 2026-04-01:")
for b in blobs:
    if b["last_modified"].date().isoformat() == "2026-04-01":
        ts = b["last_modified"].strftime("%Y-%m-%d %H:%M")
        name = b["name"]
        kb = b["size"] / 1024
        print(f"  {ts}  {name}  ({kb:.0f} KB)")
