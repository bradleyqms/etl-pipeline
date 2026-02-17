"""
core/graph_client.py — Microsoft Graph API helpers

All functions take explicit config dicts — no global state.
"""

import logging

import msal
import requests

log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


# ═════════════════════════════════════════════
# Authentication
# ═════════════════════════════════════════════

def get_graph_token(cfg: dict) -> str:
    """Acquire Microsoft Graph access token via MSAL client credentials."""
    authority = f"https://login.microsoftonline.com/{cfg['tenant_id']}"
    app = msal.ConfidentialClientApplication(
        client_id=cfg["client_id"],
        client_credential=cfg["client_secret"],
        authority=authority,
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        raise RuntimeError(
            f"Auth failed: {result.get('error_description', result.get('error'))}"
        )
    log.info("Graph API authenticated (token expires in %ss)", result.get("expires_in"))
    return result["access_token"]


# ═════════════════════════════════════════════
# Low-level REST helpers
# ═════════════════════════════════════════════

def graph_get(token: str, url: str) -> dict:
    """GET from Graph API, raise on error."""
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json()


def graph_patch(token: str, url: str, body: dict) -> None:
    """PATCH to Graph API, raise on error."""
    resp = requests.patch(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
    )
    resp.raise_for_status()


# ═════════════════════════════════════════════
# Mailbox operations
# ═════════════════════════════════════════════

def find_mail_folder(token: str, mailbox: str, folder_name: str) -> str:
    """Find a mail folder by name — checks top-level and Inbox subfolders."""
    base = f"{GRAPH_BASE}/users/{mailbox}"

    data = graph_get(token, f"{base}/mailFolders?$top=100")
    for f in data.get("value", []):
        if f["displayName"].lower() == folder_name.lower():
            log.info("Found folder '%s' (%d items)", folder_name, f["totalItemCount"])
            return f["id"]

    # Check Inbox subfolders
    inbox_id = next(
        (f["id"] for f in data.get("value", []) if f["displayName"] == "Inbox"),
        None,
    )
    if inbox_id:
        subs = graph_get(token, f"{base}/mailFolders/{inbox_id}/childFolders?$top=100")
        for f in subs.get("value", []):
            if f["displayName"].lower() == folder_name.lower():
                log.info("Found subfolder '%s' (%d items)", folder_name, f["totalItemCount"])
                return f["id"]

    raise RuntimeError(f"Mail folder '{folder_name}' not found")


def get_matching_emails(
    token: str,
    mailbox: str,
    folder_id: str,
    subject_filter: str,
    processed_ids: set | None = None,
    include_processed: bool = False,
) -> list:
    """Get emails matching a subject filter.

    Always fetches ALL emails (regardless of read status) and uses
    ``processed_ids`` to skip already-processed ones.  SAP dispatch
    emails often arrive pre-read so isRead is unreliable.

    Paginates via @odata.nextLink to handle folders where other emails
    push target emails beyond page 1.
    """
    url = (
        f"{GRAPH_BASE}/users/{mailbox}"
        f"/mailFolders/{folder_id}/messages"
        f"?$filter=hasAttachments eq true"
        f"&$select=id,subject,receivedDateTime,from,hasAttachments,isRead"
        f"&$top=50"
    )

    # Paginate through all results
    all_emails = []
    while url:
        data = graph_get(token, url)
        all_emails.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    # Filter by subject in Python
    filtered = [
        e for e in all_emails
        if subject_filter.lower() in e.get("subject", "").lower()
    ]
    # Sort oldest-first so that when processing --all, the newest email's
    # attachments overwrite older duplicates at the same blob path.
    filtered.sort(key=lambda x: x.get("receivedDateTime", ""), reverse=False)

    # Exclude already-processed emails (unless --all)
    if not include_processed and processed_ids:
        new_only = [e for e in filtered if e["id"] not in processed_ids]
        log.info(
            "[%s] Found %d matching email(s) — %d new, %d already processed "
            "(scanned %d with attachments)",
            subject_filter, len(filtered), len(new_only),
            len(filtered) - len(new_only), len(all_emails),
        )
        return new_only

    log.info("Found %d total email(s) matching '%s' (scanned %d with attachments)",
             len(filtered), subject_filter, len(all_emails))
    return filtered


def get_attachments(token: str, mailbox: str, message_id: str) -> list:
    """Get file attachments from an email."""
    url = f"{GRAPH_BASE}/users/{mailbox}/messages/{message_id}/attachments"
    data = graph_get(token, url)
    return [
        att for att in data.get("value", [])
        if att.get("@odata.type") == "#microsoft.graph.fileAttachment"
        and att.get("contentBytes")
    ]


def mark_as_read(token: str, mailbox: str, message_id: str) -> None:
    """Mark an email as read."""
    url = f"{GRAPH_BASE}/users/{mailbox}/messages/{message_id}"
    graph_patch(token, url, {"isRead": True})
