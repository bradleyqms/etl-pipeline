"""
Tests for src/core/graph_client.py — Graph API helpers.

These tests mock HTTP calls and verify filtering/sorting logic.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.core.graph_client import get_matching_emails, find_mail_folder


# ═════════════════════════════════════════════
# Fixtures
# ═════════════════════════════════════════════

def _make_email(msg_id: str, subject: str, received: str) -> dict:
    """Helper to build a fake Graph API email dict."""
    return {
        "id": msg_id,
        "subject": subject,
        "receivedDateTime": received,
        "from": {"emailAddress": {"address": "sap@example.com"}},
        "hasAttachments": True,
        "isRead": False,
    }


SAMPLE_EMAILS = [
    _make_email("id-3", "Cold_Extract 2026-02-17", "2026-02-17T08:00:00Z"),
    _make_email("id-1", "Cold_Extract 2026-02-15", "2026-02-15T08:00:00Z"),
    _make_email("id-2", "Cold_Extract 2026-02-16", "2026-02-16T08:00:00Z"),
    _make_email("id-4", "Unrelated Subject",        "2026-02-17T09:00:00Z"),
]


# ═════════════════════════════════════════════
# Tests
# ═════════════════════════════════════════════

class TestGetMatchingEmails:
    """get_matching_emails() filters, sorts, and excludes processed IDs."""

    @patch("src.core.graph_client.graph_get")
    def test_filters_by_subject_case_insensitive(self, mock_get):
        """Subject filter should be case-insensitive."""
        mock_get.return_value = {"value": SAMPLE_EMAILS}

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="cold_extract",
            include_processed=True,
        )

        subjects = [e["subject"] for e in result]
        assert len(result) == 3
        assert "Unrelated Subject" not in subjects

    @patch("src.core.graph_client.graph_get")
    def test_sorts_oldest_first(self, mock_get):
        """After the bug fix, emails should be sorted oldest-first."""
        mock_get.return_value = {"value": SAMPLE_EMAILS}

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="Cold_Extract",
            include_processed=True,
        )

        dates = [e["receivedDateTime"] for e in result]
        assert dates == sorted(dates), "Emails should be sorted oldest-first (ascending)"

    @patch("src.core.graph_client.graph_get")
    def test_excludes_processed_ids(self, mock_get):
        """Already-processed email IDs should be excluded when include_processed=False."""
        mock_get.return_value = {"value": SAMPLE_EMAILS}

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="Cold_Extract",
            processed_ids={"id-1", "id-2"},
            include_processed=False,
        )

        ids = [e["id"] for e in result]
        assert "id-1" not in ids
        assert "id-2" not in ids
        assert "id-3" in ids

    @patch("src.core.graph_client.graph_get")
    def test_include_processed_returns_all(self, mock_get):
        """With include_processed=True, all matching emails should be returned."""
        mock_get.return_value = {"value": SAMPLE_EMAILS}

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="Cold_Extract",
            processed_ids={"id-1", "id-2"},
            include_processed=True,
        )

        assert len(result) == 3  # All Cold_Extract emails

    @patch("src.core.graph_client.graph_get")
    def test_paginates_via_next_link(self, mock_get):
        """Should follow @odata.nextLink to get all pages."""
        page1 = {
            "value": [SAMPLE_EMAILS[0]],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/next-page",
        }
        page2 = {
            "value": [SAMPLE_EMAILS[1], SAMPLE_EMAILS[2]],
        }
        mock_get.side_effect = [page1, page2]

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="Cold_Extract",
            include_processed=True,
        )

        assert len(result) == 3
        assert mock_get.call_count == 2

    @patch("src.core.graph_client.graph_get")
    def test_empty_folder_returns_empty(self, mock_get):
        """Empty mailbox folder should return empty list."""
        mock_get.return_value = {"value": []}

        result = get_matching_emails(
            token="fake-token",
            mailbox="test@example.com",
            folder_id="folder-123",
            subject_filter="Cold_Extract",
            include_processed=True,
        )

        assert result == []


class TestFindMailFolder:
    """find_mail_folder() looks up folders by name."""

    @patch("src.core.graph_client.graph_get")
    def test_finds_top_level_folder(self, mock_get):
        mock_get.return_value = {
            "value": [
                {"id": "f-inbox", "displayName": "Inbox", "totalItemCount": 50},
                {"id": "f-sap", "displayName": "SAP Reports", "totalItemCount": 10},
            ]
        }

        result = find_mail_folder("token", "test@example.com", "SAP Reports")
        assert result == "f-sap"

    @patch("src.core.graph_client.graph_get")
    def test_case_insensitive_match(self, mock_get):
        mock_get.return_value = {
            "value": [
                {"id": "f-sap", "displayName": "SAP Reports", "totalItemCount": 10},
            ]
        }

        result = find_mail_folder("token", "test@example.com", "sap reports")
        assert result == "f-sap"

    @patch("src.core.graph_client.graph_get")
    def test_not_found_raises_runtime_error(self, mock_get):
        mock_get.return_value = {
            "value": [
                {"id": "f-inbox", "displayName": "Inbox", "totalItemCount": 50},
            ]
        }

        with pytest.raises(RuntimeError, match="not found"):
            find_mail_folder("token", "test@example.com", "Nonexistent Folder")
