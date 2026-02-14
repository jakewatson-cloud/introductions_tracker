"""
Configuration
=============
Loads environment variables from .env and exposes config helpers.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env", override=True)


# ---------------------------------------------------------------------------
# Email pipeline config
# ---------------------------------------------------------------------------

def get_anthropic_api_key() -> str | None:
    """Return the Anthropic API key, or None if not configured."""
    return os.environ.get("ANTHROPIC_API_KEY")


def get_pipeline_excel_path() -> Path | None:
    """Return the path to the Pipeline Excel tracker."""
    raw = os.environ.get("PIPELINE_EXCEL_PATH")
    return Path(raw) if raw else None


def get_investment_comps_path() -> Path | None:
    """Return the path to the Investment Comparables Master Excel."""
    raw = os.environ.get("INVESTMENT_COMPS_PATH")
    return Path(raw) if raw else None


def get_intros_archive_path() -> Path | None:
    """Return the path to the Investment Introductions archive folder."""
    raw = os.environ.get("INTROS_ARCHIVE_PATH")
    return Path(raw) if raw else None


def get_gmail_scan_label() -> str:
    """Return the Gmail label to scan for investment introductions."""
    return os.environ.get("GMAIL_SCAN_LABEL", "Investment Introduction")


def get_gmail_processed_label() -> str:
    """Return the Gmail label to apply after processing."""
    return os.environ.get("GMAIL_PROCESSED_LABEL", "Processed/Pipeline")


def get_sender_whitelist() -> list[str]:
    """Return list of sender domain suffixes to match (e.g. '@cbre.com')."""
    raw = os.environ.get("SENDER_WHITELIST", "")
    return [s.strip() for s in raw.split(",") if s.strip()]


def get_email_keywords() -> list[str]:
    """Return list of keywords to match in email subject/body."""
    raw = os.environ.get("EMAIL_KEYWORDS", "")
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


def get_gmail_credentials_path() -> Path:
    """Return path to the Google OAuth2 credentials.json file."""
    return _PROJECT_ROOT / "credentials.json"


def get_gmail_token_path() -> Path:
    """Return path to the stored OAuth2 token.json file."""
    return _PROJECT_ROOT / "token.json"


def get_db_path() -> Path:
    """Return path to the local SQLite tracking database."""
    return _PROJECT_ROOT / "data" / "introductions_tracker.db"


def get_occupational_comps_path() -> Path | None:
    """Return the path to the Occupational Comparables Excel (created if needed)."""
    raw = os.environ.get("OCCUPATIONAL_COMPS_PATH")
    if raw:
        return Path(raw)
    # Default: same folder as investment comps
    inv_path = get_investment_comps_path()
    if inv_path:
        return inv_path.parent / "OCCUPATIONAL COMPARABLES.xlsx"
    return None
