"""
Database Module
===============
SQLite database for tracking processed emails and pipeline state.

Provides idempotency — ensures each email is only processed once.
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = _PROJECT_ROOT / "data" / "introductions_tracker.db"


class Database:
    """SQLite database for tracking processed emails."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection.

        Parameters
        ----------
        db_path : str, optional
            Path to the SQLite database file.
            Defaults to data/introductions_tracker.db
        """
        self.db_path = Path(db_path) if db_path else _DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            # Enable WAL mode for concurrent read safety
            conn.execute("PRAGMA journal_mode=WAL")

            # Processed emails — idempotency tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gmail_message_id TEXT UNIQUE NOT NULL,
                    subject TEXT,
                    sender TEXT,
                    sender_domain TEXT,
                    email_date TEXT,
                    processed_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'processed',
                    is_introduction INTEGER NOT NULL DEFAULT 0,
                    classification_reason TEXT,
                    deal_asset_name TEXT,
                    deal_town TEXT,
                    archive_folder TEXT,
                    pipeline_row_added INTEGER DEFAULT 0,
                    brochures_parsed INTEGER DEFAULT 0,
                    error_message TEXT,
                    raw_extraction_json TEXT
                )
            """)

            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gmail_message_id
                ON processed_emails(gmail_message_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status
                ON processed_emails(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_at
                ON processed_emails(processed_at)
            """)

            # Scraped brochures — tracks which files have been parsed for comps
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scraped_brochures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_modified TEXT,
                    deal_name TEXT,
                    scraped_at TEXT NOT NULL,
                    investment_comps_found INTEGER DEFAULT 0,
                    occupational_comps_found INTEGER DEFAULT 0,
                    UNIQUE(file_path, file_size)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scraped_file_path
                ON scraped_brochures(file_path)
            """)

            # Cleaned occupational comparables — output of the cleaning pipeline
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cleaned_occupational_comps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_deal TEXT NOT NULL,
                    source_file_path TEXT,
                    entry_type TEXT NOT NULL DEFAULT 'tenancy',
                    tenant_name TEXT,
                    unit_name TEXT,
                    address TEXT,
                    town TEXT,
                    postcode TEXT,
                    total_address TEXT,
                    size_sqft REAL,
                    rent_pa REAL,
                    rent_psf REAL,
                    lease_start TEXT,
                    lease_expiry TEXT,
                    break_date TEXT,
                    rent_review_date TEXT,
                    lease_term_years REAL,
                    comp_date TEXT,
                    notes TEXT,
                    extraction_date TEXT,
                    cleaned_at TEXT NOT NULL,
                    UNIQUE(source_deal, entry_type, tenant_name, address, unit_name, rent_pa)
                )
            """)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_town ON cleaned_occupational_comps(town)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_postcode ON cleaned_occupational_comps(postcode)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_comp_date ON cleaned_occupational_comps(comp_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_entry_type ON cleaned_occupational_comps(entry_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_rent_psf ON cleaned_occupational_comps(rent_psf)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_size ON cleaned_occupational_comps(size_sqft)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_occ_source_deal ON cleaned_occupational_comps(source_deal)")

            # Raw occupational comparables — primary data store
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_occupational_comps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_deal TEXT NOT NULL,
                    entry_type TEXT NOT NULL DEFAULT 'tenancy',
                    tenant_name TEXT,
                    tenant_name_norm TEXT,
                    unit_name TEXT,
                    unit_name_norm TEXT,
                    address TEXT,
                    town TEXT,
                    postcode TEXT,
                    size_sqft REAL,
                    rent_pa REAL,
                    rent_psf REAL,
                    lease_start TEXT,
                    lease_expiry TEXT,
                    break_date TEXT,
                    rent_review_date TEXT,
                    lease_term_years REAL,
                    comp_date TEXT,
                    notes TEXT,
                    source_file_path TEXT,
                    extraction_date TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_occ_source_deal ON raw_occupational_comps(source_deal)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_occ_tenant_norm ON raw_occupational_comps(tenant_name_norm)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_occ_unit_norm ON raw_occupational_comps(unit_name_norm)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_occ_town ON raw_occupational_comps(town)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_occ_postcode ON raw_occupational_comps(postcode)")

            # Change log — audit trail for occ comps modifications
            conn.execute("""
                CREATE TABLE IF NOT EXISTS occ_comps_change_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    raw_comp_id INTEGER,
                    source_deal TEXT,
                    tenant_name TEXT,
                    field_name TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    context TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_changelog_timestamp ON occ_comps_change_log(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_changelog_action ON occ_comps_change_log(action)")

            conn.commit()

    def is_processed(self, gmail_message_id: str) -> bool:
        """Check if an email has already been processed.

        Parameters
        ----------
        gmail_message_id : str
            Gmail message ID to check.

        Returns
        -------
        bool
            True if the email has been processed.
        """
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT 1 FROM processed_emails WHERE gmail_message_id = ?",
                (gmail_message_id,),
            ).fetchone()
            return result is not None

    def mark_processed(
        self,
        gmail_message_id: str,
        subject: str = "",
        sender: str = "",
        sender_domain: str = "",
        email_date: str = "",
        status: str = "processed",
        is_introduction: bool = False,
        classification_reason: str = "",
        deal_asset_name: str = "",
        deal_town: str = "",
        archive_folder: str = "",
        pipeline_row_added: bool = False,
        brochures_parsed: int = 0,
        error_message: str = "",
        raw_extraction_json: str = "",
    ) -> None:
        """Record an email as processed.

        Parameters
        ----------
        gmail_message_id : str
            Gmail message ID.
        subject, sender, sender_domain, email_date : str
            Email metadata.
        status : str
            Processing status: 'processed', 'skipped', 'error'.
        is_introduction : bool
            True if classified as an investment introduction.
        classification_reason : str
            Why it was or wasn't classified as an introduction.
        deal_asset_name, deal_town : str
            Extracted deal info (if introduction).
        archive_folder : str
            Path to the archive folder (if saved).
        pipeline_row_added : bool
            True if a row was added to the Pipeline Excel.
        brochures_parsed : int
            Number of brochures parsed from attachments.
        error_message : str
            Error message if processing failed.
        raw_extraction_json : str
            Raw JSON from the AI extraction.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed_emails (
                    gmail_message_id, subject, sender, sender_domain,
                    email_date, processed_at, status, is_introduction,
                    classification_reason, deal_asset_name, deal_town,
                    archive_folder, pipeline_row_added, brochures_parsed,
                    error_message, raw_extraction_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    gmail_message_id,
                    subject,
                    sender,
                    sender_domain,
                    email_date,
                    datetime.now().isoformat(),
                    status,
                    1 if is_introduction else 0,
                    classification_reason,
                    deal_asset_name,
                    deal_town,
                    archive_folder,
                    1 if pipeline_row_added else 0,
                    brochures_parsed,
                    error_message,
                    raw_extraction_json,
                ),
            )
            conn.commit()

    def get_unprocessed_ids(self, message_ids: list[str]) -> list[str]:
        """Filter a list of message IDs to only those not yet processed.

        Parameters
        ----------
        message_ids : list[str]
            Gmail message IDs to check.

        Returns
        -------
        list[str]
            Message IDs that have NOT been processed.
        """
        if not message_ids:
            return []

        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" * len(message_ids))
            rows = conn.execute(
                f"SELECT gmail_message_id FROM processed_emails WHERE gmail_message_id IN ({placeholders})",
                message_ids,
            ).fetchall()
            processed = {row[0] for row in rows}

        return [mid for mid in message_ids if mid not in processed]

    def get_stats(self) -> dict:
        """Get processing statistics.

        Returns
        -------
        dict
            Processing statistics.
        """
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM processed_emails"
            ).fetchone()[0]
            introductions = conn.execute(
                "SELECT COUNT(*) FROM processed_emails WHERE is_introduction = 1"
            ).fetchone()[0]
            skipped = conn.execute(
                "SELECT COUNT(*) FROM processed_emails WHERE status = 'skipped'"
            ).fetchone()[0]
            errors = conn.execute(
                "SELECT COUNT(*) FROM processed_emails WHERE status = 'error'"
            ).fetchone()[0]
            pipeline_rows = conn.execute(
                "SELECT COUNT(*) FROM processed_emails WHERE pipeline_row_added = 1"
            ).fetchone()[0]

        return {
            "total_processed": total,
            "introductions": introductions,
            "skipped": skipped,
            "errors": errors,
            "pipeline_rows_added": pipeline_rows,
        }

    def get_recent(self, limit: int = 20) -> list[dict]:
        """Get the most recently processed emails.

        Parameters
        ----------
        limit : int
            Maximum number of records to return.

        Returns
        -------
        list[dict]
            Recent processing records.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT * FROM processed_emails
                ORDER BY processed_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Scraped brochures tracking
    # ------------------------------------------------------------------

    def is_brochure_scraped(self, file_path: str, file_size: int) -> bool:
        """Check if a brochure file has already been scraped for comps.

        Matches on file_path + file_size.  A changed size means the file
        was modified and should be re-processed.

        Parameters
        ----------
        file_path : str
            Absolute path to the brochure file.
        file_size : int
            File size in bytes.

        Returns
        -------
        bool
            True if this exact file has already been scraped.
        """
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT 1 FROM scraped_brochures WHERE file_path = ? AND file_size = ?",
                (file_path, file_size),
            ).fetchone()
            return result is not None

    def mark_brochure_scraped(
        self,
        file_path: str,
        file_name: str,
        file_size: int,
        file_modified: str = "",
        deal_name: str = "",
        investment_comps_found: int = 0,
        occupational_comps_found: int = 0,
    ) -> None:
        """Record a brochure file as scraped.

        Parameters
        ----------
        file_path : str
            Absolute path to the brochure file.
        file_name : str
            Just the filename (e.g. "brochure.pdf").
        file_size : int
            File size in bytes.
        file_modified : str
            ISO timestamp of the file's last modification time.
        deal_name : str
            Source deal name (e.g. "Birmingham, Kings Road").
        investment_comps_found : int
            Number of investment comps extracted.
        occupational_comps_found : int
            Number of occupational comps extracted.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO scraped_brochures (
                    file_path, file_name, file_size, file_modified,
                    deal_name, scraped_at,
                    investment_comps_found, occupational_comps_found
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_path,
                    file_name,
                    file_size,
                    file_modified,
                    deal_name,
                    datetime.now().isoformat(),
                    investment_comps_found,
                    occupational_comps_found,
                ),
            )
            conn.commit()

    def clear_scraped_brochures(self) -> int:
        """Delete all scraped_brochures records.

        Returns
        -------
        int
            Number of records deleted.
        """
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM scraped_brochures"
            ).fetchone()[0]
            conn.execute("DELETE FROM scraped_brochures")
            conn.commit()
            return count

    # ------------------------------------------------------------------
    # Cleaned occupational comps
    # ------------------------------------------------------------------

    def get_cleaned_occ_comps_count(self) -> int:
        """Return number of cleaned occupational comps in the database."""
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM cleaned_occupational_comps"
            ).fetchone()[0]

    def clear_cleaned_occ_comps(self) -> int:
        """Delete all cleaned_occupational_comps rows.

        Returns
        -------
        int
            Number of records deleted.
        """
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM cleaned_occupational_comps"
            ).fetchone()[0]
            conn.execute("DELETE FROM cleaned_occupational_comps")
            conn.commit()
            return count
