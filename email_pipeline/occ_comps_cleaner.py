"""
Occupational Comps Cleaner
==========================
Post-extraction cleaning pipeline for occupational comparables.

Reads the raw OCCUPATIONAL COMPARABLES.xlsx, applies cleaning rules in
cascade order, and outputs:
  1. A cleaned Excel file (OCCUPATIONAL COMPARABLES - CLEANED.xlsx)
  2. Rows in the SQLite cleaned_occupational_comps table

Rules (applied in cascade order):
    1. Date normalisation          — all date columns to ISO YYYY-MM-DD
    2. Postcode from address       — regex-extract UK postcode from address string
    3. Town from postcode          — postcodes.io bulk lookup
    4. Acres to sqft               — parse notes for site area in acres
    5. Rent arithmetic             — fill 3rd value from 2 known (size, PA, PSF)
    6. Build total_address         — concatenate address + town + postcode

Called automatically after OccupationalCompsWriter.append_comps() completes.
Also callable standalone from the GUI "Clean Occ Comps" button.
"""

import csv
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — must match OccupationalCompsWriter in excel_writer.py
# ---------------------------------------------------------------------------

COL_SOURCE = 1
COL_ENTRY_TYPE = 2
COL_TENANT = 3
COL_UNIT = 4
COL_ADDRESS = 5
COL_TOWN = 6
COL_POSTCODE = 7
COL_SIZE = 8
COL_RENT_PA = 9
COL_RENT_PSF = 10
COL_LEASE_START = 11
COL_LEASE_EXPIRY = 12
COL_BREAK = 13
COL_REVIEW = 14
COL_TERM = 15
COL_COMP_DATE = 16
COL_NOTES = 17
COL_SOURCE_FILE = 18
COL_EXTRACTION_DATE = 19
COL_TOTAL_ADDRESS = 20  # New column in cleaned file

CLEANED_HEADERS = [
    "Source Deal", "Entry Type", "Tenant", "Unit", "Address", "Town",
    "Postcode", "Size (sqft)", "Rent PA", "Rent PSF", "Lease Start",
    "Lease Expiry", "Break Date", "Review Date", "Term (yrs)",
    "Comp Date", "Notes", "Source File", "Extraction Date",
    "Total Address",
]

DATE_COLUMNS = ["lease_start", "lease_expiry", "break_date",
                "rent_review_date", "comp_date"]

# UK postcode regex (same as email_archiver.py)
_UK_POSTCODE_RE = re.compile(
    r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.IGNORECASE
)

# Acres pattern for storage yard / land site areas
_ACRES_RE = re.compile(
    r"(?:c\.?\s*|approx\.?\s*|circa\s*|~\s*)?(\d+(?:[.,]\d+)?)\s*(?:-?\s*)?(?:acres?|ac)\b",
    re.IGNORECASE,
)

SQFT_PER_ACRE = 43_560

# Month names for date parsing
_MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3, "apr": 4, "april": 4,
    "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8,
    "sep": 9, "september": 9, "oct": 10, "october": 10,
    "nov": 11, "november": 11, "dec": 12, "december": 12,
}

# Quarter start months
_QUARTER_MONTH = {"q1": 1, "q2": 4, "q3": 7, "q4": 10}

# Retry settings for OneDrive file locking
MAX_RETRIES = 3
RETRY_DELAY = 5

# postcodes.io
_POSTCODES_IO_URL = "https://api.postcodes.io/postcodes"
_POSTCODES_IO_BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def clean_occupational_comps(
    raw_excel_path: Path,
    cleaned_excel_path: Path,
    db_path: Path,
) -> dict:
    """Run the cleaning pipeline over raw occupational comparables.

    Parameters
    ----------
    raw_excel_path : Path
        Path to the raw OCCUPATIONAL COMPARABLES.xlsx.
    cleaned_excel_path : Path
        Path to write the cleaned Excel file.
    db_path : Path
        Path to the SQLite database.

    Returns
    -------
    dict
        Summary with keys: rows_scanned, cells_filled, details, db_rows.
    """
    raw_excel_path = Path(raw_excel_path)
    cleaned_excel_path = Path(cleaned_excel_path)
    db_path = Path(db_path)

    summary: dict = {
        "rows_scanned": 0,
        "cells_filled": 0,
        "details": [],
        "db_rows": 0,
    }

    if not raw_excel_path.exists():
        print(f"  Raw file not found: {raw_excel_path}")
        return summary

    # Step 1: Read raw rows
    print(f"  Reading raw data from {raw_excel_path.name}...")
    rows = _read_raw_rows(raw_excel_path)
    if not rows:
        print("  No data rows found")
        return summary

    summary["rows_scanned"] = len(rows)
    print(f"  {len(rows)} rows loaded")

    # Step 2: Batch postcode lookup for rows needing town
    postcodes_needing_town = set()
    for row in rows:
        pc = (row.get("postcode") or "").strip()
        town = (row.get("town") or "").strip()
        if pc and not town:
            postcodes_needing_town.add(_normalise_postcode(pc))
        # Also check if address contains a postcode that could be extracted
        if not pc:
            addr = (row.get("address") or "").strip()
            match = _UK_POSTCODE_RE.search(addr)
            if match:
                extracted = _normalise_postcode(match.group())
                if extracted:
                    postcodes_needing_town.add(extracted)

    postcode_cache: dict[str, dict] = {}
    if postcodes_needing_town:
        print(f"  Looking up {len(postcodes_needing_town)} postcodes via postcodes.io...")
        postcode_cache = _batch_postcode_lookup(list(postcodes_needing_town))
        print(f"  {len(postcode_cache)} postcodes resolved")

    # Step 3: Clean each row
    print("  Applying cleaning rules...")
    changes = 0
    for i, row in enumerate(rows):
        row_num = i + 2  # Excel row (1-indexed, row 1 is headers)
        row_changes = _clean_row(row, row_num, postcode_cache, summary["details"])
        changes += row_changes

    summary["cells_filled"] = changes
    print(f"  {changes} cells filled across {len(rows)} rows")

    # Step 4: Write cleaned Excel
    print(f"  Writing cleaned data to {cleaned_excel_path.name}...")
    _write_cleaned_excel(rows, cleaned_excel_path)

    # Step 5: Insert into database
    print(f"  Inserting into database...")
    db_rows = _insert_into_db(rows, db_path)
    summary["db_rows"] = db_rows
    print(f"  {db_rows} rows upserted into cleaned_occupational_comps")

    return summary


# ---------------------------------------------------------------------------
# CSV snapshot (audit trail for raw data)
# ---------------------------------------------------------------------------

def snapshot_raw_csv(raw_excel_path: Path):
    """Save a timestamped CSV snapshot of the raw occupational comps.

    Creates a snapshots/ subfolder next to the raw Excel file and writes
    a CSV named like ``occ_comps_raw_20260217_143022.csv``.  These are
    lightweight, append-only, and give a full audit trail of every
    extraction run.
    """
    raw_excel_path = Path(raw_excel_path)
    if not raw_excel_path.exists():
        return

    snapshot_dir = raw_excel_path.parent / "snapshots"
    snapshot_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = snapshot_dir / f"occ_comps_raw_{timestamp}.csv"

    rows = _read_raw_rows(raw_excel_path)
    if not rows:
        return

    # Use the raw Excel headers (excluding Total Address which is cleaned-only)
    fieldnames = [
        "source_deal", "entry_type", "tenant_name", "unit_name",
        "address", "town", "postcode", "size_sqft", "rent_pa", "rent_psf",
        "lease_start", "lease_expiry", "break_date", "rent_review_date",
        "lease_term_years", "comp_date", "notes", "source_file_path",
        "extraction_date",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("  Raw CSV snapshot: %s (%d rows)", csv_path.name, len(rows))
    print(f"  Raw CSV snapshot: {csv_path.name} ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Read raw rows from Excel
# ---------------------------------------------------------------------------

def _read_raw_rows(excel_path: Path) -> list[dict]:
    """Read all data rows from the raw occupational comps Excel."""
    rows = []

    for attempt in range(MAX_RETRIES):
        try:
            wb = load_workbook(str(excel_path), data_only=False)
            ws = wb.active

            for r in range(2, ws.max_row + 1):
                # Skip empty rows
                if not ws.cell(row=r, column=COL_SOURCE).value:
                    continue

                row = {
                    "source_deal": str(ws.cell(row=r, column=COL_SOURCE).value or ""),
                    "entry_type": str(ws.cell(row=r, column=COL_ENTRY_TYPE).value or "tenancy"),
                    "tenant_name": str(ws.cell(row=r, column=COL_TENANT).value or ""),
                    "unit_name": str(ws.cell(row=r, column=COL_UNIT).value or ""),
                    "address": str(ws.cell(row=r, column=COL_ADDRESS).value or ""),
                    "town": str(ws.cell(row=r, column=COL_TOWN).value or ""),
                    "postcode": str(ws.cell(row=r, column=COL_POSTCODE).value or ""),
                    "size_sqft": _to_number(ws.cell(row=r, column=COL_SIZE).value),
                    "rent_pa": _to_number(ws.cell(row=r, column=COL_RENT_PA).value),
                    "rent_psf": _to_number(ws.cell(row=r, column=COL_RENT_PSF).value),
                    "lease_start": ws.cell(row=r, column=COL_LEASE_START).value,
                    "lease_expiry": ws.cell(row=r, column=COL_LEASE_EXPIRY).value,
                    "break_date": ws.cell(row=r, column=COL_BREAK).value,
                    "rent_review_date": ws.cell(row=r, column=COL_REVIEW).value,
                    "lease_term_years": _to_number(ws.cell(row=r, column=COL_TERM).value),
                    "comp_date": ws.cell(row=r, column=COL_COMP_DATE).value,
                    "notes": str(ws.cell(row=r, column=COL_NOTES).value or ""),
                    "source_file_path": str(ws.cell(row=r, column=COL_SOURCE_FILE).value or ""),
                    "extraction_date": str(ws.cell(row=r, column=COL_EXTRACTION_DATE).value or ""),
                }
                rows.append(row)

            wb.close()
            return rows

        except PermissionError as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (2 ** attempt)
                print(f"  File locked, retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                print(f"  File locked after {MAX_RETRIES} attempts: {e}")
                return rows

        except Exception as e:
            print(f"  Error reading raw file: {e}")
            return rows

    return rows


# ---------------------------------------------------------------------------
# Row-level cleaning
# ---------------------------------------------------------------------------

def _clean_row(
    row: dict,
    row_num: int,
    postcode_cache: dict[str, dict],
    details: list[str],
) -> int:
    """Apply all cleaning rules to a single row. Returns count of cells changed."""
    changes = 0

    # --- Rule 1: Date normalisation ---
    for col_name in DATE_COLUMNS:
        raw_val = row.get(col_name)
        if raw_val is not None and raw_val != "":
            normalised = _rule_normalise_date(raw_val)
            if normalised and normalised != str(raw_val):
                row[col_name] = normalised
                changes += 1
                details.append(
                    f"Row {row_num}: normalised {col_name} '{raw_val}' -> '{normalised}'"
                )
        elif raw_val == "" or raw_val is None:
            row[col_name] = None

    # --- Rule 2: Extract postcode from address ---
    postcode = (row.get("postcode") or "").strip()
    address = (row.get("address") or "").strip()

    if not postcode and address:
        match = _UK_POSTCODE_RE.search(address)
        if match:
            extracted = _normalise_postcode(match.group())
            if extracted:
                row["postcode"] = extracted
                # Remove postcode from address to avoid duplication
                row["address"] = address[:match.start()].rstrip(", ") + address[match.end():]
                row["address"] = row["address"].strip().rstrip(",").strip()
                postcode = extracted
                changes += 1
                details.append(
                    f"Row {row_num}: extracted postcode '{extracted}' from address"
                )

    # Normalise existing postcode
    if postcode:
        normalised_pc = _normalise_postcode(postcode)
        if normalised_pc and normalised_pc != postcode:
            row["postcode"] = normalised_pc
            postcode = normalised_pc

    # --- Rule 3: Town from postcode (via postcodes.io cache) ---
    town = (row.get("town") or "").strip()
    if not town and postcode and postcode in postcode_cache:
        lookup = postcode_cache[postcode]
        if lookup.get("town"):
            row["town"] = lookup["town"]
            changes += 1
            details.append(
                f"Row {row_num}: filled town '{lookup['town']}' from postcode {postcode}"
            )

    # --- Rule 4: Acres to sqft from notes ---
    notes = (row.get("notes") or "").strip()
    size = row.get("size_sqft")

    if size is None and notes:
        converted = _rule_acres_to_sqft(notes)
        if converted is not None:
            row["size_sqft"] = converted
            size = converted
            changes += 1
            details.append(
                f"Row {row_num}: converted {_ACRES_RE.search(notes).group()} "
                f"-> {converted:,.0f} sqft from notes"
            )

    # --- Rule 5: Rent arithmetic ---
    rent_pa = row.get("rent_pa")
    rent_psf = row.get("rent_psf")

    if rent_pa is None and size and rent_psf:
        rent_pa = round(rent_psf * size, 2)
        row["rent_pa"] = rent_pa
        changes += 1
        details.append(
            f"Row {row_num}: derived Rent PA {rent_pa:,.0f} from PSF & Size"
        )

    if rent_psf is None and rent_pa and size:
        rent_psf = round(rent_pa / size, 2)
        row["rent_psf"] = rent_psf
        changes += 1
        details.append(
            f"Row {row_num}: derived Rent PSF {rent_psf:.2f} from PA & Size"
        )

    if size is None and rent_pa and rent_psf:
        size = round(rent_pa / rent_psf, 0)
        row["size_sqft"] = size
        changes += 1
        details.append(
            f"Row {row_num}: derived Size {size:,.0f} sqft from PA & PSF"
        )

    # --- Rule 6: Build total_address ---
    row["total_address"] = _rule_build_total_address(
        row.get("address") or "",
        row.get("town") or "",
        row.get("postcode") or "",
    )

    return changes


# ---------------------------------------------------------------------------
# Individual rule functions
# ---------------------------------------------------------------------------

def _rule_normalise_date(value) -> Optional[str]:
    """Normalise a date value to ISO YYYY-MM-DD format.

    Handles:
    - datetime objects (openpyxl date cells)
    - DD/MM/YYYY or DD-MM-YYYY
    - MM/YYYY
    - Mon YYYY or Month YYYY (e.g. "Jan 2025", "January 2025")
    - Q2 2025, Q2/2025, Q2-2025
    - YYYY (year only)
    - YYYY-MM-DD (ISO passthrough)
    """
    if value is None:
        return None

    # Handle datetime objects from openpyxl
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    s = str(value).strip()
    if not s:
        return None

    # ISO passthrough: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$", s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return s  # Invalid date, return as-is

    # MM/YYYY
    m = re.match(r"^(\d{1,2})[/\-](\d{4})$", s)
    if m:
        month, year = int(m.group(1)), int(m.group(2))
        try:
            return datetime(year, month, 1).strftime("%Y-%m-%d")
        except ValueError:
            return s

    # Month YYYY (e.g. "Jan 2025", "January 2025")
    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", s)
    if m:
        month_str = m.group(1).lower()
        year = int(m.group(2))
        month_num = _MONTH_MAP.get(month_str)
        if month_num:
            return datetime(year, month_num, 1).strftime("%Y-%m-%d")

    # Quarter: Q2 2025, Q2/2025, Q2-2025
    m = re.match(r"^(Q[1-4])\s*[/\-]?\s*(\d{4})$", s, re.IGNORECASE)
    if m:
        quarter = m.group(1).lower()
        year = int(m.group(2))
        month = _QUARTER_MONTH.get(quarter, 1)
        return datetime(year, month, 1).strftime("%Y-%m-%d")

    # Year only: YYYY
    m = re.match(r"^(\d{4})$", s)
    if m:
        return f"{m.group(1)}-01-01"

    # Couldn't parse — return as-is
    return s


def _rule_acres_to_sqft(notes: str) -> Optional[float]:
    """Parse notes for site area in acres and convert to sqft.

    Returns sqft value or None if no acres reference found.
    """
    if not notes:
        return None

    match = _ACRES_RE.search(notes)
    if match:
        acres_str = match.group(1).replace(",", ".")
        try:
            acres = float(acres_str)
            if acres > 0:
                return round(acres * SQFT_PER_ACRE, 0)
        except ValueError:
            pass

    return None


def _rule_build_total_address(address: str, town: str, postcode: str) -> str:
    """Concatenate non-empty address components, deduplicating overlaps."""
    parts = []

    address = address.strip()
    town = town.strip()
    postcode = postcode.strip()

    if address:
        parts.append(address)

    # Only add town if it's not already in the address
    if town and town.lower() not in address.lower():
        parts.append(town)

    # Only add postcode if it's not already in the address
    if postcode and postcode.lower() not in address.lower():
        parts.append(postcode)

    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Postcode helpers
# ---------------------------------------------------------------------------

def _normalise_postcode(raw: str) -> str:
    """Normalise a UK postcode: uppercase, single space before last 3 chars."""
    cleaned = re.sub(r"\s+", "", raw.strip().upper())
    if not _UK_POSTCODE_RE.match(cleaned):
        return raw.strip().upper()
    # Insert space before last 3 characters
    if len(cleaned) > 3:
        return cleaned[:-3] + " " + cleaned[-3:]
    return cleaned


def _batch_postcode_lookup(postcodes: list[str]) -> dict[str, dict]:
    """Look up postcodes via postcodes.io bulk endpoint.

    Returns dict mapping normalised postcode to {town, county, region}.
    """
    cache: dict[str, dict] = {}

    for batch_start in range(0, len(postcodes), _POSTCODES_IO_BATCH_SIZE):
        batch = postcodes[batch_start:batch_start + _POSTCODES_IO_BATCH_SIZE]

        payload = json.dumps({"postcodes": batch}).encode("utf-8")
        req = urllib.request.Request(
            _POSTCODES_IO_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))

                for item in data.get("result", []):
                    query_pc = item.get("query", "")
                    result = item.get("result")
                    if result:
                        cache[_normalise_postcode(query_pc)] = {
                            "town": result.get("admin_district", ""),
                            "county": result.get("admin_county", ""),
                            "region": result.get("region", ""),
                        }
                break  # Success

            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 2:
                    wait = 2 ** (attempt + 1)
                    logger.warning("postcodes.io rate limited, waiting %ds", wait)
                    time.sleep(wait)
                else:
                    logger.warning("postcodes.io HTTP error: %s", e)
                    break

            except Exception as e:
                logger.warning("postcodes.io lookup failed: %s", e)
                break

    return cache


# ---------------------------------------------------------------------------
# Numeric helper
# ---------------------------------------------------------------------------

def _to_number(value) -> Optional[float]:
    """Convert a cell value to a number, or None if not numeric / zero.

    Same pattern as comps_cleaner.py.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value != 0 else None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("\u00a3", "").replace(" ", "").strip()
        if not cleaned:
            return None
        try:
            result = float(cleaned)
            return result if result != 0 else None
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Output: Cleaned Excel
# ---------------------------------------------------------------------------

def _write_cleaned_excel(rows: list[dict], excel_path: Path):
    """Write cleaned rows to a new Excel file.

    Keeps one rolling backup: if the cleaned file already exists, it is
    renamed to ``<name> - BACKUP.xlsx`` before the new file is written.
    Only one backup is kept (overwritten each time).
    """
    # Rolling backup: keep one previous copy
    if excel_path.exists():
        backup_path = excel_path.with_name(
            excel_path.stem + " - BACKUP" + excel_path.suffix
        )
        try:
            shutil.copy2(str(excel_path), str(backup_path))
            logger.info("  Cleaned backup: %s", backup_path.name)
        except Exception as e:
            logger.warning("  Failed to create cleaned backup: %s", e)

    wb = Workbook()
    ws = wb.active
    ws.title = "Occupational Comps (Cleaned)"

    # Headers
    for i, header in enumerate(CLEANED_HEADERS, 1):
        cell = ws.cell(row=1, column=i, value=header)
        cell.font = Font(bold=True)

    # Data rows
    for r_idx, row in enumerate(rows, 2):
        ws.cell(row=r_idx, column=COL_SOURCE, value=row.get("source_deal", ""))
        ws.cell(row=r_idx, column=COL_ENTRY_TYPE, value=row.get("entry_type", ""))
        ws.cell(row=r_idx, column=COL_TENANT, value=row.get("tenant_name", ""))
        ws.cell(row=r_idx, column=COL_UNIT, value=row.get("unit_name", ""))
        ws.cell(row=r_idx, column=COL_ADDRESS, value=row.get("address", ""))
        ws.cell(row=r_idx, column=COL_TOWN, value=row.get("town", ""))
        ws.cell(row=r_idx, column=COL_POSTCODE, value=row.get("postcode", ""))

        size = row.get("size_sqft")
        if size is not None:
            ws.cell(row=r_idx, column=COL_SIZE, value=size)
        rent_pa = row.get("rent_pa")
        if rent_pa is not None:
            ws.cell(row=r_idx, column=COL_RENT_PA, value=rent_pa)
        rent_psf = row.get("rent_psf")
        if rent_psf is not None:
            ws.cell(row=r_idx, column=COL_RENT_PSF, value=rent_psf)

        ws.cell(row=r_idx, column=COL_LEASE_START, value=row.get("lease_start") or "")
        ws.cell(row=r_idx, column=COL_LEASE_EXPIRY, value=row.get("lease_expiry") or "")
        ws.cell(row=r_idx, column=COL_BREAK, value=row.get("break_date") or "")
        ws.cell(row=r_idx, column=COL_REVIEW, value=row.get("rent_review_date") or "")

        term = row.get("lease_term_years")
        if term is not None:
            ws.cell(row=r_idx, column=COL_TERM, value=term)

        ws.cell(row=r_idx, column=COL_COMP_DATE, value=row.get("comp_date") or "")
        ws.cell(row=r_idx, column=COL_NOTES, value=row.get("notes", ""))
        ws.cell(row=r_idx, column=COL_SOURCE_FILE, value=row.get("source_file_path", ""))
        ws.cell(row=r_idx, column=COL_EXTRACTION_DATE, value=row.get("extraction_date", ""))
        ws.cell(row=r_idx, column=COL_TOTAL_ADDRESS, value=row.get("total_address", ""))

    # Auto-width columns
    for i, header in enumerate(CLEANED_HEADERS, 1):
        ws.column_dimensions[get_column_letter(i)].width = max(len(header) + 2, 12)

    # Save via temp file (OneDrive protection)
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    try:
        wb.save(tmp_path)
        wb.close()
        shutil.copy2(tmp_path, str(excel_path))
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Output: SQLite database
# ---------------------------------------------------------------------------

def _insert_into_db(rows: list[dict], db_path: Path) -> int:
    """Upsert cleaned rows into the cleaned_occupational_comps table.

    Uses INSERT OR REPLACE so existing rows (matched by the UNIQUE
    constraint) are updated with the latest cleaned values, while new
    rows are added.  No data is deleted.
    """
    cleaned_at = datetime.now().isoformat()
    count = 0

    with sqlite3.connect(str(db_path)) as conn:
        for row in rows:
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cleaned_occupational_comps (
                        source_deal, source_file_path, entry_type,
                        tenant_name, unit_name,
                        address, town, postcode, total_address,
                        size_sqft, rent_pa, rent_psf,
                        lease_start, lease_expiry, break_date,
                        rent_review_date, lease_term_years, comp_date,
                        notes, extraction_date, cleaned_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.get("source_deal", ""),
                        row.get("source_file_path", ""),
                        row.get("entry_type", "tenancy"),
                        row.get("tenant_name", ""),
                        row.get("unit_name", ""),
                        row.get("address", ""),
                        row.get("town", ""),
                        row.get("postcode", ""),
                        row.get("total_address", ""),
                        row.get("size_sqft"),
                        row.get("rent_pa"),
                        row.get("rent_psf"),
                        row.get("lease_start"),
                        row.get("lease_expiry"),
                        row.get("break_date"),
                        row.get("rent_review_date"),
                        row.get("lease_term_years"),
                        row.get("comp_date"),
                        row.get("notes", ""),
                        row.get("extraction_date", ""),
                        cleaned_at,
                    ),
                )
                count += 1
            except Exception as e:
                logger.warning("Failed to insert row: %s", e)

        conn.commit()

    return count
