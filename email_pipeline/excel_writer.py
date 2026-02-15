"""
Excel Writer
=============
Writes extracted data to the three target Excel files:

1. Pipeline 2026.xlsx — "Intros" sheet (deal introductions)
2. INVESTMENT COMPARABLES MASTER.xlsx — "2026 Data" sheet
3. OCCUPATIONAL COMPARABLES.xlsx — new file (created if needed)

Handles:
- Backup before every write
- Formula preservation (data_only=False)
- Deduplication against existing rows
- Formatting copied from previous rows
- OneDrive file lock retry with backoff
"""

import difflib
import logging
import re
import shutil
import time
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Optional

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

from email_pipeline.models import (
    DealExtraction,
    InvestmentComp,
    OccupationalComp,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fuzzy deal-matching helpers (used by PipelineWriter + cross-thread dedup)
# ---------------------------------------------------------------------------

# Words that are common in property names but not distinctive
_STOP_WORDS = {
    "the", "a", "an", "and", "of", "at", "in", "on", "for",
    "site", "park", "estate", "industrial", "trading", "business",
    "investment", "fund", "intro", "introduction", "portfolio",
    "centre", "center", "works", "unit", "units", "road", "street",
    "lane", "way", "drive", "close", "ltd", "limited", "plc",
    "son", "sons",
}


def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", " ", name)   # punctuation → space
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _significant_words(name: str) -> set[str]:
    """Extract significant (non-stop) words from a property name."""
    normalised = _normalize_name(name)
    return {w for w in normalised.split() if w not in _STOP_WORDS and len(w) > 1}


def _is_town_match(town_a: str, town_b: str) -> bool:
    """Check if two towns match, treating empty/multi-location as wildcards."""
    a = town_a.strip().lower()
    b = town_b.strip().lower()

    # Wildcards: empty, or multi-location variants
    if not a or not b:
        return True
    if a.startswith("multi") or b.startswith("multi"):
        return True

    return a == b


def is_deal_match(
    name_a: str,
    town_a: str,
    postcode_a: str,
    name_b: str,
    town_b: str,
    postcode_b: str,
) -> tuple[bool, str]:
    """Check if two deals represent the same property.

    Returns (is_match, reason) where reason describes the match type.
    Used by both PipelineWriter._is_duplicate() and _RunDeduplicator.

    Matching tiers (first match wins):
    1. Exact name + town
    2. Postcode match (non-empty)
    3. Substring containment (normalised)
    4. Significant word overlap (≥2 words, ≥60% of shorter set)
    5. Fuzzy name match within same town (SequenceMatcher ≥ 0.65)
    """
    norm_a = _normalize_name(name_a)
    norm_b = _normalize_name(name_b)

    # Skip if either name is empty
    if not norm_a or not norm_b:
        return False, ""

    # --- Tier 1: Exact name + town ---
    if norm_a == norm_b and _is_town_match(town_a, town_b):
        return True, f"exact match"

    # --- Tier 2: Postcode match ---
    pc_a = postcode_a.strip().upper()
    pc_b = postcode_b.strip().upper()
    if pc_a and pc_b and pc_a == pc_b:
        return True, f"postcode match ({pc_a})"

    # --- Tier 3: Substring containment ---
    # Require the shorter name to have ≥2 words to avoid false positives
    # with single-word town/area names (e.g. "Warrington" matching
    # "Warrington Central Trading Estate & Causeway Park")
    shorter_norm = norm_a if len(norm_a) <= len(norm_b) else norm_b
    if len(shorter_norm.split()) >= 2:
        if norm_a in norm_b:
            return True, f'"{name_a}" contained in "{name_b}"'
        if norm_b in norm_a:
            return True, f'"{name_b}" contained in "{name_a}"'

    # --- Tier 4: Significant word overlap ---
    words_a = _significant_words(name_a)
    words_b = _significant_words(name_b)

    if words_a and words_b:
        overlap = words_a & words_b
        shorter_len = min(len(words_a), len(words_b))
        if len(overlap) >= 2 and (len(overlap) / shorter_len) >= 0.6:
            return True, f"word overlap ({', '.join(sorted(overlap))})"

    # --- Tier 5: Fuzzy name match within same town ---
    if _is_town_match(town_a, town_b):
        ratio = difflib.SequenceMatcher(None, norm_a, norm_b).ratio()
        if ratio >= 0.65:
            return True, f"fuzzy match ({ratio:.0%})"

    return False, ""

# Maximum retries for OneDrive-locked files
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# 1. Pipeline Excel Writer
# ---------------------------------------------------------------------------

class PipelineWriter:
    """Writes deal data to Pipeline 2026.xlsx (Intros sheet).

    Schema (headers in row 11, data from row 12):
        B=Date, C=Agent, D=Asset Name, E=Country, F=Town, G=Address,
        H=Classification, I=Area(acres), J=Area(sqft), K=Rent PA,
        L=Rent PSF, M=Asking Price, N=NY%, O=RY%, P=CapVal PSF,
        Q=Status, R=Considered, S=Initial IRR, T=Brochure Stored,
        U=Brochure Scraped, V=Comment
    """

    SHEET_NAME = "Intros"
    HEADER_ROW = 11
    DATA_START_ROW = 12

    # Column mapping (1-indexed)
    COLUMNS = {
        "date": 2,             # B
        "agent": 3,            # C
        "asset_name": 4,       # D
        "country": 5,          # E
        "town": 6,             # F
        "address": 7,          # G
        "classification": 8,   # H
        "area_acres": 9,       # I
        "area_sqft": 10,       # J
        "rent_pa": 11,         # K
        "rent_psf": 12,        # L
        "asking_price": 13,    # M
        "net_yield": 14,       # N
        "reversionary_yield": 15,  # O
        "capval_psf": 16,      # P
        "status": 17,          # Q
        "considered": 18,      # R
        "initial_irr": 19,     # S
        "brochure_stored": 20, # T
        "brochure_scraped": 21,  # U
        "comment": 22,         # V
    }

    def __init__(self, excel_path: Path):
        """Initialize with path to Pipeline Excel file.

        Parameters
        ----------
        excel_path : Path
            Path to Pipeline 2026.xlsx.
        """
        self.excel_path = Path(excel_path)

    def append_deal(
        self,
        deal: DealExtraction,
        has_brochure: bool = False,
        brochure_scraped: bool = False,
        comment: str = "",
    ) -> bool:
        """Append a deal to the Pipeline Excel.

        Parameters
        ----------
        deal : DealExtraction
            The deal data to write.
        has_brochure : bool
            Whether a brochure was saved to the archive.
        brochure_scraped : bool
            Whether brochure data was extracted.
        comment : str
            Optional comment (e.g. "Auto-imported from email pipeline").

        Returns
        -------
        bool
            True if successfully written.
        """
        return _retry_write(
            self.excel_path,
            lambda wb: self._do_append(wb, deal, has_brochure, brochure_scraped, comment),
        )

    def _do_append(
        self,
        wb: "Workbook",
        deal: DealExtraction,
        has_brochure: bool,
        brochure_scraped: bool,
        comment: str,
    ) -> bool:
        """Internal: append deal to the workbook."""
        if self.SHEET_NAME not in wb.sheetnames:
            logger.error("Sheet '%s' not found in %s", self.SHEET_NAME, self.excel_path)
            return False

        ws = wb[self.SHEET_NAME]

        # Check for duplicate (multi-tier fuzzy matching)
        is_dup, dup_reason = self._is_duplicate(ws, deal)
        if is_dup:
            logger.info("  Duplicate found (%s): %s — skipping", dup_reason, deal.asset_name)
            return False

        # Find next empty row (scan column D = asset_name)
        next_row = self._find_next_row(ws)
        logger.info("  Writing to row %d", next_row)

        # Copy formatting from the previous data row
        format_row = next_row - 1 if next_row > self.DATA_START_ROW else self.DATA_START_ROW
        self._copy_row_format(ws, format_row, next_row)

        # Write data
        col = self.COLUMNS
        ws.cell(row=next_row, column=col["date"], value=deal.date)
        ws.cell(row=next_row, column=col["agent"], value=deal.agent)
        ws.cell(row=next_row, column=col["asset_name"], value=deal.asset_name)
        ws.cell(row=next_row, column=col["country"], value=deal.country)
        ws.cell(row=next_row, column=col["town"], value=deal.town)
        # Append postcode to address if not already present
        address_with_postcode = deal.address or ""
        if deal.postcode and deal.postcode not in address_with_postcode:
            address_with_postcode = f"{address_with_postcode}, {deal.postcode}".strip(", ")
        ws.cell(row=next_row, column=col["address"], value=address_with_postcode)
        ws.cell(row=next_row, column=col["classification"], value=deal.classification)

        if deal.area_acres is not None:
            ws.cell(row=next_row, column=col["area_acres"], value=deal.area_acres)
        if deal.area_sqft is not None:
            ws.cell(row=next_row, column=col["area_sqft"], value=deal.area_sqft)
        if deal.rent_pa is not None:
            ws.cell(row=next_row, column=col["rent_pa"], value=deal.rent_pa)
        if deal.rent_psf is not None:
            ws.cell(row=next_row, column=col["rent_psf"], value=deal.rent_psf)
        if deal.asking_price is not None:
            ws.cell(row=next_row, column=col["asking_price"], value=deal.asking_price)
        if deal.net_yield is not None:
            ws.cell(row=next_row, column=col["net_yield"], value=deal.net_yield / 100)
        if deal.reversionary_yield is not None:
            ws.cell(row=next_row, column=col["reversionary_yield"], value=deal.reversionary_yield / 100)
        if deal.capval_psf is not None:
            ws.cell(row=next_row, column=col["capval_psf"], value=deal.capval_psf)

        ws.cell(row=next_row, column=col["status"], value="New")
        ws.cell(row=next_row, column=col["considered"], value="No")
        ws.cell(row=next_row, column=col["brochure_stored"], value="Yes" if has_brochure else "No")
        ws.cell(row=next_row, column=col["brochure_scraped"], value="Yes" if brochure_scraped else "No")

        if comment:
            ws.cell(row=next_row, column=col["comment"], value=comment)

        return True

    def _is_duplicate(self, ws, deal: DealExtraction) -> tuple[bool, str]:
        """Check if a deal already exists in the sheet using fuzzy matching.

        Returns (is_duplicate, reason) where reason describes match type.
        """
        # If asset_name is empty/None, we can't meaningfully match — allow the write
        if not deal.asset_name or not deal.asset_name.strip():
            logger.warning("  Empty asset_name — skipping duplicate check")
            return False, ""

        col_d = self.COLUMNS["asset_name"]  # D
        col_f = self.COLUMNS["town"]        # F
        col_g = self.COLUMNS["address"]     # G (contains postcode)

        deal_postcode = (deal.postcode or "").strip().upper()

        for row in range(self.DATA_START_ROW, ws.max_row + 1):
            existing_name = str(ws.cell(row=row, column=col_d).value or "").strip()
            existing_town = str(ws.cell(row=row, column=col_f).value or "").strip()
            existing_addr = str(ws.cell(row=row, column=col_g).value or "").strip()

            if not existing_name:
                continue

            # Extract postcode from existing address (last word if it looks like a UK postcode)
            existing_postcode = ""
            if existing_addr:
                # UK postcodes end like "XX1 1XX" — grab last ~8 chars worth
                addr_parts = existing_addr.split(",")
                last_part = addr_parts[-1].strip()
                # Simple UK postcode pattern
                pc_match = re.search(r"[A-Z]{1,2}\d[\dA-Z]?\s*\d[A-Z]{2}", last_part.upper())
                if pc_match:
                    existing_postcode = pc_match.group(0)

            matched, reason = is_deal_match(
                name_a=deal.asset_name,
                town_a=deal.town or "",
                postcode_a=deal_postcode,
                name_b=existing_name,
                town_b=existing_town,
                postcode_b=existing_postcode,
            )

            if matched:
                return True, f'{reason} with "{existing_name}" in row {row}'

        return False, ""

    def _find_next_row(self, ws) -> int:
        """Find the next empty row (scanning column D)."""
        col_d = self.COLUMNS["asset_name"]
        for row in range(self.DATA_START_ROW, ws.max_row + 100):
            if not ws.cell(row=row, column=col_d).value:
                return row
        return ws.max_row + 1

    def _copy_row_format(self, ws, source_row: int, target_row: int):
        """Copy cell formatting from source row to target row."""
        for col in range(2, 23):  # B to V
            source_cell = ws.cell(row=source_row, column=col)
            target_cell = ws.cell(row=target_row, column=col)
            if source_cell.has_style:
                target_cell.font = copy(source_cell.font)
                target_cell.number_format = copy(source_cell.number_format)
                target_cell.alignment = copy(source_cell.alignment)
                target_cell.border = copy(source_cell.border)
                target_cell.fill = copy(source_cell.fill)


# ---------------------------------------------------------------------------
# 2. Investment Comparables Writer
# ---------------------------------------------------------------------------

class InvestmentCompsWriter:
    """Writes investment comparables to INVESTMENT COMPARABLES MASTER.xlsx.

    Schema ("2026 Data" sheet):
        B=Town, C=Address, D=Units, E=Area, F=Rent(pa), G=Rent(psf),
        H=AWULTC, I=Price, J=Yield(NIY), K=RY, L=Cap Val psf,
        M=Vendor, N=Purchaser, O=Date
    """

    SHEET_NAME = "2026 Data"

    # Column mapping (1-indexed)
    COLUMNS = {
        "town": 2,                # B
        "address": 3,             # C
        "units": 4,               # D
        "area_sqft": 5,           # E
        "rent_pa": 6,             # F
        "rent_psf": 7,            # G
        "awultc": 8,              # H
        "price": 9,               # I
        "yield_niy": 10,          # J
        "reversionary_yield": 11, # K
        "capval_psf": 12,         # L
        "vendor": 13,             # M
        "purchaser": 14,          # N
        "date": 15,               # O
    }

    def __init__(self, excel_path: Path):
        self.excel_path = Path(excel_path)

    def append_comps(self, comps: list[InvestmentComp]) -> int:
        """Append investment comparables to the Excel file.

        Parameters
        ----------
        comps : list[InvestmentComp]
            Comparables to append.

        Returns
        -------
        int
            Number of comps successfully written.
        """
        if not comps:
            return 0

        count = [0]

        def _write(wb):
            if self.SHEET_NAME not in wb.sheetnames:
                logger.error("Sheet '%s' not found in %s", self.SHEET_NAME, self.excel_path)
                return False

            ws = wb[self.SHEET_NAME]

            for comp in comps:
                if self._is_duplicate(ws, comp):
                    logger.info("  Duplicate comp: %s, %s — skipping", comp.town, comp.address)
                    continue

                next_row = self._find_next_row(ws)
                self._write_comp(ws, next_row, comp)
                count[0] += 1

            return count[0] > 0

        _retry_write(self.excel_path, _write)
        return count[0]

    def _write_comp(self, ws, row: int, comp: InvestmentComp):
        """Write a single comp to a row."""
        col = self.COLUMNS
        ws.cell(row=row, column=col["town"], value=comp.town)
        ws.cell(row=row, column=col["address"], value=comp.address)

        if comp.units is not None:
            ws.cell(row=row, column=col["units"], value=comp.units)
        if comp.area_sqft is not None:
            ws.cell(row=row, column=col["area_sqft"], value=comp.area_sqft)
        if comp.rent_pa is not None:
            ws.cell(row=row, column=col["rent_pa"], value=comp.rent_pa)
        if comp.rent_psf is not None:
            ws.cell(row=row, column=col["rent_psf"], value=comp.rent_psf)
        if comp.awultc is not None:
            ws.cell(row=row, column=col["awultc"], value=comp.awultc)
        if comp.price is not None:
            ws.cell(row=row, column=col["price"], value=comp.price)
        if comp.yield_niy is not None:
            ws.cell(row=row, column=col["yield_niy"], value=comp.yield_niy / 100)
        if comp.reversionary_yield is not None:
            ws.cell(row=row, column=col["reversionary_yield"], value=comp.reversionary_yield / 100)
        if comp.capval_psf is not None:
            ws.cell(row=row, column=col["capval_psf"], value=comp.capval_psf)
        if comp.vendor:
            ws.cell(row=row, column=col["vendor"], value=comp.vendor)
        if comp.purchaser:
            ws.cell(row=row, column=col["purchaser"], value=comp.purchaser)
        if comp.date:
            ws.cell(row=row, column=col["date"], value=comp.date)

    def _is_duplicate(self, ws, comp: InvestmentComp) -> bool:
        """Check if a comp already exists (match on address + town)."""
        col_b = self.COLUMNS["town"]
        col_c = self.COLUMNS["address"]

        for row in range(2, ws.max_row + 1):
            existing_town = str(ws.cell(row=row, column=col_b).value or "").strip().lower()
            existing_addr = str(ws.cell(row=row, column=col_c).value or "").strip().lower()

            if not existing_addr:
                continue

            if (
                existing_town == comp.town.strip().lower()
                and existing_addr == comp.address.strip().lower()
            ):
                return True

        return False

    def _find_next_row(self, ws) -> int:
        """Find the next empty row (scanning column C = address)."""
        col_c = self.COLUMNS["address"]
        for row in range(2, ws.max_row + 100):
            if not ws.cell(row=row, column=col_c).value:
                return row
        return ws.max_row + 1


# ---------------------------------------------------------------------------
# 3. Occupational Comparables Writer
# ---------------------------------------------------------------------------

class OccupationalCompsWriter:
    """Writes occupational comparables to OCCUPATIONAL COMPARABLES.xlsx.

    Creates the file if it doesn't exist.

    Columns:
        A=Source Deal, B=Tenant, C=Unit, D=Address, E=Town, F=Postcode,
        G=Size(sqft), H=Rent PA, I=Rent PSF, J=Lease Start, K=Lease Expiry,
        L=Break Date, M=Review Date, N=Term(yrs), O=Notes, P=Extraction Date
    """

    HEADERS = [
        "Source Deal", "Tenant", "Unit", "Address", "Town", "Postcode",
        "Size (sqft)", "Rent PA", "Rent PSF", "Lease Start", "Lease Expiry",
        "Break Date", "Review Date", "Term (yrs)", "Notes", "Extraction Date",
    ]

    def __init__(self, excel_path: Path):
        self.excel_path = Path(excel_path)

    def append_comps(self, comps: list[OccupationalComp]) -> int:
        """Append occupational comparables to the Excel file.

        Creates the file with headers if it doesn't exist.

        Parameters
        ----------
        comps : list[OccupationalComp]
            Comparables to append.

        Returns
        -------
        int
            Number of comps successfully written.
        """
        if not comps:
            return 0

        # Create file if it doesn't exist
        if not self.excel_path.exists():
            self._create_file()

        count = [0]

        def _write(wb):
            ws = wb.active

            for comp in comps:
                if self._is_duplicate(ws, comp):
                    logger.info("  Duplicate occ comp: %s, %s — skipping",
                               comp.tenant_name, comp.address)
                    continue

                next_row = self._find_next_row(ws)
                self._write_comp(ws, next_row, comp)
                count[0] += 1

            return count[0] > 0

        _retry_write(self.excel_path, _write)
        return count[0]

    def _create_file(self):
        """Create a new Excel file with headers."""
        wb = Workbook()
        ws = wb.active
        ws.title = "Occupational Comps"

        # Write headers
        for i, header in enumerate(self.HEADERS, 1):
            cell = ws.cell(row=1, column=i, value=header)
            cell.font = copy(cell.font)
            from openpyxl.styles import Font
            cell.font = Font(bold=True)

        # Auto-width columns
        for i, header in enumerate(self.HEADERS, 1):
            ws.column_dimensions[get_column_letter(i)].width = max(len(header) + 2, 12)

        self.excel_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(str(self.excel_path))
        wb.close()
        logger.info("Created new file: %s", self.excel_path)

    def _write_comp(self, ws, row: int, comp: OccupationalComp):
        """Write a single occupational comp to a row."""
        ws.cell(row=row, column=1, value=comp.source_deal)
        ws.cell(row=row, column=2, value=comp.tenant_name)
        ws.cell(row=row, column=3, value=comp.unit_name or "")
        ws.cell(row=row, column=4, value=comp.address)
        ws.cell(row=row, column=5, value=comp.town)
        ws.cell(row=row, column=6, value=comp.postcode or "")

        if comp.size_sqft is not None:
            ws.cell(row=row, column=7, value=comp.size_sqft)
        if comp.rent_pa is not None:
            ws.cell(row=row, column=8, value=comp.rent_pa)
        if comp.rent_psf is not None:
            ws.cell(row=row, column=9, value=comp.rent_psf)

        ws.cell(row=row, column=10, value=comp.lease_start or "")
        ws.cell(row=row, column=11, value=comp.lease_expiry or "")
        ws.cell(row=row, column=12, value=comp.break_date or "")
        ws.cell(row=row, column=13, value=comp.rent_review_date or "")

        if comp.lease_term_years is not None:
            ws.cell(row=row, column=14, value=comp.lease_term_years)

        ws.cell(row=row, column=15, value=comp.notes or "")
        ws.cell(row=row, column=16, value=datetime.now().strftime("%d/%m/%Y"))

    def _is_duplicate(self, ws, comp: OccupationalComp) -> bool:
        """Check if a comp already exists (match on tenant + address + source)."""
        for row in range(2, ws.max_row + 1):
            existing_source = str(ws.cell(row=row, column=1).value or "").strip().lower()
            existing_tenant = str(ws.cell(row=row, column=2).value or "").strip().lower()
            existing_addr = str(ws.cell(row=row, column=4).value or "").strip().lower()

            if not existing_tenant:
                continue

            if (
                existing_source == comp.source_deal.strip().lower()
                and existing_tenant == comp.tenant_name.strip().lower()
                and existing_addr == comp.address.strip().lower()
            ):
                return True

        return False

    def _find_next_row(self, ws) -> int:
        """Find the next empty row (scanning column B = tenant)."""
        for row in range(2, ws.max_row + 100):
            if not ws.cell(row=row, column=2).value:
                return row
        return ws.max_row + 1


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _backup_file(file_path: Path) -> Optional[Path]:
    """Create a timestamped backup of an Excel file.

    Parameters
    ----------
    file_path : Path
        Path to the file to back up.

    Returns
    -------
    Path or None
        Path to the backup file, or None if failed.
    """
    if not file_path.exists():
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = file_path.parent / "backups"
    backup_dir.mkdir(exist_ok=True)

    backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    backup_path = backup_dir / backup_name

    try:
        shutil.copy2(str(file_path), str(backup_path))
        logger.info("  Backup created: %s", backup_path.name)
        return backup_path
    except Exception as e:
        logger.warning("  Failed to create backup: %s", e)
        return None


def _retry_write(file_path: Path, write_fn, max_retries: int = MAX_RETRIES) -> bool:
    """Open an Excel file, call write_fn, and save with retry logic.

    Handles OneDrive file locks by retrying with exponential backoff.

    Parameters
    ----------
    file_path : Path
        Path to the Excel file.
    write_fn : callable
        Function that takes a Workbook and returns bool (True if changes made).
    max_retries : int
        Maximum number of retries.

    Returns
    -------
    bool
        True if changes were actually written (write_fn returned True and save succeeded).
        False if skipped (duplicate), file locked, or error.
    """
    for attempt in range(max_retries):
        try:
            # Backup before write
            _backup_file(file_path)

            # Open workbook (preserve formulas)
            wb = load_workbook(str(file_path), data_only=False)

            # Call the write function
            changes_made = write_fn(wb)

            if changes_made:
                wb.save(str(file_path))
                logger.info("  Saved: %s", file_path.name)
            else:
                logger.info("  No changes to save")

            wb.close()
            return bool(changes_made)

        except PermissionError as e:
            if attempt < max_retries - 1:
                wait = RETRY_DELAY * (2 ** attempt)
                logger.warning(
                    "  File locked (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, max_retries, wait, e,
                )
                time.sleep(wait)
            else:
                logger.error("  File locked after %d attempts: %s", max_retries, e)
                return False

        except Exception as e:
            logger.error("  Excel write error: %s", e)
            return False

    return False
