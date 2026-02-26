"""Centralised column constants for occupational comparables.

Single source of truth shared by excel_writer, occ_comps_cleaner,
find_occ_dupes, and occ_comps_db.
"""

# ---------------------------------------------------------------------------
# Column indices (1-based, matching Excel layout)
# ---------------------------------------------------------------------------
COL_SOURCE = 1          # A: Source Deal
COL_ENTRY_TYPE = 2      # B: Entry Type
COL_TENANT = 3          # C: Tenant
COL_UNIT = 4            # D: Unit
COL_ADDRESS = 5         # E: Address
COL_TOWN = 6            # F: Town
COL_POSTCODE = 7        # G: Postcode
COL_SIZE = 8            # H: Size (sqft)
COL_RENT_PA = 9         # I: Rent PA
COL_RENT_PSF = 10       # J: Rent PSF
COL_LEASE_START = 11    # K: Lease Start
COL_LEASE_EXPIRY = 12   # L: Lease Expiry
COL_BREAK = 13          # M: Break Date
COL_REVIEW = 14         # N: Review Date
COL_TERM = 15           # O: Term (yrs)
COL_COMP_DATE = 16      # P: Comp Date
COL_NOTES = 17          # Q: Notes
COL_SOURCE_FILE = 18    # R: Source File
COL_EXTRACTION_DATE = 19  # S: Extraction Date
COL_TOTAL_ADDRESS = 20  # T: Total Address (cleaned file only)

# Alias used by find_occ_dupes
COL_EXTRACTION = COL_EXTRACTION_DATE

# Range covering all raw columns (A–S)
COL_RANGE = range(1, 20)

# ---------------------------------------------------------------------------
# Header lists
# ---------------------------------------------------------------------------
RAW_HEADERS = [
    "Source Deal", "Entry Type", "Tenant", "Unit", "Address", "Town",
    "Postcode", "Size (sqft)", "Rent PA", "Rent PSF", "Lease Start",
    "Lease Expiry", "Break Date", "Review Date", "Term (yrs)",
    "Comp Date", "Notes", "Source File", "Extraction Date",
]

CLEANED_HEADERS = RAW_HEADERS + ["Total Address"]

# ---------------------------------------------------------------------------
# Display-friendly column name lookup
# ---------------------------------------------------------------------------
COL_NAMES = {
    1: "Source Deal", 2: "Entry Type", 3: "Tenant", 4: "Unit",
    5: "Address", 6: "Town", 7: "Postcode", 8: "Size",
    9: "Rent PA", 10: "Rent PSF", 11: "Lease Start", 12: "Lease Expiry",
    13: "Break", 14: "Review", 15: "Term", 16: "Comp Date",
    17: "Notes", 18: "Source File", 19: "Extraction Date",
}

# ---------------------------------------------------------------------------
# Date column keys (dict-key names used in cleaned row dicts)
# ---------------------------------------------------------------------------
DATE_COLUMNS = [
    "lease_start", "lease_expiry", "break_date",
    "rent_review_date", "comp_date",
]

# ---------------------------------------------------------------------------
# Mapping: DB column name -> Excel column index (for raw table)
# ---------------------------------------------------------------------------
DB_COL_TO_EXCEL = {
    "source_deal": COL_SOURCE,
    "entry_type": COL_ENTRY_TYPE,
    "tenant_name": COL_TENANT,
    "unit_name": COL_UNIT,
    "address": COL_ADDRESS,
    "town": COL_TOWN,
    "postcode": COL_POSTCODE,
    "size_sqft": COL_SIZE,
    "rent_pa": COL_RENT_PA,
    "rent_psf": COL_RENT_PSF,
    "lease_start": COL_LEASE_START,
    "lease_expiry": COL_LEASE_EXPIRY,
    "break_date": COL_BREAK,
    "rent_review_date": COL_REVIEW,
    "lease_term_years": COL_TERM,
    "comp_date": COL_COMP_DATE,
    "notes": COL_NOTES,
    "source_file_path": COL_SOURCE_FILE,
    "extraction_date": COL_EXTRACTION_DATE,
}
