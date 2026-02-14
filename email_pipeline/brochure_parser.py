"""
Brochure Parser
===============
Extracts investment and occupational comparables from brochure files (PDF/Excel).

Two extraction modes:
- Investment comparables: from comparable evidence / transaction sections
- Occupational comparables: from tenancy schedules / letting details

Uses pdfplumber for PDF table extraction (primary), PyMuPDF for text fallback,
and openpyxl for Excel files. Claude API for structured extraction.
"""

import json
import logging
from pathlib import Path
from typing import Optional

import anthropic

from email_pipeline.models import (
    BrochureResult,
    DealExtraction,
    InvestmentComp,
    OccupationalComp,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF file.

    Tries pdfplumber first (good for tables), falls back to PyMuPDF.

    Parameters
    ----------
    pdf_path : Path
        Path to the PDF file.

    Returns
    -------
    str
        Extracted text content.
    """
    text = ""

    # Try pdfplumber first (better for tables)
    try:
        import pdfplumber

        with pdfplumber.open(str(pdf_path)) as pdf:
            pages_text = []
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""

                # Also extract tables
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        for row in table:
                            if row:
                                cells = [str(c).strip() if c else "" for c in row]
                                page_text += "\n" + " | ".join(cells)

                if page_text.strip():
                    pages_text.append(f"--- Page {i + 1} ---\n{page_text}")

            text = "\n\n".join(pages_text)

    except ImportError:
        logger.warning("pdfplumber not installed, trying PyMuPDF")
    except Exception as e:
        logger.warning("pdfplumber failed on %s: %s, trying PyMuPDF", pdf_path.name, e)

    # Fallback to PyMuPDF
    if not text.strip():
        try:
            import fitz  # PyMuPDF

            doc = fitz.open(str(pdf_path))
            pages_text = []
            for i, page in enumerate(doc):
                page_text = page.get_text()
                if page_text.strip():
                    pages_text.append(f"--- Page {i + 1} ---\n{page_text}")
            doc.close()
            text = "\n\n".join(pages_text)

        except ImportError:
            logger.error("Neither pdfplumber nor PyMuPDF available")
        except Exception as e:
            logger.error("PyMuPDF failed on %s: %s", pdf_path.name, e)

    return text


def extract_text_from_excel(excel_path: Path) -> str:
    """Extract text from an Excel file.

    Reads all sheets and formats as text tables.

    Parameters
    ----------
    excel_path : Path
        Path to the Excel file.

    Returns
    -------
    str
        Extracted text content.
    """
    try:
        from openpyxl import load_workbook

        wb = load_workbook(str(excel_path), data_only=True, read_only=True)
        sheets_text = []

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = []
            for row in ws.iter_rows(values_only=True):
                cells = [str(c).strip() if c is not None else "" for c in row]
                if any(cells):  # Skip empty rows
                    rows.append(" | ".join(cells))

            if rows:
                sheets_text.append(f"--- Sheet: {sheet_name} ---\n" + "\n".join(rows))

        wb.close()
        return "\n\n".join(sheets_text)

    except ImportError:
        logger.error("openpyxl not installed")
        return ""
    except Exception as e:
        logger.error("Failed to read Excel %s: %s", excel_path.name, e)
        return ""


def extract_text(file_path: Path) -> str:
    """Extract text from a brochure file (PDF or Excel).

    Parameters
    ----------
    file_path : Path
        Path to the file.

    Returns
    -------
    str
        Extracted text content.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf":
        return extract_text_from_pdf(file_path)
    elif suffix in (".xlsx", ".xls"):
        return extract_text_from_excel(file_path)
    else:
        logger.warning("Unsupported file type: %s", suffix)
        return ""


# ---------------------------------------------------------------------------
# AI extraction prompts
# ---------------------------------------------------------------------------

INVESTMENT_COMPS_PROMPT = """You are a commercial property analyst. Extract all **investment comparable evidence** from the brochure text below.

Investment comparables are recent property transactions (sales) that are used to benchmark pricing. They typically include:
- Property name and location
- Sale price, yield (NIY), and date
- Building size and rent details

Look for sections labelled "Comparable Evidence", "Investment Comparables", "Market Transactions", "Recent Sales", or similar. Also look in schedules and tables.

For each comparable, extract:
- **town**: Town/city
- **address**: Property name or address
- **units**: Number of units (null if not stated)
- **area_sqft**: Total area in sq ft (convert from sq m if needed: 1 sq m = 10.764 sq ft)
- **rent_pa**: Passing rent per annum in £
- **rent_psf**: Rent per sq ft (derive if possible)
- **awultc**: Average weighted unexpired lease term to certain income (years)
- **price**: Sale price in £
- **yield_niy**: Net Initial Yield as a percentage (e.g. 6.5)
- **reversionary_yield**: Reversionary yield as a percentage
- **capval_psf**: Capital value per sq ft in £
- **vendor**: Vendor/seller name
- **purchaser**: Purchaser/buyer name
- **date**: Transaction date (DD/MM/YYYY or MM/YYYY)

Return ONLY valid JSON array. Use null for unknown values:
[
    {{
        "town": "Birmingham",
        "address": "Matrix Park",
        "units": 8,
        "area_sqft": 45000,
        "rent_pa": 350000,
        "rent_psf": 7.78,
        "awultc": 5.2,
        "price": 5250000,
        "yield_niy": 6.3,
        "reversionary_yield": 7.1,
        "capval_psf": 116.67,
        "vendor": "Legal & General",
        "purchaser": "Brydell Partners",
        "date": "03/2025"
    }}
]

If no investment comparables are found, return an empty array: []

## Brochure text:

{text}
"""

OCCUPATIONAL_COMPS_PROMPT = """You are a commercial property analyst. Extract all **occupational / letting comparable evidence** and **tenancy schedule details** from the brochure text below.

Occupational comparables are lease transactions or current tenancy details. They include:
- Tenant names and unit details
- Rent levels, lease terms, break dates
- Found in "Tenancy Schedule", "Occupational Comparables", "Letting Evidence", "Income Schedule" sections

For each occupational comparable or tenancy, extract:
- **tenant_name**: Tenant / occupier name
- **unit_name**: Unit identifier or name (null if not stated)
- **address**: Property address
- **town**: Town/city
- **postcode**: Postcode (null if not stated)
- **size_sqft**: Unit size in sq ft (convert from sq m if needed)
- **rent_pa**: Annual rent in £
- **rent_psf**: Rent per sq ft (derive if possible)
- **lease_start**: Lease start date (DD/MM/YYYY)
- **lease_expiry**: Lease expiry date (DD/MM/YYYY)
- **break_date**: Break date (DD/MM/YYYY or null)
- **rent_review_date**: Next rent review date (DD/MM/YYYY or null)
- **lease_term_years**: Total lease term in years
- **notes**: Any other relevant notes

Return ONLY valid JSON array. Use null for unknown values:
[
    {{
        "tenant_name": "Amazon",
        "unit_name": "Unit 1",
        "address": "Matrix Park, Chorley",
        "town": "Chorley",
        "postcode": "PR7 7NA",
        "size_sqft": 25000,
        "rent_pa": 175000,
        "rent_psf": 7.0,
        "lease_start": "01/06/2020",
        "lease_expiry": "31/05/2030",
        "break_date": null,
        "rent_review_date": "01/06/2025",
        "lease_term_years": 10,
        "notes": "FRI lease"
    }}
]

If no occupational comparables or tenancy details are found, return an empty array: []

## Brochure text:

{text}
"""

DEAL_FROM_BROCHURE_PROMPT = """You are a commercial property analyst. Extract the main **deal / property details** from this investment brochure.

This is a brochure for a commercial property being marketed for sale. Extract the key investment metrics.

Extract:
- **asset_name**: Property or estate name
- **town**: Town/city
- **address**: Full address
- **classification**: One of "Multi-Let Industrial", "Single-Let Industrial", "Multi-Let Office", "Single-Let Office", "Retail", "Mixed Use", "Logistics", "Land", "Portfolio", "Other"
- **area_acres**: Site area in acres (null if not stated)
- **area_sqft**: Total building area in sq ft (convert from sq m if needed)
- **rent_pa**: Total passing rent per annum in £
- **rent_psf**: Rent per sq ft
- **asking_price**: Asking/quoting price in £
- **net_yield**: Net Initial Yield as percentage (e.g. 6.5)
- **reversionary_yield**: Reversionary yield as percentage
- **confidence**: Your confidence in the extraction (0.0 to 1.0)

Return ONLY valid JSON:
{{
    "asset_name": "...",
    "town": "...",
    "address": "...",
    "classification": "...",
    "area_acres": null,
    "area_sqft": null,
    "rent_pa": null,
    "rent_psf": null,
    "asking_price": null,
    "net_yield": null,
    "reversionary_yield": null,
    "confidence": 0.8
}}

## Brochure text:

{text}
"""


# ---------------------------------------------------------------------------
# Main extraction functions
# ---------------------------------------------------------------------------

def parse_brochure(
    file_path: Path,
    api_key: str,
    source_deal: str = "",
    model: str = "claude-sonnet-4-20250514",
    extract_deal: bool = True,
    extract_investment_comps: bool = True,
    extract_occupational_comps: bool = True,
) -> BrochureResult:
    """Parse a brochure file and extract all comparables.

    Parameters
    ----------
    file_path : Path
        Path to the brochure file (PDF or Excel).
    api_key : str
        Anthropic API key.
    source_deal : str
        Name of the source deal (for occupational comp tracking).
    model : str
        Claude model to use.
    extract_deal : bool
        Whether to extract deal/property details.
    extract_investment_comps : bool
        Whether to extract investment comparables.
    extract_occupational_comps : bool
        Whether to extract occupational comparables.

    Returns
    -------
    BrochureResult
        Extraction results.
    """
    file_path = Path(file_path)
    result = BrochureResult(file_path=str(file_path))

    # Step 1: Extract text
    logger.info("Extracting text from %s...", file_path.name)
    text = extract_text(file_path)

    if not text.strip():
        result.error_message = f"No text extracted from {file_path.name}"
        logger.warning(result.error_message)
        return result

    logger.info("  Extracted %d characters of text", len(text))

    # Truncate to fit within token limits
    max_chars = 15000
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[... truncated ...]"

    client = anthropic.Anthropic(api_key=api_key)

    # Step 2: Extract deal details (if requested)
    if extract_deal:
        try:
            result.deal_extraction = _extract_deal_from_text(client, text, model)
            if result.deal_extraction and not source_deal:
                source_deal = result.deal_extraction.asset_name or file_path.stem
            logger.info("  Deal extraction: %s",
                        result.deal_extraction.asset_name if result.deal_extraction else "None")
        except Exception as e:
            logger.error("  Deal extraction failed: %s", e)

    # Step 3: Extract investment comparables (if requested)
    if extract_investment_comps:
        try:
            result.investment_comps = _extract_investment_comps(client, text, model)
            logger.info("  Investment comps: %d found", len(result.investment_comps))
        except Exception as e:
            logger.error("  Investment comp extraction failed: %s", e)

    # Step 4: Extract occupational comparables (if requested)
    if extract_occupational_comps:
        try:
            result.occupational_comps = _extract_occupational_comps(
                client, text, source_deal or file_path.stem, model
            )
            logger.info("  Occupational comps: %d found", len(result.occupational_comps))
        except Exception as e:
            logger.error("  Occupational comp extraction failed: %s", e)

    return result


# ---------------------------------------------------------------------------
# Internal extraction functions
# ---------------------------------------------------------------------------

def _extract_deal_from_text(
    client: anthropic.Anthropic,
    text: str,
    model: str,
) -> Optional[DealExtraction]:
    """Extract deal details from brochure text using Claude API."""
    prompt = DEAL_FROM_BROCHURE_PROMPT.format(text=text)

    message = client.messages.create(
        model=model,
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = _strip_code_block(message.content[0].text.strip())
    data = json.loads(response_text)

    return DealExtraction(
        date="",  # Not available from brochure
        agent="",  # Not available from brochure
        asset_name=data.get("asset_name", ""),
        country="England",
        town=data.get("town", ""),
        address=data.get("address", ""),
        classification=data.get("classification", ""),
        area_acres=_to_float(data.get("area_acres")),
        area_sqft=_to_float(data.get("area_sqft")),
        rent_pa=_to_float(data.get("rent_pa")),
        rent_psf=_to_float(data.get("rent_psf")),
        asking_price=_to_float(data.get("asking_price")),
        net_yield=_to_float(data.get("net_yield")),
        reversionary_yield=_to_float(data.get("reversionary_yield")),
        confidence=data.get("confidence", 0.7),
        raw_source="brochure",
    )


def _extract_investment_comps(
    client: anthropic.Anthropic,
    text: str,
    model: str,
) -> list[InvestmentComp]:
    """Extract investment comparables from brochure text using Claude API."""
    prompt = INVESTMENT_COMPS_PROMPT.format(text=text)

    message = client.messages.create(
        model=model,
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = _strip_code_block(message.content[0].text.strip())
    data = json.loads(response_text)

    if not isinstance(data, list):
        return []

    comps = []
    for item in data:
        comps.append(
            InvestmentComp(
                town=item.get("town", ""),
                address=item.get("address", ""),
                units=_to_int(item.get("units")),
                area_sqft=_to_float(item.get("area_sqft")),
                rent_pa=_to_float(item.get("rent_pa")),
                rent_psf=_to_float(item.get("rent_psf")),
                awultc=_to_float(item.get("awultc")),
                price=_to_float(item.get("price")),
                yield_niy=_to_float(item.get("yield_niy")),
                reversionary_yield=_to_float(item.get("reversionary_yield")),
                capval_psf=_to_float(item.get("capval_psf")),
                vendor=item.get("vendor"),
                purchaser=item.get("purchaser"),
                date=item.get("date"),
            )
        )

    return comps


def _extract_occupational_comps(
    client: anthropic.Anthropic,
    text: str,
    source_deal: str,
    model: str,
) -> list[OccupationalComp]:
    """Extract occupational comparables from brochure text using Claude API."""
    prompt = OCCUPATIONAL_COMPS_PROMPT.format(text=text)

    message = client.messages.create(
        model=model,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = _strip_code_block(message.content[0].text.strip())
    data = json.loads(response_text)

    if not isinstance(data, list):
        return []

    comps = []
    for item in data:
        comps.append(
            OccupationalComp(
                source_deal=source_deal,
                tenant_name=item.get("tenant_name", ""),
                unit_name=item.get("unit_name"),
                address=item.get("address", ""),
                town=item.get("town", ""),
                postcode=item.get("postcode"),
                size_sqft=_to_float(item.get("size_sqft")),
                rent_pa=_to_float(item.get("rent_pa")),
                rent_psf=_to_float(item.get("rent_psf")),
                lease_start=item.get("lease_start"),
                lease_expiry=item.get("lease_expiry"),
                break_date=item.get("break_date"),
                rent_review_date=item.get("rent_review_date"),
                lease_term_years=_to_float(item.get("lease_term_years")),
                notes=item.get("notes"),
            )
        )

    return comps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_code_block(text: str) -> str:
    """Strip markdown code block markers from text."""
    if text.startswith("```"):
        lines = text.split("\n")
        json_lines = [l for l in lines if not l.strip().startswith("```")]
        return "\n".join(json_lines)
    return text


def _to_float(value) -> Optional[float]:
    """Convert to float, handling strings with commas/symbols."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("£", "").replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned.lower() in ("null", "none", "n/a", "tbc", "poa"):
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _to_int(value) -> Optional[int]:
    """Convert to int, handling strings."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if not cleaned or cleaned.lower() in ("null", "none", "n/a"):
            return None
        try:
            return int(float(cleaned))
        except ValueError:
            return None
    return None
