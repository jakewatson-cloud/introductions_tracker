"""
Data Models
===========
Dataclasses for structured data extracted from emails and brochures.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ProcessingStatus(Enum):
    """Status of email processing."""
    PENDING = "pending"
    CLASSIFIED = "classified"
    PROCESSED = "processed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class DealExtraction:
    """Structured investment deal data — maps to Pipeline Excel columns B-V."""

    date: str                                  # B — DD/MM/YYYY
    agent: str                                 # C
    asset_name: str                            # D
    country: str = "England"                   # E
    town: str = ""                             # F
    address: str = ""                          # G
    postcode: str = ""                         # Extracted UK postcode (e.g. "DY4 0PY")
    classification: str = ""                   # H — Multi-Let, Single-Let, etc.
    area_acres: Optional[float] = None         # I
    area_sqft: Optional[float] = None          # J
    rent_pa: Optional[float] = None            # K
    rent_psf: Optional[float] = None           # L
    asking_price: Optional[float] = None       # M
    net_yield: Optional[float] = None          # N — as percentage
    reversionary_yield: Optional[float] = None # O — as percentage
    capval_psf: Optional[float] = None         # P — derived: price / sqft
    confidence: float = 0.0                    # 0-1 extraction confidence
    raw_source: str = "email"                  # 'email' or 'brochure'

    def __post_init__(self):
        """Derive capval_psf if not provided."""
        if self.capval_psf is None and self.asking_price and self.area_sqft:
            self.capval_psf = round(self.asking_price / self.area_sqft, 2)


@dataclass
class InvestmentComp:
    """Investment comparable — maps to INVESTMENT COMPARABLES MASTER columns."""

    town: str                                  # B
    address: str                               # C
    units: Optional[int] = None                # D
    area_sqft: Optional[float] = None          # E
    rent_pa: Optional[float] = None            # F
    rent_psf: Optional[float] = None           # G
    awultc: Optional[float] = None             # H — average weighted unexpired lease term
    price: Optional[float] = None              # I
    yield_niy: Optional[float] = None          # J — net initial yield %
    reversionary_yield: Optional[float] = None # K
    capval_psf: Optional[float] = None         # L
    vendor: Optional[str] = None               # M
    purchaser: Optional[str] = None            # N
    date: Optional[str] = None                 # O


@dataclass
class OccupationalComp:
    """Letting / occupational comparable from brochure tenancy schedules."""

    source_deal: str
    tenant_name: str
    unit_name: Optional[str] = None
    address: str = ""
    town: str = ""
    postcode: Optional[str] = None
    size_sqft: Optional[float] = None
    rent_pa: Optional[float] = None
    rent_psf: Optional[float] = None
    lease_start: Optional[str] = None
    lease_expiry: Optional[str] = None
    break_date: Optional[str] = None
    rent_review_date: Optional[str] = None
    lease_term_years: Optional[float] = None
    notes: Optional[str] = None


@dataclass
class EmailSummary:
    """Summary of a scanned email — used for dry-run listing and processing decisions."""

    gmail_message_id: str
    subject: str
    sender: str
    sender_domain: str
    date: str                  # ISO 8601
    snippet: str               # Gmail snippet (first ~100 chars)
    body_preview: str = ""     # First ~500 chars of body text (for batch classifier)
    has_attachments: bool = False
    attachment_names: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    matched_keywords: list[str] = field(default_factory=list)
    matched_sender: bool = False
    matched_label: bool = False
    thread_id: str = ""        # Gmail threadId for grouping


@dataclass
class ThreadSummary:
    """A group of emails sharing the same Gmail thread ID."""

    thread_id: str
    email_count: int
    latest_date: str                       # ISO 8601 from most recent email
    latest_subject: str                    # subject of most recent email
    earliest_date: str                     # ISO 8601 from oldest email
    all_sender_domains: list[str]          # unique sender domains
    all_attachment_names: list[str]        # combined, deduplicated
    matched_sender: bool                   # True if ANY email in thread matched
    matched_label: bool                    # True if ANY email in thread matched
    matched_keywords: list[str]            # combined unique keywords
    emails: list[EmailSummary] = field(default_factory=list)  # oldest first


@dataclass
class ClassificationResult:
    """Result of AI classification — is this email an investment introduction?"""

    gmail_message_id: str
    is_introduction: bool
    confidence: float                  # 0-1
    reason: str                        # Short explanation
    suggested_asset_name: str = ""     # Quick extract if introduction
    suggested_town: str = ""           # Quick extract if introduction


@dataclass
class ProcessingResult:
    """Result of processing a single email through the full pipeline."""

    gmail_message_id: str
    status: ProcessingStatus
    is_introduction: bool = False
    deals: list[DealExtraction] = field(default_factory=list)
    archive_folders: list[str] = field(default_factory=list)
    pipeline_rows_added: int = 0
    investment_comps: list[InvestmentComp] = field(default_factory=list)
    occupational_comps: list[OccupationalComp] = field(default_factory=list)
    brochures_parsed: int = 0
    error_message: str = ""


@dataclass
class BrochureResult:
    """Result of parsing a single brochure file."""

    file_path: str
    deal_extraction: Optional[DealExtraction] = None
    investment_comps: list[InvestmentComp] = field(default_factory=list)
    occupational_comps: list[OccupationalComp] = field(default_factory=list)
    error_message: str = ""


@dataclass
class ProcessingReport:
    """Summary report for a batch processing run."""

    total_scanned: int = 0
    already_processed: int = 0
    classified_as_introduction: int = 0
    classified_as_not_introduction: int = 0
    successfully_processed: int = 0
    pipeline_rows_added: int = 0
    emails_archived: int = 0
    brochures_parsed: int = 0
    investment_comps_added: int = 0
    occupational_comps_added: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """Return a formatted summary string."""
        lines = [
            "Processing Report",
            "=" * 50,
            f"  Emails scanned:          {self.total_scanned}",
            f"  Already processed:       {self.already_processed}",
            f"  Introductions found:     {self.classified_as_introduction}",
            f"  Not introductions:       {self.classified_as_not_introduction}",
            f"  Successfully processed:  {self.successfully_processed}",
            f"  Pipeline rows added:     {self.pipeline_rows_added}",
            f"  Emails archived:         {self.emails_archived}",
            f"  Brochures parsed:        {self.brochures_parsed}",
            f"  Investment comps added:  {self.investment_comps_added}",
            f"  Occupational comps added:{self.occupational_comps_added}",
            f"  Errors:                  {self.errors}",
        ]
        if self.error_details:
            lines.append("")
            lines.append("  Error Details:")
            for err in self.error_details[:10]:
                lines.append(f"    - {err}")
            if len(self.error_details) > 10:
                lines.append(f"    ... and {len(self.error_details) - 10} more")
        return "\n".join(lines)
