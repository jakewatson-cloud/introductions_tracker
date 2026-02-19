"""
Email Pipeline Runner
=====================
CLI orchestrator for the investment email pipeline.

Usage:
    # Scan emails (dry-run — list matches without processing)
    python email_pipeline_runner.py scan --after 2025-01-01 --before 2025-02-01

    # Scan emails in a specific label
    python email_pipeline_runner.py scan --label "Investment Introduction"

    # Scan with sender + keyword filters from .env
    python email_pipeline_runner.py scan --after 2025-01-01 --use-config-senders

    # Process emails (full pipeline — classify, extract, archive, update Excel)
    python email_pipeline_runner.py process --after 2025-01-01 --before 2025-02-01

    # Process with auto-confirm (no prompt)
    python email_pipeline_runner.py process --after 2025-01-01 --auto

    # Dry-run processing (classify only, don't write anything)
    python email_pipeline_runner.py process --after 2025-01-01 --dry-run

    # Parse a brochure file for comparables
    python email_pipeline_runner.py parse-brochure path/to/brochure.pdf

    # Parse brochure — investment comps only
    python email_pipeline_runner.py parse-brochure path/to/brochure.pdf --inv-comps-only

    # Show processing stats
    python email_pipeline_runner.py stats
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on the path
_PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_PROJECT_ROOT))

from config import (
    get_anthropic_api_key,
    get_cleaned_occupational_comps_path,
    get_db_path,
    get_email_keywords,
    get_gmail_credentials_path,
    get_gmail_scan_label,
    get_gmail_token_path,
    get_intros_archive_path,
    get_investment_comps_path,
    get_occupational_comps_path,
    get_pipeline_excel_path,
    get_sender_whitelist,
)
from email_pipeline.gmail_auth import get_gmail_service
from email_pipeline.email_scanner import scan_emails, print_scan_results


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_scan(args):
    """Scan Gmail for investment introduction emails (dry-run)."""
    print("Email Pipeline — Scan Mode")
    print("=" * 60)

    # Authenticate
    creds_path = get_gmail_credentials_path()
    token_path = get_gmail_token_path()

    if not token_path.exists():
        print("ERROR: Not authenticated. Run 'python setup_gmail_auth.py' first.")
        sys.exit(1)

    print("  Authenticating with Gmail...")
    service = get_gmail_service(creds_path, token_path)
    print("  Authenticated successfully")
    print()

    # Build filters
    label = args.label or (get_gmail_scan_label() if args.use_config_label else None)

    if args.senders:
        sender_whitelist = [s.strip() for s in args.senders.split(",") if s.strip()]
    else:
        sender_whitelist = get_sender_whitelist() if args.use_config_senders else []

    if args.keywords:
        keywords = [k.strip().lower() for k in args.keywords.split(",") if k.strip()]
    else:
        keywords = get_email_keywords() if args.use_config_keywords else []

    # Print filter summary
    print("  Filters:")
    print(f"    Date range:  {args.after or '(any)'} to {args.before or '(any)'}")
    print(f"    Label:       {label or '(none)'}")
    print(f"    Senders:     {', '.join(sender_whitelist) if sender_whitelist else '(none)'}")
    print(f"    Keywords:    {', '.join(keywords) if keywords else '(none)'}")
    print(f"    Max results: {args.max}")
    print()

    # Scan
    summaries = scan_emails(
        service=service,
        after_date=args.after,
        before_date=args.before,
        label=label,
        sender_whitelist=sender_whitelist,
        keywords=keywords,
        max_results=args.max,
    )

    # Print results
    print_scan_results(summaries)


def cmd_process(args):
    """Process emails — full pipeline."""
    _setup_logging(args.verbose)

    print("Email Pipeline — Process Mode")
    print("=" * 60)

    # Check API key
    api_key = get_anthropic_api_key()
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set in .env")
        print("  Get your API key from https://console.anthropic.com")
        sys.exit(1)

    # Authenticate Gmail
    creds_path = get_gmail_credentials_path()
    token_path = get_gmail_token_path()

    if not token_path.exists():
        print("ERROR: Not authenticated. Run 'python setup_gmail_auth.py' first.")
        sys.exit(1)

    print("  Authenticating with Gmail...")
    service = get_gmail_service(creds_path, token_path)
    print("  Authenticated successfully")

    # Set up database
    from email_pipeline.database import Database
    db = Database(str(get_db_path()))
    print(f"  Database: {db.db_path}")

    # Get paths
    archive_root = get_intros_archive_path()
    pipeline_path = get_pipeline_excel_path()
    inv_comps_path = get_investment_comps_path()
    occ_comps_path = get_occupational_comps_path()

    # Validate paths
    if archive_root and not archive_root.exists():
        print(f"WARNING: Archive folder not found: {archive_root}")
        archive_root = None

    if pipeline_path and not pipeline_path.exists():
        print(f"WARNING: Pipeline Excel not found: {pipeline_path}")
        pipeline_path = None

    if inv_comps_path and not inv_comps_path.exists():
        print(f"WARNING: Investment Comps Excel not found: {inv_comps_path}")
        inv_comps_path = None

    # Build filters
    label = args.label or (get_gmail_scan_label() if args.use_config_label else None)

    if args.senders:
        sender_whitelist = [s.strip() for s in args.senders.split(",") if s.strip()]
    else:
        sender_whitelist = get_sender_whitelist() if args.use_config_senders else []

    if args.keywords:
        keywords = [k.strip().lower() for k in args.keywords.split(",") if k.strip()]
    else:
        keywords = get_email_keywords() if args.use_config_keywords else []

    # Print config summary
    print()
    print("  Configuration:")
    print(f"    Date range:      {args.after or '(any)'} to {args.before or '(any)'}")
    print(f"    Label:           {label or '(none)'}")
    print(f"    Senders:         {len(sender_whitelist)} domains")
    print(f"    Archive:         {archive_root or '(disabled)'}")
    print(f"    Pipeline Excel:  {pipeline_path.name if pipeline_path else '(disabled)'}")
    print(f"    Inv Comps Excel: {inv_comps_path.name if inv_comps_path else '(disabled)'}")
    print(f"    Occ Comps Excel: {occ_comps_path.name if occ_comps_path else '(will create)'}")
    print(f"    Dry run:         {args.dry_run}")
    print(f"    Auto confirm:    {args.auto}")

    # Run processor
    from email_pipeline.email_processor import process_emails

    report = process_emails(
        service=service,
        api_key=api_key,
        db=db,
        archive_root=archive_root,
        pipeline_excel_path=pipeline_path,
        investment_comps_path=inv_comps_path,
        occupational_comps_path=occ_comps_path,
        after_date=args.after,
        before_date=args.before,
        label=label,
        sender_whitelist=sender_whitelist,
        keywords=keywords,
        max_results=args.max,
        dry_run=args.dry_run,
        auto_confirm=args.auto,
    )

    print()
    print(report.summary())


def cmd_parse_brochure(args):
    """Parse a brochure file for comparables."""
    _setup_logging(args.verbose)

    print("Email Pipeline — Brochure Parser")
    print("=" * 60)

    # Check API key
    api_key = get_anthropic_api_key()
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set in .env")
        print("  Get your API key from https://console.anthropic.com")
        sys.exit(1)

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    print(f"  File: {file_path.name}")
    print(f"  Size: {file_path.stat().st_size / 1024:.1f} KB")
    print()

    from email_pipeline.brochure_parser import parse_brochure

    result = parse_brochure(
        file_path=file_path,
        api_key=api_key,
        source_deal=args.source_deal or file_path.stem,
        extract_deal=not args.comps_only,
        extract_investment_comps=not args.occ_comps_only,
        extract_occupational_comps=not args.inv_comps_only,
    )

    # Print results
    if result.deal_extraction:
        d = result.deal_extraction
        print("\n  Deal Details:")
        print(f"    Asset:          {d.asset_name}")
        print(f"    Town:           {d.town}")
        print(f"    Address:        {d.address}")
        print(f"    Classification: {d.classification}")
        if d.area_sqft:
            print(f"    Area:           {d.area_sqft:,.0f} sqft")
        if d.rent_pa:
            print(f"    Rent PA:        £{d.rent_pa:,.0f}")
        if d.asking_price:
            print(f"    Asking Price:   £{d.asking_price:,.0f}")
        if d.net_yield:
            print(f"    NIY:            {d.net_yield:.2f}%")
        print(f"    Confidence:     {d.confidence:.0%}")

    if result.investment_comps:
        print(f"\n  Investment Comparables ({len(result.investment_comps)}):")
        for i, comp in enumerate(result.investment_comps, 1):
            price_str = f"£{comp.price:,.0f}" if comp.price else "N/A"
            yield_str = f"{comp.yield_niy:.2f}%" if comp.yield_niy else "N/A"
            print(f"    {i}. {comp.town}, {comp.address} — {price_str} @ {yield_str}")

    if result.occupational_comps:
        print(f"\n  Occupational Comparables ({len(result.occupational_comps)}):")
        for i, comp in enumerate(result.occupational_comps, 1):
            rent_str = f"£{comp.rent_pa:,.0f} pa" if comp.rent_pa else "N/A"
            size_str = f"{comp.size_sqft:,.0f} sqft" if comp.size_sqft else "N/A"
            print(f"    {i}. {comp.tenant_name} — {size_str} @ {rent_str}")

    if result.error_message:
        print(f"\n  Error: {result.error_message}")

    # Write to Excel if requested
    if args.write:
        print("\n  Writing to Excel files...")

        if result.investment_comps:
            inv_path = get_investment_comps_path()
            if inv_path and inv_path.exists():
                from email_pipeline.excel_writer import InvestmentCompsWriter
                writer = InvestmentCompsWriter(inv_path)
                count = writer.append_comps(result.investment_comps)
                print(f"    ✓ {count} investment comps written to {inv_path.name}")
            else:
                print(f"    ✗ Investment comps file not found: {inv_path}")

        if result.occupational_comps:
            occ_path = get_occupational_comps_path()
            if occ_path:
                from email_pipeline.excel_writer import OccupationalCompsWriter
                writer = OccupationalCompsWriter(occ_path)
                count = writer.append_comps(result.occupational_comps)
                print(f"    ✓ {count} occupational comps written to {occ_path.name}")

        # Post-write: backup, snapshot, clean (once per run)
        from email_pipeline.excel_writer import _backup_file

        if result.investment_comps:
            inv_path = get_investment_comps_path()
            if inv_path and inv_path.exists():
                _backup_file(inv_path)

        if result.occupational_comps:
            occ_path = get_occupational_comps_path()
            if occ_path and occ_path.exists():
                _backup_file(occ_path)
                try:
                    from email_pipeline.occ_comps_cleaner import clean_occupational_comps
                    cleaned_path = get_cleaned_occupational_comps_path()
                    db_path = get_db_path()
                    if cleaned_path:
                        clean_occupational_comps(
                            raw_excel_path=occ_path,
                            cleaned_excel_path=cleaned_path,
                            db_path=db_path,
                        )
                except Exception as e:
                    print(f"    ⚠ Occ comps cleaner failed: {e}")

    if not result.investment_comps and not result.occupational_comps and not result.deal_extraction:
        print("\n  No data extracted from this file.")


def cmd_stats(args):
    """Show processing statistics."""
    from email_pipeline.database import Database

    db = Database(str(get_db_path()))
    stats = db.get_stats()

    print("Email Pipeline — Statistics")
    print("=" * 60)
    print(f"  Total emails processed: {stats['total_processed']}")
    print(f"  Introductions found:    {stats['introductions']}")
    print(f"  Skipped (not intros):   {stats['skipped']}")
    print(f"  Errors:                 {stats['errors']}")
    print(f"  Pipeline rows added:    {stats['pipeline_rows_added']}")

    if args.recent:
        print(f"\n  Recent ({args.recent}):")
        recent = db.get_recent(limit=args.recent)
        for r in recent:
            status_icon = "✓" if r["is_introduction"] else "✗"
            print(f"    {status_icon} {r['processed_at'][:16]} | {r['subject'][:50]}")
            if r["deal_asset_name"]:
                print(f"      → {r['deal_town']}, {r['deal_asset_name']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Investment email pipeline — scan, process, and extract deal data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s scan --after 2025-01-01 --before 2025-02-01 --use-config-senders
  %(prog)s process --after 2025-01-01 --use-config-senders --dry-run
  %(prog)s process --after 2025-01-01 --use-config-senders --auto
  %(prog)s parse-brochure "brochure.pdf" --write
  %(prog)s stats --recent 10
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # -----------------------------------------------------------------------
    # scan
    # -----------------------------------------------------------------------
    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan Gmail for investment emails (dry-run, no processing)",
    )
    _add_common_args(scan_parser)
    scan_parser.set_defaults(func=cmd_scan)

    # -----------------------------------------------------------------------
    # process
    # -----------------------------------------------------------------------
    process_parser = subparsers.add_parser(
        "process",
        help="Process emails — classify, extract, archive, and update Excel",
    )
    _add_common_args(process_parser)
    process_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify emails but don't process (no writes)",
    )
    process_parser.add_argument(
        "--auto",
        action="store_true",
        help="Skip confirmation prompt (for scheduled mode)",
    )
    process_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    process_parser.set_defaults(func=cmd_process)

    # -----------------------------------------------------------------------
    # parse-brochure
    # -----------------------------------------------------------------------
    brochure_parser = subparsers.add_parser(
        "parse-brochure",
        help="Parse a brochure PDF/Excel for comparables",
    )
    brochure_parser.add_argument("file", help="Path to brochure file (PDF or Excel)")
    brochure_parser.add_argument(
        "--source-deal",
        help="Name of the source deal (for tracking)",
    )
    brochure_parser.add_argument(
        "--write",
        action="store_true",
        help="Write extracted comps to Excel files",
    )
    brochure_parser.add_argument(
        "--inv-comps-only",
        action="store_true",
        help="Extract investment comps only (skip occupational)",
    )
    brochure_parser.add_argument(
        "--occ-comps-only",
        action="store_true",
        help="Extract occupational comps only (skip investment)",
    )
    brochure_parser.add_argument(
        "--comps-only",
        action="store_true",
        help="Extract comps only (skip deal details)",
    )
    brochure_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    brochure_parser.set_defaults(func=cmd_parse_brochure)

    # -----------------------------------------------------------------------
    # stats
    # -----------------------------------------------------------------------
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show processing statistics",
    )
    stats_parser.add_argument(
        "--recent",
        type=int,
        default=0,
        help="Show N most recent processed emails",
    )
    stats_parser.set_defaults(func=cmd_stats)

    # -----------------------------------------------------------------------
    # Parse and run
    # -----------------------------------------------------------------------
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    args.func(args)


def _add_common_args(parser):
    """Add common scan/process arguments to a subparser."""
    parser.add_argument(
        "--after",
        help="Only include emails after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--before",
        help="Only include emails before this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--label",
        help="Gmail label to filter by (e.g. 'Investment Introduction')",
    )
    parser.add_argument(
        "--senders",
        help="Comma-separated sender domain whitelist (e.g. '@cbre.com,@jll.com')",
    )
    parser.add_argument(
        "--keywords",
        help="Comma-separated keywords to match in subject/body",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=500,
        help="Maximum number of emails to scan (default: 500)",
    )
    parser.add_argument(
        "--use-config-label",
        action="store_true",
        help="Use the GMAIL_SCAN_LABEL from .env",
    )
    parser.add_argument(
        "--use-config-senders",
        action="store_true",
        help="Use the SENDER_WHITELIST from .env",
    )
    parser.add_argument(
        "--use-config-keywords",
        action="store_true",
        help="Use the EMAIL_KEYWORDS from .env",
    )


if __name__ == "__main__":
    main()
