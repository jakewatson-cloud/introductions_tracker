"""
One-time migration: seed raw_occupational_comps table from existing Excel.

Usage:
    python migrate_raw_to_db.py             # dry run (shows what would happen)
    python migrate_raw_to_db.py --run       # actually migrate
    python migrate_raw_to_db.py --run --force   # re-migrate even if table has data
"""

import argparse
import sys
from pathlib import Path

from config import get_db_path, get_occupational_comps_path
from email_pipeline.database import Database
from email_pipeline.models import OccupationalComp
from email_pipeline.occ_comps_cleaner import _read_raw_rows
from email_pipeline.occ_comps_db import RawOccCompsDB


def main():
    parser = argparse.ArgumentParser(
        description="Migrate raw occ comps from Excel to database"
    )
    parser.add_argument("--run", action="store_true",
                        help="Actually perform the migration (default is dry run)")
    parser.add_argument("--force", action="store_true",
                        help="Migrate even if the raw table already has data")
    args = parser.parse_args()

    # Resolve paths
    excel_path = get_occupational_comps_path()
    db_path = get_db_path()

    if not excel_path or not excel_path.exists():
        print(f"ERROR: Raw Excel not found at {excel_path}")
        sys.exit(1)

    # Ensure tables exist
    Database(str(db_path))

    raw_db = RawOccCompsDB(db_path)
    existing_count = raw_db.row_count()

    if existing_count > 0 and not args.force:
        print(f"WARNING: raw_occupational_comps already has {existing_count} rows.")
        print("Use --force to re-import (existing rows will be deduped against).")
        sys.exit(1)

    # Read from Excel
    print(f"Reading from: {excel_path}")
    rows = _read_raw_rows(excel_path)
    print(f"Found {len(rows)} rows in Excel")

    if not rows:
        print("No rows to migrate.")
        return

    # Migrate
    inserted = 0
    merged = 0
    duplicated = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        try:
            comp = OccupationalComp(
                source_deal=row.get("source_deal", ""),
                tenant_name=row.get("tenant_name", ""),
                entry_type=row.get("entry_type", "tenancy"),
                unit_name=row.get("unit_name") or None,
                address=row.get("address", ""),
                town=row.get("town", ""),
                postcode=row.get("postcode") or None,
                size_sqft=row.get("size_sqft"),
                rent_pa=row.get("rent_pa"),
                rent_psf=row.get("rent_psf"),
                lease_start=row.get("lease_start") or None,
                lease_expiry=row.get("lease_expiry") or None,
                break_date=row.get("break_date") or None,
                rent_review_date=row.get("rent_review_date") or None,
                lease_term_years=row.get("lease_term_years"),
                comp_date=row.get("comp_date") or None,
                notes=row.get("notes") or None,
                source_file_path=row.get("source_file_path") or None,
            )

            if args.run:
                action, row_id = raw_db.insert_comp(comp)
            else:
                # Dry run — just check what would happen
                action = "would_insert"  # simplified for dry run

            if action == "inserted" or action == "would_insert":
                inserted += 1
            elif action == "merged":
                merged += 1
            elif action == "duplicate":
                duplicated += 1

            if i % 50 == 0:
                print(f"  Processed {i}/{len(rows)}...")

        except Exception as e:
            errors += 1
            print(f"  ERROR on row {i}: {e}")

    # Summary
    mode = "MIGRATED" if args.run else "DRY RUN"
    print(f"\n{mode} — {len(rows)} Excel rows:")
    print(f"  Inserted:   {inserted}")
    if args.run:
        print(f"  Merged:     {merged}")
        print(f"  Duplicates: {duplicated}")
    print(f"  Errors:     {errors}")

    if args.run:
        final_count = raw_db.row_count()
        print(f"\nDB now has {final_count} rows in raw_occupational_comps")
    else:
        print(f"\nRe-run with --run to actually migrate.")


if __name__ == "__main__":
    main()
