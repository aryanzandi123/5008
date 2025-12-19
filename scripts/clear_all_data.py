#!/usr/bin/env python3
"""
Clear All Data Script

Truncates all data from pathway-related tables without dropping the tables.
Use this to reset the database for a fresh pipeline run.

Usage:
    python scripts/clear_all_data.py          # Dry run (shows what would be deleted)
    python scripts/clear_all_data.py --execute  # Actually delete the data
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app import app, db
from sqlalchemy import text


# Tables to clear, in order (respects foreign key constraints)
TABLES_TO_CLEAR = [
    # V2 Pipeline tables (new)
    "pathway_initial_assignments",
    "pathway_canonical_names",
    "pathway_hierarchy_history",

    # Junction/relationship tables
    "pathway_interactions",
    "pathway_parents",

    # Main tables
    "pathways",
    "interactions",
    "proteins",
]


def get_table_counts(session) -> dict:
    """Get row counts for all tables."""
    counts = {}
    for table in TABLES_TO_CLEAR:
        try:
            result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
            counts[table] = result.scalar()
        except Exception as e:
            counts[table] = f"Error: {e}"
    return counts


def clear_all_data(execute: bool = False):
    """Clear all data from pathway-related tables."""
    print("\n" + "=" * 60)
    print("CLEAR ALL DATA")
    print("=" * 60)

    with app.app_context():
        session = db.session

        # Show current counts
        print("\n[Current row counts]")
        counts = get_table_counts(session)
        total_rows = 0
        for table, count in counts.items():
            if isinstance(count, int):
                total_rows += count
                print(f"  {table}: {count:,} rows")
            else:
                print(f"  {table}: {count}")

        print(f"\n  TOTAL: {total_rows:,} rows")

        if not execute:
            print("\n[DRY RUN] No data deleted.")
            print("To actually delete data, run with --execute flag:")
            print("  python scripts/clear_all_data.py --execute")
            return

        # Confirm
        print("\n" + "!" * 60)
        print("WARNING: This will DELETE ALL DATA from the above tables!")
        print("!" * 60)
        confirm = input("\nType 'DELETE' to confirm: ")

        if confirm != "DELETE":
            print("\nAborted. No data deleted.")
            return

        print("\n[Deleting data...]")

        # Disable foreign key checks temporarily and truncate
        try:
            # Use TRUNCATE with CASCADE for PostgreSQL
            for table in TABLES_TO_CLEAR:
                try:
                    session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                    print(f"  Cleared {table}")
                except Exception as e:
                    # Table might not exist yet
                    print(f"  Skipped {table}: {e}")

            session.commit()

            print("\n[Verifying...]")
            counts_after = get_table_counts(session)
            for table, count in counts_after.items():
                if isinstance(count, int):
                    status = "OK" if count == 0 else f"WARNING: {count} rows remain"
                    print(f"  {table}: {status}")
                else:
                    print(f"  {table}: {count}")

            print("\n" + "=" * 60)
            print("DATA CLEARED SUCCESSFULLY")
            print("=" * 60)

        except Exception as e:
            session.rollback()
            print(f"\n[ERROR] Failed to clear data: {e}")
            raise


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Clear all data from pathway tables")
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually delete the data (default is dry run)"
    )
    args = parser.parse_args()

    clear_all_data(execute=args.execute)


if __name__ == "__main__":
    main()
