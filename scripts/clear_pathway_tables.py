#!/usr/bin/env python3
"""
Clear Pathway Tables Utility
============================

Clears pathway tables for fresh hierarchy rebuild while keeping
proteins and interactions intact.

Usage:
    python scripts/clear_pathway_tables.py           # Clear pathway tables only
    python scripts/clear_pathway_tables.py --all     # Clear ALL tables (nuclear)
    python scripts/clear_pathway_tables.py --dry-run # Preview what would be deleted
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app, db
from models import PathwayParent, PathwayInteraction, Pathway, Interaction, Protein


def clear_pathway_tables(dry_run: bool = False):
    """Clear pathway-related tables only."""
    with app.app_context():
        # Count rows first
        parents_count = db.session.query(PathwayParent).count()
        interactions_count = db.session.query(PathwayInteraction).count()
        pathways_count = db.session.query(Pathway).count()

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Pathway Tables Status:")
        print(f"  pathway_parents: {parents_count} rows")
        print(f"  pathway_interactions: {interactions_count} rows")
        print(f"  pathways: {pathways_count} rows")
        print()

        if dry_run:
            print("[DRY RUN] No changes made. Run without --dry-run to delete.")
            return

        # Order matters due to foreign keys!
        deleted_parents = db.session.query(PathwayParent).delete()
        deleted_pi = db.session.query(PathwayInteraction).delete()
        deleted_pathways = db.session.query(Pathway).delete()

        db.session.commit()

        print(f"✓ Deleted {deleted_parents} pathway_parents rows")
        print(f"✓ Deleted {deleted_pi} pathway_interactions rows")
        print(f"✓ Deleted {deleted_pathways} pathways rows")
        print("\n✓ Pathway tables cleared - ready for rebuild")
        print("\nNext steps:")
        print("  python scripts/pathway_hierarchy/run_all.py")


def clear_all_tables(dry_run: bool = False):
    """Clear ALL tables (nuclear option)."""
    with app.app_context():
        # Count rows first
        counts = {
            'pathway_parents': db.session.query(PathwayParent).count(),
            'pathway_interactions': db.session.query(PathwayInteraction).count(),
            'pathways': db.session.query(Pathway).count(),
            'interactions': db.session.query(Interaction).count(),
            'proteins': db.session.query(Protein).count(),
        }

        print(f"\n{'[DRY RUN] ' if dry_run else ''}ALL Tables Status:")
        for table, count in counts.items():
            print(f"  {table}: {count} rows")
        print()

        if dry_run:
            print("[DRY RUN] No changes made. Run without --dry-run to delete.")
            return

        # Confirmation for destructive operation
        total = sum(counts.values())
        print(f"⚠️  WARNING: This will delete {total} rows across ALL tables!")
        response = input("Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            print("Aborted.")
            return

        # Order matters due to foreign keys!
        db.session.query(PathwayParent).delete()
        db.session.query(PathwayInteraction).delete()
        db.session.query(Pathway).delete()
        db.session.query(Interaction).delete()
        db.session.query(Protein).delete()

        db.session.commit()

        print("\n✓ ALL tables cleared")
        print("\nNext steps:")
        print("  1. Re-query a protein: curl -X POST localhost:5000/api/query -d '{\"protein\":\"ATXN3\"}'")
        print("  2. Run hierarchy pipeline: python scripts/pathway_hierarchy/run_all.py")


def main():
    parser = argparse.ArgumentParser(
        description="Clear pathway tables for fresh hierarchy rebuild",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/clear_pathway_tables.py           # Clear pathway tables only
  python scripts/clear_pathway_tables.py --all     # Clear ALL tables (nuclear)
  python scripts/clear_pathway_tables.py --dry-run # Preview what would be deleted
        """
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Clear ALL tables including proteins and interactions (nuclear option)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be deleted without making changes'
    )

    args = parser.parse_args()

    if args.all:
        clear_all_tables(dry_run=args.dry_run)
    else:
        clear_pathway_tables(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
