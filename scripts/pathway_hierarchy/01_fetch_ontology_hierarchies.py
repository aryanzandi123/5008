#!/usr/bin/env python3
"""
Script 01: Fetch Ontology Hierarchies

Downloads and caches GO (Gene Ontology) and KEGG pathway hierarchies
from their respective APIs. These hierarchies serve as the scaffold
for organizing pathways.

Run: python scripts/pathway_hierarchy/01_fetch_ontology_hierarchies.py [--force]

Output:
- cache/ontology_hierarchies/go_hierarchy.json
- cache/ontology_hierarchies/kegg_hierarchy.json
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_hierarchy.ontology_client import (
    get_cached_go_hierarchy,
    get_cached_kegg_hierarchy,
    CACHE_DIR,
)
from scripts.pathway_hierarchy.hierarchy_utils import (
    setup_logging,
    CheckpointManager,
    ScriptStats,
    save_run_report,
)


def main(force_refresh: bool = False):
    """
    Fetch and cache ontology hierarchies.

    Args:
        force_refresh: If True, re-download even if cache exists
    """
    logger = setup_logging("01_fetch_ontologies")
    stats = ScriptStats(
        script_name="01_fetch_ontology_hierarchies",
        start_time=datetime.now()
    )

    logger.info("=" * 60)
    logger.info("Script 01: Fetch Ontology Hierarchies")
    logger.info("=" * 60)

    # Ensure cache directory exists
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cache directory: {CACHE_DIR}")

    try:
        # Fetch GO hierarchy
        logger.info("")
        logger.info("-" * 40)
        logger.info("Fetching Gene Ontology (GO) hierarchy...")
        logger.info("-" * 40)

        go_cache = CACHE_DIR / "go_hierarchy.json"
        if go_cache.exists() and not force_refresh:
            logger.info(f"GO cache exists at {go_cache}")
            logger.info("Use --force to re-download")
        else:
            logger.info("Downloading GO hierarchy from QuickGO API...")
            logger.info("This may take 5-10 minutes...")

        go_hierarchy = get_cached_go_hierarchy(force_refresh=force_refresh)
        go_terms = len(go_hierarchy.terms)
        go_roots = len(go_hierarchy.get_roots())

        logger.info(f"GO hierarchy loaded: {go_terms} terms, {go_roots} root categories")
        stats.items_processed += go_terms

        # Fetch KEGG hierarchy
        logger.info("")
        logger.info("-" * 40)
        logger.info("Fetching KEGG pathway hierarchy...")
        logger.info("-" * 40)

        kegg_cache = CACHE_DIR / "kegg_hierarchy.json"
        if kegg_cache.exists() and not force_refresh:
            logger.info(f"KEGG cache exists at {kegg_cache}")
            logger.info("Use --force to re-download")
        else:
            logger.info("Downloading KEGG hierarchy from KEGG REST API...")
            logger.info("This may take 2-5 minutes...")

        kegg_hierarchy = get_cached_kegg_hierarchy(force_refresh=force_refresh)
        kegg_terms = len(kegg_hierarchy.terms)

        logger.info(f"KEGG hierarchy loaded: {kegg_terms} pathways")
        stats.items_processed += kegg_terms

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"GO terms: {go_terms}")
        logger.info(f"KEGG pathways: {kegg_terms}")
        logger.info(f"Total: {go_terms + kegg_terms}")
        logger.info("")
        logger.info("Cache files:")
        logger.info(f"  - {go_cache}")
        logger.info(f"  - {kegg_cache}")

        # Show some example GO terms
        logger.info("")
        logger.info("Sample GO root categories:")
        for root in list(go_hierarchy.get_roots())[:5]:
            logger.info(f"  - {root.id}: {root.name}")

        stats.end_time = datetime.now()
        stats.items_created = go_terms + kegg_terms

        # Save report
        report_path = save_run_report(stats)
        logger.info("")
        logger.info(f"Report saved to: {report_path}")
        logger.info("")
        logger.info("Script 01 completed successfully!")
        logger.info(stats.summary())

        return True

    except Exception as e:
        logger.error(f"Script failed: {e}")
        stats.errors += 1
        stats.end_time = datetime.now()
        save_run_report(stats)
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and cache GO/KEGG ontology hierarchies"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force re-download even if cache exists"
    )

    args = parser.parse_args()

    try:
        success = main(force_refresh=args.force)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
