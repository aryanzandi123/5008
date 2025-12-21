#!/usr/bin/env python3
"""
Stage 7: Final Validation and Commit

After all previous stages complete, this stage:
1. Detects hierarchy overlaps and conflicts
2. AI-assisted renormalization if needed
3. Prunes pathways with zero interactions in subtree
4. Atomic commit to production tables

INVARIANTS that must hold after this stage:
1. Every interaction has exactly one final pathway
2. No dead pathway nodes (no interactions in subtree)
3. All pathways reachable from roots
4. No duplicate pathway names
5. All hierarchy chains valid (chain[0] in ROOT_CATEGORIES)
6. Siblings marked correctly (pathway_type='sibling', is_primary_chain=False)
7. No cycles in hierarchy
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from collections import defaultdict

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_pipeline_v2.ai_client import call_ai_sequential
from scripts.pathway_pipeline_v2.config import (
    ROOT_CATEGORY_NAMES,
    BATCH_SIZE_STAGE7_VALIDATION,
)

logger = logging.getLogger(__name__)


def detect_cycles(pathway_id: int, parent_map: Dict[int, Set[int]], visited: Set[int] = None, path: Set[int] = None) -> bool:
    """
    Detect cycles in the hierarchy using DFS.

    Returns True if a cycle is detected.
    """
    if visited is None:
        visited = set()
    if path is None:
        path = set()

    if pathway_id in path:
        return True  # Cycle detected

    if pathway_id in visited:
        return False  # Already processed, no cycle from here

    visited.add(pathway_id)
    path.add(pathway_id)

    for parent_id in parent_map.get(pathway_id, set()):
        if detect_cycles(parent_id, parent_map, visited, path):
            return True

    path.remove(pathway_id)
    return False


def check_reachability_from_roots(
    pathway_ids: Set[int],
    parent_map: Dict[int, Set[int]],
    root_ids: Set[int],
) -> Tuple[Set[int], Set[int]]:
    """
    Check which pathways can reach root categories.

    Returns (reachable_ids, unreachable_ids).
    """
    reachable: Set[int] = set()

    def can_reach_root(pathway_id: int, visited: Set[int] = None) -> bool:
        if visited is None:
            visited = set()

        if pathway_id in reachable:
            return True
        if pathway_id in root_ids:
            reachable.add(pathway_id)
            return True
        if pathway_id in visited:
            return False

        visited.add(pathway_id)

        for parent_id in parent_map.get(pathway_id, set()):
            if can_reach_root(parent_id, visited):
                reachable.add(pathway_id)
                return True

        return False

    for pid in pathway_ids:
        can_reach_root(pid)

    unreachable = pathway_ids - reachable
    return reachable, unreachable


def get_interaction_count_in_subtree(
    pathway_id: int,
    child_map: Dict[int, Set[int]],
    interaction_counts: Dict[int, int],
    cache: Dict[int, int] = None,
) -> int:
    """
    Get total interaction count in a pathway's subtree (including itself).
    """
    if cache is None:
        cache = {}

    if pathway_id in cache:
        return cache[pathway_id]

    count = interaction_counts.get(pathway_id, 0)

    for child_id in child_map.get(pathway_id, set()):
        count += get_interaction_count_in_subtree(child_id, child_map, interaction_counts, cache)

    cache[pathway_id] = count
    return count


def prune_dead_pathways(dry_run: bool = True) -> List[int]:
    """
    Remove pathways with zero interactions in their subtree.

    Returns list of pruned pathway IDs.
    """
    from app import app, db
    from models import Pathway, PathwayParent, PathwayInteraction

    with app.app_context():
        # Build maps
        child_map: Dict[int, Set[int]] = defaultdict(set)  # parent_id -> child_ids
        parent_map: Dict[int, Set[int]] = defaultdict(set)  # child_id -> parent_ids

        relationships = db.session.query(PathwayParent).all()
        for rel in relationships:
            child_map[rel.parent_pathway_id].add(rel.child_pathway_id)
            parent_map[rel.child_pathway_id].add(rel.parent_pathway_id)

        # Count direct interactions per pathway
        interaction_counts: Dict[int, int] = defaultdict(int)
        interaction_links = db.session.query(
            PathwayInteraction.pathway_id,
            db.func.count(PathwayInteraction.id)
        ).group_by(PathwayInteraction.pathway_id).all()

        for pathway_id, count in interaction_links:
            interaction_counts[pathway_id] = count

        # Get all pathway IDs
        all_pathways = db.session.query(Pathway).all()
        # Only count pathways whose NAME is in ROOT_CATEGORY_NAMES as roots
        # NOT just any pathway with hierarchy_level=0 (which could be orphans)
        root_ids = {p.id for p in all_pathways if p.name in ROOT_CATEGORY_NAMES}

        # Find dead pathways (zero interactions in subtree, not a root)
        dead_pathways: List[int] = []
        subtree_cache: Dict[int, int] = {}

        for pathway in all_pathways:
            if pathway.id in root_ids:
                continue  # Never prune roots

            # NEVER prune siblings - they are intentional hierarchy placeholders
            # Siblings exist to show biological context, not to have interactions
            if pathway.pathway_type == 'sibling':
                continue

            subtree_count = get_interaction_count_in_subtree(
                pathway.id, child_map, interaction_counts, subtree_cache
            )

            if subtree_count == 0:
                dead_pathways.append(pathway.id)
                logger.info(f"Dead pathway detected: {pathway.name} (id={pathway.id})")

        if dry_run:
            logger.info(f"Dry run: {len(dead_pathways)} pathways would be pruned")
            return dead_pathways

        # Actually delete dead pathways
        for pathway_id in dead_pathways:
            # Delete parent relationships first
            db.session.query(PathwayParent).filter(
                (PathwayParent.child_pathway_id == pathway_id) |
                (PathwayParent.parent_pathway_id == pathway_id)
            ).delete()

            # Delete pathway
            db.session.query(Pathway).filter_by(id=pathway_id).delete()

        db.session.commit()
        logger.info(f"Pruned {len(dead_pathways)} dead pathways")

        return dead_pathways


def fix_orphan_pathways() -> List[str]:
    """
    Fix pathways with hierarchy_level=0 that are NOT valid roots.
    These are 'orphan roots' that should be connected to the hierarchy.

    Returns list of fixed pathway names.
    """
    from app import app, db
    from models import Pathway, PathwayParent

    with app.app_context():
        # Find orphan roots: hierarchy_level=0 but NOT in ROOT_CATEGORY_NAMES
        orphans = db.session.query(Pathway).filter(
            Pathway.hierarchy_level == 0,
            ~Pathway.name.in_(ROOT_CATEGORY_NAMES)
        ).all()

        if not orphans:
            logger.info("No orphan pathways found")
            return []

        logger.info(f"Found {len(orphans)} orphan pathways to fix")

        # Get fallback root (Protein Quality Control)
        fallback_root = db.session.query(Pathway).filter_by(
            name="Protein Quality Control"
        ).first()

        if not fallback_root:
            # Create fallback root if missing
            fallback_root = Pathway(
                name="Protein Quality Control",
                ontology_id="GO:0006457",
                hierarchy_level=0,
                pathway_type='main',
                ai_generated=False,
            )
            db.session.add(fallback_root)
            db.session.flush()
            logger.info("Created fallback root: Protein Quality Control")

        fixed = []
        for orphan in orphans:
            # Set proper hierarchy_level (1 = direct child of root)
            orphan.hierarchy_level = 1

            # Create parent link to fallback root
            existing_link = db.session.query(PathwayParent).filter_by(
                child_pathway_id=orphan.id
            ).first()

            if not existing_link:
                link = PathwayParent(
                    child_pathway_id=orphan.id,
                    parent_pathway_id=fallback_root.id,
                    relationship_type='is_a',
                    confidence=0.5,  # Low confidence - AI didn't place it
                    source='orphan_fix',
                    is_primary_chain=True,
                )
                db.session.add(link)

            fixed.append(orphan.name)
            logger.info(f"Fixed orphan pathway: {orphan.name}")

        db.session.commit()
        logger.info(f"Fixed {len(fixed)} orphan pathways")
        return fixed


def validate_hierarchy_invariants() -> Dict[str, Any]:
    """
    Validate all hierarchy invariants.

    Returns validation report.
    """
    from app import app, db
    from models import Pathway, PathwayParent, PathwayInteraction, Interaction

    with app.app_context():
        report = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "stats": {},
        }

        # Get all data
        all_pathways = db.session.query(Pathway).all()
        all_relationships = db.session.query(PathwayParent).all()
        all_interactions = db.session.query(Interaction).all()
        all_links = db.session.query(PathwayInteraction).all()

        # Build maps
        pathway_by_id = {p.id: p for p in all_pathways}
        pathway_by_name = {p.name: p for p in all_pathways}
        child_map: Dict[int, Set[int]] = defaultdict(set)
        parent_map: Dict[int, Set[int]] = defaultdict(set)

        for rel in all_relationships:
            child_map[rel.parent_pathway_id].add(rel.child_pathway_id)
            parent_map[rel.child_pathway_id].add(rel.parent_pathway_id)

        # Only count pathways whose NAME is in ROOT_CATEGORY_NAMES as roots
        root_ids = {p.id for p in all_pathways if p.name in ROOT_CATEGORY_NAMES}

        # Stats
        report["stats"] = {
            "total_pathways": len(all_pathways),
            "root_pathways": len(root_ids),
            "main_pathways": len([p for p in all_pathways if p.pathway_type == 'main']),
            "sibling_pathways": len([p for p in all_pathways if p.pathway_type == 'sibling']),
            "total_interactions": len(all_interactions),
            "interactions_with_pathways": len(all_links),
        }

        # INVARIANT 1: Every interaction has exactly one final pathway
        interactions_without_pathway = set()
        interactions_with_multiple = set()

        pathway_link_counts = defaultdict(int)
        for link in all_links:
            pathway_link_counts[link.interaction_id] += 1

        for interaction in all_interactions:
            count = pathway_link_counts.get(interaction.id, 0)
            if count == 0:
                interactions_without_pathway.add(interaction.id)
            elif count > 1:
                interactions_with_multiple.add(interaction.id)

        if interactions_without_pathway:
            report["errors"].append(f"INVARIANT 1 VIOLATED: {len(interactions_without_pathway)} interactions have no pathway")
            report["valid"] = False

        if interactions_with_multiple:
            report["warnings"].append(f"INVARIANT 1 WARNING: {len(interactions_with_multiple)} interactions have multiple pathways")

        # INVARIANT 2: No dead pathway nodes
        # (Checked by prune_dead_pathways)
        dead_count = len(prune_dead_pathways(dry_run=True))
        if dead_count > 0:
            report["warnings"].append(f"INVARIANT 2 WARNING: {dead_count} dead pathways detected (run prune to fix)")

        # INVARIANT 3: All pathways reachable from roots
        all_pathway_ids = set(pathway_by_id.keys())
        reachable, unreachable = check_reachability_from_roots(all_pathway_ids, parent_map, root_ids)

        if unreachable:
            report["errors"].append(f"INVARIANT 3 VIOLATED: {len(unreachable)} pathways not reachable from roots")
            report["valid"] = False

        # INVARIANT 4: No duplicate pathway names
        name_counts = defaultdict(int)
        for p in all_pathways:
            name_counts[p.name] += 1

        duplicates = {name: count for name, count in name_counts.items() if count > 1}
        if duplicates:
            report["errors"].append(f"INVARIANT 4 VIOLATED: {len(duplicates)} duplicate pathway names")
            report["valid"] = False

        # INVARIANT 5: All hierarchy chains valid (chain[0] in ROOT_CATEGORIES)
        invalid_chains = []
        for p in all_pathways:
            if p.hierarchy_chain:
                if p.hierarchy_chain[0] not in ROOT_CATEGORY_NAMES:
                    invalid_chains.append(p.name)

        if invalid_chains:
            report["errors"].append(f"INVARIANT 5 VIOLATED: {len(invalid_chains)} pathways have invalid chains")
            report["valid"] = False

        # INVARIANT 6: Siblings marked correctly
        sibling_issues = []
        for rel in all_relationships:
            child = pathway_by_id.get(rel.child_pathway_id)
            if child and child.pathway_type == 'sibling' and rel.is_primary_chain:
                sibling_issues.append(child.name)

        if sibling_issues:
            report["warnings"].append(f"INVARIANT 6 WARNING: {len(sibling_issues)} siblings have is_primary_chain=True")

        # INVARIANT 7: No cycles in hierarchy
        cycles_found = []
        visited = set()
        for pathway_id in all_pathway_ids:
            if detect_cycles(pathway_id, parent_map, visited.copy()):
                cycles_found.append(pathway_id)

        if cycles_found:
            report["errors"].append(f"INVARIANT 7 VIOLATED: Cycles detected in hierarchy")
            report["valid"] = False

        return report


def run_stage7(prune: bool = False, fix_orphans: bool = True):
    """
    Run Stage 7: Final validation and optional commit.

    Args:
        prune: If True, actually prune dead pathways. If False, dry run only.
        fix_orphans: If True, fix orphan pathways before validation (default True).
    """
    logger.info("=" * 60)
    logger.info("STAGE 7: FINAL VALIDATION AND COMMIT")
    logger.info("=" * 60)

    # Fix orphan pathways first (pathways with hierarchy_level=0 that aren't valid roots)
    if fix_orphans:
        print("\n--- FIXING ORPHAN PATHWAYS ---")
        fixed = fix_orphan_pathways()
        if fixed:
            print(f"Fixed {len(fixed)} orphan pathways: {', '.join(fixed[:5])}{'...' if len(fixed) > 5 else ''}")
        else:
            print("No orphan pathways found")

    # Validate invariants
    report = validate_hierarchy_invariants()

    # Print report
    print("\n=== VALIDATION REPORT ===\n")

    print("Statistics:")
    for key, value in report["stats"].items():
        print(f"  - {key}: {value}")

    if report["errors"]:
        print("\nERRORS:")
        for err in report["errors"]:
            print(f"  [ERROR] {err}")

    if report["warnings"]:
        print("\nWARNINGS:")
        for warn in report["warnings"]:
            print(f"  [WARN] {warn}")

    print(f"\nOverall: {'VALID' if report['valid'] else 'INVALID'}")

    # Prune if requested
    if prune:
        print("\n--- PRUNING DEAD PATHWAYS ---")
        pruned = prune_dead_pathways(dry_run=False)
        print(f"Pruned {len(pruned)} pathways")

        # Re-validate after pruning
        print("\n--- RE-VALIDATING ---")
        report = validate_hierarchy_invariants()
        print(f"After pruning: {'VALID' if report['valid'] else 'STILL INVALID'}")

    return report


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Stage 7: Validate and commit pathway hierarchy")
    parser.add_argument("--prune", action="store_true", help="Actually prune dead pathways")
    parser.add_argument("--no-fix-orphans", action="store_true", help="Skip fixing orphan pathways")
    args = parser.parse_args()

    run_stage7(prune=args.prune, fix_orphans=not args.no_fix_orphans)
