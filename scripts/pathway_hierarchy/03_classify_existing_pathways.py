#!/usr/bin/env python3
"""
Script 03: Classify Existing Pathways

Maps existing pathways in the database into the hierarchy scaffold
created by Script 02. Uses AI (Gemini) for pathways that don't have
direct ontology mappings.

Run: python scripts/pathway_hierarchy/03_classify_existing_pathways.py

Prerequisites:
- Scripts 01 and 02 must have run successfully
- Database must contain pathways from pipeline runs

Batching: 10 pathways per AI call to avoid context window issues
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_hierarchy.ontology_client import (
    get_cached_go_hierarchy,
    find_best_ontology_match,
)
from scripts.pathway_hierarchy.ai_hierarchy_builder import (
    classify_pathways_batch,
    format_hierarchy_tree,
)
from scripts.pathway_hierarchy.hierarchy_utils import (
    setup_logging,
    CheckpointManager,
    ScriptStats,
    save_run_report,
    get_app_context,
    process_in_batches,
    ProgressTracker,
    normalize_pathway_name,
)


BATCH_SIZE = 10  # Pathways per AI call


def get_hierarchy_tree_string(session) -> str:
    """Get formatted hierarchy tree for AI prompts."""
    from models import Pathway, PathwayParent

    # Get all pathways with their levels
    pathways = session.query(Pathway).filter(
        Pathway.hierarchy_level <= 3  # Only show first 3 levels
    ).order_by(Pathway.hierarchy_level).all()

    # Build tree structure
    tree_data = []
    for pw in pathways:
        # Get parent names
        parent_links = session.query(PathwayParent).filter_by(
            child_pathway_id=pw.id
        ).all()
        parent_names = []
        for link in parent_links:
            parent = session.query(Pathway).get(link.parent_pathway_id)
            if parent:
                parent_names.append(parent.name)

        tree_data.append({
            'name': pw.name,
            'level': pw.hierarchy_level or 0,
            'parent_names': parent_names,
        })

    return format_hierarchy_tree(tree_data, max_depth=4)


def get_existing_pathways_without_parents(session) -> List[Dict]:
    """Get pathways that haven't been classified into the hierarchy yet."""
    from models import Pathway, PathwayParent

    # Get IDs of pathways that already have parents
    has_parent = session.query(PathwayParent.child_pathway_id).distinct().all()
    has_parent_ids = {p[0] for p in has_parent}

    # Get pathways without parents (excluding root categories)
    orphans = session.query(Pathway).filter(
        ~Pathway.id.in_(has_parent_ids) if has_parent_ids else True,
        Pathway.hierarchy_level != 0  # Exclude roots
    ).all()

    return [
        {
            'id': pw.id,
            'name': pw.name,
            'description': pw.description or '',
            'ontology_id': pw.ontology_id,
            'ontology_source': pw.ontology_source,
        }
        for pw in orphans
    ]


def classify_by_ontology(session, pathway: Dict, go_hierarchy) -> Optional[int]:
    """
    Try to classify pathway using GO ontology hierarchy.

    Returns parent pathway ID if found, None otherwise.
    """
    from models import Pathway

    if not pathway.get('ontology_id'):
        return None

    go_id = pathway['ontology_id']

    # Check if GO term exists in our hierarchy
    go_term = go_hierarchy.get_term(go_id)
    if not go_term:
        return None

    # Find ancestors and look for ones that exist in our database
    for parent_go_id in go_term.parent_ids:
        parent_term = go_hierarchy.get_term(parent_go_id)
        if parent_term:
            # Check if this GO term's name matches a pathway in our DB
            db_pathway = session.query(Pathway).filter(
                Pathway.ontology_id == parent_go_id
            ).first()

            if db_pathway:
                return db_pathway.id

            # Try by name match
            db_pathway = session.query(Pathway).filter(
                Pathway.name.ilike(f"%{parent_term.name}%")
            ).first()

            if db_pathway:
                return db_pathway.id

    return None


def classify_by_name_similarity(session, pathway_name: str) -> Optional[int]:
    """
    Try to classify pathway by name similarity to existing hierarchy.

    Returns parent pathway ID if good match found, None otherwise.
    """
    from models import Pathway

    normalized = normalize_pathway_name(pathway_name)

    # Get all existing hierarchy pathways
    hierarchy_pathways = session.query(Pathway).filter(
        Pathway.hierarchy_level.isnot(None),
        Pathway.hierarchy_level <= 2  # Only match to top-level categories
    ).all()

    best_match = None
    best_score = 0.0

    for hp in hierarchy_pathways:
        hp_normalized = normalize_pathway_name(hp.name)

        # Check for substring match
        if normalized in hp_normalized or hp_normalized in normalized:
            score = 0.8
        else:
            # Fuzzy match
            from difflib import SequenceMatcher
            score = SequenceMatcher(None, normalized, hp_normalized).ratio()

        if score > best_score and score >= 0.6:
            best_score = score
            best_match = hp.id

    return best_match


def process_ai_classification_batch(
    batch: List[Dict],
    hierarchy_tree: str,
    session,
    logger
) -> Dict[int, List[Dict]]:
    """Process a batch of pathways through AI classification."""
    from models import Pathway

    # Format batch for AI
    batch_for_ai = [
        {'name': pw['name'], 'description': pw['description']}
        for pw in batch
    ]

    try:
        classifications = classify_pathways_batch(batch_for_ai, hierarchy_tree)

        # Map results back to pathway IDs
        results = {}
        for pw in batch:
            ai_parents = classifications.get(pw['name'], [])
            if ai_parents:
                # Resolve parent names to IDs
                parent_ids = []
                for parent in ai_parents:
                    parent_pathway = session.query(Pathway).filter_by(
                        name=parent['name']
                    ).first()
                    if parent_pathway:
                        parent_ids.append({
                            'id': parent_pathway.id,
                            'confidence': parent.get('confidence', 0.8),
                            'relationship': parent.get('relationship', 'is_a'),
                        })
                if parent_ids:
                    results[pw['id']] = parent_ids

        return results

    except Exception as e:
        logger.error(f"AI classification failed: {e}")
        return {}


def main():
    """Classify existing pathways into the hierarchy."""
    logger = setup_logging("03_classify_pathways")
    checkpoint_mgr = CheckpointManager("03_classify_existing_pathways")
    stats = ScriptStats(
        script_name="03_classify_existing_pathways",
        start_time=datetime.now()
    )

    logger.info("=" * 60)
    logger.info("Script 03: Classify Existing Pathways")
    logger.info("=" * 60)

    # Check for existing checkpoint
    checkpoint = checkpoint_mgr.load()
    processed_ids = set()
    if checkpoint:
        logger.info(f"Found checkpoint from {checkpoint.timestamp}")
        processed_ids = set(checkpoint.data.get('processed_ids', []))
        logger.info(f"Resuming with {len(processed_ids)} already processed")

    try:
        # Load GO hierarchy for ontology-based classification
        logger.info("Loading GO hierarchy...")
        go_hierarchy = get_cached_go_hierarchy()
        logger.info(f"Loaded {len(go_hierarchy.terms)} GO terms")

        with get_app_context():
            from models import db, Pathway, PathwayParent

            # Phase 1: Get pathways needing classification
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 1: Identifying pathways to classify")
            logger.info("-" * 40)

            orphan_pathways = get_existing_pathways_without_parents(db.session)
            # Filter out already processed
            orphan_pathways = [p for p in orphan_pathways if p['id'] not in processed_ids]

            logger.info(f"Found {len(orphan_pathways)} pathways needing classification")

            if not orphan_pathways:
                logger.info("No pathways need classification. Done!")
                return True

            # Phase 2: Try ontology-based classification first
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 2: Ontology-based classification")
            logger.info("-" * 40)

            ontology_classified = []
            ai_needed = []

            for pw in orphan_pathways:
                parent_id = classify_by_ontology(db.session, pw, go_hierarchy)

                if not parent_id:
                    parent_id = classify_by_name_similarity(db.session, pw['name'])

                if parent_id:
                    ontology_classified.append((pw['id'], parent_id))
                    logger.info(f"  Ontology match: {pw['name']} -> parent ID {parent_id}")
                else:
                    ai_needed.append(pw)

            logger.info(f"Classified by ontology/similarity: {len(ontology_classified)}")
            logger.info(f"Need AI classification: {len(ai_needed)}")

            # Create links for ontology-classified
            for child_id, parent_id in ontology_classified:
                existing = db.session.query(PathwayParent).filter_by(
                    child_pathway_id=child_id,
                    parent_pathway_id=parent_id
                ).first()

                if not existing:
                    link = PathwayParent(
                        child_pathway_id=child_id,
                        parent_pathway_id=parent_id,
                        relationship_type='is_a',
                        confidence=0.9,
                        source='ontology_match',
                    )
                    db.session.add(link)
                    stats.items_created += 1

                processed_ids.add(child_id)
                stats.items_processed += 1

            db.session.commit()

            # Phase 3: AI-based classification
            if ai_needed:
                logger.info("")
                logger.info("-" * 40)
                logger.info("Phase 3: AI-based classification")
                logger.info("-" * 40)

                # Get hierarchy tree for prompts
                hierarchy_tree = get_hierarchy_tree_string(db.session)
                logger.info(f"Hierarchy tree prepared ({len(hierarchy_tree)} chars)")

                # Process in batches
                progress = ProgressTracker(len(ai_needed), "AI classification")

                for i in range(0, len(ai_needed), BATCH_SIZE):
                    batch = ai_needed[i:i + BATCH_SIZE]
                    batch_num = i // BATCH_SIZE + 1
                    total_batches = (len(ai_needed) + BATCH_SIZE - 1) // BATCH_SIZE

                    logger.info(f"[Batch {batch_num}/{total_batches}] Processing {len(batch)} pathways...")

                    results = process_ai_classification_batch(
                        batch, hierarchy_tree, db.session, logger
                    )

                    # Create links
                    for child_id, parents in results.items():
                        for parent in parents:
                            existing = db.session.query(PathwayParent).filter_by(
                                child_pathway_id=child_id,
                                parent_pathway_id=parent['id']
                            ).first()

                            if not existing:
                                link = PathwayParent(
                                    child_pathway_id=child_id,
                                    parent_pathway_id=parent['id'],
                                    relationship_type=parent.get('relationship', 'is_a'),
                                    confidence=parent.get('confidence', 0.8),
                                    source='AI',
                                )
                                db.session.add(link)
                                stats.items_created += 1

                            logger.info(f"  {db.session.query(Pathway).get(child_id).name} -> parent ID {parent['id']}")

                        processed_ids.add(child_id)
                        stats.items_processed += 1

                    db.session.commit()
                    progress.update(len(batch))

                    # Save checkpoint
                    checkpoint_mgr.save(phase=3, data={'processed_ids': list(processed_ids)})

                    # Rate limiting
                    import time
                    time.sleep(1.5)

            # Phase 4: Update hierarchy levels
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 4: Updating hierarchy levels")
            logger.info("-" * 40)

            # Recompute levels based on parent relationships
            def compute_level(pathway_id: int, visited: Set[int] = None) -> int:
                if visited is None:
                    visited = set()
                if pathway_id in visited:
                    return 0  # Cycle detected, shouldn't happen

                visited.add(pathway_id)
                parent_links = db.session.query(PathwayParent).filter_by(
                    child_pathway_id=pathway_id
                ).all()

                if not parent_links:
                    return 0  # Root

                max_parent_level = 0
                for link in parent_links:
                    parent_level = compute_level(link.parent_pathway_id, visited.copy())
                    max_parent_level = max(max_parent_level, parent_level)

                return max_parent_level + 1

            for pathway in db.session.query(Pathway).all():
                new_level = compute_level(pathway.id)
                if pathway.hierarchy_level != new_level:
                    pathway.hierarchy_level = new_level

            # Update is_leaf status
            parent_ids = {p[0] for p in db.session.query(PathwayParent.parent_pathway_id).distinct().all()}
            for pathway in db.session.query(Pathway).all():
                pathway.is_leaf = pathway.id not in parent_ids

            db.session.commit()

            # Summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)

            total_pathways = db.session.query(Pathway).count()
            total_links = db.session.query(PathwayParent).count()
            classified = len(processed_ids)

            logger.info(f"Total pathways: {total_pathways}")
            logger.info(f"Total parent-child links: {total_links}")
            logger.info(f"Pathways classified this run: {classified}")
            logger.info(f"  - By ontology/similarity: {len(ontology_classified)}")
            logger.info(f"  - By AI: {len(ai_needed)}")

            # Count by level
            logger.info("")
            logger.info("Pathways by hierarchy level:")
            for level in range(6):
                count = db.session.query(Pathway).filter_by(hierarchy_level=level).count()
                if count > 0:
                    logger.info(f"  Level {level}: {count} pathways")

            # Clear checkpoint on success
            checkpoint_mgr.clear()

            stats.end_time = datetime.now()
            report_path = save_run_report(stats)
            logger.info("")
            logger.info(f"Report saved to: {report_path}")
            logger.info("")
            logger.info("Script 03 completed successfully!")
            logger.info(stats.summary())

            return True

    except Exception as e:
        logger.error(f"Script failed: {e}")
        import traceback
        traceback.print_exc()
        stats.errors += 1
        stats.end_time = datetime.now()
        save_run_report(stats)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
