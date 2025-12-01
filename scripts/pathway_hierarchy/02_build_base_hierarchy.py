#!/usr/bin/env python3
"""
Script 02: Build Base Hierarchy Scaffold

Creates the foundational pathway hierarchy in the database using
GO and KEGG ontologies as scaffold. This establishes the root
categories and their primary sub-categories.

Run: python scripts/pathway_hierarchy/02_build_base_hierarchy.py

Prerequisites:
- Script 01 must have run successfully (cache files exist)
- Database must be accessible

Output:
- Root pathway categories in database
- Primary sub-categories linked via pathway_parents table
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_hierarchy.ontology_client import (
    get_cached_go_hierarchy,
    get_cached_kegg_hierarchy,
    OntologyHierarchy,
)
from scripts.pathway_hierarchy.dag_models import PathwayDAG, PathwayNode
from scripts.pathway_hierarchy.hierarchy_utils import (
    setup_logging,
    CheckpointManager,
    ScriptStats,
    save_run_report,
    get_app_context,
)


# Root categories for the hierarchy (biologically relevant to PPI)
ROOT_CATEGORIES = [
    {
        "name": "Cellular Signaling",
        "go_id": "GO:0007165",
        "description": "Signal transduction pathways that regulate cellular responses",
    },
    {
        "name": "Metabolism",
        "go_id": "GO:0008152",
        "description": "Metabolic processes including energy production and biosynthesis",
    },
    {
        "name": "Protein Quality Control",
        "go_id": "GO:0006457",
        "description": "Protein folding, degradation, and homeostasis mechanisms",
    },
    {
        "name": "Cell Death",
        "go_id": "GO:0008219",
        "description": "Programmed cell death pathways including apoptosis and autophagy-dependent cell death",
    },
    {
        "name": "Cell Cycle",
        "go_id": "GO:0007049",
        "description": "Cell division and proliferation control",
    },
    {
        "name": "DNA Damage Response",
        "go_id": "GO:0006974",
        "description": "DNA repair and genomic stability mechanisms",
    },
    {
        "name": "Vesicle Transport",
        "go_id": "GO:0016192",
        "description": "Intracellular vesicle trafficking and secretion",
    },
    {
        "name": "Immune Response",
        "go_id": "GO:0006955",
        "description": "Innate and adaptive immune system pathways",
    },
    {
        "name": "Neuronal Function",
        "go_id": "GO:0050877",
        "description": "Nervous system development and synaptic signaling",
    },
    {
        "name": "Cytoskeleton Organization",
        "go_id": "GO:0007015",
        "description": "Actin, microtubule, and intermediate filament dynamics",
    },
]

# Sub-categories to create under each root (from GO hierarchy)
SUB_CATEGORIES = {
    "Cellular Signaling": [
        {"name": "mTOR Signaling", "go_id": "GO:0031929"},
        {"name": "MAPK Signaling", "go_id": "GO:0000165"},
        {"name": "NF-kB Signaling", "go_id": "GO:0038061"},
        {"name": "Wnt Signaling", "go_id": "GO:0016055"},
        {"name": "Notch Signaling", "go_id": "GO:0007219"},
        {"name": "TGF-beta Signaling", "go_id": "GO:0007179"},
        {"name": "JAK-STAT Signaling", "go_id": "GO:0007259"},
        {"name": "Calcium Signaling", "go_id": "GO:0019722"},
        {"name": "cAMP Signaling", "go_id": "GO:0019933"},
        {"name": "Cell Growth Regulation", "go_id": "GO:0001558"},
        {"name": "Cell Migration", "go_id": "GO:0016477"},
    ],
    "Protein Quality Control": [
        {"name": "Autophagy", "go_id": "GO:0006914"},
        {"name": "Ubiquitin-Proteasome System", "go_id": "GO:0010415"},
        {"name": "ER-Associated Degradation", "go_id": "GO:0036503"},
        {"name": "Protein Folding", "go_id": "GO:0006457"},
        {"name": "Chaperone-Mediated Protein Folding", "go_id": "GO:0061077"},
        {"name": "Unfolded Protein Response", "go_id": "GO:0030968"},
        {"name": "Aggrephagy", "go_id": "GO:0035973"},
    ],
    "Cell Death": [
        {"name": "Apoptosis", "go_id": "GO:0006915"},
        {"name": "Necroptosis", "go_id": "GO:0070266"},
        {"name": "Pyroptosis", "go_id": "GO:0070269"},
        {"name": "Ferroptosis", "go_id": "GO:0097707"},
        {"name": "Autophagy-Dependent Cell Death", "go_id": "GO:0048102"},
    ],
    "Autophagy": [  # Sub-sub-category (under Protein Quality Control > Autophagy)
        {"name": "Macroautophagy", "go_id": "GO:0016236"},
        {"name": "Selective Autophagy", "go_id": "GO:0061912"},
        {"name": "Mitophagy", "go_id": "GO:0000423"},
        {"name": "ER-phagy", "go_id": "GO:0061709"},
        {"name": "Lipophagy", "go_id": "GO:0061724"},
        {"name": "Pexophagy", "go_id": "GO:0030242"},
    ],
    "Metabolism": [
        {"name": "Glycolysis", "go_id": "GO:0006096"},
        {"name": "Oxidative Phosphorylation", "go_id": "GO:0006119"},
        {"name": "Lipid Metabolism", "go_id": "GO:0006629"},
        {"name": "Amino Acid Metabolism", "go_id": "GO:0006520"},
        {"name": "Mitochondrial Function", "go_id": "GO:0007005"},
    ],
    "Cell Cycle": [
        {"name": "G1/S Transition", "go_id": "GO:0000082"},
        {"name": "G2/M Transition", "go_id": "GO:0000086"},
        {"name": "Mitosis", "go_id": "GO:0007067"},
        {"name": "DNA Replication", "go_id": "GO:0006260"},
        {"name": "Cell Cycle Checkpoint", "go_id": "GO:0000075"},
    ],
    "DNA Damage Response": [
        {"name": "DNA Repair", "go_id": "GO:0006281"},
        {"name": "Homologous Recombination", "go_id": "GO:0035825"},
        {"name": "Non-Homologous End Joining", "go_id": "GO:0006303"},
        {"name": "Nucleotide Excision Repair", "go_id": "GO:0006289"},
        {"name": "Base Excision Repair", "go_id": "GO:0006284"},
    ],
    "Immune Response": [
        {"name": "Innate Immunity", "go_id": "GO:0045087"},
        {"name": "Adaptive Immunity", "go_id": "GO:0002250"},
        {"name": "Inflammatory Response", "go_id": "GO:0006954"},
        {"name": "Antiviral Response", "go_id": "GO:0051607"},
        {"name": "Cytokine Signaling", "go_id": "GO:0019221"},
    ],
    "Vesicle Transport": [
        {"name": "Endocytosis", "go_id": "GO:0006897"},
        {"name": "Exocytosis", "go_id": "GO:0006887"},
        {"name": "ER-Golgi Transport", "go_id": "GO:0006888"},
        {"name": "Lysosomal Transport", "go_id": "GO:0007041"},
    ],
    "Neuronal Function": [
        {"name": "Synaptic Transmission", "go_id": "GO:0007268"},
        {"name": "Axon Guidance", "go_id": "GO:0007411"},
        {"name": "Neuronal Development", "go_id": "GO:0048666"},
        {"name": "Neurotransmitter Release", "go_id": "GO:0007269"},
    ],
}


def create_or_get_pathway(session, name: str, go_id: str = None, description: str = None, level: int = 0) -> int:
    """Create a pathway in the database or get existing ID."""
    from models import Pathway

    # Check if exists
    existing = session.query(Pathway).filter_by(name=name).first()
    if existing:
        # Update fields if needed
        if go_id and not existing.ontology_id:
            existing.ontology_id = go_id
            existing.ontology_source = 'GO'
        if description and not existing.description:
            existing.description = description
        existing.hierarchy_level = level
        existing.ai_generated = False
        return existing.id

    # Create new
    pathway = Pathway(
        name=name,
        description=description,
        ontology_id=go_id,
        ontology_source='GO' if go_id and go_id.startswith('GO:') else None,
        ai_generated=False,
        hierarchy_level=level,
        is_leaf=True,  # Will be updated later
    )
    session.add(pathway)
    session.flush()
    return pathway.id


def create_parent_link(session, child_id: int, parent_id: int, source: str = 'ontology') -> bool:
    """Create a parent-child relationship if it doesn't exist."""
    from models import PathwayParent

    # Check if exists
    existing = session.query(PathwayParent).filter_by(
        child_pathway_id=child_id,
        parent_pathway_id=parent_id
    ).first()

    if existing:
        return False

    link = PathwayParent(
        child_pathway_id=child_id,
        parent_pathway_id=parent_id,
        relationship_type='is_a',
        confidence=1.0,
        source=source,
    )
    session.add(link)
    return True


def update_leaf_status(session):
    """Update is_leaf status for all pathways."""
    from models import Pathway, PathwayParent

    # Get all pathways that are parents (have children)
    parent_ids = session.query(PathwayParent.parent_pathway_id).distinct().all()
    parent_ids = {p[0] for p in parent_ids}

    # Update is_leaf
    for pathway in session.query(Pathway).all():
        pathway.is_leaf = pathway.id not in parent_ids


def main():
    """Build the base hierarchy scaffold from ontologies."""
    logger = setup_logging("02_build_hierarchy")
    checkpoint_mgr = CheckpointManager("02_build_base_hierarchy")
    stats = ScriptStats(
        script_name="02_build_base_hierarchy",
        start_time=datetime.now()
    )

    logger.info("=" * 60)
    logger.info("Script 02: Build Base Hierarchy Scaffold")
    logger.info("=" * 60)

    # Check for existing checkpoint
    checkpoint = checkpoint_mgr.load()
    if checkpoint:
        logger.info(f"Found checkpoint from {checkpoint.timestamp}")
        logger.info(f"Last completed phase: {checkpoint.phase}")

    try:
        with get_app_context():
            from models import db, Pathway, PathwayParent

            # Phase 1: Create root categories
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 1: Creating root categories")
            logger.info("-" * 40)

            root_ids = {}
            for cat in ROOT_CATEGORIES:
                pw_id = create_or_get_pathway(
                    db.session,
                    name=cat['name'],
                    go_id=cat.get('go_id'),
                    description=cat.get('description'),
                    level=0
                )
                root_ids[cat['name']] = pw_id
                logger.info(f"  Created/updated root: {cat['name']} (ID: {pw_id})")
                stats.items_processed += 1

            db.session.commit()
            checkpoint_mgr.save(phase=1, data={'root_ids': root_ids})
            logger.info(f"Created {len(root_ids)} root categories")

            # Phase 2: Create sub-categories
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 2: Creating sub-categories")
            logger.info("-" * 40)

            sub_ids = {}
            links_created = 0

            for parent_name, subcats in SUB_CATEGORIES.items():
                # Find parent ID
                parent_id = root_ids.get(parent_name)
                if not parent_id:
                    # Check if it's a sub-category of something else
                    parent_id = sub_ids.get(parent_name)

                if not parent_id:
                    logger.warning(f"  Parent '{parent_name}' not found, skipping sub-categories")
                    continue

                # Determine level based on parent
                parent_pathway = db.session.query(Pathway).get(parent_id)
                child_level = (parent_pathway.hierarchy_level or 0) + 1

                logger.info(f"  Under '{parent_name}' (level {child_level}):")

                for subcat in subcats:
                    sub_id = create_or_get_pathway(
                        db.session,
                        name=subcat['name'],
                        go_id=subcat.get('go_id'),
                        description=subcat.get('description'),
                        level=child_level
                    )
                    sub_ids[subcat['name']] = sub_id

                    # Create parent link
                    if create_parent_link(db.session, sub_id, parent_id, source='ontology'):
                        links_created += 1

                    logger.info(f"    - {subcat['name']} (ID: {sub_id})")
                    stats.items_processed += 1

            db.session.commit()
            checkpoint_mgr.save(phase=2, data={'sub_ids': sub_ids})
            logger.info(f"Created {len(sub_ids)} sub-categories, {links_created} parent links")

            # Phase 3: Update leaf status
            logger.info("")
            logger.info("-" * 40)
            logger.info("Phase 3: Updating leaf status")
            logger.info("-" * 40)

            update_leaf_status(db.session)
            db.session.commit()

            # Count leaves
            leaf_count = db.session.query(Pathway).filter_by(is_leaf=True).count()
            non_leaf_count = db.session.query(Pathway).filter_by(is_leaf=False).count()
            logger.info(f"  Leaf pathways: {leaf_count}")
            logger.info(f"  Non-leaf (parent) pathways: {non_leaf_count}")

            # Phase 4: Summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)

            total_pathways = db.session.query(Pathway).count()
            total_links = db.session.query(PathwayParent).count()

            logger.info(f"Total pathways in database: {total_pathways}")
            logger.info(f"Total parent-child links: {total_links}")
            logger.info(f"Root categories: {len(root_ids)}")
            logger.info(f"Sub-categories created: {len(sub_ids)}")

            # Show hierarchy tree
            logger.info("")
            logger.info("Hierarchy structure:")
            for root_name in sorted(root_ids.keys()):
                logger.info(f"  {root_name}")
                if root_name in SUB_CATEGORIES:
                    for sub in SUB_CATEGORIES[root_name][:3]:
                        logger.info(f"    ├── {sub['name']}")
                        if sub['name'] in SUB_CATEGORIES:
                            for subsub in SUB_CATEGORIES[sub['name']][:2]:
                                logger.info(f"    │   ├── {subsub['name']}")
                    if len(SUB_CATEGORIES[root_name]) > 3:
                        logger.info(f"    └── ... and {len(SUB_CATEGORIES[root_name]) - 3} more")

            # Clear checkpoint on success
            checkpoint_mgr.clear()

            stats.end_time = datetime.now()
            stats.items_created = len(root_ids) + len(sub_ids)

            report_path = save_run_report(stats)
            logger.info("")
            logger.info(f"Report saved to: {report_path}")
            logger.info("")
            logger.info("Script 02 completed successfully!")
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
