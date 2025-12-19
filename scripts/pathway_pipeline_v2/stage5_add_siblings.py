#!/usr/bin/env python3
"""
Stage 5: Add Siblings for Each Main Level

For each main chain level (except root), add sibling pathways that are
"also types of the same parent".

IMPORTANT: Do NOT recursively expand sibling children at this stage.
Siblings are placeholders - they become "mains" later if interactions
are assigned to them.

Example for Aggrephagy chain:
- Level 6 siblings of Aggrephagy: Mitophagy, ER-phagy, Ribophagy, Lipophagy
- Level 5 siblings of Selective Macroautophagy: Nonselective Macroautophagy
- Level 4 siblings of Macroautophagy: Microautophagy, Chaperone Mediated Autophagy
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_pipeline_v2.ai_client import call_ai_sequential, get_pipeline_memory
from scripts.pathway_pipeline_v2.config import (
    ROOT_CATEGORY_NAMES,
    MAX_SIBLINGS_PER_LEVEL,
)

logger = logging.getLogger(__name__)


def build_sibling_finder_prompt(
    main_pathway: str,
    parent_pathway: str,
    existing_siblings: List[str],
) -> str:
    """
    Build prompt for finding sibling pathways at a given level.
    """
    existing_text = ""
    if existing_siblings:
        existing_text = f"""
## ALREADY KNOWN SIBLINGS (do not duplicate):
{chr(10).join([f'- {s}' for s in existing_siblings])}
"""

    prompt = f"""You are a biological pathway classification expert. Your task is to find sibling pathways - other pathways that are "types of" the same parent category.

## CONTEXT

**Parent pathway**: {parent_pathway}
**Main pathway** (already classified): {main_pathway}

We know that "{main_pathway}" is a type of "{parent_pathway}".
Find OTHER pathways that are ALSO types of "{parent_pathway}".
{existing_text}

## INSTRUCTIONS

List pathways that are:
1. Also "types of" {parent_pathway}
2. At the SAME hierarchical level as {main_pathway}
3. NOT overlapping conceptually with {main_pathway}
4. Biologically meaningful and recognized (GO, KEGG, or literature terms)

Example:
- If parent = "Selective Autophagy" and main = "Mitophagy"
- Siblings could be: Aggrephagy, ER-phagy, Ribophagy, Lipophagy, Pexophagy

Return JSON:

```json
{{
  "siblings": [
    {{
      "name": "Sibling Pathway Name",
      "description": "Brief description",
      "confidence": 0.85
    }}
  ],
  "reasoning": "Why these are valid siblings"
}}
```

IMPORTANT:
- Return up to {MAX_SIBLINGS_PER_LEVEL} siblings
- Only include well-established biological pathways
- Siblings should be mutually exclusive (no overlap)
- Do NOT include the main pathway "{main_pathway}" or any existing siblings
"""

    return prompt


def find_siblings_for_level(
    main_pathway: str,
    parent_pathway: str,
    existing_siblings: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    Find sibling pathways for a given level in the hierarchy.
    """
    prompt = build_sibling_finder_prompt(
        main_pathway=main_pathway,
        parent_pathway=parent_pathway,
        existing_siblings=existing_siblings or [],
    )

    result = call_ai_sequential(
        prompt=prompt,
        stage="stage5",
        use_search=True,  # Use search for biological knowledge
    )

    if not result.success:
        logger.error(f"Stage 5 AI call failed: {result.error}")
        return []

    try:
        siblings = result.data.get("siblings", [])

        # Validate and filter
        valid_siblings = []
        for sib in siblings:
            name = sib.get("name", "")
            if not name:
                continue
            if name == main_pathway:
                continue
            if existing_siblings and name in existing_siblings:
                continue
            if len(valid_siblings) >= MAX_SIBLINGS_PER_LEVEL:
                break

            valid_siblings.append({
                "name": name,
                "description": sib.get("description", ""),
                "confidence": float(sib.get("confidence", 0.8)),
            })

        return valid_siblings

    except Exception as e:
        logger.error(f"Failed to parse Stage 5 response: {e}")
        return []


def add_siblings_to_db(
    siblings: List[Dict[str, Any]],
    parent_pathway_id: int,
    hierarchy_level: int,
):
    """
    Add sibling pathways to the database.
    """
    from app import app, db
    from models import Pathway, PathwayParent

    with app.app_context():
        for sib in siblings:
            name = sib["name"]

            # Check if pathway already exists
            existing = db.session.query(Pathway).filter_by(name=name).first()

            if existing:
                # Update to mark as sibling if not already main
                if existing.pathway_type != 'main':
                    existing.pathway_type = 'sibling'
                logger.info(f"Sibling '{name}' already exists")
                continue

            # Create new sibling pathway
            sibling_pathway = Pathway(
                name=name,
                description=sib.get("description"),
                hierarchy_level=hierarchy_level,
                is_leaf=True,  # Siblings are leaves until expanded
                ai_generated=True,
                pathway_type='sibling',
            )
            db.session.add(sibling_pathway)
            db.session.flush()

            # Create parent link with is_primary_chain=False
            link = PathwayParent(
                child_pathway_id=sibling_pathway.id,
                parent_pathway_id=parent_pathway_id,
                relationship_type='is_a',
                confidence=sib.get("confidence", 0.8),
                source='AI',
                is_primary_chain=False,  # This is a sibling, not main chain
            )
            db.session.add(link)

            logger.info(f"Created sibling pathway: {name} (level {hierarchy_level})")

        db.session.commit()


def run_stage5_from_db():
    """
    Run Stage 5 to add siblings for all main chain levels.
    """
    from app import app, db
    from models import Pathway, PathwayParent, PathwayHierarchyHistory

    with app.app_context():
        # Get all hierarchy chains (main chains built in Stage 4)
        histories = db.session.query(PathwayHierarchyHistory).all()

        logger.info(f"Stage 5: Processing {len(histories)} hierarchy chains")

        # Track all sibling expansions to avoid duplicates
        processed_pairs: Set[tuple] = set()  # (parent, main) pairs

        for history in histories:
            chain = history.hierarchy_chain
            if not chain or len(chain) < 2:
                continue

            logger.info(f"Adding siblings for chain: {' -> '.join(chain)}")

            # For each level in chain (except root at level 0)
            for level in range(1, len(chain)):
                main_pathway = chain[level]
                parent_pathway = chain[level - 1]

                # Skip if already processed this parent-main pair
                pair_key = (parent_pathway, main_pathway)
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)

                # Get parent pathway from DB
                parent = db.session.query(Pathway).filter_by(name=parent_pathway).first()
                if not parent:
                    logger.warning(f"Parent pathway not found: {parent_pathway}")
                    continue

                # Get existing siblings (children of parent)
                existing_children = db.session.query(Pathway).join(
                    PathwayParent,
                    PathwayParent.child_pathway_id == Pathway.id
                ).filter(
                    PathwayParent.parent_pathway_id == parent.id
                ).all()

                existing_names = [c.name for c in existing_children]

                # Find new siblings
                siblings = find_siblings_for_level(
                    main_pathway=main_pathway,
                    parent_pathway=parent_pathway,
                    existing_siblings=existing_names,
                )

                if siblings:
                    add_siblings_to_db(
                        siblings=siblings,
                        parent_pathway_id=parent.id,
                        hierarchy_level=level,
                    )
                    logger.info(f"Added {len(siblings)} siblings for {main_pathway}")

        logger.info("Stage 5 complete")


def print_hierarchy_with_siblings():
    """
    Print the full hierarchy with main and sibling pathways.
    """
    from app import app, db
    from models import Pathway, PathwayParent

    with app.app_context():
        # Get all root pathways
        roots = db.session.query(Pathway).filter_by(hierarchy_level=0).all()

        def print_tree(pathway: Pathway, indent: int = 0):
            type_marker = "[Main]" if pathway.pathway_type == 'main' else "[Sibling]"
            print(f"{'  ' * indent}{type_marker} {pathway.name}")

            # Get children
            children = db.session.query(Pathway).join(
                PathwayParent,
                PathwayParent.child_pathway_id == Pathway.id
            ).filter(
                PathwayParent.parent_pathway_id == pathway.id
            ).order_by(
                Pathway.pathway_type.desc(),  # Main first
                Pathway.name
            ).all()

            for child in children:
                print_tree(child, indent + 1)

        print("\n=== PATHWAY HIERARCHY ===\n")
        for root in sorted(roots, key=lambda r: r.name):
            print_tree(root)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_stage5_from_db()

    # Print the hierarchy
    print_hierarchy_with_siblings()
