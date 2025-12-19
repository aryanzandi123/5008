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


def build_batch_sibling_prompt(
    pairs_with_siblings: List[Dict[str, Any]],
) -> str:
    """
    Build prompt for finding siblings for MULTIPLE parent-main pairs at once.
    """
    pairs_text = ""
    for i, pair in enumerate(pairs_with_siblings, 1):
        main = pair["main"]
        parent = pair["parent"]
        existing = pair.get("existing", [])
        existing_str = ", ".join(existing[:3]) if existing else "None"
        pairs_text += f"{i}. **{main}** (parent: {parent}, existing siblings: {existing_str})\n"

    prompt = f"""You are a biological pathway classification expert. Find sibling pathways for MULTIPLE parent-main pairs.

## PAIRS TO FIND SIBLINGS FOR ({len(pairs_with_siblings)} total)

{pairs_text}

## INSTRUCTIONS

For EACH pair above, find OTHER pathways that are also "types of" the parent.

Example:
- If parent = "Selective Autophagy" and main = "Mitophagy"
- Siblings could be: Aggrephagy, ER-phagy, Ribophagy, Pexophagy

Return JSON with siblings for ALL {len(pairs_with_siblings)} pairs:

```json
{{
  "sibling_sets": [
    {{
      "main_pathway": "{pairs_with_siblings[0]['main']}",
      "parent_pathway": "{pairs_with_siblings[0]['parent']}",
      "siblings": [
        {{"name": "Sibling1", "description": "Brief desc", "confidence": 0.85}},
        {{"name": "Sibling2", "description": "Brief desc", "confidence": 0.80}}
      ]
    }},
    {{
      "main_pathway": "{pairs_with_siblings[1]['main'] if len(pairs_with_siblings) > 1 else 'Example'}",
      "parent_pathway": "{pairs_with_siblings[1]['parent'] if len(pairs_with_siblings) > 1 else 'Example'}",
      "siblings": [...]
    }}
  ]
}}
```

IMPORTANT:
- Return EXACTLY {len(pairs_with_siblings)} sibling_sets (one per pair)
- Up to {MAX_SIBLINGS_PER_LEVEL} siblings per set
- Only include well-established biological pathways
- Do NOT include the main pathway or existing siblings in results
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


def process_batch_sibling_response(
    batch: List[Dict[str, Any]],
    result: Any,
) -> Dict[str, List[Dict[str, Any]]]:
    """Process batch AI response and extract siblings for each parent-main pair."""
    siblings_by_key: Dict[str, List[Dict[str, Any]]] = {}

    try:
        sibling_sets = result.data.get("sibling_sets", [])

        # Build lookup by main pathway name (case-insensitive)
        sets_map = {}
        for s in sibling_sets:
            main_name = s.get("main_pathway", "").strip().upper()
            if main_name:
                sets_map[main_name] = s

        for pair_data in batch:
            main_pathway = pair_data["main"]
            main_upper = main_pathway.strip().upper()
            existing_siblings = pair_data.get("existing", [])

            # Create key for this pair
            pair_key = f"{pair_data['parent']}:{main_pathway}"

            # Try to find matching sibling set
            sibling_set = sets_map.get(main_upper)

            if sibling_set and sibling_set.get("siblings"):
                raw_siblings = sibling_set["siblings"]

                # Validate and filter siblings
                valid_siblings = []
                for sib in raw_siblings:
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

                siblings_by_key[pair_key] = valid_siblings
                logger.info(f"Found {len(valid_siblings)} siblings for '{main_pathway}'")
            else:
                siblings_by_key[pair_key] = []
                logger.warning(f"No siblings found in batch response for '{main_pathway}'")

    except Exception as e:
        logger.error(f"Failed to parse batch sibling response: {e}")

    return siblings_by_key


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
    Uses batch processing for efficiency (BATCH_SIZE_STAGE5 pairs per AI call).
    """
    from app import app, db
    from models import Pathway, PathwayParent, PathwayHierarchyHistory
    from scripts.pathway_pipeline_v2.config import BATCH_SIZE_STAGE5

    with app.app_context():
        # Get all hierarchy chains (main chains built in Stage 4)
        histories = db.session.query(PathwayHierarchyHistory).all()

        logger.info(f"Stage 5: Processing {len(histories)} hierarchy chains")

        # Track all sibling expansions to avoid duplicates
        processed_pairs: Set[tuple] = set()  # (parent, main) pairs

        # Collect all pairs that need processing
        pairs_to_process = []
        parent_id_map = {}  # Map pair_key to parent_id and level

        for history in histories:
            chain = history.hierarchy_chain
            if not chain or len(chain) < 2:
                continue

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

                pairs_to_process.append({
                    "main": main_pathway,
                    "parent": parent_pathway,
                    "existing": existing_names,
                    "level": level,
                })

                # Store parent info for later
                pair_str_key = f"{parent_pathway}:{main_pathway}"
                parent_id_map[pair_str_key] = {
                    "parent_id": parent.id,
                    "level": level,
                }

        if not pairs_to_process:
            logger.info("Stage 5 complete: No pairs to process")
            return

        # Process in batches
        total = len(pairs_to_process)
        num_batches = (total + BATCH_SIZE_STAGE5 - 1) // BATCH_SIZE_STAGE5
        logger.info(f"Stage 5: Processing {total} pairs in {num_batches} batches of {BATCH_SIZE_STAGE5}")

        for batch_idx in range(num_batches):
            start = batch_idx * BATCH_SIZE_STAGE5
            end = min(start + BATCH_SIZE_STAGE5, total)
            batch = pairs_to_process[start:end]

            batch_mains = [p["main"] for p in batch]
            logger.info(f"Stage 5: Batch {batch_idx + 1}/{num_batches} ({len(batch)} pairs: {', '.join(batch_mains[:3])}...)")

            # Build batch prompt
            prompt = build_batch_sibling_prompt(batch)

            # Call AI for entire batch
            result = call_ai_sequential(
                prompt=prompt,
                stage="stage5",
                use_search=True,
            )

            if result.success:
                siblings_by_key = process_batch_sibling_response(batch, result)

                # Add siblings to database
                for pair_data in batch:
                    pair_key = f"{pair_data['parent']}:{pair_data['main']}"
                    siblings = siblings_by_key.get(pair_key, [])

                    if siblings:
                        parent_info = parent_id_map.get(pair_key)
                        if parent_info:
                            add_siblings_to_db(
                                siblings=siblings,
                                parent_pathway_id=parent_info["parent_id"],
                                hierarchy_level=parent_info["level"],
                            )
                            logger.info(f"Added {len(siblings)} siblings for {pair_data['main']}")
            else:
                # Batch failed - fallback to individual processing
                logger.warning(f"Batch {batch_idx + 1} failed, falling back to individual calls")
                for pair_data in batch:
                    siblings = find_siblings_for_level(
                        main_pathway=pair_data["main"],
                        parent_pathway=pair_data["parent"],
                        existing_siblings=pair_data.get("existing", []),
                    )

                    if siblings:
                        pair_key = f"{pair_data['parent']}:{pair_data['main']}"
                        parent_info = parent_id_map.get(pair_key)
                        if parent_info:
                            add_siblings_to_db(
                                siblings=siblings,
                                parent_pathway_id=parent_info["parent_id"],
                                hierarchy_level=parent_info["level"],
                            )
                            logger.info(f"Added {len(siblings)} siblings for {pair_data['main']}")

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
