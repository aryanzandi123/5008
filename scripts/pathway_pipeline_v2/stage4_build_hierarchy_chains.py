#!/usr/bin/env python3
"""
Stage 4: Build Hierarchical Families Per Pathway Name (Work Backwards to Roots)

For each canonical pathway name:
1. Gather all interactions under that pathway
2. Use AI + search to determine "X is a type of Y" chain up to Level 1 root
3. Create missing intermediate pathway nodes
4. Store chain in history for reuse

This stage includes Stage 6 logic (history check before AI calls).

Example output for Aggrephagy:
Level 1 = [Protein Quality Control]
Level 2 =   --> [Sequestration and Aggregate Clearance]
Level 3 =       --> [Autophagy]
Level 4 =           --> [Macroautophagy]
Level 5 =               --> [Selective Macroautophagy]
Level 6 =                   --> [Aggrephagy]
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
    ROOT_CATEGORIES,
    MAX_HIERARCHY_DEPTH,
    MIN_CONFIDENCE_HIERARCHY,
    get_root_categories_prompt_section,
)

logger = logging.getLogger(__name__)


def build_hierarchy_chain_prompt(
    pathway_name: str,
    interaction_context: List[Dict[str, Any]],
) -> str:
    """
    Build prompt for determining the hierarchy chain from pathway to root.
    """
    # Format interaction context
    context_text = ""
    for ix in interaction_context[:5]:  # Top 5 interactions for context
        proteins = f"{ix.get('main_protein', '?')} - {ix.get('primary', '?')}"
        funcs = "; ".join([
            f.get("description", "")[:100]
            for f in ix.get("functions", [])[:2]
        ])
        context_text += f"  - {proteins}: {funcs}\n"

    prompt = f"""You are a biological pathway hierarchy expert. Your task is to build a complete "is-a-type-of" chain from a specific pathway UP TO one of the root categories.

## PATHWAY TO CLASSIFY

**Pathway name**: {pathway_name}

**Interactions using this pathway** (for context):
{context_text}

{get_root_categories_prompt_section()}

## INSTRUCTIONS

Build a hierarchy chain from "{pathway_name}" UP TO one of the root categories. Work BACKWARDS:
1. Start with "{pathway_name}"
2. Ask: "What is {pathway_name} a type of?"
3. Continue asking until you reach a ROOT category

Example for "Aggrephagy":
- Aggrephagy is a type of Selective Macroautophagy
- Selective Macroautophagy is a type of Macroautophagy
- Macroautophagy is a type of Autophagy
- Autophagy is a type of Sequestration and Aggregate Clearance
- Sequestration and Aggregate Clearance is a type of Protein Quality Control (ROOT)

Chain: ["Protein Quality Control", "Sequestration and Aggregate Clearance", "Autophagy", "Macroautophagy", "Selective Macroautophagy", "Aggrephagy"]

Return JSON:

```json
{{
  "hierarchy_chain": {{
    "chain": ["Root Category", "Level 2", "Level 3", "...", "{pathway_name}"],
    "root_category": "The Root Category Name",
    "confidence": 0.90,
    "reasoning": "Explanation of the hierarchy logic"
  }}
}}
```

IMPORTANT:
- chain[0] MUST be one of the valid root categories
- chain[-1] MUST be "{pathway_name}"
- Each intermediate level should be a recognized biological term
- Use standard nomenclature (GO, KEGG, or accepted literature terms)
- Maximum depth is {MAX_HIERARCHY_DEPTH} levels
"""

    return prompt


def check_history_for_chain(pathway_name: str) -> Optional[List[str]]:
    """
    Check if we already have a hierarchy chain for this pathway.

    Stage 6 logic: Reuse existing chains to avoid duplicate AI calls.
    """
    from app import app, db
    from models import PathwayHierarchyHistory

    with app.app_context():
        existing = db.session.query(PathwayHierarchyHistory).filter_by(
            canonical_name=pathway_name
        ).first()

        if existing:
            logger.info(f"Found existing chain for '{pathway_name}' in history")
            return existing.hierarchy_chain

    return None


def check_if_fits_existing_hierarchy(
    pathway_name: str,
    existing_pathways: Dict[str, List[str]],
) -> Optional[Dict[str, Any]]:
    """
    Check if this pathway can attach to an existing hierarchy node.

    Stage 6 logic: If a pathway fits under an existing node, attach it
    rather than building a new chain.
    """
    # Check if pathway already exists somewhere in existing chains
    for canonical, chain in existing_pathways.items():
        if pathway_name in chain:
            # This pathway is already in a chain - find where
            idx = chain.index(pathway_name)
            return {
                "parent_chain": chain[:idx + 1],
                "attach_to": chain[idx - 1] if idx > 0 else None,
            }

        # Check if this pathway could logically be a child of an existing leaf
        # (This would require AI, so we skip for now and just check direct matches)

    return None


def build_hierarchy_chain(
    pathway_name: str,
    interaction_context: List[Dict[str, Any]],
    existing_pathways: Dict[str, List[str]] = None,
) -> Optional[List[str]]:
    """
    Build the hierarchy chain for a pathway.

    Includes Stage 6 logic:
    1. Check history first
    2. Check if fits existing hierarchy
    3. Only build new chain if needed
    """
    # Stage 6: Check history first
    cached_chain = check_history_for_chain(pathway_name)
    if cached_chain:
        return cached_chain

    # Stage 6: Check if fits existing hierarchy
    if existing_pathways:
        fit_result = check_if_fits_existing_hierarchy(pathway_name, existing_pathways)
        if fit_result and fit_result.get("parent_chain"):
            logger.info(f"Pathway '{pathway_name}' fits under existing hierarchy")
            return fit_result["parent_chain"]

    # Need to build new chain via AI
    logger.info(f"Building new hierarchy chain for '{pathway_name}'")

    prompt = build_hierarchy_chain_prompt(pathway_name, interaction_context)

    result = call_ai_sequential(
        prompt=prompt,
        stage="stage4",
        use_search=True,  # Enable search for biological knowledge
    )

    if not result.success:
        logger.error(f"Stage 4 AI call failed for '{pathway_name}': {result.error}")
        return None

    try:
        hierarchy = result.data.get("hierarchy_chain", {})
        chain = hierarchy.get("chain", [])
        root = hierarchy.get("root_category")
        confidence = float(hierarchy.get("confidence", 0.0))

        # Validate chain
        if not chain:
            logger.error(f"Empty chain returned for '{pathway_name}'")
            return None

        if chain[0] not in ROOT_CATEGORY_NAMES:
            logger.error(f"Invalid root '{chain[0]}' - not in ROOT_CATEGORIES")
            # Try to fix by prepending a valid root
            # This is a fallback - the AI should have gotten it right
            return None

        if chain[-1] != pathway_name:
            # AI might have used slightly different name - append our name
            chain.append(pathway_name)

        if len(chain) > MAX_HIERARCHY_DEPTH:
            logger.warning(f"Chain too deep ({len(chain)}) - truncating")
            chain = chain[:MAX_HIERARCHY_DEPTH]

        return chain

    except Exception as e:
        logger.error(f"Failed to parse Stage 4 response for '{pathway_name}': {e}")
        return None


def ensure_pathway_chain_in_db(chain: List[str], source: str = 'ai_built'):
    """
    Ensure all pathways in a chain exist in the database with proper relationships.
    """
    from app import app, db
    from models import Pathway, PathwayParent, PathwayHierarchyHistory

    with app.app_context():
        pathway_ids = []

        for level, name in enumerate(chain):
            # Get or create pathway
            pathway = db.session.query(Pathway).filter_by(name=name).first()

            if not pathway:
                is_root = name in ROOT_CATEGORY_NAMES
                pathway = Pathway(
                    name=name,
                    ontology_id=ROOT_CATEGORIES.get(name) if is_root else None,
                    ontology_source='GO' if is_root else None,
                    hierarchy_level=level,
                    is_leaf=(level == len(chain) - 1),
                    ai_generated=not is_root,
                    pathway_type='main',
                    hierarchy_chain=chain[:level + 1],
                )
                db.session.add(pathway)
                db.session.flush()
                logger.info(f"Created pathway: {name} (level {level})")
            else:
                # Update existing pathway
                pathway.hierarchy_level = min(pathway.hierarchy_level, level)
                pathway.is_leaf = False if level < len(chain) - 1 else pathway.is_leaf
                pathway.hierarchy_chain = chain[:level + 1]

            pathway_ids.append(pathway.id)

            # Create parent-child relationship (if not root)
            if level > 0:
                parent_id = pathway_ids[level - 1]

                existing_link = db.session.query(PathwayParent).filter_by(
                    child_pathway_id=pathway.id,
                    parent_pathway_id=parent_id,
                ).first()

                if not existing_link:
                    link = PathwayParent(
                        child_pathway_id=pathway.id,
                        parent_pathway_id=parent_id,
                        relationship_type='is_a',
                        confidence=1.0,
                        source='AI',
                        is_primary_chain=True,
                    )
                    db.session.add(link)

        # Store in history
        leaf_name = chain[-1]
        existing_history = db.session.query(PathwayHierarchyHistory).filter_by(
            canonical_name=leaf_name
        ).first()

        if existing_history:
            existing_history.hierarchy_chain = chain
            existing_history.chain_length = len(chain)
            existing_history.source = source
        else:
            history = PathwayHierarchyHistory(
                canonical_name=leaf_name,
                hierarchy_chain=chain,
                chain_length=len(chain),
                source=source,
            )
            db.session.add(history)

        db.session.commit()
        return pathway_ids


def run_stage4_from_db():
    """
    Run Stage 4 by processing all canonical pathways from database.
    """
    from app import app, db
    from models import PathwayCanonicalName, PathwayInteraction, Interaction

    with app.app_context():
        # Get all unique canonical pathway names
        canonical_names = db.session.query(
            PathwayCanonicalName.canonical_name
        ).distinct().all()

        pathways = list(set(row[0] for row in canonical_names))
        logger.info(f"Stage 4: Processing {len(pathways)} canonical pathways")

        # Track existing chains for Stage 6 reuse
        existing_chains: Dict[str, List[str]] = {}

        for idx, pathway_name in enumerate(pathways):
            logger.info(f"Processing pathway {idx + 1}/{len(pathways)}: {pathway_name}")

            # Skip root categories - they don't need chains
            if pathway_name in ROOT_CATEGORY_NAMES:
                logger.info(f"Skipping root category: {pathway_name}")
                continue

            # Get interaction context for this pathway
            # (Find interactions that have this canonical pathway assigned)
            interactions = db.session.query(Interaction).join(
                PathwayInteraction
            ).join(
                PathwayInteraction.pathway
            ).filter(
                PathwayInteraction.pathway.has(name=pathway_name)
            ).limit(5).all()

            interaction_context = []
            for ix in interactions:
                ix_data = ix.data or {}
                interaction_context.append({
                    "main_protein": ix.discovered_in_query or "Unknown",
                    "primary": ix_data.get("primary", "Unknown"),
                    "functions": ix_data.get("functions", []),
                })

            # Build chain (includes Stage 6 history check)
            chain = build_hierarchy_chain(
                pathway_name=pathway_name,
                interaction_context=interaction_context,
                existing_pathways=existing_chains,
            )

            if chain:
                # Save to database
                ensure_pathway_chain_in_db(chain, source='ai_built')

                # Track for Stage 6 reuse
                existing_chains[pathway_name] = chain
                logger.info(f"Built chain for '{pathway_name}': {' -> '.join(chain)}")
            else:
                logger.warning(f"Failed to build chain for '{pathway_name}'")

        logger.info(f"Stage 4 complete: {len(existing_chains)} chains built")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_stage4_from_db()
