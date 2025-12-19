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
    Build prompt for determining the hierarchy chain from pathway to root (single pathway).
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


def build_batch_hierarchy_prompt(
    pathways_with_context: List[Dict[str, Any]],
) -> str:
    """
    Build prompt for determining hierarchy chains for MULTIPLE pathways at once.
    """
    # Format all pathways
    pathways_text = ""
    for i, pw in enumerate(pathways_with_context, 1):
        name = pw["name"]
        context = pw.get("context", [])
        context_str = ""
        for ix in context[:2]:  # Top 2 interactions per pathway for context
            proteins = f"{ix.get('main_protein', '?')}-{ix.get('primary', '?')}"
            context_str += f"{proteins}; "
        pathways_text += f"{i}. **{name}** (context: {context_str.strip('; ')})\n"

    pathway_names = [pw["name"] for pw in pathways_with_context]

    prompt = f"""You are a biological pathway hierarchy expert. Build "is-a-type-of" chains for MULTIPLE pathways.

## PATHWAYS TO CLASSIFY ({len(pathways_with_context)} total)

{pathways_text}

{get_root_categories_prompt_section()}

## INSTRUCTIONS

For EACH pathway above, build a hierarchy chain UP TO a root category.

Example chain for "Aggrephagy":
["Protein Quality Control", "Sequestration and Aggregate Clearance", "Autophagy", "Macroautophagy", "Selective Macroautophagy", "Aggrephagy"]

Return JSON with ALL {len(pathways_with_context)} pathways:

```json
{{
  "hierarchy_chains": [
    {{
      "pathway_name": "{pathway_names[0]}",
      "chain": ["Root Category", "Level 2", "...", "{pathway_names[0]}"],
      "confidence": 0.90
    }},
    {{
      "pathway_name": "{pathway_names[1] if len(pathway_names) > 1 else 'Example'}",
      "chain": ["Root Category", "Level 2", "...", "{pathway_names[1] if len(pathway_names) > 1 else 'Example'}"],
      "confidence": 0.85
    }}
  ]
}}
```

IMPORTANT:
- Return EXACTLY {len(pathways_with_context)} chains
- Each chain[0] MUST be a valid root category
- Each chain[-1] MUST be the pathway name
- Max depth: {MAX_HIERARCHY_DEPTH} levels per chain
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


def process_batch_hierarchy_response(
    batch: List[Dict[str, Any]],
    result: Any,
    existing_chains: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Process batch AI response and extract hierarchy chains for each pathway."""
    new_chains: Dict[str, List[str]] = {}

    try:
        chains = result.data.get("hierarchy_chains", [])

        # Build lookup by pathway name (case-insensitive)
        chain_map = {}
        for c in chains:
            name = c.get("pathway_name", "").strip()
            if name:
                chain_map[name.upper()] = c

        for pw_data in batch:
            pathway_name = pw_data["name"]
            pathway_upper = pathway_name.strip().upper()

            # Try to find matching chain
            chain_data = chain_map.get(pathway_upper)

            if chain_data and chain_data.get("chain"):
                chain = chain_data["chain"]

                # Validate chain
                if chain[0] not in ROOT_CATEGORY_NAMES:
                    logger.warning(f"Invalid root '{chain[0]}' for '{pathway_name}' - skipping")
                    continue

                if chain[-1] != pathway_name:
                    # AI might have used slightly different name
                    chain.append(pathway_name)

                if len(chain) > MAX_HIERARCHY_DEPTH:
                    chain = chain[:MAX_HIERARCHY_DEPTH]

                new_chains[pathway_name] = chain
                logger.info(f"Built chain for '{pathway_name}': {' -> '.join(chain)}")
            else:
                logger.warning(f"No chain found in batch response for '{pathway_name}'")

    except Exception as e:
        logger.error(f"Failed to parse batch hierarchy response: {e}")

    return new_chains


def run_stage4_from_db():
    """
    Run Stage 4 by processing all canonical pathways from database.
    Uses batch processing for efficiency (BATCH_SIZE_STAGE4 pathways per AI call).
    """
    from app import app, db
    from models import PathwayCanonicalName, PathwayInteraction, Interaction
    from scripts.pathway_pipeline_v2.config import BATCH_SIZE_STAGE4

    with app.app_context():
        # Get all unique canonical pathway names
        canonical_names = db.session.query(
            PathwayCanonicalName.canonical_name
        ).distinct().all()

        all_pathways = list(set(row[0] for row in canonical_names))

        # Filter out root categories
        pathways = [p for p in all_pathways if p not in ROOT_CATEGORY_NAMES]
        logger.info(f"Stage 4: Processing {len(pathways)} canonical pathways (excluding {len(all_pathways) - len(pathways)} roots)")

        # Track existing chains for Stage 6 reuse
        existing_chains: Dict[str, List[str]] = {}

        # Pre-check history for all pathways (Stage 6 optimization)
        for pathway_name in pathways:
            cached_chain = check_history_for_chain(pathway_name)
            if cached_chain:
                existing_chains[pathway_name] = cached_chain
                ensure_pathway_chain_in_db(cached_chain, source='history_reuse')
                logger.info(f"Reused cached chain for '{pathway_name}'")

        # Filter to only pathways that need new chains
        pathways_needing_chains = [p for p in pathways if p not in existing_chains]
        logger.info(f"Stage 4: {len(pathways_needing_chains)} pathways need new chains ({len(existing_chains)} from cache)")

        if not pathways_needing_chains:
            logger.info("Stage 4 complete: All chains from cache")
            return

        # Get interaction context for each pathway
        pathways_with_context = []
        for pathway_name in pathways_needing_chains:
            # Check if fits existing hierarchy (Stage 6)
            fit_result = check_if_fits_existing_hierarchy(pathway_name, existing_chains)
            if fit_result and fit_result.get("parent_chain"):
                existing_chains[pathway_name] = fit_result["parent_chain"]
                ensure_pathway_chain_in_db(fit_result["parent_chain"], source='attach_existing')
                logger.info(f"Attached '{pathway_name}' to existing hierarchy")
                continue

            # Get interaction context
            interactions = db.session.query(Interaction).join(
                PathwayInteraction
            ).join(
                PathwayInteraction.pathway
            ).filter(
                PathwayInteraction.pathway.has(name=pathway_name)
            ).limit(3).all()  # Reduced from 5 to 3 for batch efficiency

            interaction_context = []
            for ix in interactions:
                ix_data = ix.data or {}
                interaction_context.append({
                    "main_protein": ix.discovered_in_query or "Unknown",
                    "primary": ix_data.get("primary", "Unknown"),
                    "functions": ix_data.get("functions", []),
                })

            pathways_with_context.append({
                "name": pathway_name,
                "context": interaction_context,
            })

        if not pathways_with_context:
            logger.info("Stage 4 complete: All chains resolved from cache/existing")
            return

        # Process in batches
        total = len(pathways_with_context)
        num_batches = (total + BATCH_SIZE_STAGE4 - 1) // BATCH_SIZE_STAGE4
        logger.info(f"Stage 4: Processing {total} pathways in {num_batches} batches of {BATCH_SIZE_STAGE4}")

        for batch_idx in range(num_batches):
            start = batch_idx * BATCH_SIZE_STAGE4
            end = min(start + BATCH_SIZE_STAGE4, total)
            batch = pathways_with_context[start:end]

            batch_names = [p["name"] for p in batch]
            logger.info(f"Stage 4: Batch {batch_idx + 1}/{num_batches} ({len(batch)} pathways: {', '.join(batch_names[:3])}...)")

            # Build batch prompt
            prompt = build_batch_hierarchy_prompt(batch)

            # Call AI for entire batch
            result = call_ai_sequential(
                prompt=prompt,
                stage="stage4",
                use_search=True,
            )

            if result.success:
                new_chains = process_batch_hierarchy_response(batch, result, existing_chains)

                # Save chains to database and track for reuse
                for pathway_name, chain in new_chains.items():
                    ensure_pathway_chain_in_db(chain, source='ai_built')
                    existing_chains[pathway_name] = chain
            else:
                # Batch failed - fallback to individual processing
                logger.warning(f"Batch {batch_idx + 1} failed, falling back to individual calls")
                for pw_data in batch:
                    pathway_name = pw_data["name"]
                    chain = build_hierarchy_chain(
                        pathway_name=pathway_name,
                        interaction_context=pw_data["context"],
                        existing_pathways=existing_chains,
                    )
                    if chain:
                        ensure_pathway_chain_in_db(chain, source='ai_built')
                        existing_chains[pathway_name] = chain
                        logger.info(f"Built chain for '{pathway_name}': {' -> '.join(chain)}")
                    else:
                        logger.warning(f"Failed to build chain for '{pathway_name}'")

        logger.info(f"Stage 4 complete: {len(existing_chains)} chains built")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_stage4_from_db()
