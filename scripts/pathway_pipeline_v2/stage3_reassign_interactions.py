#!/usr/bin/env python3
"""
Stage 3: Reassign Interactions to Best Cleaned Pathway

For each interaction (in batches of BATCH_SIZE_STAGE3), consider ALL cleaned
pathway names and assign the interaction to the single most specific pathway.

Uses configurable batch size (default: 15) for efficiency.

Output: Updated PathwayInteraction records in database
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
    BATCH_SIZE_STAGE3,
    MIN_CONFIDENCE_STAGE3,
)

logger = logging.getLogger(__name__)


def build_reassignment_prompt(
    interactions: List[Dict[str, Any]],
    all_pathways: List[str],
) -> str:
    """
    Build prompt for reassigning interactions to best pathways.

    The AI sees ALL cleaned pathway names and picks the most specific for each.
    """
    # Format pathway list
    pathways_text = "\n".join([f"- {p}" for p in sorted(all_pathways)])

    # Format interactions
    interactions_text = ""
    for i, ix in enumerate(interactions, 1):
        primary = ix.get("primary", "Unknown")
        main = ix.get("main_protein", "Unknown")
        arrow = ix.get("arrow", "binds")
        current = ix.get("canonical_pathway", ix.get("initial_pathway", {}).get("pathway_name", "Unknown"))

        functions = ix.get("functions", [])
        func_text = "; ".join([
            f.get("description", f.get("name", ""))
            for f in functions[:3]
        ]) or "No functions specified"

        interactions_text += f"""
Interaction {i}:
  - Proteins: {main} {arrow} {primary}
  - Current pathway: {current}
  - Functions: {func_text}
"""

    prompt = f"""You are a biological pathway classification expert. Your task is to assign each interaction to the SINGLE BEST and MOST SPECIFIC pathway from the provided list.

## ALL AVAILABLE PATHWAYS

Choose from ONLY these pathways (do NOT create new names):

{pathways_text}

## INTERACTIONS TO CLASSIFY
{interactions_text}

## INSTRUCTIONS

For each interaction:
1. Review its proteins, functions, and current pathway assignment
2. Consider ALL pathways in the list above
3. Assign to the MOST SPECIFIC pathway that accurately describes the interaction
4. Do NOT use generic pathways if a more specific one fits

Return JSON in this format:

```json
{{
  "reassignments": [
    {{
      "interaction_index": 1,
      "best_pathway": "Most Specific Pathway Name",
      "confidence": 0.90,
      "reasoning": "Why this is the best fit"
    }}
  ]
}}
```

IMPORTANT:
- Use EXACT pathway names from the list above
- Pick the MOST SPECIFIC pathway that fits
- confidence should be 0.7-1.0
- Every interaction must be assigned
"""

    return prompt


def reassign_interactions_batch(
    interactions: List[Dict[str, Any]],
    all_pathways: List[str],
) -> List[Dict[str, Any]]:
    """
    Reassign a batch of interactions to their best pathways.

    Args:
        interactions: List of interaction dicts (max BATCH_SIZE_STAGE3)
        all_pathways: All available canonical pathway names

    Returns:
        Interactions with reassigned pathways
    """
    if len(interactions) > BATCH_SIZE_STAGE3:
        raise ValueError(f"Batch size must be <= {BATCH_SIZE_STAGE3}")

    prompt = build_reassignment_prompt(interactions, all_pathways)

    result = call_ai_sequential(
        prompt=prompt,
        stage="stage3",
        use_search=False,
    )

    if not result.success:
        logger.error(f"Stage 3 AI call failed: {result.error}")
        # Return interactions unchanged
        return interactions

    try:
        reassignments = result.data.get("reassignments", [])

        # Apply reassignments
        for reassign in reassignments:
            idx = reassign.get("interaction_index", 0) - 1  # Convert to 0-based
            if 0 <= idx < len(interactions):
                best_pathway = reassign.get("best_pathway")
                confidence = float(reassign.get("confidence", 0.0))

                if best_pathway in all_pathways:
                    interactions[idx]["final_pathway"] = best_pathway
                    interactions[idx]["final_confidence"] = confidence
                    interactions[idx]["reassignment_reasoning"] = reassign.get("reasoning", "")
                else:
                    logger.warning(f"AI returned unknown pathway: {best_pathway}")
                    # Keep current assignment
                    interactions[idx]["final_pathway"] = interactions[idx].get(
                        "canonical_pathway",
                        interactions[idx].get("initial_pathway", {}).get("pathway_name")
                    )

        return interactions

    except Exception as e:
        logger.error(f"Failed to parse Stage 3 response: {e}")
        return interactions


def run_stage3_from_db():
    """
    Run Stage 3 by reading from database.

    Processes all interactions with initial assignments in batches of BATCH_SIZE_STAGE3.
    """
    from app import app, db
    from models import (
        Interaction, PathwayInitialAssignment, PathwayCanonicalName,
        Pathway, PathwayInteraction,
    )

    with app.app_context():
        # Get all canonical pathway names
        canonical_names = db.session.query(
            PathwayCanonicalName.canonical_name
        ).distinct().all()
        all_pathways = list(set(row[0] for row in canonical_names))

        if not all_pathways:
            logger.error("No canonical pathways found. Run Stage 2 first.")
            return

        logger.info(f"Stage 3: {len(all_pathways)} canonical pathways available")

        # Get all initial assignments
        assignments = db.session.query(
            PathwayInitialAssignment
        ).all()

        if not assignments:
            logger.warning("No initial assignments found. Run Stages 1-2 first.")
            return

        logger.info(f"Processing {len(assignments)} interactions in batches of {BATCH_SIZE_STAGE3}")

        # Process in batches of 5
        processed = 0
        for batch_start in range(0, len(assignments), BATCH_SIZE_STAGE3):
            batch_assignments = assignments[batch_start:batch_start + BATCH_SIZE_STAGE3]

            # Build interaction dicts for the batch
            batch_interactions = []
            for assign in batch_assignments:
                interaction = assign.interaction
                if not interaction:
                    continue

                ix_data = interaction.data or {}
                batch_interactions.append({
                    "db_id": interaction.id,
                    "assignment_id": assign.id,
                    "main_protein": interaction.discovered_in_query or "Unknown",
                    "primary": ix_data.get("primary", "Unknown"),
                    "arrow": ix_data.get("arrow", interaction.arrow or "binds"),
                    "functions": ix_data.get("functions", []),
                    "canonical_pathway": assign.canonical_name,
                    "initial_pathway": {"pathway_name": assign.initial_name},
                })

            if not batch_interactions:
                continue

            # Reassign
            results = reassign_interactions_batch(batch_interactions, all_pathways)

            # Update database with final assignments
            for ix in results:
                final_pathway = ix.get("final_pathway")
                if not final_pathway:
                    continue

                # Get or create the pathway record
                pathway = db.session.query(Pathway).filter_by(name=final_pathway).first()
                if not pathway:
                    pathway = Pathway(
                        name=final_pathway,
                        ai_generated=True,
                        pathway_type='main',
                        hierarchy_level=999,  # Mark as unprocessed - Stage 4 will set proper level
                    )
                    db.session.add(pathway)
                    db.session.flush()

                # Create or update PathwayInteraction
                existing_link = db.session.query(PathwayInteraction).filter_by(
                    interaction_id=ix["db_id"]
                ).first()

                if existing_link:
                    existing_link.pathway_id = pathway.id
                    existing_link.assignment_confidence = ix.get("final_confidence", 0.8)
                    existing_link.assignment_method = 'ai_pipeline_v2'
                else:
                    new_link = PathwayInteraction(
                        pathway_id=pathway.id,
                        interaction_id=ix["db_id"],
                        assignment_confidence=ix.get("final_confidence", 0.8),
                        assignment_method='ai_pipeline_v2',
                    )
                    db.session.add(new_link)

            processed += len(batch_interactions)
            logger.info(f"Processed {processed}/{len(assignments)} interactions")

        db.session.commit()
        logger.info(f"Stage 3 complete: {processed} interactions reassigned")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_stage3_from_db()
