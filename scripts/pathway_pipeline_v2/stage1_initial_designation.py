#!/usr/bin/env python3
"""
Stage 1: Initial Pathway Designation

For each interaction, AI assigns an initial pathway name that is:
- As SPECIFIC as reasonably possible
- Not absurdly broad (e.g., avoid "Cell Signaling" if "mTORC1 Signaling Regulation" fits)

This stage runs INLINE during query execution (integrated into runner.py).

Output: PathwayInitialAssignment records in database
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_pipeline_v2.ai_client import call_ai_sequential, get_pipeline_memory
from scripts.pathway_pipeline_v2.config import (
    get_root_categories_prompt_section,
    MIN_CONFIDENCE_STAGE1,
)

logger = logging.getLogger(__name__)


BATCH_SIZE_STAGE1 = 10  # Process 10 interactions per AI call


def format_interaction_for_batch(interaction: Dict[str, Any], idx: int) -> str:
    """Format a single interaction for inclusion in a batch prompt."""
    primary = interaction.get("primary", "Unknown")
    arrow = interaction.get("arrow", "binds")
    direction = interaction.get("direction", "bidirectional")
    functions = interaction.get("functions", [])
    evidence = interaction.get("evidence", [])

    # Compact functions
    func_list = []
    for f in functions[:3]:
        desc = f.get('description', f.get('name', 'Unknown'))
        if desc:
            func_list.append(desc[:100])  # Truncate long descriptions
    functions_text = "; ".join(func_list) if func_list else "No specific functions"

    # Compact evidence
    ev_list = []
    for e in evidence[:2]:
        if isinstance(e, str):
            ev_list.append(e[:80])
        elif isinstance(e, dict):
            ev_list.append(e.get('summary', str(e))[:80])
    evidence_text = "; ".join(ev_list) if ev_list else "No evidence"

    return f"""### Interaction {idx + 1}: {primary}
- Type: {arrow} ({direction})
- Functions: {functions_text}
- Evidence: {evidence_text}"""


def build_batch_designation_prompt(
    interactions: List[Dict[str, Any]],
    main_protein: str,
) -> str:
    """Build prompt for assigning pathways to multiple interactions at once."""

    # Format all interactions
    interactions_text = "\n\n".join([
        format_interaction_for_batch(inter, idx)
        for idx, inter in enumerate(interactions)
    ])

    # Get list of interactor names for JSON structure
    interactor_names = [inter.get("primary", f"Unknown{i}") for i, inter in enumerate(interactions)]

    prompt = f"""You are a biological pathway classification expert. Assign the MOST SPECIFIC appropriate biological pathway to each protein-protein interaction below.

## MAIN PROTEIN: {main_protein}

## INTERACTIONS TO CLASSIFY ({len(interactions)} total)

{interactions_text}

{get_root_categories_prompt_section()}

## INSTRUCTIONS

1. For EACH interaction, determine the MOST SPECIFIC biological pathway it belongs to.

2. Be SPECIFIC, not generic:
   - BAD: "Autophagy" (too broad) → GOOD: "Aggrephagy"
   - BAD: "Cell Signaling" (too broad) → GOOD: "mTORC1 Nutrient Sensing"

3. Each pathway name should be a recognized biological term (GO, KEGG, or standard literature).

4. Return valid JSON with assignments for ALL {len(interactions)} interactions:

```json
{{
  "pathway_assignments": [
    {{"interactor": "{interactor_names[0]}", "pathway_name": "Specific Pathway", "confidence": 0.85, "reasoning": "Brief reason"}},
    {{"interactor": "{interactor_names[1] if len(interactor_names) > 1 else 'Example'}", "pathway_name": "Another Pathway", "confidence": 0.80, "reasoning": "Brief reason"}}
  ]
}}
```

IMPORTANT:
- Return EXACTLY {len(interactions)} assignments in the array
- confidence between 0.7 and 1.0
- Do NOT use root categories - be more specific
"""
    return prompt


def build_initial_designation_prompt(
    interaction: Dict[str, Any],
    main_protein: str,
) -> str:
    """
    Build the prompt for assigning an initial pathway to a single interaction.
    Used as fallback when batch processing fails.
    """
    # Extract interaction details
    primary = interaction.get("primary", "Unknown")
    arrow = interaction.get("arrow", "binds")
    direction = interaction.get("direction", "bidirectional")
    functions = interaction.get("functions", [])
    evidence = interaction.get("evidence", [])

    # Format functions for prompt
    functions_text = ""
    if functions:
        functions_text = "\n".join([
            f"  - {f.get('description', f.get('name', 'Unknown function'))}"
            for f in functions[:5]  # Top 5 functions
        ])
    else:
        functions_text = "  (No specific functions provided)"

    # Format evidence for prompt
    evidence_text = ""
    if evidence:
        evidence_text = "\n".join([
            f"  - {e}" if isinstance(e, str) else f"  - {e.get('summary', str(e))}"
            for e in evidence[:3]  # Top 3 evidence items
        ])
    else:
        evidence_text = "  (No specific evidence provided)"

    prompt = f"""You are a biological pathway classification expert. Your task is to assign the MOST SPECIFIC appropriate biological pathway name to a protein-protein interaction.

## INTERACTION TO CLASSIFY

**Main protein**: {main_protein}
**Interactor**: {primary}
**Interaction type**: {main_protein} {arrow} {primary}
**Direction**: {direction}

**Functions**:
{functions_text}

**Evidence**:
{evidence_text}

{get_root_categories_prompt_section()}

## INSTRUCTIONS

1. Based on the interaction details above, determine the MOST SPECIFIC biological pathway this interaction belongs to.

2. Be SPECIFIC, not generic:
   - BAD: "Autophagy" (too broad)
   - GOOD: "Aggrephagy" (specific type of autophagy)
   - BAD: "Cell Signaling" (too broad)
   - GOOD: "mTORC1 Nutrient Sensing" (specific signaling pathway)

3. The pathway name should:
   - Be a recognized biological term (GO, KEGG, or standard literature terminology)
   - Accurately describe what this interaction does biologically
   - Be specific enough to distinguish from other pathways

4. You MUST return valid JSON in this exact format:

```json
{{
  "pathway_assignment": {{
    "pathway_name": "Specific Pathway Name",
    "confidence": 0.85,
    "reasoning": "Brief explanation of why this pathway fits"
  }}
}}
```

IMPORTANT:
- confidence should be between 0.7 and 1.0
- pathway_name should be a specific biological pathway term
- Do NOT just echo back a root category - be more specific
"""

    return prompt


def assign_initial_pathway(
    interaction: Dict[str, Any],
    main_protein: str,
    api_key: str = None,
) -> Optional[Dict[str, Any]]:
    """
    Assign an initial pathway to a single interaction.

    Args:
        interaction: Interaction data with primary, arrow, functions, evidence
        main_protein: The main protein being queried
        api_key: Google API key (optional, uses env if not provided)

    Returns:
        Dict with pathway_name, confidence, reasoning, or None on failure
    """
    prompt = build_initial_designation_prompt(interaction, main_protein)

    result = call_ai_sequential(
        prompt=prompt,
        stage="stage1",
        use_search=False,  # No search needed for initial designation
    )

    if not result.success:
        logger.error(f"Stage 1 AI call failed: {result.error}")
        return None

    try:
        assignment = result.data.get("pathway_assignment", {})
        pathway_name = assignment.get("pathway_name")
        confidence = float(assignment.get("confidence", 0.0))
        reasoning = assignment.get("reasoning", "")

        if not pathway_name:
            logger.error("No pathway_name in AI response")
            return None

        if confidence < MIN_CONFIDENCE_STAGE1:
            logger.warning(f"Low confidence ({confidence}) for pathway assignment")
            # Still return it, but flag the low confidence

        return {
            "pathway_name": pathway_name,
            "confidence": confidence,
            "reasoning": reasoning,
        }

    except Exception as e:
        logger.error(f"Failed to parse Stage 1 response: {e}")
        return None


def process_batch_response(
    batch: List[Dict[str, Any]],
    result: Any,
    main_protein: str,
    memory: Any,
) -> List[Dict[str, Any]]:
    """Process batch AI response and assign pathways to interactions."""
    results = []

    try:
        assignments = result.data.get("pathway_assignments", [])

        # Build lookup by interactor name (case-insensitive)
        assignment_map = {}
        for a in assignments:
            interactor_name = a.get("interactor", "").strip().upper()
            if interactor_name:
                assignment_map[interactor_name] = a

        for interactor in batch:
            primary = interactor.get("primary", "Unknown")
            primary_upper = primary.strip().upper()

            # Try to find matching assignment
            assignment = assignment_map.get(primary_upper)

            if assignment and assignment.get("pathway_name"):
                pathway_data = {
                    "pathway_name": assignment["pathway_name"],
                    "confidence": float(assignment.get("confidence", 0.8)),
                    "reasoning": assignment.get("reasoning", ""),
                }
                interactor["initial_pathway"] = pathway_data

                # Store in memory
                interaction_key = f"{main_protein}:{primary}"
                memory.initial_assignments[interaction_key] = pathway_data
                memory.all_pathways.add(pathway_data["pathway_name"])
            else:
                # Fallback for missing assignment
                logger.warning(f"No assignment found for {primary}, using fallback")
                interactor["initial_pathway"] = {
                    "pathway_name": "Protein Quality Control",
                    "confidence": 0.5,
                    "reasoning": "Fallback - not found in batch response",
                }

            results.append(interactor)

    except Exception as e:
        logger.error(f"Failed to process batch response: {e}")
        # Fallback all interactions in this batch
        for interactor in batch:
            interactor["initial_pathway"] = {
                "pathway_name": "Protein Quality Control",
                "confidence": 0.5,
                "reasoning": f"Fallback - batch parse error: {e}",
            }
            results.append(interactor)

    return results


def assign_initial_pathways_batch(
    interactors: List[Dict[str, Any]],
    main_protein: str,
    api_key: str = None,
) -> List[Dict[str, Any]]:
    """
    Assign initial pathways to a list of interactions using batch processing.

    Processes interactions in batches of BATCH_SIZE_STAGE1 (10) for efficiency.
    This reduces API calls from N to N/10, dramatically improving speed.

    Args:
        interactors: List of interactor dicts
        main_protein: The main protein being queried
        api_key: Google API key (optional)

    Returns:
        List of interactor dicts with initial_pathway field added
    """
    memory = get_pipeline_memory()
    results = []

    total = len(interactors)
    num_batches = (total + BATCH_SIZE_STAGE1 - 1) // BATCH_SIZE_STAGE1

    logger.info(f"Stage 1: Processing {total} interactions in {num_batches} batches of {BATCH_SIZE_STAGE1}")

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_SIZE_STAGE1
        end = min(start + BATCH_SIZE_STAGE1, total)
        batch = interactors[start:end]

        batch_interactors = [i.get("primary", "?") for i in batch]
        logger.info(f"Stage 1: Batch {batch_idx + 1}/{num_batches} ({len(batch)} interactions: {', '.join(batch_interactors[:3])}...)")

        # Build batch prompt
        prompt = build_batch_designation_prompt(batch, main_protein)

        # Call AI for entire batch
        result = call_ai_sequential(
            prompt=prompt,
            stage="stage1",
            use_search=False,
        )

        if result.success:
            batch_results = process_batch_response(batch, result, main_protein, memory)
            results.extend(batch_results)
        else:
            # Batch failed - fallback to individual calls for this batch
            logger.warning(f"Batch {batch_idx + 1} failed, falling back to individual calls")
            for interactor in batch:
                primary = interactor.get("primary", "Unknown")

                assignment = assign_initial_pathway(
                    interaction=interactor,
                    main_protein=main_protein,
                    api_key=api_key,
                )

                if assignment:
                    interactor["initial_pathway"] = assignment
                    interaction_key = f"{main_protein}:{primary}"
                    memory.initial_assignments[interaction_key] = assignment
                    memory.all_pathways.add(assignment["pathway_name"])
                else:
                    interactor["initial_pathway"] = {
                        "pathway_name": "Protein Quality Control",
                        "confidence": 0.5,
                        "reasoning": "Fallback assignment due to AI failure",
                    }

                results.append(interactor)

    logger.info(f"Stage 1 complete: {len(results)} interactions processed")
    logger.info(f"Unique pathways discovered: {len(memory.all_pathways)}")

    return results


# Convenience function for runner.py integration
def run_stage1(
    interactors: List[Dict[str, Any]],
    main_protein: str,
    api_key: str = None,
) -> List[Dict[str, Any]]:
    """
    Run Stage 1 of the pathway pipeline.

    This is the main entry point called from runner.py.

    Args:
        interactors: List of interactor dicts from the query pipeline
        main_protein: The protein being queried
        api_key: Google API key (optional)

    Returns:
        Interactors with initial_pathway field added to each
    """
    return assign_initial_pathways_batch(
        interactors=interactors,
        main_protein=main_protein,
        api_key=api_key,
    )


if __name__ == "__main__":
    # Test with sample data
    import json

    logging.basicConfig(level=logging.INFO)

    test_interaction = {
        "primary": "VCP",
        "arrow": "binds",
        "direction": "bidirectional",
        "functions": [
            {"description": "Protein extraction from the ER membrane during ERAD"},
            {"description": "Delivery of ubiquitinated substrates to proteasome"},
        ],
        "evidence": [
            "VCP/p97 is a AAA+ ATPase involved in protein quality control",
            "Required for ERAD pathway to function properly",
        ],
    }

    result = assign_initial_pathway(
        interaction=test_interaction,
        main_protein="ATXN3",
    )

    print("\nStage 1 Test Result:")
    print(json.dumps(result, indent=2))
