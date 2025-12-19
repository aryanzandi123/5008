#!/usr/bin/env python3
"""
Stage 2: Clean and Normalize Pathway Names

Takes all initial pathway names across ALL interactions and:
- Detects duplicates
- Detects spelling variants
- Detects synonyms
- Normalizes them into a cleaned list of canonical pathway names

Output: PathwayCanonicalName records in database
"""

import sys
import logging
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
from difflib import SequenceMatcher

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pathway_pipeline_v2.ai_client import call_ai_sequential, get_pipeline_memory
from scripts.pathway_pipeline_v2.config import (
    FUZZY_MATCH_THRESHOLD,
    BATCH_SIZE_STAGE2,
    get_root_categories_prompt_section,
)

logger = logging.getLogger(__name__)


# Greek letter normalization
GREEK_MAP = {
    'α': 'alpha', 'β': 'beta', 'γ': 'gamma', 'δ': 'delta',
    'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta',
    'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu',
    'ξ': 'xi', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma',
    'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi',
    'ψ': 'psi', 'ω': 'omega',
}

# Common suffixes to strip for comparison
STRIP_SUFFIXES = [
    ' pathway', ' signaling', ' regulation', ' process',
    ' system', ' cascade', ' network', ' response',
]


def normalize_for_comparison(name: str) -> str:
    """
    Normalize a pathway name for comparison purposes.

    - Lowercase
    - Replace Greek letters
    - Remove punctuation
    - Strip common suffixes
    """
    normalized = name.lower()

    # Replace Greek letters
    for greek, ascii_val in GREEK_MAP.items():
        normalized = normalized.replace(greek, ascii_val)

    # Strip common suffixes
    for suffix in STRIP_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]

    # Remove punctuation and extra spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = ' '.join(normalized.split())

    return normalized


def calculate_similarity(name1: str, name2: str) -> float:
    """Calculate similarity score between two pathway names."""
    norm1 = normalize_for_comparison(name1)
    norm2 = normalize_for_comparison(name2)

    # Use SequenceMatcher for fuzzy comparison
    return SequenceMatcher(None, norm1, norm2).ratio()


def group_similar_names(names: List[str], threshold: float = FUZZY_MATCH_THRESHOLD) -> List[Set[str]]:
    """
    Group pathway names that appear to be duplicates/synonyms.

    Returns list of sets, where each set contains similar names.
    """
    groups: List[Set[str]] = []
    used: Set[str] = set()

    for name in names:
        if name in used:
            continue

        # Start a new group with this name
        group = {name}
        used.add(name)

        # Find all similar names
        for other in names:
            if other in used:
                continue

            similarity = calculate_similarity(name, other)
            if similarity >= threshold:
                group.add(other)
                used.add(other)

        groups.append(group)

    return groups


def build_normalization_prompt(name_groups: List[Set[str]]) -> str:
    """
    Build prompt for AI to confirm/refine canonical names for groups.
    """
    groups_text = ""
    for i, group in enumerate(name_groups, 1):
        names = sorted(group)
        groups_text += f"\nGroup {i}: {names}\n"

    prompt = f"""You are a biological pathway naming expert. Your task is to normalize groups of similar pathway names into canonical forms.

## GROUPS OF SIMILAR PATHWAY NAMES

The following groups contain pathway names that appear similar (possible duplicates, synonyms, or spelling variants):
{groups_text}

## INSTRUCTIONS

For each group, determine:
1. Whether these names truly refer to the same biological pathway
2. If so, what the canonical (standard) name should be
3. If not, which names should be separated into different canonical names

Use standard biological nomenclature (GO, KEGG, or accepted literature terms).

Return your analysis as JSON:

```json
{{
  "normalizations": [
    {{
      "group_id": 1,
      "are_synonyms": true,
      "canonical_name": "The Standard Name",
      "mappings": {{
        "Original Name 1": "The Standard Name",
        "Original Name 2": "The Standard Name"
      }},
      "reasoning": "Why these are/aren't synonyms"
    }}
  ]
}}
```

IMPORTANT:
- If names in a group are NOT synonyms, set are_synonyms to false and provide separate canonical names in mappings
- canonical_name should use standard biological terminology
- Be precise - "Autophagy" and "Macroautophagy" are DIFFERENT (though related)
"""

    return prompt


def normalize_pathway_names_batch(
    names: List[str],
) -> Dict[str, str]:
    """
    Normalize a batch of pathway names using fuzzy matching + AI confirmation.

    Args:
        names: List of unique pathway names to normalize

    Returns:
        Dict mapping each original name to its canonical name
    """
    if not names:
        return {}

    logger.info(f"Stage 2: Normalizing {len(names)} pathway names")

    # Step 1: Group similar names using fuzzy matching
    groups = group_similar_names(names)
    logger.info(f"Found {len(groups)} initial groups (before AI confirmation)")

    # Step 2: For groups with multiple names, use AI to confirm
    mappings: Dict[str, str] = {}
    ambiguous_groups = [g for g in groups if len(g) > 1]

    if ambiguous_groups:
        # Process ambiguous groups in batches
        for batch_start in range(0, len(ambiguous_groups), BATCH_SIZE_STAGE2):
            batch = ambiguous_groups[batch_start:batch_start + BATCH_SIZE_STAGE2]

            prompt = build_normalization_prompt(batch)
            result = call_ai_sequential(
                prompt=prompt,
                stage="stage2",
                use_search=False,
            )

            if result.success and result.data:
                normalizations = result.data.get("normalizations", [])
                for norm in normalizations:
                    group_mappings = norm.get("mappings", {})
                    for original, canonical in group_mappings.items():
                        mappings[original] = canonical
            else:
                # Fallback: use first name in each group as canonical
                logger.warning("AI normalization failed, using fallback")
                for group in batch:
                    canonical = sorted(group)[0]  # Alphabetically first
                    for name in group:
                        mappings[name] = canonical

    # Step 3: Single-name groups map to themselves
    for group in groups:
        if len(group) == 1:
            name = list(group)[0]
            if name not in mappings:
                mappings[name] = name

    logger.info(f"Stage 2 complete: {len(mappings)} mappings created")

    # Store in memory
    memory = get_pipeline_memory()
    memory.canonical_mappings.update(mappings)

    return mappings


def run_stage2_from_db() -> Dict[str, str]:
    """
    Run Stage 2 by reading initial assignments from database.

    This is the main entry point for batch processing.
    """
    from app import app, db
    from models import PathwayInitialAssignment, PathwayCanonicalName

    with app.app_context():
        # Get all unique initial names
        initial_names = db.session.query(
            PathwayInitialAssignment.initial_name
        ).distinct().all()

        names = [row[0] for row in initial_names]
        logger.info(f"Found {len(names)} unique initial pathway names in database")

        if not names:
            logger.warning("No initial assignments found. Run Stage 1 first.")
            return {}

        # Normalize
        mappings = normalize_pathway_names_batch(names)

        # Save to database
        for initial_name, canonical_name in mappings.items():
            # Check if mapping already exists
            existing = db.session.query(PathwayCanonicalName).filter_by(
                initial_name=initial_name
            ).first()

            if existing:
                existing.canonical_name = canonical_name
                existing.match_method = 'ai_confirmed'
            else:
                new_mapping = PathwayCanonicalName(
                    initial_name=initial_name,
                    canonical_name=canonical_name,
                    similarity_score=1.0 if initial_name == canonical_name else 0.9,
                    match_method='exact' if initial_name == canonical_name else 'ai_confirmed',
                )
                db.session.add(new_mapping)

        # Update initial assignments with canonical names
        for initial_name, canonical_name in mappings.items():
            db.session.query(PathwayInitialAssignment).filter_by(
                initial_name=initial_name
            ).update({"canonical_name": canonical_name})

        db.session.commit()
        logger.info(f"Saved {len(mappings)} canonical name mappings to database")

        return mappings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Run from database
    mappings = run_stage2_from_db()

    print(f"\nStage 2 Results: {len(mappings)} mappings")
    for initial, canonical in sorted(mappings.items()):
        if initial != canonical:
            print(f"  {initial} -> {canonical}")
