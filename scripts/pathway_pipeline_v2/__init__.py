"""
Pathway Pipeline V2

A multi-stage AI pipeline for assigning biological pathways to protein interactions.

Stages:
1. Initial Designation - AI assigns initial pathway name per interaction
2. Normalize Names - Clean/normalize pathway names, detect synonyms
3. Reassign Interactions - Reassign to best pathway (batches of 5)
4. Build Hierarchy Chains - Build is-a chains backwards to roots
5. Add Siblings - Add sibling pathways at each level
6. (Integrated in Stage 4) Use history across batches
7. Validate and Commit - Final validation, prune dead nodes, atomic commit

Usage:
    # Stage 1 runs inline during query (integrated into runner.py)

    # Stages 2-7 run as batch:
    python scripts/pathway_pipeline_v2/run_batch.py

    # Or run individual stages:
    python scripts/pathway_pipeline_v2/stage2_normalize_names.py
"""

from .config import (
    ROOT_CATEGORIES,
    ROOT_CATEGORY_NAMES,
    AI_MODEL,
    BATCH_SIZE_STAGE3,
)
