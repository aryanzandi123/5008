#!/usr/bin/env python3
"""
Configuration for Pathway Pipeline V2

Contains:
- ROOT_CATEGORIES: Fixed set of Level 1 root pathways (12 total)
- AI model configuration
- Batch sizes and other constants
"""

from typing import Dict, Set

# =============================================================================
# ROOT CATEGORIES (Level 1)
# =============================================================================
# These are the ONLY valid root pathways. All other pathways must fall under
# one of these roots at some level. The pipeline MUST NOT create new roots.

ROOT_CATEGORIES: Dict[str, str] = {
    # Existing 10 roots
    "Cellular Signaling": "GO:0007165",
    "Metabolism": "GO:0008152",
    "Protein Quality Control": "GO:0006457",
    "Cell Death": "GO:0008219",
    "Cell Cycle": "GO:0007049",
    "DNA Damage Response": "GO:0006974",
    "Vesicle Transport": "GO:0016192",
    "Immune Response": "GO:0006955",
    "Neuronal Function": "GO:0050877",
    "Cytoskeleton Organization": "GO:0007015",
    # New additions for comprehensive coverage
    "Transcriptional Regulation": "GO:0006355",
    "Chromatin Organization": "GO:0006325",
}

# Set of root category names for quick lookup
ROOT_CATEGORY_NAMES: Set[str] = set(ROOT_CATEGORIES.keys())


def is_root_category(name: str) -> bool:
    """Check if a pathway name is a root category."""
    return name in ROOT_CATEGORY_NAMES


def get_root_go_id(name: str) -> str:
    """Get the GO ID for a root category."""
    return ROOT_CATEGORIES.get(name, "")


# =============================================================================
# AI MODEL CONFIGURATION
# =============================================================================

# Primary AI model for all pipeline stages
AI_MODEL = "gemini-3-flash-preview"

# Model parameters
AI_TEMPERATURE = 0.3  # Lower = more deterministic
AI_TOP_P = 0.5
AI_MAX_OUTPUT_TOKENS = 62048

# Retry configuration
AI_MAX_RETRIES = 3
AI_RETRY_DELAY_BASE = 1.5  # Seconds, multiplied by attempt number


# =============================================================================
# BATCH SIZES
# =============================================================================

# Stage 1: Initial pathway designation
# Process interactions one at a time during query (inline)
BATCH_SIZE_STAGE1 = 1

# Stage 2: Normalize pathway names
# Process all unique names in a single batch (or chunks if too large)
BATCH_SIZE_STAGE2 = 50  # Names per AI call

# Stage 3: Reassign interactions to best pathway
# HARD CONSTRAINT from requirements: 5 interactions per batch
BATCH_SIZE_STAGE3 = 5

# Stage 4: Build hierarchy chains
# One pathway at a time (each needs full chain context)
BATCH_SIZE_STAGE4 = 1

# Stage 5: Add siblings
# Process one main chain level at a time
BATCH_SIZE_STAGE5 = 1

# Stage 7: Validation
# Validate entire hierarchy in chunks
BATCH_SIZE_STAGE7_VALIDATION = 100  # Pathways per validation batch


# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================

# Confidence thresholds
MIN_CONFIDENCE_STAGE1 = 0.70  # Minimum confidence for initial assignment
MIN_CONFIDENCE_STAGE3 = 0.70  # Minimum confidence for reassignment
MIN_CONFIDENCE_HIERARCHY = 0.80  # Minimum confidence for hierarchy placement

# Fuzzy matching threshold for Stage 2 normalization
FUZZY_MATCH_THRESHOLD = 0.70  # 70% similarity

# Maximum hierarchy depth (prevent infinite chains)
MAX_HIERARCHY_DEPTH = 10

# Maximum siblings per level (prevent explosion)
MAX_SIBLINGS_PER_LEVEL = 10


# =============================================================================
# DATABASE TABLE NAMES (for reference)
# =============================================================================

TABLE_PATHWAY_INITIAL_ASSIGNMENTS = "pathway_initial_assignments"
TABLE_PATHWAY_CANONICAL_NAMES = "pathway_canonical_names"
TABLE_PATHWAY_HIERARCHY_HISTORY = "pathway_hierarchy_history"


# =============================================================================
# PROMPTS CONFIGURATION
# =============================================================================

# Include root categories in prompts
def get_root_categories_prompt_section() -> str:
    """Generate the root categories section for AI prompts."""
    lines = ["## VALID ROOT CATEGORIES (Level 1):"]
    lines.append("These are the ONLY valid starting points for any pathway hierarchy.")
    lines.append("Every pathway must ultimately trace back to one of these roots.\n")

    for name, go_id in sorted(ROOT_CATEGORIES.items()):
        lines.append(f"- {name} ({go_id})")

    lines.append("\n**IMPORTANT**: Do NOT create new root categories. All pathways must")
    lines.append("fit under one of the above roots at some level (2, 3, 4, ... to any depth).")

    return "\n".join(lines)
