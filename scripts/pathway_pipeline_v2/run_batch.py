#!/usr/bin/env python3
"""
Pathway Pipeline V2 - Batch Orchestrator

Runs Stages 2-7 of the pathway pipeline in sequence.
Stage 1 runs inline during query (integrated into runner.py).

Usage:
    # Run all batch stages
    python scripts/pathway_pipeline_v2/run_batch.py

    # Run from specific stage
    python scripts/pathway_pipeline_v2/run_batch.py --from 4

    # Run up to specific stage
    python scripts/pathway_pipeline_v2/run_batch.py --to 5

    # Run with pruning at end
    python scripts/pathway_pipeline_v2/run_batch.py --prune
"""

import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Stage modules
from scripts.pathway_pipeline_v2 import stage2_normalize_names
from scripts.pathway_pipeline_v2 import stage3_reassign_interactions
from scripts.pathway_pipeline_v2 import stage4_build_hierarchy_chains
from scripts.pathway_pipeline_v2 import stage5_add_siblings
from scripts.pathway_pipeline_v2 import stage7_validate_and_commit

logger = logging.getLogger(__name__)


STAGES = [
    {
        "number": 2,
        "name": "Normalize Names",
        "description": "Clean and normalize pathway names, detect synonyms",
        "function": stage2_normalize_names.run_stage2_from_db,
    },
    {
        "number": 3,
        "name": "Reassign Interactions",
        "description": "Reassign interactions to best pathway (batches of 5)",
        "function": stage3_reassign_interactions.run_stage3_from_db,
    },
    {
        "number": 4,
        "name": "Build Hierarchy Chains",
        "description": "Build is-a chains backwards to roots (includes Stage 6 history)",
        "function": stage4_build_hierarchy_chains.run_stage4_from_db,
    },
    {
        "number": 5,
        "name": "Add Siblings",
        "description": "Add sibling pathways at each hierarchy level",
        "function": stage5_add_siblings.run_stage5_from_db,
    },
    {
        "number": 7,
        "name": "Validate and Commit",
        "description": "Final validation, prune dead nodes, commit",
        "function": None,  # Special handling below
    },
]


def run_stage(stage: dict, prune: bool = False):
    """Run a single stage."""
    print(f"\n{'='*60}")
    print(f"STAGE {stage['number']}: {stage['name'].upper()}")
    print(f"{stage['description']}")
    print(f"{'='*60}\n")

    start_time = time.time()

    if stage["number"] == 7:
        # Special handling for Stage 7
        stage7_validate_and_commit.run_stage7(prune=prune)
    else:
        stage["function"]()

    elapsed = time.time() - start_time
    print(f"\n[Stage {stage['number']} completed in {elapsed:.1f}s]")


def run_batch_pipeline(
    from_stage: int = 2,
    to_stage: int = 7,
    prune: bool = False,
):
    """
    Run the batch stages of the pathway pipeline.

    Args:
        from_stage: Start from this stage (inclusive, default 2)
        to_stage: Run up to this stage (inclusive, default 7)
        prune: Whether to prune dead pathways in Stage 7
    """
    print("\n" + "=" * 60)
    print("PATHWAY PIPELINE V2 - BATCH PROCESSING")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Stages: {from_stage} to {to_stage}")
    print("=" * 60)

    # Filter stages to run
    stages_to_run = [s for s in STAGES if from_stage <= s["number"] <= to_stage]

    if not stages_to_run:
        print(f"\nNo stages to run between {from_stage} and {to_stage}")
        return

    total_start = time.time()

    # Run each stage
    for stage in stages_to_run:
        try:
            run_stage(stage, prune=prune)
        except Exception as e:
            logger.error(f"Stage {stage['number']} failed: {e}")
            print(f"\n[ERROR] Stage {stage['number']} failed: {e}")
            print("Pipeline stopped. Fix the error and resume with --from")
            return

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Run Pathway Pipeline V2 batch stages (2-7)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all batch stages
    python run_batch.py

    # Run from Stage 4 (if earlier stages already complete)
    python run_batch.py --from 4

    # Run only Stages 2-3
    python run_batch.py --to 3

    # Run with pruning at the end
    python run_batch.py --prune

Note: Stage 1 runs inline during query (integrated into runner.py).
      Use this script to run Stages 2-7 after queries complete.
        """
    )

    parser.add_argument(
        "--from", dest="from_stage", type=int, default=2,
        help="Start from this stage (default: 2)"
    )
    parser.add_argument(
        "--to", dest="to_stage", type=int, default=7,
        help="Run up to this stage (default: 7)"
    )
    parser.add_argument(
        "--prune", action="store_true",
        help="Actually prune dead pathways in Stage 7 (default: dry run)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Validate stage range
    if args.from_stage < 2 or args.from_stage > 7:
        print(f"Error: --from must be between 2 and 7 (got {args.from_stage})")
        sys.exit(1)

    if args.to_stage < 2 or args.to_stage > 7:
        print(f"Error: --to must be between 2 and 7 (got {args.to_stage})")
        sys.exit(1)

    if args.from_stage > args.to_stage:
        print(f"Error: --from ({args.from_stage}) cannot be greater than --to ({args.to_stage})")
        sys.exit(1)

    # Run pipeline
    run_batch_pipeline(
        from_stage=args.from_stage,
        to_stage=args.to_stage,
        prune=args.prune,
    )


if __name__ == "__main__":
    main()
