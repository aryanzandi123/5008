from typing import Dict, Any, List

def aggregate_function_arrows(interactor: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggregate function-level arrows into interaction-level arrows field.

    UPDATED: Now handles both interaction_effect (protein-level) and arrow (function-level).
    Aggregates interaction_direction and interaction_effect into interactor-level fields.

    This function computes:
    - `arrows`: Dict mapping direction → list of unique interaction_effect types
    - `arrow`: Backward-compat field (most common interaction_effect or 'complex' if mixed)
    - `direction`: main_to_primary | primary_to_main | bidirectional

    Args:
        interactor: Interactor dict with functions[] containing interaction_effect/interaction_direction fields

    Returns:
        Updated interactor dict with arrows and arrow fields
    """
    functions = interactor.get("functions", [])

    if not functions:
        # No functions: default to 'binds' main_to_primary (NOT bidirectional)
        interactor["arrow"] = "binds"
        interactor["arrows"] = {"main_to_primary": ["binds"]}
        interactor["direction"] = "main_to_primary"
        return interactor

    # Collect arrows by direction AND count function directions
    # NOTE: We now use interaction_effect instead of arrow for protein-level aggregation
    arrows_by_direction = {
        "main_to_primary": set(),
        "primary_to_main": set(),
        "bidirectional": set()
    }

    direction_counts = {
        "main_to_primary": 0,
        "primary_to_main": 0,
        "bidirectional": 0
    }

    for fn in functions:
        if not isinstance(fn, dict):
            continue

        # NEW: Use interaction_effect for protein-level aggregation
        # Fallback to arrow for backward compatibility with old data
        interaction_effect = fn.get("interaction_effect", fn.get("arrow", "complex"))

        # NEW: Use interaction_direction for per-function direction
        # Fallback to direction for backward compatibility with old data
        interaction_direction = fn.get("interaction_direction", fn.get("direction", "main_to_primary"))

        # Count this function's direction
        direction_counts[interaction_direction] += 1

        # Add interaction_effect to direction set (NO automatic cross-addition for bidirectional!)
        arrows_by_direction[interaction_direction].add(interaction_effect)

    # Convert sets to lists
    arrows = {
        k: sorted(list(v)) if v else []
        for k, v in arrows_by_direction.items()
    }

    # Remove empty directions
    arrows = {k: v for k, v in arrows.items() if v}

    # Determine summary arrow field (align with metadata generator semantics)
    all_arrows = set()
    for arrow_list in arrows.values():
        all_arrows.update(arrow_list)

    if len(all_arrows) == 0:
        arrow = "binds"  # Fallback
    elif len(all_arrows) == 1:
        arrow = list(all_arrows)[0]  # Single arrow type
    else:
        # Mixed effects -> use 'regulates' to match user-facing semantics
        arrow = "regulates"

    # Determine primary direction (FIXED LOGIC)
    total_functions = len(functions)
    bidirectional_count = direction_counts["bidirectional"]
    main_to_primary_count = direction_counts["main_to_primary"]
    primary_to_main_count = direction_counts["primary_to_main"]

    # Only mark as bidirectional if:
    # 1. Majority (>50%) of functions are explicitly bidirectional, OR
    # 2. At least 30% of functions are in EACH direction (main_to_primary AND primary_to_main)
    # Otherwise, use the dominant direction
    if bidirectional_count > total_functions / 2:
        # Majority explicitly bidirectional
        direction = "bidirectional"
    elif (main_to_primary_count >= total_functions * 0.3 and
          primary_to_main_count >= total_functions * 0.3):
        # Significant functions in BOTH directions
        direction = "bidirectional"
    elif primary_to_main_count > main_to_primary_count:
        # More primary_to_main functions
        direction = "primary_to_main"
    else:
        # Default to main_to_primary (includes ties)
        direction = "main_to_primary"

    # Update interactor
    interactor["arrows"] = arrows
    interactor["arrow"] = arrow
    interactor["direction"] = direction

    return interactor
