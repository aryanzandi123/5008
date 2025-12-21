#!/usr/bin/env python3
"""
Evidence Validator & Citation Enricher (Integrated Fact-Checker)
Post-processes pipeline JSON to validate biological accuracy, check mechanisms, and enrich with citations.
Uses Gemini 3.0 Pro Preview with Google Search for maximum rigor.
"""

from __future__ import annotations

import json
import os
import sys
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

# Fix Windows console encoding
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from google import genai
from google.genai import types
from dotenv import load_dotenv

# Constants
MAX_OUTPUT_TOKENS = 60192
MAX_THINKING_TOKENS = 32768  # Generous thinking budget for rigorous validation
# MODEL ID: Using Gemini 3.0 Flash Preview with thinking for maximum reasoning power
MODEL_ID = "gemini-3-flash-preview"

class EvidenceValidatorError(RuntimeError):
    """Raised when evidence validation fails."""
    pass


def load_json_file(json_path: Path) -> Dict[str, Any]:
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise EvidenceValidatorError(f"Failed to load JSON: {e}")


def save_json_file(data: Dict[str, Any], output_path: Path) -> None:
    try:
        output_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8"
        )
        print(f"[OK]Saved validated output to: {output_path}")
    except Exception as e:
        raise EvidenceValidatorError(f"Failed to save JSON: {e}")


def extract_json_from_response(text: str) -> Dict[str, Any]:
    """Extract JSON from model response, handling markdown fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].lstrip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        # Try fuzzy extraction
        start = cleaned.find('{')
        end = cleaned.rfind('}') + 1
        if start >= 0 and end > start:
            try:
                return json.loads(cleaned[start:end])
            except:
                pass
        raise EvidenceValidatorError(f"Failed to parse JSON: {e}")


def call_gemini_validation(
    prompt: str,
    api_key: str,
    verbose: bool = False
) -> str:
    """
    Call Gemini with Google Search for rigorous validation.
    """
    client = genai.Client(api_key=api_key)
    
    # Configuration: High reasoning with thinking + Search enabled
    config = types.GenerateContentConfig(
        tools=[types.Tool(google_search=types.GoogleSearch())],
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=0.3,  # Low temp for factual rigor
        thinking_config=types.ThinkingConfig(
            thinking_budget=MAX_THINKING_TOKENS,  # 32K tokens for deep reasoning
        ),
    )

    if verbose:
        print(f"\n--- Calling {MODEL_ID} for Validation ---")

    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=config
        )
        return response.text
    except Exception as e:
        print(f"[WARN] {MODEL_ID} failed ({e}), falling back to gemini-3-flash-preview")
        try:
            response = client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=prompt,
                config=config
            )
            return response.text
        except Exception as e2:
            raise EvidenceValidatorError(f"Validation failed: {e2}")


def create_validation_prompt(
    main_protein: str,
    interactors: List[Dict[str, Any]],
    batch_start: int,
    batch_end: int,
    total: int
) -> str:
    """
    Constructs a rigorous "Scientific Adversary" prompt.
    """
    
    items_str = json.dumps(interactors, indent=2)
    
    return f"""
You are a RIGOROUS SCIENTIFIC ADVERSARY and FACT-CHECKER.
Your task is to validate protein interaction claims between {main_protein} and a list of interactors.
You must use Google Search to verify every claim against primary literature.

**CORE OBJECTIVE:**
Detect and FIX "Mechanistic Opposites" and "Contextual Errors".
A common error is conflating **Transcriptional Repression** with **Protein Instability**, or **Activator** with **Repressor**.

**CRITICAL FAILURE EXAMPLES (DO NOT COMMIT THESE):**
1. **The "ATXN3-PTEN" Fallacy:**
   - *Input Claim:* ATXN3 deubiquitinates and STABILIZES PTEN protein (Activates).
   - *Reality:* ATXN3 transcriptionally REPRESSES the PTEN gene (Inhibits).
   - *Verdict:* WRONG MECHANISM. The effect is INHIBITORY (lowers PTEN levels), not ACTIVATING.
   
2. **The "Transcriptional vs Post-Translational" Confusion:**
   - *Input:* Protein A degrades Protein B.
   - *Reality:* Protein A represses Protein B's mRNA.
   - *Verdict:* The OUTCOME (lower Protein B) is the same, but the MECHANISM is different. You must be precise.

**INSTRUCTIONS:**

1. **INDEPENDENT RESEARCH:** For each interactor, search for the interaction mechanism *from scratch*. Do not blindly trust the input.
   - Search queries like: "{main_protein} {interactors[0]['primary']} interaction mechanism", "{main_protein} regulates {interactors[0]['primary']} transcription or stability".

2. **BIOLOGICAL CASCADE (MUST BE DETAILED):**
   - **REQUIREMENT:** Create detailed, multi-step molecular pathways.
   - **FORMAT:** "Event A (upstream) → Molecular Intermediate B → Downstream Effector C → Cellular Consequence D".
   - **DETAIL:** Include specific phosphorylation sites (e.g. Ser473), domains (e.g. SH2), co-factors, and cellular locations (e.g. Nuclear translocation).
   - **EXAMPLE:** "ATXN3 binds VCP → Deubiquitinates K48-linked chains on substrates → Prevents proteasomal degradation → Stabilizes protein X → Induces Autophagy."
   - **BAN:** Do NOT use vague single-step descriptions like "ATXN3 regulates VCP".

3. **SPECIFIC EFFECTS (MOLECULAR PRECISION):**
   - **REQUIREMENT:** Describe the EXACT molecular change.
   - **DETAIL:** Use precise terms: "Increases binding affinity by 2-fold", "Promotes nuclear translocation", "Inhibits enzymatic activity at site X", "Stabilizes protein half-life".
   - **AVOID:** Generic terms like "Regulates", "Affects", "Modulates", "Controls" without specific qualification.

4. **EVIDENCE & PUBLICATIONS (VERBATIM PROOF):**
   - **REQUIREMENT:** Evidence must be IRREFUTABLE and VERIFIABLE.
   - **FIELDS:** You MUST provide the **EXACT paper title**, **Journal**, **Year**.
   - **QUOTE:** You MUST include a **VERBATIM QUOTE** from the paper's abstract or results that proves the specific mechanism.
   - **RULE:** If you cannot find a specific paper supporting the mechanism, mark the claim as INVALID or CORRECT it to what the literature actually says.

**INPUT DATA (Batch {batch_start+1}-{batch_end} of {total}):**
{items_str}

**OUTPUT SCHEMA (JSON):**
{{
  "interactors": [
    {{
      "primary": "ProteinSymbol",
      "is_valid": true, // Set false if NO interaction exists
      "mechanism_correction": "Corrected detailed mechanism...", // Explain the REAL mechanism if input was wrong
      "functions": [
        {{
            "function": "Specific Function Name", // Corrected if necessary
            "arrow": "activates" | "inhibits" | "binds" | "regulates", // CRITICAL: Verify direction!
            "cellular_process": "Detailed biological explanation...",
            "effect_description": "Outcome of the interaction...",
            "biological_consequence": [ "Step 1 -> Step 2 -> Step 3 (Detailed Pathway)" ],
            "specific_effects": [ "Precise molecular effect 1", "Precise molecular effect 2" ],
            "evidence": [
                {{
                    "paper_title": "EXACT Title from PubMed",
                    "journal": "Journal Name",
                    "year": 2024,
                    "relevant_quote": "Verbatim quote supporting the mechanism."
                }}
            ]
        }}
      ]
    }}
  ]
}}
"""


def _process_single_batch(
    batch_info: Tuple[int, List[Dict[str, Any]]],
    main_protein: str,
    total_interactors: int,
    api_key: str,
    verbose: bool,
    print_lock: Lock
) -> Tuple[int, List[Dict[str, Any]], Optional[str]]:
    """
    Process a single batch of interactors. Thread-safe.
    Returns: (batch_index, validated_interactors, error_message or None)
    """
    batch_idx, batch = batch_info
    batch_start = batch_idx
    batch_end = batch_idx + len(batch)

    with print_lock:
        print(f"\n[Batch {batch_idx // 3 + 1}] Validating {len(batch)} interactors ({batch[0]['primary']}...)...")

    try:
        prompt = create_validation_prompt(main_protein, batch, batch_start, batch_end, total_interactors)
        response_text = call_gemini_validation(prompt, api_key, verbose)
        result = extract_json_from_response(response_text)

        validated = []
        if 'interactors' in result:
            for val_int in result['interactors']:
                orig = next((x for x in batch if x['primary'] == val_int['primary']), None)
                if orig:
                    if not val_int.get('is_valid', True):
                        with print_lock:
                            print(f"  [Batch {batch_idx // 3 + 1}] {val_int['primary']} flagged as INVALID.")
                        orig['_validation_status'] = 'rejected'
                        orig['mechanism'] = "EVIDENCE REJECTED: " + val_int.get('mechanism_correction', 'No interaction found')
                        validated.append(orig)
                    else:
                        with print_lock:
                            print(f"  [Batch {batch_idx // 3 + 1}] {val_int['primary']} validated.")
                        orig.update(val_int)
                        validated.append(orig)
        else:
            with print_lock:
                print(f"  [Batch {batch_idx // 3 + 1}] No 'interactors' in response, keeping originals.")
            validated = list(batch)

        return (batch_idx, validated, None)

    except Exception as e:
        with print_lock:
            print(f"  [Batch {batch_idx // 3 + 1}] Failed: {e}. Keeping originals.")
        return (batch_idx, list(batch), str(e))


def validate_and_enrich_evidence(
    json_data: Dict[str, Any],
    api_key: str,
    verbose: bool = False,
    batch_size: int = 3,
    step_logger = None,
    max_workers: int = 3  # Conservative parallelization
) -> Dict[str, Any]:
    """
    Main validation function with PARALLEL batch processing.

    Uses ThreadPoolExecutor to process multiple batches concurrently,
    significantly reducing wall-clock time for large interactor sets.

    Args:
        json_data: Pipeline JSON with ctx_json containing interactors
        api_key: Gemini API key
        verbose: Enable verbose logging
        batch_size: Interactors per batch (default 3)
        step_logger: Optional logger
        max_workers: Max concurrent API calls (default 3, conservative)

    Returns:
        Updated json_data with validated interactors
    """
    if 'ctx_json' not in json_data:
        print("[WARN] No ctx_json found, skipping validation.")
        return json_data

    main_protein = json_data['ctx_json'].get('main', 'Unknown')
    interactors = json_data['ctx_json'].get('interactors', [])
    total_interactors = len(interactors)

    if total_interactors == 0:
        print("[WARN] No interactors to validate.")
        return json_data

    # Calculate batch count
    num_batches = (total_interactors + batch_size - 1) // batch_size

    print(f"\n{'='*60}")
    print(f"RIGOROUS EVIDENCE VALIDATION FOR: {main_protein}")
    print(f"   Model: {MODEL_ID} (Scientific Adversary Mode)")
    print(f"   Total interactors: {total_interactors}")
    print(f"   Batches: {num_batches} (size={batch_size})")
    print(f"   Parallel workers: {max_workers}")
    print(f"{'='*60}")

    # Prepare batches: (start_index, batch_list)
    batches: List[Tuple[int, List[Dict[str, Any]]]] = []
    for i in range(0, total_interactors, batch_size):
        batches.append((i, interactors[i : i + batch_size]))

    # Results storage - preserves original order
    results: List[Tuple[int, List[Dict[str, Any]]]] = [None] * len(batches)
    errors: List[str] = []
    print_lock = Lock()

    start_time = time.time()

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch jobs
        future_to_batch = {
            executor.submit(
                _process_single_batch,
                batch_info,
                main_protein,
                total_interactors,
                api_key,
                verbose,
                print_lock
            ): batch_info[0]
            for batch_info in batches
        }

        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_batch):
            batch_start_idx = future_to_batch[future]
            batch_list_idx = batch_start_idx // batch_size

            try:
                batch_idx, validated, error = future.result()
                results[batch_list_idx] = (batch_idx, validated)
                if error:
                    errors.append(f"Batch {batch_list_idx + 1}: {error}")
            except Exception as e:
                # Fallback: keep original batch
                orig_batch = batches[batch_list_idx][1]
                results[batch_list_idx] = (batch_start_idx, list(orig_batch))
                errors.append(f"Batch {batch_list_idx + 1}: {e}")

            completed += 1
            with print_lock:
                print(f"   Progress: {completed}/{len(batches)} batches complete")

    elapsed = time.time() - start_time

    # Flatten results in original order
    validated_interactors = []
    for batch_idx, validated_batch in results:
        validated_interactors.extend(validated_batch)

    print(f"\n{'='*60}")
    print(f"VALIDATION COMPLETE")
    print(f"   Validated: {len(validated_interactors)} interactors")
    print(f"   Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    if errors:
        print(f"   Errors: {len(errors)} batches had issues (kept originals)")
    print(f"{'='*60}")

    # Update payload
    json_data['ctx_json']['interactors'] = validated_interactors

    # Also update snapshot if present
    if 'snapshot_json' in json_data:
        json_data['snapshot_json']['interactors'] = validated_interactors

    return json_data


if __name__ == "__main__":
    # CLI testing
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json")
    parser.add_argument("--output", default="validated_output.json")
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_API_KEY"))
    args = parser.parse_args()
    
    if not args.api_key:
        sys.exit("GOOGLE_API_KEY required.")
        
    data = load_json_file(Path(args.input_json))
    validated = validate_and_enrich_evidence(data, args.api_key, verbose=True)
    save_json_file(validated, Path(args.output))