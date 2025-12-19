#!/usr/bin/env python3
"""
AI Client for Pathway Pipeline V2

Provides a unified interface for all AI calls in the pipeline with:
- Sequential call enforcement (next call waits for previous)
- Memory management (store outputs for subsequent calls)
- Retry logic with exponential backoff
- Strict JSON parsing

All calls use gemini-3-flash-preview model.
"""

import os
import sys
import json
import time
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.llm_response_parser import extract_json_from_llm_response
from scripts.pathway_pipeline_v2.config import (
    AI_MODEL,
    AI_TEMPERATURE,
    AI_TOP_P,
    AI_MAX_OUTPUT_TOKENS,
    AI_MAX_RETRIES,
    AI_RETRY_DELAY_BASE,
)

logger = logging.getLogger(__name__)


@dataclass
class AICallResult:
    """Result from an AI call."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    raw_text: Optional[str] = None
    error: Optional[str] = None
    attempt_count: int = 0
    duration_ms: int = 0


@dataclass
class PipelineMemory:
    """
    Memory structure for storing outputs across pipeline stages.

    This ensures sequential calls have access to previous outputs.
    """
    # Stage 1: Initial pathway assignments
    initial_assignments: Dict[int, Dict] = field(default_factory=dict)
    # interaction_id -> {"initial_name": str, "confidence": float, "reasoning": str}

    # Stage 2: Canonical name mappings
    canonical_mappings: Dict[str, str] = field(default_factory=dict)
    # initial_name -> canonical_name

    # Stage 3: Final assignments
    final_assignments: Dict[int, str] = field(default_factory=dict)
    # interaction_id -> canonical_pathway_name

    # Stage 4-6: Hierarchy chains
    hierarchy_chains: Dict[str, List[str]] = field(default_factory=dict)
    # canonical_name -> ["Root", "Level1", "Level2", "This"]

    # Stage 5: Siblings per level
    siblings: Dict[str, Dict[int, List[str]]] = field(default_factory=dict)
    # canonical_name -> {level: [sibling_names]}

    # All unique pathway names discovered
    all_pathways: set = field(default_factory=set)

    # Timestamp tracking
    last_updated: Optional[datetime] = None

    def update(self):
        """Update the last_updated timestamp."""
        self.last_updated = datetime.utcnow()


class AIClient:
    """
    Singleton AI client for the pathway pipeline.

    Ensures:
    - Sequential calls (enforced via lock)
    - Memory persistence between calls
    - Consistent error handling
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._api_key = None
        self._client = None
        self._memory = PipelineMemory()
        self._call_lock = threading.Lock()  # Ensures sequential calls
        self._call_count = 0
        self._initialized = True

    def _get_api_key(self) -> str:
        """Get Google API key from environment."""
        if self._api_key:
            return self._api_key

        api_key = os.environ.get('GOOGLE_API_KEY')
        if not api_key:
            from dotenv import load_dotenv
            load_dotenv(PROJECT_ROOT / '.env')
            api_key = os.environ.get('GOOGLE_API_KEY')

        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY not found in environment")

        self._api_key = api_key
        return api_key

    def _get_client(self):
        """Get or create the Gemini client."""
        if self._client is None:
            from google import genai as google_genai
            self._client = google_genai.Client(api_key=self._get_api_key())
        return self._client

    @property
    def memory(self) -> PipelineMemory:
        """Access the pipeline memory."""
        return self._memory

    def reset_memory(self):
        """Reset the pipeline memory (for new pipeline run)."""
        self._memory = PipelineMemory()
        logger.info("Pipeline memory reset")

    def call_sequential(
        self,
        prompt: str,
        stage: str,
        temperature: float = None,
        max_output_tokens: int = None,
        use_search: bool = False,
    ) -> AICallResult:
        """
        Make a sequential AI call (blocks until previous call completes).

        Args:
            prompt: The prompt to send
            stage: Stage identifier for logging (e.g., "stage1", "stage4")
            temperature: Override default temperature
            max_output_tokens: Override default max tokens
            use_search: Enable web search (for hierarchy building)

        Returns:
            AICallResult with success status and data/error
        """
        from google.genai import types

        # Acquire lock to ensure sequential execution
        with self._call_lock:
            self._call_count += 1
            call_num = self._call_count
            start_time = time.time()

            logger.info(f"[{stage}] AI call #{call_num} starting...")

            client = self._get_client()

            # Build config
            tools = []
            if use_search:
                # Enable Google Search for hierarchy building
                tools = [types.Tool(google_search=types.GoogleSearch())]

            config = types.GenerateContentConfig(
                max_output_tokens=max_output_tokens or AI_MAX_OUTPUT_TOKENS,
                temperature=temperature or AI_TEMPERATURE,
                top_p=AI_TOP_P,
                tools=tools,
                thinking_config=types.ThinkingConfig(),  # Auto thinking - model decides budget
            )

            last_error = None
            for attempt in range(1, AI_MAX_RETRIES + 1):
                try:
                    resp = client.models.generate_content(
                        model=AI_MODEL,
                        contents=prompt,
                        config=config,
                    )

                    # Extract text from response
                    raw_text = None
                    if hasattr(resp, "text") and resp.text:
                        raw_text = resp.text
                    elif hasattr(resp, "candidates") and resp.candidates:
                        parts = resp.candidates[0].content.parts
                        raw_text = "".join(p.text for p in parts if hasattr(p, "text"))

                    if not raw_text:
                        raise RuntimeError("Empty model response")

                    # Parse JSON from response
                    data = extract_json_from_llm_response(raw_text)

                    duration_ms = int((time.time() - start_time) * 1000)
                    logger.info(f"[{stage}] AI call #{call_num} succeeded ({duration_ms}ms)")

                    # Update memory timestamp
                    self._memory.update()

                    return AICallResult(
                        success=True,
                        data=data,
                        raw_text=raw_text,
                        attempt_count=attempt,
                        duration_ms=duration_ms,
                    )

                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"[{stage}] Attempt {attempt} failed: {e}")
                    if attempt < AI_MAX_RETRIES:
                        delay = AI_RETRY_DELAY_BASE * attempt
                        time.sleep(delay)

            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"[{stage}] AI call #{call_num} failed after {AI_MAX_RETRIES} attempts")

            return AICallResult(
                success=False,
                error=last_error,
                attempt_count=AI_MAX_RETRIES,
                duration_ms=duration_ms,
            )


# Global client instance
_client: Optional[AIClient] = None


def get_ai_client() -> AIClient:
    """Get the global AI client instance."""
    global _client
    if _client is None:
        _client = AIClient()
    return _client


def call_ai_sequential(
    prompt: str,
    stage: str,
    temperature: float = None,
    max_output_tokens: int = None,
    use_search: bool = False,
) -> AICallResult:
    """
    Convenience function for making sequential AI calls.

    Args:
        prompt: The prompt to send
        stage: Stage identifier for logging
        temperature: Override default temperature
        max_output_tokens: Override default max tokens
        use_search: Enable web search

    Returns:
        AICallResult with success status and data/error
    """
    return get_ai_client().call_sequential(
        prompt=prompt,
        stage=stage,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        use_search=use_search,
    )


def get_pipeline_memory() -> PipelineMemory:
    """Get the pipeline memory from the global client."""
    return get_ai_client().memory


def reset_pipeline_memory():
    """Reset the pipeline memory."""
    get_ai_client().reset_memory()
