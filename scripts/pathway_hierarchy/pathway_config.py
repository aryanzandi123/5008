#!/usr/bin/env python3
"""
Pathway Hierarchy Configuration
================================
Central config for ROOT_CATEGORIES and SUB_CATEGORIES.
Edit this file to add new pathways - all scripts will pick up changes.

This is the SINGLE SOURCE OF TRUTH for pathway categories.
Do NOT define ROOT_CATEGORIES or SUB_CATEGORIES in other files.

To add a new root category:
    1. Add to ROOT_CATEGORIES list below
    2. Optionally add sub-categories in SUB_CATEGORIES dict
    3. Run hierarchy pipeline: python scripts/pathway_hierarchy/run_all.py

Future expansion goals:
    - Cover ALL known cellular pathways
    - Enable dynamic loading from JSON/database
    - Support user-defined custom categories
"""

# =============================================================================
# ROOT CATEGORIES (hierarchy_level = 0)
# =============================================================================
# These are the top-level pathway categories that appear in the visualization
# sidebar. Add new roots here as your pathway database grows.

ROOT_CATEGORIES = [
    {
        "name": "Cellular Signaling",
        "go_id": "GO:0007165",
        "description": "Signal transduction pathways that regulate cellular responses",
    },
    {
        "name": "Metabolism",
        "go_id": "GO:0008152",
        "description": "Metabolic processes including energy production and biosynthesis",
    },
    {
        "name": "Protein Quality Control",
        "go_id": "GO:0006457",
        "description": "Protein folding, degradation, and homeostasis mechanisms",
    },
    {
        "name": "Cell Death",
        "go_id": "GO:0008219",
        "description": "Programmed cell death pathways including apoptosis and autophagy-dependent cell death",
    },
    {
        "name": "Cell Cycle",
        "go_id": "GO:0007049",
        "description": "Cell division and proliferation control",
    },
    {
        "name": "DNA Damage Response",
        "go_id": "GO:0006974",
        "description": "DNA repair and genomic stability mechanisms",
    },
    {
        "name": "Vesicle Transport",
        "go_id": "GO:0016192",
        "description": "Intracellular vesicle trafficking and secretion",
    },
    {
        "name": "Immune Response",
        "go_id": "GO:0006955",
        "description": "Innate and adaptive immune system pathways",
    },
    {
        "name": "Neuronal Function",
        "go_id": "GO:0050877",
        "description": "Nervous system development and synaptic signaling",
    },
    {
        "name": "Cytoskeleton Organization",
        "go_id": "GO:0007015",
        "description": "Actin, microtubule, and intermediate filament dynamics",
    },
    # =========================================================================
    # === ADD NEW ROOT CATEGORIES BELOW ===
    # =========================================================================
    # Uncomment and modify these as needed when expanding the pathway database:
    #
    # {
    #     "name": "Gene Expression",
    #     "go_id": "GO:0010467",
    #     "description": "Transcription, RNA processing, and translation",
    # },
    # {
    #     "name": "RNA Processing",
    #     "go_id": "GO:0006396",
    #     "description": "RNA splicing, editing, modification, and decay",
    # },
    # {
    #     "name": "Chromatin Organization",
    #     "go_id": "GO:0006325",
    #     "description": "Epigenetic regulation and histone modification",
    # },
    # {
    #     "name": "Cell Adhesion",
    #     "go_id": "GO:0007155",
    #     "description": "Cell-cell and cell-matrix adhesion",
    # },
    # {
    #     "name": "Membrane Transport",
    #     "go_id": "GO:0055085",
    #     "description": "Ion channels, transporters, and membrane receptors",
    # },
    # {
    #     "name": "Redox Homeostasis",
    #     "go_id": "GO:0045454",
    #     "description": "Oxidative stress response and antioxidant systems",
    # },
    # {
    #     "name": "Lipid Signaling",
    #     "go_id": "GO:0046578",
    #     "description": "Phospholipid signaling and lipid-mediated regulation",
    # },
    # {
    #     "name": "Protein Localization",
    #     "go_id": "GO:0008104",
    #     "description": "Protein targeting and subcellular localization",
    # },
]

# =============================================================================
# CONVENIENCE SETS (auto-generated from ROOT_CATEGORIES)
# =============================================================================
# Use these for quick lookups and validation in other scripts

ROOT_CATEGORY_NAMES = {cat["name"] for cat in ROOT_CATEGORIES}


# =============================================================================
# SUB-CATEGORIES (organized by parent pathway name)
# =============================================================================
# Key = parent pathway name
# Value = list of child pathways with name, go_id, and optional description
#
# Hierarchy levels are computed automatically:
#   - Root categories: level 0
#   - Direct children: level 1
#   - Grandchildren: level 2
#   - etc.

SUB_CATEGORIES = {
    # -------------------------------------------------------------------------
    # Cellular Signaling (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Cellular Signaling": [
        {"name": "mTOR Signaling", "go_id": "GO:0031929"},
        {"name": "MAPK Signaling", "go_id": "GO:0000165"},
        {"name": "NF-kB Signaling", "go_id": "GO:0038061"},
        {"name": "Wnt Signaling", "go_id": "GO:0016055"},
        {"name": "Notch Signaling", "go_id": "GO:0007219"},
        {"name": "TGF-beta Signaling", "go_id": "GO:0007179"},
        {"name": "JAK-STAT Signaling", "go_id": "GO:0007259"},
        {"name": "Calcium Signaling", "go_id": "GO:0019722"},
        {"name": "cAMP Signaling", "go_id": "GO:0019933"},
        {"name": "Cell Growth Regulation", "go_id": "GO:0001558"},
        {"name": "Cell Migration", "go_id": "GO:0016477"},
    ],

    # -------------------------------------------------------------------------
    # Protein Quality Control (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Protein Quality Control": [
        {"name": "Autophagy", "go_id": "GO:0006914"},
        {"name": "Ubiquitin-Proteasome System", "go_id": "GO:0010415"},
        {"name": "ER-Associated Degradation", "go_id": "GO:0036503"},
        {"name": "Protein Folding", "go_id": "GO:0006457"},
        {"name": "Chaperone-Mediated Protein Folding", "go_id": "GO:0061077"},
        {"name": "Unfolded Protein Response", "go_id": "GO:0030968"},
        {"name": "Aggrephagy", "go_id": "GO:0035973"},
    ],

    # Autophagy (Level 1) -> Level 2 sub-categories
    "Autophagy": [
        {"name": "Macroautophagy", "go_id": "GO:0016236"},
        {"name": "Selective Autophagy", "go_id": "GO:0061912"},
        {"name": "Mitophagy", "go_id": "GO:0000423"},
        {"name": "ER-phagy", "go_id": "GO:0061709"},
        {"name": "Lipophagy", "go_id": "GO:0061724"},
        {"name": "Pexophagy", "go_id": "GO:0030242"},
    ],

    # -------------------------------------------------------------------------
    # Cell Death (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Cell Death": [
        {"name": "Apoptosis", "go_id": "GO:0006915"},
        {"name": "Necroptosis", "go_id": "GO:0070266"},
        {"name": "Pyroptosis", "go_id": "GO:0070269"},
        {"name": "Ferroptosis", "go_id": "GO:0097707"},
        {"name": "Autophagy-Dependent Cell Death", "go_id": "GO:0048102"},
    ],

    # -------------------------------------------------------------------------
    # Metabolism (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Metabolism": [
        {"name": "Glycolysis", "go_id": "GO:0006096"},
        {"name": "Oxidative Phosphorylation", "go_id": "GO:0006119"},
        {"name": "Lipid Metabolism", "go_id": "GO:0006629"},
        {"name": "Amino Acid Metabolism", "go_id": "GO:0006520"},
        {"name": "Mitochondrial Function", "go_id": "GO:0007005"},
    ],

    # -------------------------------------------------------------------------
    # Cell Cycle (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Cell Cycle": [
        {"name": "G1/S Transition", "go_id": "GO:0000082"},
        {"name": "G2/M Transition", "go_id": "GO:0000086"},
        {"name": "Mitosis", "go_id": "GO:0007067"},
        {"name": "DNA Replication", "go_id": "GO:0006260"},
        {"name": "Cell Cycle Checkpoint", "go_id": "GO:0000075"},
    ],

    # -------------------------------------------------------------------------
    # DNA Damage Response (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    # FIXED: Proper hierarchy - repair mechanisms are under DNA Repair, not parallel
    "DNA Damage Response": [
        {"name": "DNA Repair", "go_id": "GO:0006281"},  # Parent for all repair mechanisms
        {"name": "DNA Damage Checkpoint", "go_id": "GO:0000077"},  # Checkpoint signaling
    ],

    # DNA Repair (Level 1) -> Level 2 sub-categories
    "DNA Repair": [
        {"name": "Double-Strand Break Repair", "go_id": "GO:0006302"},
        {"name": "Nucleotide Excision Repair", "go_id": "GO:0006289"},
        {"name": "Base Excision Repair", "go_id": "GO:0006284"},
        {"name": "Mismatch Repair", "go_id": "GO:0006298"},
    ],

    # Double-Strand Break Repair (Level 2) -> Level 3 sub-categories
    "Double-Strand Break Repair": [
        {"name": "Homologous Recombination", "go_id": "GO:0035825"},
        {"name": "Non-Homologous End Joining", "go_id": "GO:0006303"},
    ],

    # -------------------------------------------------------------------------
    # Immune Response (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Immune Response": [
        {"name": "Innate Immunity", "go_id": "GO:0045087"},
        {"name": "Adaptive Immunity", "go_id": "GO:0002250"},
        {"name": "Inflammatory Response", "go_id": "GO:0006954"},
        {"name": "Antiviral Response", "go_id": "GO:0051607"},
        {"name": "Cytokine Signaling", "go_id": "GO:0019221"},
    ],

    # -------------------------------------------------------------------------
    # Vesicle Transport (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Vesicle Transport": [
        {"name": "Endocytosis", "go_id": "GO:0006897"},
        {"name": "Exocytosis", "go_id": "GO:0006887"},
        {"name": "ER-Golgi Transport", "go_id": "GO:0006888"},
        {"name": "Lysosomal Transport", "go_id": "GO:0007041"},
    ],

    # -------------------------------------------------------------------------
    # Neuronal Function (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    "Neuronal Function": [
        {"name": "Synaptic Transmission", "go_id": "GO:0007268"},
        {"name": "Axon Guidance", "go_id": "GO:0007411"},
        {"name": "Neuronal Development", "go_id": "GO:0048666"},
        {"name": "Neurotransmitter Release", "go_id": "GO:0007269"},
    ],

    # -------------------------------------------------------------------------
    # Cytoskeleton Organization (Level 0) -> Level 1 sub-categories
    # -------------------------------------------------------------------------
    # Note: No sub-categories defined yet. Add here when needed.
    # "Cytoskeleton Organization": [
    #     {"name": "Actin Cytoskeleton", "go_id": "GO:0030031"},
    #     {"name": "Microtubule Organization", "go_id": "GO:0000226"},
    #     {"name": "Intermediate Filament", "go_id": "GO:0045104"},
    # ],
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_all_pathway_names() -> set:
    """Get all pathway names (roots + sub-categories)."""
    names = ROOT_CATEGORY_NAMES.copy()
    for subcats in SUB_CATEGORIES.values():
        for sub in subcats:
            names.add(sub["name"])
    return names


def get_parent_for_pathway(pathway_name: str) -> str | None:
    """Get the parent pathway name for a given pathway."""
    for parent, children in SUB_CATEGORIES.items():
        for child in children:
            if child["name"] == pathway_name:
                return parent
    return None


def get_children_for_pathway(pathway_name: str) -> list:
    """Get child pathway names for a given pathway."""
    return [child["name"] for child in SUB_CATEGORIES.get(pathway_name, [])]


def is_root_category(pathway_name: str) -> bool:
    """Check if a pathway is a root category."""
    return pathway_name in ROOT_CATEGORY_NAMES


# =============================================================================
# VALIDATION (runs on import in debug mode)
# =============================================================================

def _validate_config():
    """Validate configuration integrity."""
    # Check for duplicate names
    all_names = []
    for cat in ROOT_CATEGORIES:
        all_names.append(cat["name"])
    for subcats in SUB_CATEGORIES.values():
        for sub in subcats:
            all_names.append(sub["name"])

    duplicates = [name for name in all_names if all_names.count(name) > 1]
    if duplicates:
        raise ValueError(f"Duplicate pathway names found: {set(duplicates)}")

    # Check that all SUB_CATEGORIES parents exist
    all_pathway_names = get_all_pathway_names()
    for parent_name in SUB_CATEGORIES.keys():
        if parent_name not in all_pathway_names:
            raise ValueError(
                f"SUB_CATEGORIES parent '{parent_name}' is not defined as a pathway"
            )


# Run validation on import (can be disabled in production)
try:
    _validate_config()
except ValueError as e:
    import warnings
    warnings.warn(f"Pathway config validation failed: {e}")
