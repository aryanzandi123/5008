#!/usr/bin/env python3
"""
Migration: Pathway Pipeline V2 Schema Updates

This migration:
1. Adds new columns to pathways table:
   - pathway_type (VARCHAR(20) DEFAULT 'main')
   - hierarchy_chain (JSONB)

2. Adds new column to pathway_parents table:
   - is_primary_chain (BOOLEAN DEFAULT TRUE)

3. Creates 3 new tables:
   - pathway_initial_assignments (Stage 1 temp storage)
   - pathway_canonical_names (Stage 2 name mapping)
   - pathway_hierarchy_history (Stage 4-6 history cache)

4. Seeds 2 new root categories:
   - Transcriptional Regulation (GO:0006355)
   - Chromatin Organization (GO:0006325)

Run: python scripts/migrate_pathway_v2.py
"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app import app, db
from sqlalchemy import text


# New root categories to add (for comprehensive coverage)
NEW_ROOT_CATEGORIES = [
    {
        "name": "Transcriptional Regulation",
        "go_id": "GO:0006355",
        "description": "Regulation of gene expression at the transcriptional level",
    },
    {
        "name": "Chromatin Organization",
        "go_id": "GO:0006325",
        "description": "Chromatin structure, histone modifications, and epigenetic regulation",
    },
]


def add_column_if_not_exists(session, table: str, column: str, column_def: str):
    """Add a column to a table if it doesn't already exist."""
    check_sql = text(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{table}' AND column_name = '{column}'
    """)
    result = session.execute(check_sql).fetchone()

    if result is None:
        alter_sql = text(f"ALTER TABLE {table} ADD COLUMN {column} {column_def}")
        session.execute(alter_sql)
        print(f"   ✓ Added column {column} to {table}")
        return True
    else:
        print(f"   ✓ Column {column} already exists in {table}")
        return False


def create_table_if_not_exists(session, table_name: str, create_sql: str):
    """Create a table if it doesn't already exist."""
    check_sql = text(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_name = '{table_name}'
    """)
    result = session.execute(check_sql).fetchone()

    if result is None:
        session.execute(text(create_sql))
        print(f"   ✓ Created table {table_name}")
        return True
    else:
        print(f"   ✓ Table {table_name} already exists")
        return False


def seed_new_root_categories(session):
    """Seed new root categories into database."""
    from models import Pathway

    print("   Checking for new root categories to seed...")
    for root in NEW_ROOT_CATEGORIES:
        existing = session.query(Pathway).filter_by(name=root["name"]).first()
        if existing:
            print(f"   ✓ Root '{root['name']}' already exists")
            continue

        # Create new root pathway
        new_root = Pathway(
            name=root["name"],
            ontology_id=root["go_id"],
            ontology_source="GO",
            description=root["description"],
            hierarchy_level=0,
            is_leaf=True,  # Will be updated when children are added
            ai_generated=False,
            pathway_type='main',
        )
        session.add(new_root)
        print(f"   ✓ Seeded new root: {root['name']}")

    session.commit()


def run_migration():
    """Run the V2 pathway pipeline migration."""
    print("\n" + "=" * 60)
    print("PATHWAY PIPELINE V2 MIGRATION")
    print("=" * 60)

    with app.app_context():
        session = db.session

        # =====================================================================
        # STEP 1: Add new columns to existing tables
        # =====================================================================
        print("\n[Step 1] Adding new columns to existing tables...")

        # Add pathway_type to pathways
        add_column_if_not_exists(
            session, 'pathways', 'pathway_type',
            "VARCHAR(20) NOT NULL DEFAULT 'main'"
        )

        # Add hierarchy_chain to pathways
        add_column_if_not_exists(
            session, 'pathways', 'hierarchy_chain',
            "JSONB"
        )

        # Add is_primary_chain to pathway_parents
        add_column_if_not_exists(
            session, 'pathway_parents', 'is_primary_chain',
            "BOOLEAN NOT NULL DEFAULT TRUE"
        )

        session.commit()

        # =====================================================================
        # STEP 2: Create new tables
        # =====================================================================
        print("\n[Step 2] Creating new tables...")

        # Create pathway_initial_assignments table
        create_table_if_not_exists(session, 'pathway_initial_assignments', """
            CREATE TABLE pathway_initial_assignments (
                id SERIAL PRIMARY KEY,
                interaction_id INTEGER REFERENCES interactions(id) ON DELETE CASCADE UNIQUE NOT NULL,
                initial_name VARCHAR(200) NOT NULL,
                canonical_name VARCHAR(200),
                confidence NUMERIC(3, 2) DEFAULT 0.80,
                ai_reasoning TEXT,
                created_at TIMESTAMP DEFAULT NOW() NOT NULL
            )
        """)

        # Create indexes for pathway_initial_assignments
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pia_interaction
            ON pathway_initial_assignments(interaction_id)
        """))
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pia_initial_name
            ON pathway_initial_assignments(initial_name)
        """))
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pia_canonical_name
            ON pathway_initial_assignments(canonical_name)
        """))

        # Create pathway_canonical_names table
        create_table_if_not_exists(session, 'pathway_canonical_names', """
            CREATE TABLE pathway_canonical_names (
                id SERIAL PRIMARY KEY,
                initial_name VARCHAR(200) UNIQUE NOT NULL,
                canonical_name VARCHAR(200) NOT NULL,
                similarity_score NUMERIC(3, 2),
                match_method VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW() NOT NULL
            )
        """)

        # Create indexes for pathway_canonical_names
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pcn_initial_name
            ON pathway_canonical_names(initial_name)
        """))
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_pcn_canonical_name
            ON pathway_canonical_names(canonical_name)
        """))

        # Create pathway_hierarchy_history table
        create_table_if_not_exists(session, 'pathway_hierarchy_history', """
            CREATE TABLE pathway_hierarchy_history (
                id SERIAL PRIMARY KEY,
                canonical_name VARCHAR(200) UNIQUE NOT NULL,
                hierarchy_chain JSONB NOT NULL,
                chain_length INTEGER NOT NULL,
                source VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW() NOT NULL,
                last_used TIMESTAMP DEFAULT NOW() NOT NULL
            )
        """)

        # Create index for pathway_hierarchy_history
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_phh_canonical_name
            ON pathway_hierarchy_history(canonical_name)
        """))

        session.commit()

        # =====================================================================
        # STEP 3: Seed new root categories
        # =====================================================================
        print("\n[Step 3] Seeding new root categories...")
        seed_new_root_categories(session)

        # =====================================================================
        # STEP 4: Verify migration
        # =====================================================================
        print("\n[Step 4] Verifying migration...")

        # Count root categories
        from models import Pathway
        root_count = session.query(Pathway).filter_by(hierarchy_level=0).count()
        print(f"   ✓ Total root categories: {root_count}")

        # List all root categories
        roots = session.query(Pathway).filter_by(hierarchy_level=0).order_by(Pathway.name).all()
        print("   Root categories:")
        for root in roots:
            print(f"      - {root.name} ({root.ontology_id})")

        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Create scripts/pathway_pipeline_v2/ directory")
        print("2. Implement Stage 1-7 scripts")
        print("3. Update runner.py to integrate Stage 1")
        print("4. Test with: python scripts/pathway_pipeline_v2/run_batch.py")


if __name__ == '__main__':
    run_migration()
