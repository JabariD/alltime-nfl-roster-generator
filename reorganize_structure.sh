#!/bin/bash
#
# Reorganization script for Madden Legends Roster Generator
# Reorganizes project structure to match design.md Section 12
#
# IMPORTANT: Run from project root: ./reorganize_structure.sh
# This script is idempotent - safe to run multiple times

set -euo pipefail

PROJECT_ROOT="/Users/paytondennis/Documents/Code/legends"
cd "$PROJECT_ROOT"

echo "=== Starting directory reorganization ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# ============================================================================
# STEP 1: Create new directory structure
# ============================================================================
echo "[STEP 1] Creating new directories..."

# frcs/ subdirectories
mkdir -p frcs/migrations

# adapters/ already exists

# data/ subdirectories
mkdir -p data/raw/nflverse_exports
mkdir -p data/staging
mkdir -p data/snapshots
mkdir -p data/exports/madden26
mkdir -p data/manual_curation/overrides

# pipeline/ subdirectories (complete restructure)
mkdir -p pipeline/01_ingest
mkdir -p pipeline/02_qualification
mkdir -p pipeline/03_attributes
mkdir -p pipeline/04_enrichment
mkdir -p pipeline/05_export
mkdir -p pipeline/validation

# notebooks/ (new)
mkdir -p notebooks

# scripts/ already exists

# config/ (new)
mkdir -p config

# tests/ already exists

echo "  ✓ Created all required directories"
echo ""

# ============================================================================
# STEP 2: Move existing files to new locations
# ============================================================================
echo "[STEP 2] Moving existing files..."

# Move legend_scores.py to 02_qualification/
if [ -f "pipeline/legend_scores.py" ]; then
    mv pipeline/legend_scores.py pipeline/02_qualification/legend_scores.py
    echo "  ✓ Moved pipeline/legend_scores.py → pipeline/02_qualification/legend_scores.py"
fi

# Move corresponding test file
if [ -f "tests/test_legend_scores.py" ]; then
    # Keep in tests/ but we'll note it for future refactoring
    echo "  ℹ tests/test_legend_scores.py remains in place (update imports later)"
fi

# frcs/models.py already in correct location
echo "  ✓ frcs/models.py already in correct location"

# adapters/madden_26.yaml already in correct location
echo "  ✓ adapters/madden_26.yaml already in correct location"

echo ""

# ============================================================================
# STEP 3: Create __init__.py files for all Python packages
# ============================================================================
echo "[STEP 3] Creating __init__.py files..."

# pipeline subdirectories
touch pipeline/01_ingest/__init__.py
touch pipeline/02_qualification/__init__.py
touch pipeline/03_attributes/__init__.py
touch pipeline/04_enrichment/__init__.py
touch pipeline/05_export/__init__.py
touch pipeline/validation/__init__.py

# frcs/migrations
touch frcs/migrations/__init__.py

echo "  ✓ Created __init__.py files for all Python packages"
echo ""

# ============================================================================
# STEP 4: Create .gitkeep files for empty data directories
# ============================================================================
echo "[STEP 4] Creating .gitkeep files for version control..."

touch data/raw/nflverse_exports/.gitkeep
touch data/staging/.gitkeep
touch data/snapshots/.gitkeep
touch data/exports/madden26/.gitkeep

echo "  ✓ Created .gitkeep files"
echo ""

# ============================================================================
# STEP 5: Summary
# ============================================================================
echo "=== Reorganization Complete ==="
echo ""
echo "Next steps:"
echo "  1. Run placeholder file generation scripts (see output files)"
echo "  2. Update imports in test files to match new structure"
echo "  3. Run 'uv run ruff .' to check for import errors"
echo "  4. Run 'uv run pytest' to verify tests still pass"
echo ""
echo "New structure created:"
echo "  - frcs/migrations/"
echo "  - data/staging/"
echo "  - data/snapshots/"
echo "  - data/raw/nflverse_exports/"
echo "  - pipeline/01_ingest/"
echo "  - pipeline/02_qualification/"
echo "  - pipeline/03_attributes/"
echo "  - pipeline/04_enrichment/"
echo "  - pipeline/05_export/"
echo "  - pipeline/validation/"
echo "  - notebooks/"
echo "  - config/"
echo ""
echo "Files moved:"
echo "  - pipeline/legend_scores.py → pipeline/02_qualification/legend_scores.py"
echo ""
