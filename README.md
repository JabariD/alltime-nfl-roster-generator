# Legends - NFL Player Rating System

A data-driven system for generating comprehensive, cross-era NFL player ratings with immutable snapshot architecture and game-specific adapters.

## Quick Start

### Test Run (Quick Validation)
```bash
# Run pipeline with recent data (100 players, 2022-2024)
# Creates timestamped output in data/test/
uv run python pipeline/ingest/players_index.py --test --verbose
```

### Production Build (Full Dataset)
```bash
# Build complete player index (all players, 1970-2024)
# Overwrites data/raw/ with latest production data
uv run python pipeline/ingest/players_index.py --full --verbose
```

### Connection Test Only
```bash
# Verify nflverse data connection without building
uv run python pipeline/ingest/players_index.py --connection-test
```

## Project Structure

```
legends/
├── pipeline/              # Data processing modules
│   ├── ingest/           # Player index building (ETL)
│   ├── ratings/          # Rating calculations (future)
│   ├── ai/               # AI-powered rating inference (future)
│   └── export/           # Adapter-based exports (future)
│
├── frcs/                  # Football Ratings Canonical Schema
│   ├── models.py         # Pydantic models
│   ├── schema_v1.json    # JSON schema
│   └── validation.py     # Data validation
│
├── adapters/              # Game-specific declarative adapters
│   ├── madden_26.yaml    # Madden 26 field mappings
│   └── utils.py          # Adapter utilities
│
├── data/                  # Data storage (see DATA_STRUCTURE.md)
│   ├── raw/              # Production player index (source of truth)
│   ├── test/             # Test runs (timestamped, gitignored)
│   ├── snapshots/        # Immutable dated snapshots
│   └── exports/          # Adapter outputs (gitignored)
│
├── docs/                  # Documentation
│   ├── design.md         # System architecture
│   ├── milestones.md     # Project roadmap
│   └── research/         # Temporary research (gitignored)
│
├── tests/                 # Pytest test suite
└── scripts/               # Utility scripts
```

See [DATA_STRUCTURE.md](DATA_STRUCTURE.md) for detailed data organization.

## Data Pipeline

### Phase 1: Ingest (Current)
Builds comprehensive player index from nflverse sources:
- Player biographical data (24K+ players)
- Career statistics (1970-2024)
- Playoff stats
- Draft and honors data
- NFL Combine measurements
- **Next Gen Stats (2016+)** - Advanced tracking metrics

**Output:** `data/raw/players_index.csv` (60+ columns including NGS metrics)

### Phase 2: Ratings (In Progress)
Position-specific rating calculations using tiered data sources:
- TIER_1: Direct measurements (combine, NGS tracking)
- TIER_2: Advanced metrics (NGS stats, EPA)
- TIER_3: Basic stats (yards, TDs, completions)
- TIER_4: Honors proxy (Pro Bowls, All-Pro)

### Phase 3: AI Enhancement (Planned)
ML-powered rating inference for players with sparse data.

### Phase 4: Export (Planned)
Adapter-based exports to game formats (Madden, NCAA, etc.).

## Key Features

### Next Gen Stats Integration (2016+)
Advanced tracking metrics for modern players:
- **QB:** Completion above expectation, time to throw, aggressiveness
- **RB:** Yards over expected, rush efficiency, time to LOS
- **WR/TE:** Average separation, YAC above expected, catch percentage

Coverage: ~32% of players (2016+ only)

### Era-Aware Evaluation
Players assigned to historical eras for fair cross-era comparison:
- Dead Ball (1920-1945)
- Post-WWII (1946-1977)
- Passing Revolution (1978-1993)
- Salary Cap (1994-2003)
- Modern Pass-Happy (2004-2015)
- Analytics Era (2016-present)

### Immutable Snapshots
All outputs are versioned with manifests tracking:
- Data sources and versions
- Configuration hashes
- Player counts and coverage statistics
- Timestamp and build metadata

## Development

### Setup
```bash
# Install dependencies
uv sync

# Run linting
uv run ruff check .
uv run mypy .

# Run tests
uv run pytest
```

### Code Quality
- Python 3.11+ with full type annotations
- Follows PEP 8 (enforced by ruff + black)
- Vectorized pandas operations (no row iteration)
- Comprehensive error handling and logging

### Testing Strategy
```bash
# Quick validation (recent data only)
uv run python pipeline/ingest/players_index.py --test

# Full integration test
uv run python pipeline/ingest/players_index.py --full --out data/test/integration

# Unit tests
uv run pytest tests/
```

## Maintenance

### Cleanup Test Runs
```bash
# Keep last 3 test runs, delete older
./scripts/cleanup_test_runs.sh 3

# Remove all test runs
rm -rf data/test/*
```

### Cleanup Research Artifacts
```bash
# Remove temporary research files
rm -rf docs/research/*
# (Keeps README.md via .gitignore)
```

## Architecture Principles

From [CLAUDE.md](CLAUDE.md):

1. **Single change per task** - Narrow, testable changes
2. **Determinism first** - Pin versions, fixed seeds, no silent config changes
3. **Core schema = FRCS** - Semantic truth, not game-specific
4. **Adapters are declarative** - YAML configs, no business logic
5. **Snapshots are immutable** - Read-only after creation
6. **Exports are throwaway** - Regenerable from snapshots

## Documentation

- [Design Document](docs/design.md) - Architecture and schema details
- [Milestones](docs/milestones.md) - Project roadmap and progress
- [Data Structure](DATA_STRUCTURE.md) - Directory organization
- [CLAUDE.md](CLAUDE.md) - Development guidelines for LLM-assisted work

## License

MIT License - See LICENSE file for details.
