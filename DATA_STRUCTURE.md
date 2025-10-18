# Data Directory Structure

Clean separation between test runs, production builds, and research artifacts.

## Directory Layout

```
data/
├── raw/                    # Source of truth - production builds only
│   ├── players_index.csv   # Latest production player index
│   ├── player_data_profiles.parquet
│   └── ingest_manifest.json
│
├── test/                   # Test runs (gitignored, ephemeral)
│   └── YYYYMMDD_HHMMSS/   # Timestamped test runs
│       ├── players_index.csv
│       ├── player_data_profiles.parquet
│       └── ingest_manifest.json
│
├── snapshots/              # Immutable dated snapshots (per CLAUDE.md)
│   └── YYYY-MM-DD/
│       ├── players.parquet
│       ├── ratings.parquet
│       └── manifest.json
│
└── exports/                # Throwaway adapter outputs (gitignored)
    └── madden_26/
        └── YYYY-MM-DD/
            └── roster.csv

docs/
├── design.md               # Core design documentation
├── milestones.md           # Project roadmap
└── research/               # Temporary research artifacts (gitignored)
    └── (ngs integration notes, examples, etc.)
```

## Usage Patterns

### Test Run (Quick Validation)
```bash
# Creates timestamped output in data/test/
uv run python pipeline/ingest/players_index.py --test
```

**Output:** `data/test/20251017_230322/` (100 players, recent data only)

### Production Build (Source of Truth)
```bash
# Overwrites data/raw/ with latest production index
uv run python pipeline/ingest/players_index.py --out data/raw --full
```

**Output:** `data/raw/` (all players, 1970-2024)

### Create Immutable Snapshot
```bash
# Creates dated snapshot from production index
uv run python scripts/run_snapshot.py \
  --raw data/raw \
  --out data/snapshots/$(date +%F)
```

**Output:** `data/snapshots/2025-10-17/` (immutable, archived)

## Gitignore Rules

```gitignore
# Test runs are ephemeral
data/test/

# Research artifacts are temporary
docs/research/

# Exports are regenerable
data/exports/

# Keep production raw data and snapshots
!data/raw/
!data/snapshots/
```

## Cleanup Commands

```bash
# Remove old test runs (keep last 3)
find data/test -maxdepth 1 -type d | sort -r | tail -n +4 | xargs rm -rf

# Remove all test runs
rm -rf data/test/*

# Remove research artifacts
rm -rf docs/research/*
```
