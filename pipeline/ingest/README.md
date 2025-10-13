# Pipeline Ingest Module

**Purpose**: Build comprehensive player index and data availability profiles from nflverse data sources.

**Outputs**:
- `data/raw/players_index.csv` - Master player pool (~27K NFL players with career stats, honors, physicals)
- `data/raw/player_data_profiles.parquet` - Data availability metadata (era, tiers, richness scores)
- `data/raw/ingest_manifest.json` - Run provenance (config hashes, player counts, git commit)

---

## Quick Start

```bash
# Test nflverse connection (no data download)
uv run python -m pipeline.ingest.players_index --test-only -v

# Build sample dataset (~100 players, fast for testing)
uv run python -m pipeline.ingest.players_index --out data/raw -v

# Full historical build (1970-2024, ~15 min)
uv run python -m pipeline.ingest.players_index --out data/raw --full -v
```

---

## Architecture

### Data Flow
```
NFLVerseLoader → Aggregators → Merge → Era Bucketer → Data Profiler → Save
```

### Module Responsibilities

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| `types.py` | Type definitions, enums | `PlayerRow`, `SourceTier`, `EraBucket` |
| `nflverse_loader.py` | Data source abstraction | `load_players()`, `load_seasonal_data()`, `load_combine()` |
| `aggregators.py` | Pure aggregation functions | `aggregate_career_stats()`, `aggregate_playoff_stats()` |
| `era_bucketer.py` | Era assignment + metadata | `assign_era_bucket()`, `get_era_bonus()` |
| `data_profiler.py` | Data availability tracking | `generate_player_data_profiles()` |
| `players_index.py` | CLI orchestrator | `build_players_index()` (main entry point) |

---

## Player Index Schema

**36 columns** including:

**Identity**: `player_id`, `full_name`, `primary_pos`, `secondary_pos`
**Career Span**: `first_year`, `last_year`, `career_seasons`, `teams`, `games_played`
**Offense**: `pass_yards`, `pass_tds`, `rush_yards`, `rush_tds`, `rec_yards`, `rec_tds`
**Defense**: `tackles`, `sacks`, `interceptions`, `forced_fumbles`, `def_tds`
**Playoffs**: `playoff_games`, `playoff_pass_yards`, `playoff_rush_yards`, `playoff_rec_yards`
**Honors**: `pro_bowls`, `all_pros`, `hof`, `mvp`, `dpoy`, `opoy`, `sb_mvp`
**Physicals**: `height_in`, `weight_lb`, `forty_time`, `bench_press`, `vertical`, `broad_jump`
**Draft**: `draft_year`, `draft_round`, `draft_pick`, `college`

---

## PlayerDataProfile Schema

**FRCS Section 4.3 metadata** for era-aware attribute mapping:

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | str | GSIS ID (foreign key) |
| `era_bucket` | str | One of 6 eras (e.g., "Analytics Era") |
| `available_tiers` | list[str] | Which TIER_1/2/3/4 are available |
| `has_combine` | bool | 40-time, bench, vertical, etc. |
| `has_nextgen` | bool | Separation, route tracking, speed |
| `has_advanced_stats` | bool | EPA, PACR, target share |
| `has_basic_stats` | bool | Yards, TDs, completions |
| `has_honors` | bool | Pro Bowl, All-Pro, awards |
| `data_richness_score` | float | 0.0-1.0 weighted availability metric |

**Usage**: PASS 2 attribute mapping selects highest available tier per player to prevent data availability bias.

---

## Era Definitions

Configured in `config/eras.yaml`:

| Era | Years | Data Richness | Available Metrics |
|-----|-------|---------------|-------------------|
| **Dead Ball Era** | 1920-1945 | 0.15 | Basic totals, All-Pro |
| **Post-WWII Traditional** | 1946-1977 | 0.35 | Seasonal totals, championships |
| **Passing Revolution** | 1978-1993 | 0.50 | Sacks (1982+), detailed box scores |
| **Salary Cap & Parity** | 1994-2003 | 0.65 | Tackles (1994+), combine data |
| **Modern Pass-Happy** | 2004-2015 | 0.85 | Advanced stats, PFF grades |
| **Analytics Era** | 2016-2024 | 1.00 | NextGen, EPA, full metrics |

**Era Assignment**: Based on career midpoint (`(first_year + last_year) / 2`) to avoid edge cases.

---

## Design Principles

1. **Config-Driven**: Era boundaries, tier weights from `config/eras.yaml`
2. **Vectorized**: No row-by-row iteration (except logging samples)
3. **Pure Functions**: Aggregators have no side effects (testable with fixtures)
4. **Type-Safe**: Full pandas type hints, mypy compatible
5. **Fail Loud**: Clear ValueError messages for missing required columns
6. **Reproducible**: Manifest includes config hashes, git commit, timestamps

---

## Performance

- **Full Build (1970-2024)**: ~10-15 min (depends on network)
- **Memory Usage**: ~3-4 GB peak
- **Output Size**: ~5 MB CSV + ~2 MB Parquet

**Optimization Notes**:
- Seasonal data loaded in single call (not year-by-year)
- Combine/draft loaded once and cached
- All aggregations vectorized via pandas groupby

---

## Error Handling

**Graceful Fallbacks**:
- Pre-1999 data gaps → warning + fallback to 1999+
- Missing combine data → null values (not errors)
- Empty playoff stats → zeros with flag

**Hard Failures**:
- nflverse connection failure → `NFLVerseLoadError`
- Missing required columns (player_id, position) → `ValueError`
- Invalid era config → schema validation error

---

## Testing

```bash
# Unit tests (aggregators, profiler)
uv run pytest tests/pipeline/ingest/test_aggregators.py -v

# Integration test (full pipeline with fixtures)
uv run pytest tests/pipeline/ingest/test_players_index_integration.py -v

# Type checking
uv run mypy pipeline/ingest/

# Linting
uv run ruff pipeline/ingest/
```

---

## Common Issues

**Issue**: "No data returned for years 1970-1998"
**Fix**: Expected behavior. nflverse seasonal data starts 1999. Pre-1999 players get draft/bio data only.

**Issue**: "PlayerDataProfile has all False flags"
**Fix**: Check player's career span. Pre-1970 players legitimately have sparse data.

**Issue**: "Manifest config hash mismatch"
**Fix**: `config/eras.yaml` changed. Delete old snapshots or update manifest manually.

---

## Integration with Downstream Pipeline

**Used By**:
- **PASS 1 (Qualification)**: `players_index.csv` for 4-path criteria
- **PASS 2 (Attributes)**: `player_data_profiles.parquet` for tier selection
- **Validation**: Era distribution, data richness audits

**Blocking**: This module is Milestone 2 and blocks M3/M4/M5 (qualification → attributes → export).

---

## Maintenance

**Config Changes**:
- Edit `config/eras.yaml` to adjust era boundaries, tier weights, bonuses
- Re-run pipeline to regenerate manifest with new config hash

**Schema Changes**:
- PlayerRow/CareerStats types in `types.py` must match output schema
- Update both types AND `build_output_schema()` in `players_index.py`

**Adding Data Sources**:
1. Add load method to `NFLVerseLoader`
2. Add merge logic to `aggregators.merge_player_datasets()`
3. Update `DataProfiler` if new tier type
4. Update output schema in `players_index.py`

---

## References

- **Design Doc**: `docs/design.md` Section 3 (Data Sources), Section 4.3 (PlayerDataProfile)
- **Milestones**: `docs/milestones.md` Milestone 2
- **Coding Standards**: `CLAUDE.md` Section 2 (Python conventions)
- **FRCS Schema**: `frcs/models.py` (Pydantic models for downstream use)
