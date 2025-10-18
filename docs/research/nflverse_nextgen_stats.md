# NFL Next Gen Stats via nflverse: Integration Research

**Date:** 2025-10-17
**Status:** ✅ Recommended for integration
**Confidence:** High

---

## Executive Summary

NFL Next Gen Stats (NGS) data is **available and well-suited** for integration into the Legends pipeline via nflverse. The data provides advanced tracking metrics for passing, rushing, and receiving performance from 2016-present with nightly updates during the current season.

**Recommendation:** Integrate NGS data into the ingest step to enrich player profiles with advanced performance metrics. Use the existing `nfl_data_py` library (already in dependencies) via the `import_ngs_data()` function.

---

## 1. nflverse Next Gen Stats Availability

### Coverage
- **Years:** 2016-present (9 seasons of historical data)
- **Update Frequency:** Nightly during current season
- **Granularity:** Player-week level (with week 0 = season aggregate)
- **Positions:** QB, RB, WR, TE (offensive skill positions only)
- **Minimum Threshold:** NGS only provides data for players above minimum attempt thresholds

### Data Sources
- **Primary:** NFL Next Gen Stats (official NFL tracking data)
- **Access Method:** nflverse-data GitHub releases
- **Formats Available:** RDS, QS, CSV, Parquet
- **Python Interface:** `nfl_data_py.import_ngs_data()` (already in your dependencies)

### Available Stat Types
1. **Passing** - 29 columns
2. **Rushing** - 22 columns
3. **Receiving** - 23 columns

**Note on Defensive Data:** NGS does collect defensive metrics (tackle probability, coverage classification, pressure probability), but these are **NOT currently available** through nflverse. Defensive NGS data would require alternate sourcing or scraping from nextgenstats.nfl.com (with ToS considerations).

---

## 2. Data Structure & Schema

### Common Fields (all stat types)
```python
# Identity/Context
- season: int
- season_type: str ('REG' or 'POST')
- week: int (0 = season aggregate, 1-18 = regular season, 19+ = playoffs)
- player_gsis_id: str (joins to your player_id)
- player_display_name: str
- player_first_name: str
- player_last_name: str
- player_short_name: str
- player_jersey_number: int
- player_position: str
- team_abbr: str
```

### Passing Stats (29 columns)
**Advanced Metrics:**
- `avg_time_to_throw`: Average seconds from snap to release
- `avg_completed_air_yards`: Average air yards on completions
- `avg_intended_air_yards`: Average air yards on all attempts
- `avg_air_yards_differential`: Difference between intended and completed
- `aggressiveness`: % of attempts into tight windows
- `max_completed_air_distance`: Longest completed air yards
- `avg_air_yards_to_sticks`: Air yards relative to first down marker
- `expected_completion_percentage`: AI-predicted completion rate
- `completion_percentage_above_expectation`: Actual vs expected (xCOMP)

**Traditional Stats:**
- `attempts`, `completions`, `completion_percentage`
- `pass_yards`, `pass_touchdowns`, `interceptions`
- `passer_rating`

**Additional:**
- `avg_air_distance`: Average depth of all targets
- `max_air_distance`: Maximum depth of target

### Rushing Stats (22 columns)
**Advanced Metrics:**
- `efficiency`: NGS rushing efficiency rating
- `percent_attempts_gte_eight_defenders`: % against stacked boxes
- `avg_time_to_los`: Average time behind line of scrimmage
- `expected_rush_yards`: AI-predicted yards based on blocking/defense
- `rush_yards_over_expected`: Actual minus expected (RYOE)
- `rush_yards_over_expected_per_att`: RYOE per attempt
- `rush_pct_over_expected`: Percentage over expected

**Traditional Stats:**
- `rush_attempts`, `rush_yards`, `avg_rush_yards`
- `rush_touchdowns`

### Receiving Stats (23 columns)
**Advanced Metrics:**
- `avg_cushion`: Average distance from nearest defender at snap
- `avg_separation`: Average distance from defender at catch point
- `avg_intended_air_yards`: Average depth of target
- `percent_share_of_intended_air_yards`: % of team's total air yards
- `avg_yac`: Average yards after catch
- `avg_expected_yac`: AI-predicted YAC
- `avg_yac_above_expectation`: Actual minus expected YAC

**Traditional Stats:**
- `targets`, `receptions`, `catch_percentage`
- `yards`, `rec_touchdowns`

---

## 3. Integration Approach

### Access Method

**Current Library (Already Installed):**
```python
import nfl_data_py as nfl

# Load season-aggregated passing NGS data
passing_ngs = nfl.import_ngs_data(stat_type='passing', years=[2023, 2024])
rushing_ngs = nfl.import_ngs_data(stat_type='rushing', years=[2023, 2024])
receiving_ngs = nfl.import_ngs_data(stat_type='receiving', years=[2023, 2024])

# Returns pandas DataFrame with columns listed in section 2
```

**Important Note on Deprecation:**
- `nfl_data_py` has been deprecated in favor of `nflreadpy`
- No further maintenance planned for `nfl_data_py`
- **Migration Recommended:** Switch to `nflreadpy` when convenient
- `nflreadpy` uses Polars DataFrames (faster) but supports `.to_pandas()` conversion

**Migration Example:**
```python
# Future: nflreadpy (uses Polars)
import nflreadpy as nfl

ngs_passing = nfl.load_nextgen_stats([2023, 2024], stat_type="passing")
# Convert to pandas if needed
ngs_passing_pd = ngs_passing.to_pandas()
```

### Join Strategy

**Primary Key:** `player_gsis_id` → joins to `player_id` in your `players_index.csv`

**Aggregation Options:**

1. **Career Aggregates (Recommended for Ratings)**
   ```python
   # Aggregate NGS metrics across career (2016-2024)
   career_ngs = passing_ngs[passing_ngs['week'] == 0].groupby('player_gsis_id').agg({
       'avg_time_to_throw': 'mean',
       'completion_percentage_above_expectation': 'mean',
       'attempts': 'sum',
       # Weight by attempts for weighted averages
   })
   ```

2. **Peak Season (Align with Peak Algorithm)**
   ```python
   # For each player, identify peak season by passer_rating or xCOMP
   peak_ngs = passing_ngs.loc[
       passing_ngs.groupby('player_gsis_id')['passer_rating'].idxmax()
   ]
   ```

3. **Era-Adjusted (Post-2016 only)**
   - NGS data only available 2016+
   - Mark players with NGS data vs without for source_tier tracking
   - Older legends won't have NGS data; handle gracefully

**Sample Join Code:**
```python
# In build_players_index.py or new ngs_loader module
import nfl_data_py as nfl

# Load career NGS data (week 0 = season aggregates)
ngs_passing = nfl.import_ngs_data('passing', list(range(2016, 2025)))
ngs_passing_career = ngs_passing[ngs_passing['week'] == 0]

# Aggregate across seasons (weighted by attempts)
ngs_agg = ngs_passing_career.groupby('player_gsis_id').apply(
    lambda x: pd.Series({
        'ngs_avg_time_to_throw': (x['avg_time_to_throw'] * x['attempts']).sum() / x['attempts'].sum(),
        'ngs_xcomp': (x['completion_percentage_above_expectation'] * x['attempts']).sum() / x['attempts'].sum(),
        'ngs_aggressiveness': (x['aggressiveness'] * x['attempts']).sum() / x['attempts'].sum(),
        'ngs_air_yards': (x['avg_completed_air_yards'] * x['completions']).sum() / x['completions'].sum(),
        'ngs_total_attempts': x['attempts'].sum(),
    })
).reset_index()

# Merge into enhanced_players
enhanced_players = enhanced_players.merge(
    ngs_agg,
    left_on='gsis_id',
    right_on='player_gsis_id',
    how='left'
)
```

### Update Frequency

- **Historical Data:** Load once (2016-2023 complete)
- **Current Season:** Could update weekly/nightly if building mid-season rosters
- **Caching:** nflverse supports filesystem caching to avoid repeated downloads
- **Snapshot Architecture:** Fits well with immutable snapshots (load at snapshot creation time)

---

## 4. Specific Datasets Detail

### Passing NGS (29 columns)
**Best for:** QB ratings (awareness, throw power, accuracy)
**Key Metrics:**
- `completion_percentage_above_expectation`: Skill beyond scheme/talent
- `avg_time_to_throw`: Decision-making speed (awareness proxy)
- `aggressiveness`: Risk-taking tendency
- `avg_completed_air_yards`: Deep ball ability (throw power proxy)
- `avg_air_yards_to_sticks`: Decision-making in critical situations

**Coverage:** All QBs with minimum attempts (typically ~100+ pass attempts)

**Example Players (2023 season aggregate):**
```
Jake Browning (CIN QB): 243 attempts, 98.4 rating, +3.3 xCOMP, 2.73s avg TT
```

### Rushing NGS (22 columns)
**Best for:** RB ratings (speed, agility, vision)
**Key Metrics:**
- `rush_yards_over_expected_per_att`: Vision/patience (positive = creates own yards)
- `efficiency`: Overall rushing effectiveness
- `percent_attempts_gte_eight_defenders`: Difficulty of carries (context)
- `avg_time_to_los`: Decisiveness (speed/agility proxy)

**Coverage:** All RBs with minimum attempts (typically ~50+ carries)

**Use Cases:**
- Vision/Elusiveness: High RYOE per attempt
- Speed: Low time to LOS + high efficiency
- Power: High success rate vs 8+ defenders

### Receiving NGS (23 columns)
**Best for:** WR/TE ratings (route running, hands, separation)
**Key Metrics:**
- `avg_separation`: Route running ability + speed
- `catch_percentage`: Hands/concentration
- `avg_yac_above_expectation`: Elusiveness after catch
- `avg_intended_air_yards`: Route tree depth (deep threat vs possession)
- `percent_share_of_intended_air_yards`: Target quality/alpha role

**Coverage:** All WR/TE with minimum targets (typically ~25+ targets)

**Use Cases:**
- Route Running: High avg_separation + high catch_percentage
- Deep Threat: High avg_intended_air_yards + high avg_separation
- YAC Specialist: High avg_yac_above_expectation

### Defensive NGS
**Status:** ❌ NOT available via nflverse

**What NFL Tracks (but not in nflverse):**
- Tackle Probability (2024+)
- Coverage Classification (man/zone, 2022+)
- Pressure Probability for pass rushers
- Coverage Responsibility assignments (2025+)

**Alternatives:**
1. Use PFF grades (requires subscription)
2. Use traditional stats (tackles, sacks, INTs) from seasonal data
3. Custom scraping from nextgenstats.nfl.com (check ToS)
4. Wait for nflverse to add defensive NGS (may happen in future)

---

## 5. Code Examples

### Example 1: Load and Inspect NGS Data
```python
import nfl_data_py as nfl
import pandas as pd

# Load passing NGS for recent seasons
ngs_passing = nfl.import_ngs_data(stat_type='passing', years=[2022, 2023, 2024])

print(f"Shape: {ngs_passing.shape}")
print(f"Columns: {list(ngs_passing.columns)}")
print(f"Players: {ngs_passing['player_gsis_id'].nunique()}")

# Filter to season aggregates (week 0)
season_agg = ngs_passing[ngs_passing['week'] == 0]
print(f"Season aggregates: {len(season_agg)} player-seasons")

# Top 5 by xCOMP in 2023
top_xcomp = season_agg[season_agg['season'] == 2023].nlargest(5, 'completion_percentage_above_expectation')
print(top_xcomp[['player_display_name', 'team_abbr', 'attempts', 'completion_percentage_above_expectation']])
```

### Example 2: Integrate into Player Index Build
```python
# In pipeline/ingest/ngs_loader.py (new module)

import logging
import pandas as pd
import nfl_data_py as nfl

logger = logging.getLogger(__name__)

def load_ngs_career_stats(year_range: range) -> pd.DataFrame:
    """Load and aggregate NGS stats across player careers.

    Args:
        year_range: Range of years to load (e.g., range(2016, 2025))

    Returns:
        DataFrame with player_gsis_id and aggregated NGS metrics
    """
    logger.info(f"Loading NGS data for {min(year_range)}-{max(year_range)}...")

    # Load all three stat types
    passing = nfl.import_ngs_data('passing', list(year_range))
    rushing = nfl.import_ngs_data('rushing', list(year_range))
    receiving = nfl.import_ngs_data('receiving', list(year_range))

    # Filter to season aggregates (week 0)
    passing_agg = passing[passing['week'] == 0]
    rushing_agg = rushing[rushing['week'] == 0]
    receiving_agg = receiving[receiving['week'] == 0]

    # Aggregate passing metrics (weighted by attempts)
    ngs_passing = passing_agg.groupby('player_gsis_id').apply(
        lambda x: pd.Series({
            'ngs_pass_xcomp': (x['completion_percentage_above_expectation'] * x['attempts']).sum() / x['attempts'].sum(),
            'ngs_pass_time_to_throw': (x['avg_time_to_throw'] * x['attempts']).sum() / x['attempts'].sum(),
            'ngs_pass_aggressiveness': (x['aggressiveness'] * x['attempts']).sum() / x['attempts'].sum(),
            'ngs_pass_air_yards': (x['avg_completed_air_yards'] * x['completions']).sum() / x['completions'].sum(),
            'ngs_pass_attempts_total': x['attempts'].sum(),
        })
    ).reset_index()

    # Aggregate rushing metrics (weighted by attempts)
    ngs_rushing = rushing_agg.groupby('player_gsis_id').apply(
        lambda x: pd.Series({
            'ngs_rush_yards_over_exp': (x['rush_yards_over_expected_per_att'] * x['rush_attempts']).sum() / x['rush_attempts'].sum(),
            'ngs_rush_efficiency': (x['efficiency'] * x['rush_attempts']).sum() / x['rush_attempts'].sum(),
            'ngs_rush_time_to_los': (x['avg_time_to_los'] * x['rush_attempts']).sum() / x['rush_attempts'].sum(),
            'ngs_rush_attempts_total': x['rush_attempts'].sum(),
        })
    ).reset_index()

    # Aggregate receiving metrics (weighted by targets)
    ngs_receiving = receiving_agg.groupby('player_gsis_id').apply(
        lambda x: pd.Series({
            'ngs_rec_separation': (x['avg_separation'] * x['targets']).sum() / x['targets'].sum(),
            'ngs_rec_yac_over_exp': (x['avg_yac_above_expectation'] * x['receptions']).sum() / x['receptions'].sum(),
            'ngs_rec_cushion': (x['avg_cushion'] * x['targets']).sum() / x['targets'].sum(),
            'ngs_rec_air_yards': (x['avg_intended_air_yards'] * x['targets']).sum() / x['targets'].sum(),
            'ngs_rec_catch_pct': (x['catch_percentage'] * x['targets']).sum() / x['targets'].sum(),
            'ngs_rec_targets_total': x['targets'].sum(),
        })
    ).reset_index()

    # Merge all three stat types
    ngs_combined = ngs_passing.merge(ngs_rushing, on='player_gsis_id', how='outer')
    ngs_combined = ngs_combined.merge(ngs_receiving, on='player_gsis_id', how='outer')

    logger.info(f"NGS data loaded: {len(ngs_combined)} players with NGS metrics")

    return ngs_combined


# In pipeline/ingest/players_index.py (modify merge_player_datasets)

from pipeline.ingest.ngs_loader import load_ngs_career_stats

def merge_player_datasets(
    players: pd.DataFrame,
    career_stats: pd.DataFrame,
    playoff_stats: pd.DataFrame,
    draft: pd.DataFrame,
    combine: pd.DataFrame,
) -> pd.DataFrame:
    """Merge all player datasets including NGS stats."""

    # ... existing merges ...

    # NEW: Merge NGS data
    logger.info("Merging Next Gen Stats...")
    ngs_stats = load_ngs_career_stats(range(2016, 2025))
    enhanced_players = enhanced_players.merge(
        ngs_stats,
        left_on='gsis_id',
        right_on='player_gsis_id',
        how='left'
    )
    logger.info(f"Players with NGS data: {enhanced_players['ngs_pass_attempts_total'].notna().sum()}")

    return enhanced_players
```

### Example 3: Export NGS Columns to CSV
```python
# In build_output_schema() - add NGS columns

def build_output_schema(enhanced_players: pd.DataFrame) -> pd.DataFrame:
    """Build output schema with NGS fields."""

    return pd.DataFrame({
        # ... existing fields ...

        # Next Gen Stats (Passing)
        "ngs_pass_xcomp": enhanced_players.get("ngs_pass_xcomp", pd.NA),
        "ngs_pass_time_to_throw": enhanced_players.get("ngs_pass_time_to_throw", pd.NA),
        "ngs_pass_aggressiveness": enhanced_players.get("ngs_pass_aggressiveness", pd.NA),
        "ngs_pass_air_yards": enhanced_players.get("ngs_pass_air_yards", pd.NA),
        "ngs_pass_attempts": enhanced_players.get("ngs_pass_attempts_total", 0).fillna(0).astype(int),

        # Next Gen Stats (Rushing)
        "ngs_rush_yards_over_exp": enhanced_players.get("ngs_rush_yards_over_exp", pd.NA),
        "ngs_rush_efficiency": enhanced_players.get("ngs_rush_efficiency", pd.NA),
        "ngs_rush_time_to_los": enhanced_players.get("ngs_rush_time_to_los", pd.NA),
        "ngs_rush_attempts": enhanced_players.get("ngs_rush_attempts_total", 0).fillna(0).astype(int),

        # Next Gen Stats (Receiving)
        "ngs_rec_separation": enhanced_players.get("ngs_rec_separation", pd.NA),
        "ngs_rec_yac_over_exp": enhanced_players.get("ngs_rec_yac_over_exp", pd.NA),
        "ngs_rec_catch_pct": enhanced_players.get("ngs_rec_catch_pct", pd.NA),
        "ngs_rec_air_yards": enhanced_players.get("ngs_rec_air_yards", pd.NA),
        "ngs_rec_targets": enhanced_players.get("ngs_rec_targets_total", 0).fillna(0).astype(int),
    })
```

### Example 4: Data Size and Performance
```python
# Expected data size (as of 2024)
# Passing: ~620 player-seasons per year × 9 years = ~5,580 rows
# Rushing: ~623 player-seasons per year × 9 years = ~5,607 rows
# Receiving: ~1,473 player-seasons per year × 9 years = ~13,257 rows

# Total: ~24,444 rows across all stat types (manageable in memory)

# Loading time: ~5-15 seconds per stat type (first load, then cached)
# Memory: ~10-20 MB total for all NGS data
```

---

## 6. Alternatives & Completeness Assessment

### Data Completeness: ✅ Excellent for Offense, ❌ Missing Defense

**Strengths:**
- Comprehensive offensive metrics (passing, rushing, receiving)
- 9 years of historical data (2016-2024)
- Advanced AI-derived metrics (xCOMP, RYOE, YAC over expected)
- Official NFL source (high quality, consistent methodology)
- Free and openly accessible via nflverse
- Well-documented schema and data dictionary
- Active maintenance (nightly updates during season)

**Gaps:**
- No defensive player tracking data in nflverse
- Only covers 2016+ (older legends won't have NGS data)
- Minimum attempt thresholds (excludes backups, special teamers)
- No college stats or pre-NFL data
- No situational breakdowns (red zone, 3rd down, etc.) in aggregates

### Alternative Sources

#### 1. Pro Football Focus (PFF)
**Coverage:** Offensive + Defensive grades, 2006-present
**Cost:** Subscription required (~$200-$500/year depending on tier)
**Pros:** Human-graded film analysis, defensive metrics, situational breakdowns
**Cons:** Proprietary grades (less transparent), costly, API access limited
**Use Case:** Consider for defensive player ratings if budget allows

#### 2. Sports Info Solutions (SIS)
**Coverage:** Advanced stats including ball-tracking, 2008+
**Cost:** Enterprise pricing (likely $1,000+/year)
**Pros:** Highly detailed, some unique metrics
**Cons:** Expensive, B2B focus, may not have public API

#### 3. Pro Football Reference (PFR)
**Coverage:** Traditional stats, 1920-present
**Cost:** Free (scraping) or Stathead subscription ($8/month)
**Pros:** Historical depth, all positions, situational stats
**Cons:** No advanced tracking metrics, scraping challenges
**Use Case:** Already using via nflverse (seasonal stats)

#### 4. ESPN Analytics
**Coverage:** QBR, other proprietary metrics
**Cost:** Free (public website)
**Pros:** Additional perspective on QB performance
**Cons:** Limited API, proprietary formula, scraping needed

#### 5. Custom Scraping of nextgenstats.nfl.com
**Coverage:** May have defensive NGS not in nflverse
**Cost:** Free (but ToS risk)
**Pros:** Direct source, may have additional metrics
**Cons:** Violates ToS, fragile to website changes, unethical
**Recommendation:** ❌ Avoid - use nflverse instead

### Recommended Data Strategy

**For this project:**

1. **Use nflverse NGS for offensive players** (2016+)
   - Integrate into player index build
   - Mark availability in data profiles
   - Use for ratings where available

2. **Use existing nflverse seasonal stats for defense**
   - Tackles, sacks, INTs (already in your index)
   - Sufficient for basic defensive ratings

3. **Consider PFF for Phase 2** (if budget allows)
   - Would unlock defensive ratings quality
   - Provides pre-2016 grades for modern-era players
   - Adds situational context

4. **Accept NGS limitations:**
   - Source tier 2-3 for players with NGS data
   - Source tier 3-4 for pre-2016 legends (use traditional stats)
   - Document in data profiles (has_ngs_passing, has_ngs_rushing, etc.)

---

## 7. Data Quality & Reliability

### Quality Assessment: ✅ High

**Strengths:**
- Official NFL source (derived from RFID tracking chips)
- Consistent methodology (same sensors, same calculations since 2016)
- Regular validation (NFL publishes methodology, works with AWS)
- Community vetted (used by nflverse, PFF, media analysts)

**Known Issues:**
- Minimum attempt thresholds vary by year (not documented)
- Some players missing despite meeting thresholds (data quality issue at source)
- Week 0 aggregates may not exactly match sum of weekly data (rounding?)
- Position labels sometimes inconsistent (e.g., Taysom Hill as QB vs TE)

**Validation Steps:**
1. Check player_gsis_id matches your player index (join success rate)
2. Verify attempt/target counts align with seasonal stats (sanity check)
3. Confirm no null values in key metrics for players with attempts > threshold
4. Test on known players (e.g., Patrick Mahomes should have complete passing NGS)

**Error Handling:**
```python
# Check for missing NGS data
logger.info(f"Players in index: {len(enhanced_players)}")
logger.info(f"Players with NGS passing data: {enhanced_players['ngs_pass_attempts'].notna().sum()}")
logger.info(f"Players with NGS rushing data: {enhanced_players['ngs_rush_attempts'].notna().sum()}")
logger.info(f"Players with NGS receiving data: {enhanced_players['ngs_rec_targets'].notna().sum()}")

# Warn if join rate is unexpectedly low
join_rate = enhanced_players['ngs_pass_attempts'].notna().sum() / len(enhanced_players)
if join_rate < 0.1:  # Expect at least 10% of all players to have NGS passing data
    logger.warning(f"Low NGS join rate: {join_rate:.1%} - check player_gsis_id mapping")
```

---

## 8. Integration Checklist

### Phase 1: Add to Player Index (Ingest Step)
- [ ] Create `pipeline/ingest/ngs_loader.py` module
- [ ] Add `load_ngs_career_stats()` function (weighted aggregation)
- [ ] Modify `merge_player_datasets()` to merge NGS data
- [ ] Add NGS columns to `build_output_schema()`
- [ ] Update `players_index.csv` header documentation
- [ ] Test with `--full` build (verify performance)
- [ ] Validate join success rate (expect ~20-30% of players to have NGS data)

### Phase 2: Update Data Profiler
- [ ] Add `has_ngs_passing`, `has_ngs_rushing`, `has_ngs_receiving` flags
- [ ] Update `player_data_profiles.parquet` schema
- [ ] Document NGS availability by era/position

### Phase 3: Use in Ratings Pipeline
- [ ] Map NGS metrics to Madden attributes (e.g., xCOMP → Accuracy)
- [ ] Update `source_tier` logic (tier 1-2 for players with NGS data)
- [ ] Add NGS-based mappers to `pipeline/ratings/` modules
- [ ] Document which attributes benefit from NGS data

### Phase 4: Testing & Validation
- [ ] Unit tests for NGS loader (fixture data)
- [ ] Integration test for full build with NGS
- [ ] Validate known players (spot check Mahomes, Brady, etc.)
- [ ] Check data profiles show correct NGS availability
- [ ] Verify parquet size increase is reasonable (<50MB for full dataset)

### Phase 5: Documentation
- [ ] Update `design/data_sources.md` with NGS details
- [ ] Add NGS to `CLAUDE.md` data ethics section
- [ ] Document NGS limitations (2016+, offense only) in README
- [ ] Add NGS examples to any user-facing docs

---

## 9. Migration Path (nfl_data_py → nflreadpy)

**Status:** nfl_data_py is deprecated, but still functional

**Timeline:**
- **Short term (now):** Use existing `nfl_data_py` for NGS integration
- **Medium term (next 3-6 months):** Migrate to `nflreadpy` when convenient
- **Long term:** `nfl_data_py` will likely stop working (1-2 years)

**Migration Steps:**

1. Update `pyproject.toml`:
   ```toml
   dependencies = [
       # "nfl-data-py>=0.3.0",  # Remove
       "nflreadpy>=0.1.0",       # Add
       # ... other deps
   ]
   ```

2. Update imports:
   ```python
   # Old
   import nfl_data_py as nfl

   # New
   import nflreadpy as nfl
   ```

3. Update function calls:
   ```python
   # Old
   ngs = nfl.import_ngs_data('passing', [2023, 2024])

   # New
   ngs = nfl.load_nextgen_stats([2023, 2024], stat_type='passing')
   # If you need pandas instead of polars:
   ngs_pd = ngs.to_pandas()
   ```

4. Test thoroughly (nflreadpy uses Polars, may have subtle differences)

**Recommendation:** Proceed with `nfl_data_py` integration now, plan migration to `nflreadpy` in next major refactor.

---

## 10. Final Recommendation

### ✅ Integrate NGS Data into Player Index

**Rationale:**
1. High-quality official data from NFL
2. Already have required library (`nfl_data_py`)
3. Fits snapshot architecture (batch load during ingest)
4. Provides advanced metrics not available elsewhere for free
5. Improves rating quality for modern players (2016+)
6. Minimal implementation effort (1-2 days)
7. Respects data ethics (official nflverse source, no scraping)

**Integration Scope:**
- Add NGS passing, rushing, receiving stats to `players_index.csv`
- Career aggregates (weighted by attempts/targets)
- Mark availability in `player_data_profiles.parquet`
- Use in ratings pipeline where available (source_tier 2)
- Gracefully handle missing data (pre-2016 players)

**Effort Estimate:**
- Module creation: 2-4 hours
- Schema updates: 1-2 hours
- Testing: 2-3 hours
- Documentation: 1 hour
- **Total: ~1 day of development time**

**Risks:**
- Low: nflverse is stable, data quality is high
- Medium: Deprecation of nfl_data_py (but nflreadpy is drop-in replacement)
- Low: Performance impact (data size is small, caching available)

**Next Steps:**
1. Create `pipeline/ingest/ngs_loader.py` with code from Example 2
2. Update `merge_player_datasets()` to call NGS loader
3. Add NGS columns to output schema
4. Run test build (`--test-only` then `--full`)
5. Validate output in `players_index.csv`
6. Update data profiler to mark NGS availability

---

## Appendix: Direct Data Access URLs

For reference, nflverse data is available at:

**GitHub Releases:**
- https://github.com/nflverse/nflverse-data/releases

**Direct Parquet Download (example):**
- Pattern: `https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/nextgen_stats_{stat_type}.parquet`
- Passing: `nextgen_stats_passing.parquet`
- Rushing: `nextgen_stats_rushing.parquet`
- Receiving: `nextgen_stats_receiving.parquet`

**Note:** Using `nfl_data_py` or `nflreadpy` is strongly recommended over direct downloads (handles caching, versioning, and schema changes).

---

## References

- nflverse GitHub: https://github.com/nflverse
- nflreadr documentation: https://nflreadr.nflverse.com/
- NGS data dictionary: https://nflreadr.nflverse.com/articles/dictionary_nextgen_stats.html
- NFL Next Gen Stats: https://nextgenstats.nfl.com/
- nfl_data_py (deprecated): https://github.com/nflverse/nfl_data_py
- nflreadpy (replacement): https://github.com/nflverse/nflreadpy
