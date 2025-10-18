# Next Gen Stats Integration - Quick Reference

**Status:** ✅ Ready for implementation
**Effort:** ~1 day
**Confidence:** High

---

## TL;DR

NFL Next Gen Stats are **available and recommended** for integration via nflverse:

- **Coverage:** 2016-present, offensive positions (QB/RB/WR/TE)
- **Access:** Already in your dependencies (`nfl_data_py`)
- **Quality:** Official NFL tracking data, high reliability
- **Cost:** Free via nflverse
- **Ethics:** ✅ Respects ToS (no scraping needed)

---

## Quick Start

```python
import nfl_data_py as nfl

# Load NGS data
passing_ngs = nfl.import_ngs_data('passing', [2023, 2024])
rushing_ngs = nfl.import_ngs_data('rushing', [2023, 2024])
receiving_ngs = nfl.import_ngs_data('receiving', [2023, 2024])

# Join to player index via player_gsis_id → player_id
```

---

## Available Metrics

### Passing (29 columns)
- **Key:** xCOMP (completion % above expected), time to throw, aggressiveness, air yards
- **Best for:** QB awareness, accuracy, throw power

### Rushing (22 columns)
- **Key:** Yards over expected, efficiency, time to LOS
- **Best for:** RB vision, speed, agility

### Receiving (23 columns)
- **Key:** Separation, YAC over expected, catch %, cushion
- **Best for:** WR/TE route running, hands, speed

---

## What's NOT Available

- ❌ Defensive player tracking (not in nflverse)
- ❌ Pre-2016 data (NGS started in 2016)
- ❌ Players below minimum attempt thresholds

---

## Integration Plan

### 1. Create NGS Loader Module
**File:** `/pipeline/ingest/ngs_loader.py`
**Function:** `load_ngs_career_stats(year_range) -> DataFrame`
**See:** `/docs/research/ngs_integration_example.py` (lines 31-204)

### 2. Modify Player Index Merge
**File:** `/pipeline/ingest/players_index.py`
**Function:** `merge_player_datasets()` - add NGS merge step
**See:** Example code in research doc (lines 215-285)

### 3. Update Output Schema
**File:** `/scripts/build_players_index.py`
**Function:** `build_output_schema()` - add 21 NGS columns
**See:** Example code in research doc (lines 295-390)

### 4. Test & Validate
```bash
# Test build with NGS data
uv run python scripts/build_players_index.py --out data/raw --full

# Check output
head -1 data/raw/players_index.csv | grep "ngs_pass_xcomp"
```

---

## Expected Results

- **Join rate:** 20-30% of players will have NGS data (post-2016 players with min attempts)
- **File size:** +21 columns (~10-20KB increase per player)
- **Performance:** ~10-20 seconds additional load time (cached after first run)
- **Data quality:** High (official NFL source)

---

## Usage in Ratings Pipeline

```python
# Example: Use NGS xCOMP for QB accuracy rating
if pd.notna(player['ngs_pass_xcomp']):
    accuracy = map_xcomp_to_accuracy(player['ngs_pass_xcomp'], era_ctx)
    source_tier = 2  # High quality source
else:
    accuracy = fallback_accuracy_from_completion_pct(player['completion_pct'])
    source_tier = 3  # Lower quality fallback
```

---

## Migration Note

`nfl_data_py` is deprecated → migrate to `nflreadpy` later:

```python
# Current (works now)
import nfl_data_py as nfl
ngs = nfl.import_ngs_data('passing', [2023])

# Future (when migrating)
import nflreadpy as nfl
ngs = nfl.load_nextgen_stats([2023], stat_type='passing')
ngs_pd = ngs.to_pandas()  # Convert from Polars if needed
```

---

## Files Created

1. `/docs/research/nflverse_nextgen_stats.md` - Full research report (10 sections)
2. `/docs/research/ngs_integration_example.py` - Reference implementation
3. `/docs/research/NGS_INTEGRATION_SUMMARY.md` - This file (quick reference)

---

## Next Steps

1. Review full research doc: `/docs/research/nflverse_nextgen_stats.md`
2. Review code example: `/docs/research/ngs_integration_example.py`
3. Create `/pipeline/ingest/ngs_loader.py` (copy from example)
4. Modify `merge_player_datasets()` to call NGS loader
5. Add NGS columns to `build_output_schema()`
6. Test with `--full` build
7. Update data profiler to mark NGS availability
8. Use NGS metrics in ratings mappers

---

## Questions?

- **How to join?** `player_gsis_id` (NGS) → `player_id` (your index)
- **What about old players?** Gracefully handle nulls (pre-2016 won't have NGS)
- **Source tier?** Tier 2 for NGS data, tier 3-4 for fallbacks
- **Defense?** Not available; use traditional stats (tackles, sacks, INTs)
- **Cost?** Free via nflverse
- **Legal?** ✅ Official nflverse distribution, no ToS issues

---

**Recommendation:** Proceed with integration. High value, low effort, ethically sound.
