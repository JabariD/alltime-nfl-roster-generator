# Design Document: All-Time Madden Legends Roster Generator

## 1. Overview

I want to create a **systematic, programmatic pipeline** that generates an All-Time Madden YYYY Legends roster (≈3,500 players). The system uses available data sources as the primary data source and translates football stats, honors, and physicals into **Madden attributes**. The design emphasizes:

* **Reproducibility:** Deterministic runs with versioned configs and data snapshots.
* **Explainability:** Every attribute traceable to data and documented rules.
* **Future-proofing:** Game-agnostic core schema (FRCS) with adapters for Madden 26 and beyond.

---

## 2. System Goals

* Build a **canonical player ratings dataset** (FRCS) decoupled from Madden’s shifting schemas.
* Use **rules + AI/ML hybrid mapping (in version 2)** to convert football data into Madden-style 0–99 ratings.
* Handle **era normalization** so players are evaluated fairly across history.
* Allow **position quotas and rankings** to ensure roster balance (\~3,500 total).
* Support **export adapters** for Madden 26 and future versions.

---

## 3. Data Sources

* **nflverse-data** (via nflreadr R package): comprehensive NFL data ecosystem with player stats, rosters, biographical info, combine data, and draft information.
  * `load_players()`: Player biographical information and position mappings
  * `load_player_stats()`: Historical weekly/seasonal player performance statistics including:
    * **Passing**: completions, attempts, yards, TDs, INTs, sacks, air yards, EPA
    * **Rushing**: carries, yards, TDs, fumbles, first downs, EPA
    * **Receiving**: receptions, targets, yards, TDs, air yards, YAC
    * **Kicking**: FG made/attempted by distance, XP, blocked kicks
    * **Defense**: tackles, sacks, interceptions, etc.
    * **Advanced metrics**: fantasy points, PACR, target share, WOPR
  * `load_rosters()`: Team rosters dating back to 2002
  * `load_combine()`: NFL Combine performance data
  * `load_draft_picks()`: Draft history and pick information
  * `load_nextgen_stats()`: Advanced player performance metrics
* **Madden historical ratings dataset** (for supervised learning).

### 3.1 Data Availability Across Eras

**CRITICAL DESIGN CONSTRAINT:** Data richness varies dramatically across NFL history, requiring era-aware attribute mapping to prevent "data availability bias."

**Era-Specific Data Coverage:**

| Era | Years | Available Data | Missing Data |
|-----|-------|----------------|--------------|
| **Analytics Era** | 2016-Present | NextGen stats (separation, route tracking, speed), EPA, PACR, target share, combine, weekly stats | None (most complete) |
| **Modern Pass-Happy** | 2004-2015 | Combine data, PFF grades (2006+), advanced QB stats (air yards 2009+), tackles, TFLs, snap counts | NextGen stats, EPA for most years |
| **Salary Cap & Parity** | 1994-2003 | Tackles (official 1994+), TFLs, passes defended, combine data (widespread), basic weekly stats | Advanced metrics, NextGen, PFF grades |
| **Passing Revolution** | 1978-1993 | Sacks tracked (1982+), detailed box scores, 16-game season stats | Tackles (unofficial), combine data (rare), advanced metrics |
| **Post-WWII Traditional** | 1946-1977 | Basic seasonal totals (yards, TDs, INTs), All-Pro data, championships | Sacks, tackles, combine, weekly stats |
| **Dead Ball Era** | 1920-1945 | Sparse seasonal data, championships, All-Pro (if available) | Nearly all modern metrics |

**Key Insight:** The system uses ALL available data for modern players (NextGen, EPA, etc.) while preventing data richness from artificially inflating ratings via source tier tracking and confidence adjustments (see Section 5.1).

---

## 4. Core Schema (FRCS v1)

The Football Ratings Canonical Schema (FRCS) is our **truth layer**. It uses semantic keys rather than Madden-specific names.

### 4.1 Player Identity

* `player_id` (nflverse gsis_id or other stable identifier)
* `full_name`
* `primary_pos`, `secondary_pos`
* `era_bucket`
* `birth_year`, `height_in`, `weight_lb`
* `years_active`, `teams`
* `honors`: All-Pro, Pro Bowls, HOF
* `draft_info`: Draft year, round, pick number, college

### 4.2 Ratings (long form)

* `player_id`
* `attribute_key` (semantic, e.g., `deep_accuracy`, `run_block`, `speed`)
* `value_0_99`
* `source_tier` (TIER_1, TIER_2, TIER_3, TIER_4 - see Section 5.1)
* `metrics_used` (list of input stats, e.g., `['forty_time']` or `['yards_per_catch', 'all_pro_count']`)
* `notes` (human-readable provenance, e.g., "Era-adjusted from TIER_3 basic stats")

**Source Tier Definitions:**
* **TIER_1 (Direct Measurement)**: Combine data (40-time, 3-cone, vertical), measured physicals
* **TIER_2 (Rich Derived Stats)**: NextGen metrics, EPA, PACR, target share, PFF grades
* **TIER_3 (Basic Stats)**: Box score statistics (yards, TDs, completions, INTs, sacks)
* **TIER_4 (Honors Proxy)**: All-Pro selections, awards, positional averages, era context

### 4.3 Player Data Profiles (NEW in FRCS v1.1)

Metadata table tracking data availability per player (used for era-aware attribute mapping):

* `player_id`
* `era_bucket` (see Section 3.1 for definitions)
* `available_tiers` (list of TIER_1/2/3/4 available for this player)
* `has_combine` (boolean: 40-time, bench, vertical, broad, 3-cone, shuttle)
* `has_nextgen` (boolean: separation, route tracking, speed metrics)
* `has_advanced_stats` (boolean: EPA, PACR, target share, air yards)
* `has_basic_stats` (boolean: yards, TDs, completions, attempts)
* `has_honors` (boolean: Pro Bowl, All-Pro, awards, HOF)
* `data_richness_score` (float 0.0-1.0: for auditing, not used in calculations)

**Purpose:** Enable tiered fallback logic in attribute mapping (PASS 2) and validate that data availability doesn't create era bias.

### 4.4 Metadata

* Run manifest: FRCS version, config hash, PFR snapshot date.
* Provenance logs for each attribute.
* Era attribute recipe version (tracks which fallback rules were used).

---

## 5. Attribute Mapping Framework

Every rating follows a **5-step pipeline** with era-aware tiered fallbacks:

1. **Data Profile Check:** Determine which source tiers are available for this player (TIER_1/2/3/4).
2. **Inputs:** Select 1–3 primary stats per attribute based on highest available tier.
3. **Normalization:** Convert to percentile within era & position peers.
4. **Mapping:** Percentile → Madden scale (default 40–99, tier-dependent range).
5. **Modifiers:** Apply archetype adjustments + era fairness adjustments.

### 5.1 Tiered Fallback System (Era-Aware Mapping)

**PHILOSOPHY:** Use the best available data for each player while preventing "data availability bias" where modern players with NextGen stats automatically rate higher than historical players with only basic stats.

**Tier Selection Logic (per attribute, per player):**

```python
def select_attribute_tier(player_data_profile, attribute_key, position):
    """Select highest available tier for this player's attribute."""

    if attribute_key in TIER_1_ATTRIBUTES:  # Speed, acceleration, agility
        if player_data_profile.has_combine:
            return 'TIER_1', get_combine_recipe(attribute_key, position)

    if attribute_key in TIER_2_ATTRIBUTES:  # Route running, coverage, pass rush
        if player_data_profile.has_nextgen or player_data_profile.has_advanced_stats:
            return 'TIER_2', get_advanced_recipe(attribute_key, position)

    if player_data_profile.has_basic_stats:
        return 'TIER_3', get_basic_recipe(attribute_key, position)

    # Fallback to honors-based proxy
    return 'TIER_4', get_honors_recipe(attribute_key, position)
```

**Era Fairness Adjustment (prevents data availability bias):**

```python
def apply_era_fairness(value, source_tier, era_bucket):
    """
    Adjust ratings to prevent modern players from dominating solely due to data richness.

    Confidence penalties reflect measurement uncertainty, not player quality.
    """
    confidence_penalty = {
        'TIER_1': 0,   # Direct measurement (40-time) - no penalty
        'TIER_2': 2,   # Rich stats (NextGen, EPA) - slight uncertainty
        'TIER_3': 4,   # Basic stats (yards, TDs) - more uncertainty
        'TIER_4': 6    # Honors proxy (All-Pro) - most uncertainty
    }

    # Pre-1970 players get era bonus to offset data scarcity
    era_bonus = {
        'pre_1945': 5,
        '1946_1977': 3,
        '1978_1993': 0,
        '1994_2003': 0,
        '2004_2015': 0,
        '2016_present': 0
    }

    adjusted = value - confidence_penalty[source_tier] + era_bonus[era_bucket]
    return clamp(adjusted, 40, 99)
```

**Example: Modern players with NextGen data get HIGHER PRECISION (not higher ratings).** A 2023 WR with separation data might rate 92 SPD (TIER_2, -2 penalty → 90), while a 1985 WR with only yards/catch rates 88 SPD (TIER_3, -4 penalty → 84). The modern player is still higher (earned via measurables), but not by 20+ points just from data richness.

### 5.2 Position-Specific Attribute Recipes

Each position has recipes for 40+ attributes, with tiered fallbacks per era. Stored in `config/era_attribute_recipes.yaml`.

**Example: WR Speed**

```yaml
speed:
  position: WR
  TIER_1:  # 1999+ with combine data
    inputs: [forty_time]
    formula: "percentile_to_scale(forty_time, position=WR, era=player_era)"
    range: [70, 99]
    confidence: 1.0

  TIER_2:  # 2016+ with NextGen or advanced stats
    inputs: [avg_rush_speed_mph, long_play_rate]
    formula: "blend(rush_speed_percentile * 0.6, long_play_percentile * 0.4)"
    range: [65, 96]
    confidence: 0.85

  TIER_3:  # 1970+ with basic stats
    inputs: [yards_per_catch, long_td_rate, kr_pr_average]
    formula: "blend(ypc_percentile * 0.5, long_td_percentile * 0.3, return_avg_percentile * 0.2)"
    range: [60, 93]
    confidence: 0.70

  TIER_4:  # Pre-1970 with honors only
    inputs: [all_pro_count, years_as_starter, position_avg_speed]
    formula: "position_avg + (all_pro_count * 2) + era_context"
    range: [55, 90]
    confidence: 0.55
    era_bonus: 3  # Offset data scarcity
```

**Statistical Surrogates (Historical → Modern Metrics):**

| Attribute | Modern Metric (TIER_1/2) | Historical Surrogate (TIER_3/4) |
|-----------|---------------------------|----------------------------------|
| **Speed (WR/RB)** | 40-yard dash, NextGen speed | Yards/catch, long TD rate, KR/PR average |
| **Route Running (WR)** | NextGen separation, YPRR | Catch rate, receptions/game (era-adjusted) |
| **Man Coverage (DB)** | Completion % allowed, target rate | INT rate, All-Pro count, era dominance |
| **Pass Rush (EDGE)** | Win rate, pressure rate | Sack rate (1982+), TFL rate (1994+), All-Pro |
| **Throw Power (QB)** | Air yards, deep ball rate | Yards/attempt, body type (6'5" = strong arm) |
| **Catching (WR/TE)** | Contested catch rate, drop rate | Catch rate, TD rate in red zone |

### 5.3 Cross-Era Normalization (Rule Changes)

Certain stats require era adjustments due to rule changes:

**Passing Stats (QB/WR/TE):**
* Pre-1978 passing yards × 1.25 (account for Mel Blount rule opening passing game)
* Pre-2004 completion % + 5% (account for illegal contact emphasis)

**Rushing Stats (RB):**
* Pre-1978 yards × 1.14 (14-game → 16-game season adjustment)
* 1978-2020 yards × 1.06 (16-game → 17-game season adjustment)

**Defensive Stats:**
* Pre-1978 INTs × 0.7 (more pass attempts in modern era, but lower INT rate)
* Pre-1982 sacks = N/A (not tracked; use All-Pro count + TFL as proxy)
* Pre-1994 tackles = N/A (not official; use All-Pro + positional proxy)

**Season Length Adjustments:**
* Pre-1961: 12-game season → multiply by 1.33
* 1961-1977: 14-game season → multiply by 1.14
* 1978-2020: 16-game season (baseline)
* 2021-present: 17-game season → divide by 1.06

### 5.4 Timeless Greatness Indicators (Era-Agnostic Anchors)

These indicators work across ALL eras because they represent peer-validated excellence:

**Tier 1: Peer/Expert Recognition (Highest Weight)**
* All-Pro selections (especially 1st team) - coaches/media voting relative to contemporaries
* Pro Bowl selections - peer recognition (though inflated post-2000)
* Hall of Fame - long-term historical consensus
* MVP/DPOY/OPOY/SB MVP - single-season dominance
* Championship performance - elevated play in biggest games

**Tier 2: Era-Relative Statistical Dominance**
* Percentile ranks within era (top-5 in a stat among era peers)
* Rate stats over volume (INTs per game > total INTs, accounts for season length)
* Multi-year peaks (3+ year windows of sustained excellence)
* Positional scarcity (elite LT/EDGE/CB1 > elite RB/WR due to supply)

**Tier 3: Longevity & Durability**
* Games played (availability is a skill, adjust for season length)
* Consecutive years starting (longevity at high level > flash-in-pan)
* Late-career performance (playing well at 33+ suggests technical mastery)

**Football Truth:** Honors + era-relative dominance + longevity are the ONLY truly comparable metrics across 100 years of football. The 4-path qualification system (Section 6.1) is already era-fair because it uses these timeless indicators.

---

## 6. Ranking & Selection

### 6.1 Player Pool Rules (Legend Qualification Paths)

**FOUR LEGEND QUALIFICATION PATHS (any path qualifies):**

**PATH 1 - PEAK DOMINANCE** (addresses short amazing careers):
- ≥3 Pro Bowls in ≤8 seasons (high peer recognition rate)
  * Examples: Quenton Nelson (7 PB/7 seasons), Andrew Luck (4 PB/7 seasons)
- OR ≥1 All-Pro selection in any span (top 1% at position)
- OR Hall of Fame flag (automatic legend status)
- OR Major Award: NFL MVP, DPOY, OPOY, or Super Bowl MVP (transcendent single-season impact)
- Minimum: ≥32 total games AND ≥3 seasons active (prevents fluke 2-season peaks)

**PATH 2 - SUSTAINED EXCELLENCE** (longevity + recognition):
- ≥12 seasons + ≥2 Pro Bowls (durability + peer recognition; changed from 1 PB to avoid flukes)
  * Examples: Andy Lee (19 seasons/3 PB), Duane Brown (16 seasons/5 PB)
- OR ≥15 seasons + ≥150 games played (pure longevity value with games-played floor)
  * Examples: Morten Andersen (26 seasons/382 games), Sebastian Janikowski (19 seasons/268 games)
  * Games-played floor prevents practice squad/backup inflators

**PATH 3 - STATISTICAL DOMINANCE** (career achievement):
- Top-5 all-time in position-specific primary stat (era-adjusted) + ≥6 seasons
  * QB: passing yards, RB: rushing yards, WR: receiving yards, etc.
  * Era-adjusted: Pre-1978 stats multiplied by 1.14 (16-game/14-game adjustment)
  * Examples: Barry Sanders (#3 rush yards), Jerry Rice (#1 rec yards), Tom Brady (#1 pass yards)
- OR Draft pick ≤10 + ≥8 seasons + ≥1 Pro Bowl/All-Pro (pedigree + longevity + recognition)
  * Changed: Added honor requirement to prevent busts qualifying

**PATH 4 - AWARD EXCELLENCE** (NEW - transcendent performers):
- ≥1 of: NFL MVP, Defensive Player of the Year, Offensive Player of the Year, or Super Bowl MVP
- AND ≥3 seasons active
- AND ≥32 games played
- Examples: Terrell Davis (1998 MVP, 2x SB MVP), Kurt Warner (2x MVP, 1 SB MVP), Rich Gannon (2002 MVP)

**IMPLEMENTATION NOTES:**
- All criteria work for any position (OL, specialists, defense, offense)
- No subjective "clutch" metrics - only measurable peer/organizational recognition
- Era-neutral: Pro Bowls are relative to contemporaries in each season
- Handles both short peaks (Paths 1 & 4) and long careers (Path 2) objectively
- PATH 3 changes: "Team continuity" criterion REMOVED (too team-dependent, not player-driven)

**VALIDATION TEST CASES (MUST PASS):**
- ✅ Patrick Mahomes (6 PB/7 seasons, 3 AP, 2 MVP, 3 SB MVP) → PATH 1 + PATH 4
- ✅ Justin Jefferson (4 PB/4 seasons, 2 AP) → PATH 1
- ✅ Quenton Nelson (7 PB/7 seasons, 3 AP) → PATH 1
- ✅ Terrell Davis (3 PB/7 seasons, 2 AP, 1 MVP, 2 SB MVP) → PATH 1 + PATH 4
- ✅ Kurt Warner (4 PB/12 seasons, 2 AP, 2 MVP, 1 SB MVP, HOF) → PATH 1 + PATH 2 + PATH 4
- ❌ Bo Jackson (1 PB/4 seasons, 0 AP, 38 games) → FAILS ALL PATHS (acceptable exclusion)
- ❌ Malcolm Butler (1 PB/7 seasons, 0 AP) → FAILS ALL PATHS (one play doesn't make a legend)

**THIS IS THE ACTUAL QUALIFICATION LOGIC - NOT JUST CONCEPTUAL:**
- These 4-path criteria are boolean pass/fail gates applied in Pass 1 (Qualification)
- Players who pass ANY path proceed to Pass 2 (Attribute Mapping)
- Qualification is separate from ranking - see Section 6.1.4 below
- Current implementation in `pipeline/legend_scores.py` uses weighted percentile scoring and DOES NOT match this design - it needs refactoring

**KNOWN GAPS & EDGE CASES:**
- Pre-1970 Pro Bowl data may be incomplete - use All-Pro as fallback for players with first_season < 1970
- Bo Jackson (cultural icon, 1 PB, 4 seasons) fails qualification but is subjectively legendary - this is an acceptable trade-off for objective criteria
- Long snappers pre-2015 have no Pro Bowl opportunities - rely on PATH 2 longevity criterion (15 seasons)

### 6.1.4 Qualification vs. Ranking (Two-Pass Architecture)

**CRITICAL DISTINCTION:**

The pipeline uses a **two-pass system** that separates who qualifies from how qualified players are ordered:

**PASS 1 - QUALIFICATION (Boolean Pass/Fail):**
- Apply 4-path criteria from Section 6.1 to ALL ~27,000 NFL players
- Each player either passes (≥1 path satisfied) or fails (no paths satisfied)
- Output: ~3,500 qualified players (boolean selection, not ranked)
- Also compute a `qualification_score` metric for each qualified player (used later for validation)
- Record which path(s) each player satisfied for auditing purposes

**PASS 2 - ATTRIBUTE MAPPING:**
- **Only run for the ~3,500 qualified players** (performance optimization)
- Translate stats/honors → 40+ FRCS attributes per player (speed, accuracy, coverage, etc.)
- Apply position-specific mapping recipes (Section 5)
- This is expensive computation - don't waste it on players who won't make the roster

**KEY INSIGHT:**
- Qualification determines WHO is in the pool (binary gate)
- Ranking (Section 6.2) determines ORDER within qualified pool (for tiebreaks, display, etc.)
- Attribute mapping determines WHAT their ratings are (gameplay impact)

**VALIDATION USE:**
- The `qualification_score` from Pass 1 should correlate with final Madden OVR
- If high qualification_score players have low OVR, investigate mapping logic
- If low qualification_score players slip through, tighten qualification criteria

### 6.2 Ranking Within Qualified Pool

Once the ~3,500 qualified players are selected, rank them for tiebreaking and display purposes:

* `RankScore = w1 * PeakScore + w2 * CareerScore + w3 * EraDominance + w4 * HonorsIndex`
* Peak = top-3 consecutive seasons.
* Career = AV, Weighted AV, HOF monitor, honors.
* Era dominance = z-scores of rate+ metrics vs peers.

**NOTE:** This RankScore is for ORDERING within the qualified pool, NOT for qualification itself. Do not use weighted percentile scoring for qualification - use the boolean 4-path criteria from Section 6.1.

### 6.3 Quotas (\~3,500 total)

* QB 180, RB 420, WR 560, TE 220
* OL 760 (OT/OG/C)
* DL/EDGE/LB 1,020
* DB 660
* K/P/RET/LS 240
* Historical flex 100

---

## 7. AI/ML Integration

* **Rule helper:** LLM drafts formulas, fallback rules, unit tests.
* **Attribute predictor models:** Tree models (LightGBM/CatBoost) trained on recent Madden ratings to learn mappings from PFR stats.
* **Cluster labeling:** LLM interprets feature clusters into archetypes.
* **Imputation:** ML predicts missing combine data from physicals/stats.
* **Audit:** LLM reviews distributions for fairness and surfaces outliers.

---

## 8. Adapters (Game-Specific)

Adapters map FRCS → specific Madden schemas. Each adapter is declarative (YAML).

### Example: Madden 26 Adapter (madden\_26.yaml)

```yaml
fields:
  SPD: {from: speed, scale: linear, clamp: [40,99]}
  ACC: {from: acceleration}
  DAC: {from: deep_accuracy}
  MAC: {from: medium_accuracy}
  SAC: {from: short_accuracy}
  MCV: {from: man_coverage}
  ZCV: {from: zone_coverage}
  THP: {from: throw_power}

defaults:
  long_snap_accuracy: 75
```

Adapters are versioned per Madden release (26, 27, …). FRCS stays stable.

---

## 9. Pipeline Flow

**UPDATED TO REFLECT TWO-PASS ARCHITECTURE:**

1. **Ingest:** Load nflverse data, normalize identities.
2. **Normalize:** Clean data, resolve identities, assign era buckets.
3. **Qualification (PASS 1 - Boolean):** Apply 4-path criteria to ALL ~27,000 players → select ~3,500 qualified legends.
4. **Apply quotas:** Ensure position balance within qualified pool (QB 180, RB 420, etc.).
5. **Compute season metrics:** Per player-season stats (ONLY for qualified ~3,500 players).
6. **Peak/career scoring:** Build RankScore for ordering within qualified pool.
7. **Attribute mapping (PASS 2):** Apply recipes/ML → FRCS ratings (40+ attributes per qualified player).
8. **Archetyping:** Cluster & modify attributes based on play style.
9. **Validation:** Sanity checks, outlier detection, qualification_score vs OVR correlation.
10. **Export:** Apply adapter → game schema CSV.
11. **Manifest:** Store FRCS version, config hash, adapter version.

**KEY CHANGES FROM ORIGINAL:**
- Qualification (step 3) now happens BEFORE attribute mapping (step 7)
- Expensive computations (steps 5-7) only run on ~3,500 qualified players, not all 27,000
- Quotas (step 4) applied after boolean qualification, before ranking/mapping
- Added PATH 4 (Award Excellence) to catch MVP/DPOY/SB MVP winners with short careers

---

## 10. Edge Case Handling

* **Two-way players:** Assign primary position by peak value; allow secondary label.
* **Short peaks:** Weight peak windows more heavily.
* **Era gaps (pre-target data):** Use honors/AV proxies.
* **Specialists:** Separate pools.
* **Position changers:** Use best-season position.
* **Data gaps:** Tiered fallbacks with metadata.

---

## 11. Testing & Validation

### 11.1 Standard Tests

* **Golden players:** Sample of 20–30 with known expected ranges.
* **Distribution tests:** Attribute histograms by position.
* **Cross-era fairness check:** Averages across eras should not skew > ±3.
* **Outlier list:** Flag top 100 anomalies.
* **Round-trip check:** Import/export consistency.

### 11.2 Era Fairness Validation (NEW)

**Purpose:** Ensure data availability doesn't create systematic bias where modern players automatically rate higher than historical players.

**Success Metrics:**

1. **Era Distribution Balance:**
   - Mean OVR by era_bucket: ±5 points max variance
   - Visualization: `sns.boxplot(x='era_bucket', y='ovr', data=ratings_df)`
   - **Target:** Pre-1945 mean ~80, 1946-1977 mean ~82, 1978-1993 mean ~83, 2016-present mean ~84
   - Slight modern edge is acceptable (better training, nutrition), but not 15+ points

2. **Source Tier Diversity:**
   - % of attributes by tier: TIER_1 (20%), TIER_2 (15%), TIER_3 (40%), TIER_4 (25%)
   - If TIER_1 > 50%, system is over-relying on combine data (modern bias)
   - If TIER_4 > 40%, system is under-utilizing available stats (imprecision)

3. **Qualification Score vs OVR Correlation:**
   - From Section 6.1.4: "qualification_score should correlate with final OVR"
   - **Target:** Pearson r = 0.6-0.8 (strong but not perfect correlation)
   - If r < 0.4, mapping recipes are broken (qualification doesn't predict rating)
   - If r > 0.9, you're just recomputing the same thing (redundant)

4. **Golden Player Cross-Era Spot Checks:**
   - 30 hand-picked legends across eras (user acceptance testing)
   - **Example comparisons:**
     - Jerry Rice (1985-2000, 98 OVR, TIER_2-3) vs Justin Jefferson (2020+, 95 OVR, TIER_1-2)
     - Night Train Lane (1952-1965, 94 OVR, TIER_3-4) vs Darrelle Revis (2007-2016, 96 OVR, TIER_1-2)
     - Deacon Jones (1961-1974, 95 OVR, TIER_4) vs Myles Garrett (2017+, 94 OVR, TIER_1-2)
   - **Validation question:** "Does this feel right given era context?" (qualitative)

5. **Outlier Detection:**
   - Flag any player with `source_tier=TIER_4` AND `value_0_99 > 95` (investigate)
   - Flag any pre-1970 player with `ovr < 75` (might be under-represented)
   - Flag any 2016+ player with `avg_source_tier > 2.5` (should have TIER_1-2 data)

**Implementation:** `pipeline/validation/era_fairness_audit.py`

**Run frequency:** After every snapshot generation (step 9 in pipeline flow)

**Acceptance criteria:** All 5 metrics pass before snapshot is considered valid for export.

---

## 12. Repo Layout

```
madden-roster/
  frcs/
    __init__.py
    models.py              # Pydantic models for FRCS v1 (Players, Ratings, Metadata, PlayerDataProfile)
    schema_v1.json         # JSON Schema for FRCS (machine-readable validation)
    validation.py          # FRCS validators (ranges, enums, cross-field checks)
    migrations/
      v1_0_to_v1_1.py     # Schema migration: add PlayerDataProfile table, update source_tier enum

  adapters/
    madden_26.yaml         # Declarative adapter: FRCS → Madden 26
    madden_27.yaml         # Future adapter: FRCS → Madden 27
    utils.py               # Safe expr eval, scaling, clamping helpers used by exporters

  data/
    raw/                   # Unprocessed pulls/exports straight from source (CSV/Parquet files)
      players_index.csv    # MASTER PLAYER LIST from nflverse (source of truth for IDs)
      nflverse_exports/    # Raw datasets from nflverse-data (players, rosters, stats, combine)
    staging/               # Temporary joins/intermediate parquet during a run
    snapshots/             # IMMUTABLE versioned datasets produced by pipeline
      2025-08-23/
        players.parquet          # FRCS Players table (identity/bio/era/archetype)
        player_data_profiles.parquet  # NEW: Data availability metadata per player
        ratings.parquet          # FRCS Ratings long-form (player_id, attribute_key, value, source_tier)
        context.parquet          # Optional: season/team context used in mapping
        manifest.json            # Snapshot metadata: commit hash, config checksums, FRCS version, era_recipe_version
    exports/
      madden26/
        2025-08-23/
          roster.csv       # Game-ready CSV produced by adapters/exporter
          export_manifest.json

  pipeline/
    __init__.py
    01_ingest/
      ingest_nflverse.py         # Load nflverse datasets, build unified players_index
      build_data_profiles.py     # NEW: Generate player_data_profiles table (era, has_combine, has_nextgen, etc.)
    02_qualification/
      qualify.py                 # Apply 4-path boolean criteria (PASS 1)
      legend_scores.py           # Compute qualification_score for validation (NEEDS REFACTOR)
    03_attributes/
      __init__.py
      qb.py                      # QB FRCS attribute recipes (PASS 2, tiered fallbacks)
      rb.py
      wr.py
      te.py
      ol.py
      dl_edge.py
      lb.py
      db.py
      k_p_ret.py
      common.py                  # Shared mappers (speed, injury, stamina, apply_era_fairness)
      tier_selection.py          # NEW: select_attribute_tier() logic for fallback recipes
    04_enrichment/
      impute.py                  # ML imputers for missing combine/physicals
      archetypes.py              # Archetype clustering + LLM labeler
      equipment.py               # Equipment, skills, packages (Phase 2)
    05_export/
      export.py                  # FRCS → Adapter → CSV exporter
    validation/
      audits.py                  # Outlier & fairness audits, qualification_score vs OVR checks
      era_fairness_audit.py      # NEW: Era distribution balance, source tier diversity, correlation tests
      spot_checks.py             # Golden player tests, distribution plots
    normalize.py                 # Clean, dedupe, identity resolution; era bucketing (imported by 01_ingest)
    peaks.py                     # Peak-window detection and scoring (imported by 02_qualification)
    rank.py                      # Combine peak/career/era scores → RankScore; apply quotas (imported by 02_qualification)

  notebooks/
    01_eda_pfr.ipynb       # Explore PFR structure/columns
    02_attribute_sanity.ipynb # Visualize distributions & spot-check ratings

  scripts/
    build_players_index.py # CLI: load nflverse datasets → data/raw/players_index.csv
    run_snapshot.py        # CLI: full snapshot build from raw → snapshots/DATE
    make_export.py         # CLI: export given snapshot with selected adapter

  config/
    eras.yaml                    # Era buckets and boundaries (6 eras from Section 3.1)
    weights.yaml                 # Peak/career weights, per-position quotas
    mapping_policies.yaml        # Attribute recipes, caps, floors, archetype nudges (legacy)
    era_attribute_recipes.yaml   # NEW: Tiered fallback recipes per position/attribute (TIER_1/2/3/4)
    era_fairness_thresholds.yaml # NEW: Validation thresholds (mean OVR variance, tier diversity, correlation targets)

  tests/
    test_frcs_validation.py   # Validates FRCS schema & value ranges
    test_adapter_madden26.py  # Adapter mapping & schema compliance tests
    test_percentile_mapping.py# Unit tests for normalization/scaling
    test_peaks_and_ranks.py   # Peak window + ranking logic

  .pre-commit-config.yaml
  pyproject.toml             # Managed by uv/poetry; pins deps & tooling
  README.md                  # Quickstart; links to design doc
```

### Folder-by-folder details

**frcs/**

* *models.py*: Pydantic classes (`Player`, `Rating`, `SnapshotManifest`) with enums and type hints.
* *schema\_v1.json*: JSON Schema used by CI and Great Expectations.
* *validation.py*: Functions for semantic checks; callable from tests and pipeline.
* *migrations/*: Version bump scripts; record how to backfill new fields.

**adapters/**

* *madden\_26.yaml*: Field maps, scales, clamps, enums; declares `supports: [...]`.
* *madden\_27.yaml*: Same pattern for future versions.
* *utils.py*: Helper functions shared by exporter (safe expression eval, linear scaling, rounding).

**data/**

* *raw/*: **Source-of-truth inputs** untouched. Place **`players_index.csv` here** after running `scripts/build_players_index.py`.
* *staging/*: Scratch space for temporary joins/derived tables during a run (ok to delete).
* *snapshots/*: Immutable, versioned Parquet outputs (players/ratings/context + manifest). Treat each dated folder as read-only after creation.
* *exports/*: Final game-ready CSVs per adapter and date. Keep an `export_manifest.json` with adapter id, FRCS version, and git hash.

**pipeline/**

* *01\_ingest/*: Load nflverse datasets, build unified `players_index.csv`, generate `player_data_profiles.parquet` (era, has_combine, has_nextgen, data_richness_score).
* *02\_qualification/*: **PASS 1** - Apply 4-path boolean criteria; compute qualification_score for validation. **NOTE:** `legend_scores.py` currently uses weighted percentile logic and needs refactoring to match 4-path design.
* *03\_attributes/*: **PASS 2** - FRCS attribute mappers by position with tiered fallbacks (TIER_1/2/3/4). Only runs for ~3,500 qualified players. Includes `tier_selection.py` for selecting highest available data tier per attribute.
* *04\_enrichment/*: Archetypes, imputers, equipment (Phase 2 features).
* *05\_export/*: Loads snapshot + adapter YAML → writes game CSV and export manifest.
* *validation/*: Audits, spot checks, qualification_score vs OVR correlation. **NEW:** `era_fairness_audit.py` validates era distribution balance, source tier diversity, and cross-era fairness.
* *normalize.py, peaks.py, rank.py*: Shared utility modules imported by qualification and enrichment stages.

**notebooks/**

* Exploratory work only. Any logic that graduates to production moves to `pipeline/` modules.

**scripts/**

* *build\_players\_index.py*: CLI to load nflverse datasets and write unified `data/raw/players_index.csv`.
* *run\_snapshot.py*: Orchestrates ingest → normalize → peaks → rank → ratings → snapshot write.
* *make\_export.py*: Runs exporter for a chosen snapshot and adapter.

**config/**

* Human-editable YAMLs for eras, weights (quotas + scoring), mapping policies (attribute recipes & caps), and **new era-aware recipes** (tiered fallbacks per position/attribute). Config files are hashed into the snapshot manifest to ensure reproducibility.
* **era_attribute_recipes.yaml**: Defines TIER_1/2/3/4 fallback logic for each attribute (speed, coverage, accuracy, etc.) per position. Each tier specifies: inputs (which stats), formula (how to combine), range (min/max output), confidence (0.0-1.0).
* **era_fairness_thresholds.yaml**: Validation targets for era fairness audits (max OVR variance across eras, source tier distribution targets, correlation thresholds).

**tests/**

* Unit and integration tests covering schema validation, adapter correctness, normalization math, peak/rank logic.

---

## 13. Long-Term Considerations

* Maintain FRCS as stable, semantic schema (semver).
* Add new attributes via **minor bumps**, redefine only with **major bumps**.
* Keep adapters declarative to isolate game-specific changes.
* Store provenance so old runs are reproducible forever.

---

## 14. Manual Overrides System

### 14.1 Philosophy: Determinism + Human Judgment

**CRITICAL DESIGN DECISION:** The pipeline is data-driven and deterministic, but edge cases require human judgment. Manual overrides solve this WITHOUT sacrificing reproducibility.

**Use Cases:**
- **Fixing pipeline bugs**: Algorithm says Bo Jackson has 85 SPD, but he's documented at 4.12 40-time
- **Cultural/historical corrections**: Night Train Lane's dominance not captured by sparse 1950s data
- **Edge cases**: Two-way players (Deion Sanders at CB+WR), position changers
- **Subjective "eye test"**: When stats don't capture greatness (Jim Brown, Lawrence Taylor)

**Non-Goals:**
- Overrides are NOT for "I like this player more" - they require concrete justification
- Overrides are NOT for fixing every rating - if >10% of players need overrides, fix the pipeline recipe instead

### 14.2 Architecture: Post-Pipeline Override Layer

**Overrides are applied AFTER base ratings but BEFORE export:**

```
[Pipeline PASS 2] → [Base Ratings] → [Override Layer] → [Final Ratings] → [Export]
                         ↓                                      ↓
                  [Immutable Snapshot]                [Override Snapshot]
```

**Key Principles:**
1. **Immutable base snapshots**: Pipeline output never modified, preserves reproducibility
2. **Versioned overrides**: YAML files in version control, hashed into manifests
3. **Provenance first-class**: Every override tracks WHO, WHEN, WHY, and base value
4. **Validation by default**: Overrides respect era fairness unless explicitly forced

### 14.3 Schema Extensions (FRCS v1.1)

#### 14.3.1 New Source Tier: TIER_MANUAL

```python
# frcs/models.py

class SourceTier(str, Enum):
    TIER_1 = "TIER_1"  # Direct measurement (combine)
    TIER_2 = "TIER_2"  # Rich stats (NextGen, EPA)
    TIER_3 = "TIER_3"  # Basic stats (yards, TDs)
    TIER_4 = "TIER_4"  # Honors proxy
    TIER_MANUAL = "TIER_MANUAL"  # NEW: Manual override (human judgment)
```

#### 14.3.2 Extended Rating Model

```python
# frcs/models.py (extend existing Rating model)

class Rating(BaseModel):
    player_id: str
    attribute_key: str
    value_0_99: int
    source_tier: SourceTier
    metrics_used: List[str]
    notes: str

    # NEW FIELDS FOR OVERRIDE SUPPORT:
    is_manual_override: bool = False  # Quick filter flag
    override_metadata: Optional[OverrideMetadata] = None  # Full provenance
    base_value: Optional[int] = None  # Original value before override (null if none)
```

#### 14.3.3 Override Provenance Model

```python
# frcs/models.py (new model)

class OverrideMetadata(BaseModel):
    """Tracks WHO, WHEN, WHY for manual overrides"""
    reason: str  # Required (min 20 chars): human-readable justification
    curator: str  # Who made the override (e.g., "paytondennis")
    date_applied: str  # ISO date (YYYY-MM-DD)
    base_value: int  # Original pipeline value
    base_source_tier: SourceTier  # Original source tier
    confidence: Literal["LOW", "MEDIUM", "HIGH"]  # Curator's confidence
    tags: List[str] = []  # Optional (e.g., ["edge_case", "era_adjustment"])
    force: bool = False  # Bypass validation (use sparingly)
```

### 14.4 Override Storage: Position-Specific YAML

Overrides stored in `data/manual_curation/overrides/` as human-editable YAML files:

```yaml
# data/manual_curation/overrides/rb_overrides.yaml

position: RB
overrides:
  - player_id: "00-0002391"  # Bo Jackson
    full_name: "Bo Jackson"  # For readability
    attributes:
      speed:
        value: 99
        reason: |
          Bo Jackson is widely recognized as the fastest player in NFL history.
          Multiple documented sources cite a 4.12 40-yard dash (faster than any
          modern combine result). His legendary runs demonstrate unmatched speed.

          TIER_3 basic stats (85 SPD) don't capture his true ability due to
          limited 4-season sample size. This is a rare case where cultural
          consensus and eye test override statistical methodology.
        curator: "paytondennis"
        date_applied: "2025-08-25"
        confidence: HIGH
        force: true  # Bypass era fairness (pre-1990 RBs typically max at 95)
        tags: ["cultural_icon", "era_outlier", "combine_equivalent"]
```

### 14.5 Snapshot Versioning with Overrides

**Base snapshots remain immutable. Overrides create NEW snapshot variants:**

```
data/snapshots/
  2025-08-23/                     # Base snapshot (no overrides)
    players.parquet
    ratings.parquet               # All is_manual_override = False
    manifest.json

  2025-08-23-overrides-v1/       # First override pass
    players.parquet                # Same as base (symlink or copy)
    ratings.parquet                # Some is_manual_override = True
    manifest.json                  # References base + override YAML hashes
    override_summary.json          # Stats: 12 players, 28 attributes, avg delta +8.3
```

**Manifest Example:**

```json
// data/snapshots/2025-08-23-overrides-v1/manifest.json
{
  "frcs_version": "v1.1",
  "snapshot_date": "2025-08-23",
  "base_snapshot": "data/snapshots/2025-08-23",
  "override_version": "v1",
  "override_files": [
    {
      "path": "data/manual_curation/overrides/rb_overrides.yaml",
      "sha256": "a1b2c3d4..."
    }
  ],
  "override_stats": {
    "total_players": 3500,
    "players_with_overrides": 12,
    "total_overrides": 28,
    "average_delta": 8.3,
    "max_delta": 14
  }
}
```

### 14.6 Validation Rules

Overrides are validated BEFORE application to prevent breaking era fairness:

**Validation Checks:**
1. **Range check**: All attributes must be 40-99
2. **Era fairness**: Override should be within ±10 points of era/position mean (WARN if violated)
3. **Delta check**: Override shouldn't change rating by >15 points (WARN if violated, suggests pipeline bug)
4. **Tier confidence**: Overriding TIER_1 (combine data) requires HIGH confidence

**Validation Outcomes:**
- **PASS**: Override applied silently
- **WARN**: Override applied, logged in audit report for review
- **FAIL**: Override rejected, must fix before applying

**Escape Hatch:** Set `force: true` to bypass validation for exceptional cases (Bo Jackson, cultural icons).

### 14.7 CLI Workflow

```bash
# Add a single override
uv run python scripts/override.py add \
  --player-id "00-0033873" \
  --attribute "throw_power" \
  --value 99 \
  --reason "Strongest arm in NFL history, regularly throws 70+ yard bombs" \
  --curator "paytondennis" \
  --confidence HIGH

# List all overrides for a player
uv run python scripts/override.py list --player-id "00-0033873"

# Remove an override
uv run python scripts/override.py remove \
  --player-id "00-0033873" \
  --attribute "throw_power"

# Apply overrides from YAML to create new snapshot
uv run python scripts/override.py apply \
  --overrides data/manual_curation/overrides/rb_overrides.yaml \
  --snapshot data/snapshots/2025-08-23 \
  --output data/snapshots/2025-08-23-overrides-v1

# Audit overrides (detect drift, outliers, staleness)
uv run python scripts/override.py audit \
  --snapshot data/snapshots/2025-08-23-overrides-v1 \
  --report override_audit.json
```

### 14.8 Risk Mitigation

**Risk 1: Override Creep (>10% of roster becomes manual)**
- **Mitigation**: Quota enforcement (warn if >10% of players have overrides)
- **Detection**: Monthly audit reports track override growth rate
- **Fix**: Each override should include `TODO: fix recipe for [attribute]` to drive pipeline improvement

**Risk 2: Stale Overrides (pipeline improves, overrides become obsolete)**
- **Mitigation**: Staleness detection in audit tool (compare base_value at override time vs current base_value)
- **Alert**: Flag overrides where `abs(override.base_value - current_base_value) > 5`
- **Review**: Overrides >6 months old with MEDIUM confidence get flagged for re-review

**Risk 3: Invisible Justifications (overrides without clear reasons)**
- **Mitigation**: Minimum reason length (20 characters) enforced by CLI
- **Template prompts**: CLI suggests reason templates ("Combine measurement error: ...", "Historical context: ...", etc.)
- **Review checklist**: Before applying, answer: Is this a pipeline bug or subjective judgment? Would 5 experts agree?

**Risk 4: Breaking Reproducibility**
- **Mitigation**: Override YAML in version control (Git tracks all changes)
- **Verification**: Manifest includes SHA256 hashes of override files
- **CI checks**: Pre-commit hook validates YAML syntax + player_id existence

### 14.9 Implementation Phases

**Phase 1 (MVP): Basic Override Support**
- Extend FRCS models with `TIER_MANUAL`, `OverrideMetadata`, new Rating fields
- Create `scripts/override.py` CLI with add/list/remove/apply commands
- Implement validation checks (range, delta, era fairness)
- Write unit tests for override application + validation
- **Time**: 1 week

**Phase 2: Audit & Reporting**
- Implement `override.py audit` command (staleness, drift, fairness violations)
- Create `notebooks/override_review.ipynb` for visual audit (scatter plots, histograms)
- Add `override.py diff` to compare snapshots
- **Time**: 3 days

**Phase 3: Scale to All Positions**
- Create YAML templates for all 10+ positions
- Implement bulk import from position-specific YAML files
- Document override curation workflow in `docs/curation_guide.md`
- **Time**: 2 days

### 14.10 Example: Bo Jackson Speed Override

**Problem**: Pipeline computes Bo Jackson SPD = 85 (TIER_3, only 4 seasons). Culturally recognized as fastest player ever (4.12 unofficial 40-time).

**Override YAML**:
```yaml
# data/manual_curation/overrides/rb_overrides.yaml
overrides:
  - player_id: "00-0002391"
    full_name: "Bo Jackson"
    attributes:
      speed:
        value: 99
        reason: "Cultural icon with documented 4.12 40-yard dash, fastest player in NFL history"
        curator: "paytondennis"
        date_applied: "2025-08-25"
        confidence: HIGH
        force: true  # Bypass era fairness
        tags: ["cultural_icon", "era_outlier"]
```

**Result in ratings.parquet**:
| player_id | attribute_key | value_0_99 | source_tier | is_manual_override | base_value |
|-----------|--------------|------------|-------------|-------------------|------------|
| 00-0002391 | speed | 99 | TIER_MANUAL | TRUE | 85 |

**Key Insight**: Override preserves base_value (85) for auditability. Can always compare base vs override snapshots.

---

## 15. Deliverables

* **FRCS v1 schema** (JSON + Pydantic models).
* **Attribute mapping recipes** per position.
* **AI/ML models** for selected attributes.
* **Madden 26 adapter YAML.**
* **Export tool**: FRCS → Madden schema CSV.
* **Validation tests** and **run manifests.**
* **Manual override system** (NEW): CLI tools, YAML templates, provenance tracking.

---

**In short:** FRCS is the backbone. Rules + AI fill ratings. Adapters handle Madden's schema churn. Manual overrides handle edge cases. The system stays explainable, testable, and usable years into the future.
