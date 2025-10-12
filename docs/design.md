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
* `source_tier` (1–4: direct → fallback)
* `metrics_used`
* `notes`

### 4.3 Metadata

* Run manifest: FRCS version, config hash, PFR snapshot date.
* Provenance logs for each attribute.

---

## 5. Attribute Mapping Framework

Every rating follows a **4-step pipeline**:

1. **Inputs:** 1–3 primary stats per attribute, per position, per era.
2. **Normalization:** Convert to percentile within era & position.
3. **Mapping:** Percentile → Madden scale (default 40–99, tunable).
4. **Modifiers:** Apply archetype/style adjustments (± up to 5).

### Example: WR Speed

* If 40 time exists: percentile among WRs in era → 70–99.
* Else: blend explosive-play rate + YAC + KR/PR average.
* Apply archetype modifier (e.g., deep threat +3).

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

* **Golden players:** Sample of 20–30 with known expected ranges.
* **Distribution tests:** Attribute histograms by position.
* **Cross-era fairness check:** Averages across eras should not skew > ±3.
* **Outlier list:** Flag top 100 anomalies.
* **Round-trip check:** Import/export consistency.

---

## 12. Repo Layout

```
madden-roster/
  frcs/
    __init__.py
    models.py              # Pydantic models for FRCS v1 (Players, Ratings, Metadata)
    schema_v1.json         # JSON Schema for FRCS (machine-readable validation)
    validation.py          # FRCS validators (ranges, enums, cross-field checks)
    migrations/
      v1_0_to_v1_1.py     # Example forward migration for schema changes

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
        players.parquet    # FRCS Players table (identity/bio/era/archetype)
        ratings.parquet    # FRCS Ratings long-form (player_id, attribute_key, value)
        context.parquet    # Optional: season/team context used in mapping
        manifest.json      # Snapshot metadata: commit hash, config checksums, FRCS version
    exports/
      madden26/
        2025-08-23/
          roster.csv       # Game-ready CSV produced by adapters/exporter
          export_manifest.json

  pipeline/
    __init__.py
    01_ingest/
      ingest_nflverse.py   # Load nflverse datasets, build unified players_index
    02_qualification/
      qualify.py           # Apply 4-path boolean criteria (PASS 1)
      legend_scores.py     # Compute qualification_score for validation (NEEDS REFACTOR)
    03_attributes/
      __init__.py
      qb.py                # QB FRCS attribute recipes (PASS 2, rules + ML blend)
      rb.py
      wr.py
      te.py
      ol.py
      dl_edge.py
      lb.py
      db.py
      k_p_ret.py
      common.py            # Shared mappers (speed, injury, stamina, etc.)
    04_enrichment/
      impute.py            # ML imputers for missing combine/physicals
      archetypes.py        # Archetype clustering + LLM labeler
      equipment.py         # Equipment, skills, packages (Phase 2)
    05_export/
      export.py            # FRCS → Adapter → CSV exporter
    validation/
      audits.py            # Outlier & fairness audits, qualification_score vs OVR checks
      spot_checks.py       # Golden player tests, distribution plots
    normalize.py           # Clean, dedupe, identity resolution; era bucketing (imported by 01_ingest)
    peaks.py               # Peak-window detection and scoring (imported by 02_qualification)
    rank.py                # Combine peak/career/era scores → RankScore; apply quotas (imported by 02_qualification)

  notebooks/
    01_eda_pfr.ipynb       # Explore PFR structure/columns
    02_attribute_sanity.ipynb # Visualize distributions & spot-check ratings

  scripts/
    build_players_index.py # CLI: load nflverse datasets → data/raw/players_index.csv
    run_snapshot.py        # CLI: full snapshot build from raw → snapshots/DATE
    make_export.py         # CLI: export given snapshot with selected adapter

  config/
    eras.yaml              # Era buckets and boundaries
    weights.yaml           # Peak/career weights, per-position quotas
    mapping_policies.yaml  # Attribute recipes, caps, floors, archetype nudges

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

* *01\_ingest/*: Functions to load nflverse datasets and build unified `players_index.csv` from multiple data sources.
* *02\_qualification/*: **PASS 1** - Apply 4-path boolean criteria; compute qualification_score for validation. **NOTE:** `legend_scores.py` currently uses weighted percentile logic and needs refactoring to match 4-path design.
* *03\_attributes/*: **PASS 2** - FRCS attribute mappers by position; split into small testable functions (e.g., `map_wr_speed(row, era_ctx)`). Only runs for ~3,500 qualified players.
* *04\_enrichment/*: Archetypes, imputers, equipment (Phase 2 features).
* *05\_export/*: Loads snapshot + adapter YAML → writes game CSV and export manifest.
* *validation/*: Audits, spot checks, qualification_score vs OVR correlation tests.
* *normalize.py, peaks.py, rank.py*: Shared utility modules imported by qualification and enrichment stages.

**notebooks/**

* Exploratory work only. Any logic that graduates to production moves to `pipeline/` modules.

**scripts/**

* *build\_players\_index.py*: CLI to load nflverse datasets and write unified `data/raw/players_index.csv`.
* *run\_snapshot.py*: Orchestrates ingest → normalize → peaks → rank → ratings → snapshot write.
* *make\_export.py*: Runs exporter for a chosen snapshot and adapter.

**config/**

* Human-editable YAMLs for eras, weights (quotas + scoring), and mapping policies (attribute recipes & caps). These are hashed into the snapshot manifest.

**tests/**

* Unit and integration tests covering schema validation, adapter correctness, normalization math, peak/rank logic.

---

## 13. Long-Term Considerations

* Maintain FRCS as stable, semantic schema (semver).
* Add new attributes via **minor bumps**, redefine only with **major bumps**.
* Keep adapters declarative to isolate game-specific changes.
* Store provenance so old runs are reproducible forever.

---

## 14. Deliverables

* **FRCS v1 schema** (JSON + Pydantic models).
* **Attribute mapping recipes** per position.
* **AI/ML models** for selected attributes.
* **Madden 26 adapter YAML.**
* **Export tool**: FRCS → Madden schema CSV.
* **Validation tests** and **run manifests.**

---

**In short:** FRCS is the backbone. Rules + AI fill ratings. Adapters handle Madden’s schema churn. The system stays explainable, testable, and usable years into the future.
