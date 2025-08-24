# Design Document: All-Time Madden Legends Roster Generator

## 1. Overview

We want to create a **systematic, programmatic pipeline** that generates an All-Time Madden 26 Legends roster (≈3,500 players). The system uses **Pro Football Reference (PFR)** as the primary data source and translates football stats, honors, and physicals into **Madden attributes**. The design emphasizes:

* **Reproducibility:** Deterministic runs with versioned configs and data snapshots.
* **Explainability:** Every attribute traceable to data and documented rules.
* **Future-proofing:** Game-agnostic core schema (FRCS) with adapters for Madden 26 and beyond.

---

## 2. System Goals

* Build a **canonical player ratings dataset** (FRCS) decoupled from Madden’s shifting schemas.
* Use **rules + AI/ML hybrid mapping** to convert football data into Madden-style 0–99 ratings.
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

**THREE LEGEND QUALIFICATION PATHS (any path qualifies):**

**PATH 1 - PEAK DOMINANCE** (addresses short amazing careers):
- ≥3 Pro Bowls in ≤8 seasons (high peer recognition rate)
  * Examples: Quenton Nelson (7 PB/7 seasons), Andrew Luck (4 PB/7 seasons)
- OR ≥1 All-Pro selection in any span (top 1% at position)
- OR Hall of Fame flag (automatic legend status)
- Minimum: >32 total games (injury/early retirement protection)

**PATH 2 - SUSTAINED EXCELLENCE** (longevity + recognition):
- ≥12 seasons + ≥1 Pro Bowl (durability + peer recognition)
  * Examples: Andy Lee (19 seasons/3 PB), Duane Brown (16 seasons/5 PB)
- OR ≥15 seasons regardless of honors (pure longevity value)
  * Examples: Morten Andersen (26 seasons), Sebastian Janikowski (19 seasons)

**PATH 3 - POSITIONAL IMPACT** (high investment + performance):
- Draft pick ≤10 + ≥8 seasons active (early investment + durability)
- OR Top-5 position in career stats + ≥6 seasons (statistical dominance)
- OR Team continuity: same team ≥10 seasons + playoff appearances

**IMPLEMENTATION NOTES:**
- All criteria work for any position (OL, specialists, defense, offense)
- No subjective "clutch" metrics - only measurable peer/organizational recognition  
- Era-neutral: Pro Bowls are relative to contemporaries in each season
- Handles both short peaks (Path 1) and long careers (Path 2) objectively

### 6.2 Peak vs Career Scoring

* `RankScore = w1 * PeakScore + w2 * CareerScore + w3 * EraDominance + w4 * HonorsIndex`
* Peak = top-3 consecutive seasons.
* Career = AV, Weighted AV, HOF monitor, honors.
* Era dominance = z-scores of rate+ metrics vs peers.

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

1. **Ingest:** Load PFR exports, normalize identities.
2. **Compute season metrics:** Per player-season by position.
3. **Era normalization:** Percentiles/z-scores.
4. **Peak/career scoring:** Build RankScore.
5. **Roster selection:** Apply quotas & cutoffs.
6. **Attribute mapping:** Apply recipes/ML → FRCS ratings.
7. **Archetyping:** Cluster & modify attributes.
8. **Validation:** Sanity checks, outlier detection.
9. **Export:** Apply adapter → game schema CSV.
10. **Manifest:** Store FRCS version, config hash, adapter version.

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
    ingest_nflverse.py     # Load nflverse datasets, build unified players_index
    normalize.py           # Clean, dedupe, identity resolution; era bucketing
    peaks.py               # Peak-window detection and scoring
    rank.py                # Combine peak/career/era scores → RankScore; apply quotas
    ratings/
      __init__.py
      qb.py                # QB attribute recipes (rules + ML blend)
      rb.py
      wr.py
      te.py
      ol.py
      dl_edge.py
      lb.py
      db.py
      k_p_ret.py
      common.py            # Shared mappers (speed, injury, stamina, etc.)
    ai/
      impute.py            # ML imputers for missing combine/physicals
      predict_attributes.py# LightGBM/CatBoost models predicting ratings from stats
      cluster_labels.py    # Archetype clustering + LLM labeler
      audits.py            # LLM/heuristics for outlier & fairness audits
    export.py              # FRCS → Adapter → CSV exporter

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

* *ingest\_nflverse.py*: Functions to load nflverse datasets and build unified `players_index.csv` from multiple data sources.
* *normalize.py*: Identity cleanup, name diacritics, position harmonization, era buckets.
* *peaks.py / rank.py*: Peak window finder + composite ranking and quota allocation.
* *ratings/*: Attribute mappers by position; split into small testable functions (e.g., `map_wr_speed(row, era_ctx)`).
* *ai/*: ML components (imputers, predictors, clustering, audits) gated by configs so you can disable easily.
* *export.py*: Loads snapshot + adapter YAML → writes game CSV and export manifest.

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
