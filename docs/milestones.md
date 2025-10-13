# 🏈 Milestones for All-Time Madden Legends Roster

## Overview

This project uses a **two-pass architecture** to build a ~3,500 player All-Time Madden Legends roster:

- **PASS 1 (Qualification)**: Apply 4-path boolean criteria to ALL ~27,000 NFL players → select ~3,500 qualified legends
- **PASS 2 (Attribute Mapping)**: Translate stats/honors → 40+ FRCS attributes per qualified player (era-aware recipes)

**Development Strategy**: Incremental delivery with QB-only v0.1, expanding to all positions in v0.2.

**Milestone Status Legend**:
- ✅ **Done**: Implemented, tested, committed
- 🚧 **In Progress**: Actively working
- ⭕ **Not Started**: Planned, ready to start
- ❌ **Blocked**: Waiting on dependencies

---

## **Milestone 1: Environment & Repo Skeleton** ✅

**Status**: ✅ **Done** (Completed 2025-08-23)

**Outcome**: Working repo with folders, environment, and tests stubbed.

**Deliverables**:
- ✅ Install Python 3.11+ + `uv` package manager
- ✅ Repo structure matches design.md layout (frcs/, pipeline/, adapters/, data/, scripts/, tests/)
- ✅ Pre-commit hooks configured (.pre-commit-config.yaml with ruff, black, mypy)
- ✅ Placeholder files: frcs/models.py, adapters/madden_26.yaml, scripts/build_players_index.py
- ✅ `pytest` runs successfully with placeholder tests

**Acceptance Criteria**:
- [x] `uv run pytest` passes with ≥1 test
- [x] All directories exist and match design.md Section 12
- [x] Pre-commit hooks installed (run `pre-commit install`)

---

## **Milestone 2: Player Index (The Pool)** ✅

**Status**: ✅ **Done** (Completed 2025-08-24)

**Outcome**: Comprehensive `data/raw/players_index_full.csv` with 13,320 players from nflverse-data.

**Deliverables**:
- ✅ Implemented `scripts/build_players_index.py` (730 lines with logging, error handling, validation)
- ✅ Loaded nflverse datasets: players, rosters (1999-2024), combine (1987-2024), draft (1970-2024), nextgen stats
- ✅ Aggregated career stats (seasonal), playoff stats, defensive stats (tackles, sacks, INTs)
- ✅ Merged honors (Pro Bowls, All-Pro, HOF), physical measurements (height, weight, 40-time, bench, vertical)
- ✅ Saved CSV with 38 columns: identity, career spans, offensive/defensive/playoff stats, honors, combine data, next gen stats.
- ✅ Manual curation files: `qb_legends.yaml` (26 QBs), `historical_legends_pre1974.csv` (163 players)

**Acceptance Criteria**:
- [x] CSV exists with ≥13,000 rows (27K players filtered to those with meaningful data)
- [x] Tom Brady, Peyton Manning, Jerry Rice all present with complete stats
- [x] Data spans 1999+ for seasonal stats (nflverse coverage), draft history back to 1970
- [x] No null player_id or full_name values
- [x] Playoff stats present for players with postseason experience

**Known Limitations**:
- Pre-1974 data sparse (addressed by manual curation CSV)
- Some notable players may be missing (e.g., James Harrison TODO in script) - audit before M3

---

## **Milestone 2.5: FRCS Pydantic Models** ⭕

**Status**: ⭕ **Not Started** (BLOCKING M3, M5)

**Outcome**: Fully implemented FRCS v1.1 schema with Pydantic models, validation, and tests.

**Deliverables**:
- ⭕ Implement `frcs/models.py` with Pydantic classes (currently stub-only):
  - `Player` (identity, bio, era_bucket, honors, primary_pos, secondary_pos)
  - `Rating` (player_id, attribute_key, value_0_99, source_tier, metrics_used, notes, is_manual_override, override_metadata, base_value)
  - `PlayerDataProfile` (player_id, era_bucket, available_tiers, has_combine, has_nextgen, has_advanced_stats, data_richness_score)
  - `OverrideMetadata` (reason, curator, date_applied, base_value, base_source_tier, confidence, tags, force)
  - `SnapshotManifest` (frcs_version, snapshot_date, config_hashes, player_count, rating_count)
  - `SourceTier` enum (TIER_1, TIER_2, TIER_3, TIER_4, TIER_MANUAL)
- ⭕ Write `frcs/schema_v1.json` (JSON Schema for external validation)
- ⭕ Implement `frcs/validation.py` (semantic checks: ranges 40-99, enum validation, cross-field checks)
- ⭕ Write unit tests: `tests/test_frcs_validation.py` (test range clamps, enum validation, edge cases)

**Acceptance Criteria**:
- [ ] All Pydantic models defined with type hints
- [ ] `SourceTier` enum includes TIER_MANUAL for overrides
- [ ] Rating model includes override provenance fields (is_manual_override, override_metadata, base_value)
- [ ] PlayerDataProfile model tracks data availability per player
- [ ] `uv run mypy frcs/` passes with no errors
- [ ] `uv run pytest tests/test_frcs_validation.py` passes with ≥10 test cases

**Estimated Effort**: 2-3 days

---

## **Milestone 3: Boolean Legend Qualification (4-Path System)** ❌

**Status**: ❌ **Blocked** (Requires M2.5 FRCS Models) + **CRITICAL REFACTOR REQUIRED**

**Current Implementation Issue**: Existing `pipeline/legend_scores.py` uses **weighted percentile scoring** (position-specific weights × attribute percentiles → legend_score 50-97). This **DOES NOT match** the 4-path boolean qualification criteria from design.md Section 6.1.

**Outcome**: ~3,500 qualified players selected via 4-path boolean criteria (PASS 1).

**Required Changes**:
- ❌ **REFACTOR** `pipeline/legend_scores.py` to implement 4-path boolean logic (NOT percentile scoring)
- ⭕ Create `pipeline/02_qualification/qualify.py` with 4-path boolean pass/fail logic:
  - **PATH 1 (Peak Dominance)**: ≥3 Pro Bowls in ≤8 seasons OR ≥1 All-Pro OR HOF OR Major Award (MVP/DPOY/OPOY/SB MVP)
  - **PATH 2 (Sustained Excellence)**: (≥12 seasons + ≥2 Pro Bowls) OR (≥15 seasons + ≥150 games played)
  - **PATH 3 (Statistical Dominance)**: Top-5 all-time in position primary stat (era-adjusted) + ≥6 seasons OR (Draft ≤10 + ≥8 seasons + ≥1 honor)
  - **PATH 4 (Award Excellence)**: ≥1 Major Award (MVP/DPOY/OPOY/SB MVP) + ≥3 seasons + ≥32 games
- ⭕ Also compute `qualification_score` (numeric metric 0-100 for validation, NOT for filtering)
- ⭕ Output `data/snapshots/<DATE>/qualified_players.parquet` with columns:
  - `player_id`, `full_name`, `position`, `qualified` (bool), `qualification_score` (float), `path_satisfied` (list), `qualification_reason` (str)

**Validation Requirements (MUST PASS BEFORE M4)**:

```python
# BLOCKING VALIDATION GATE
def test_qualification_validation_gate():
    """M3 → M4 transition gate: Required test cases from design.md Section 6.1."""

    # Required PASSES
    assert mahomes['qualified'] == True      # 6 PB/7 seasons, 3 AP, 2 MVP → PATH 1 + PATH 4
    assert jefferson['qualified'] == True    # 4 PB/4 seasons, 2 AP → PATH 1
    assert nelson['qualified'] == True       # 7 PB/7 seasons, 3 AP → PATH 1
    assert t_davis['qualified'] == True      # 3 PB/7 seasons, 2 AP, 1 MVP → PATH 1 + PATH 4
    assert warner['qualified'] == True       # 4 PB/12 seasons, 2 AP, 2 MVP, HOF → PATH 1 + PATH 2 + PATH 4

    # Required FAILURES
    assert bo_jackson['qualified'] == False  # 1 PB/4 seasons, 0 AP, 38 games → FAILS ALL PATHS
    assert butler['qualified'] == False      # 1 PB/7 seasons, 0 AP → FAILS ALL PATHS

    # Pool size check
    qualified_count = len([p for p in results if p['qualified']])
    assert 3000 <= qualified_count <= 4000, f"Expected ~3500, got {qualified_count}"
```

**Acceptance Criteria**:
- [ ] All 7 validation test cases pass (5 passes, 2 failures)
- [ ] Qualified pool size: 3,000-4,000 players
- [ ] Each qualified player has ≥1 path_satisfied entry
- [ ] qualification_score computed for all qualified players (range 0-100)
- [ ] Output parquet file saved with manifest.json (config hashes, FRCS version)
- [ ] Existing `tests/test_legend_scores.py` updated to test 4-path logic (not percentiles)

**Estimated Effort**: 3-5 days (refactor + validation + debugging)

---

## **Milestone 4: Apply Position Quotas** ⭕

**Status**: ⭕ **Not Started** (Depends on M3 Qualification)

**Outcome**: Balanced roster pool with quotas enforced (~3,500 total from qualified pool).

**Deliverables**:
- ⭕ Add position quotas to `config/weights.yaml`:
  - QB: 180, RB: 420, WR: 560, TE: 220
  - OL: 760 (OT/OG/C), DL/EDGE/LB: 1,020, DB: 660
  - K/P/RET/LS: 240, Historical Flex: 100
- ⭕ Implement `pipeline/rank.py`:
  - Within qualified pool (M3 output), rank players by RankScore (peak/career/era/honors)
  - If position has MORE qualified players than quota → take top-X by RankScore
  - If position has FEWER qualified players than quota → take all, note shortfall in manifest
- ⭕ Output `data/snapshots/<DATE>/roster_pool.parquet` with columns:
  - `player_id`, `position`, `qualified`, `rank_score`, `roster_slot` (int or null), `quota_status` (made_cut | below_quota | cut)
- ⭕ Save selection manifest: `data/snapshots/<DATE>/quota_manifest.json` (per-position stats: quota, qualified, selected, cut_count)

**Acceptance Criteria**:
- [ ] Total selected players: 3,400-3,600 (target ~3,500)
- [ ] Each position quota enforced (QB ≤180, RB ≤420, etc.)
- [ ] If QB has 250 qualified → take top 180 by RankScore
- [ ] If LS has 15 qualified → take all 15, note "shortfall: 225" in manifest
- [ ] Manifest includes cut players for audit (who barely missed quota, for manual review)

**Estimated Effort**: 1-2 days

---

## **Milestone 5: FRCS Attribute Mapping (PASS 2 - QB Only for v0.1)** ⭕

**Status**: ⭕ **Not Started** (Depends on M2.5 FRCS Models, M3 Qualification, M4 Quotas)

**Outcome**: First-pass Madden-style FRCS ratings (rules only) for ~180 qualified QBs.

**Phase Strategy**: Start with QB-only for v0.1 to validate end-to-end pipeline before scaling to all positions.

**Deliverables**:
- ⭕ Create `config/era_attribute_recipes.yaml` with QB attribute recipes (~15 attributes):
  - Passing: `throw_power`, `deep_accuracy`, `medium_accuracy`, `short_accuracy`, `throw_on_run`
  - Mobility: `speed`, `acceleration`, `agility`, `break_tackle`
  - Mental: `awareness`, `play_action`, `throw_under_pressure`
  - Physical: `injury`, `stamina`, `toughness`
- ⭕ Implement `pipeline/03_attributes/qb.py` with tiered fallback recipes:
  - **TIER_1**: Combine data (40-time, 3-cone) → speed/acceleration
  - **TIER_2**: EPA, air yards, completion % over expectation → accuracy attributes
  - **TIER_3**: Basic stats (yards, TDs, INTs, sack rate) → accuracy/awareness
  - **TIER_4**: All-Pro count, HOF status, era context → positional averages + bonus
- ⭕ Implement `pipeline/03_attributes/common.py`:
  - `percentile_to_scale()`: Convert era percentile → Madden scale (40-99)
  - `apply_era_fairness()`: Adjust for data availability bias (TIER_1: -0, TIER_2: -2, TIER_3: -4, TIER_4: -6 penalty)
  - `select_attribute_tier()`: Choose highest available tier for each player
- ⭕ Output `data/snapshots/<DATE>/frcs_ratings.parquet` (long-form):
  - Columns: `player_id`, `attribute_key`, `value_0_99`, `source_tier`, `metrics_used`, `notes`
  - ~180 QBs × 15 attributes = ~2,700 rows
- ⭕ Compute OVR (overall rating) from attributes for validation

**Validation Requirements**:
- [ ] All attributes in range 40-99 (no violations)
- [ ] qualification_score from M3 correlates with OVR (Pearson r = 0.6-0.8)
- [ ] Otto Graham (TIER_4) rates 95-99 OVR, not 82 (era fairness check)
- [ ] Modern QB with NextGen data (TIER_2) and historical QB with basic stats (TIER_3) have comparable OVRs if both are HOF legends
- [ ] Source tier distribution: TIER_1 (~20%), TIER_2 (~15%), TIER_3 (~40%), TIER_4 (~25%)

**Acceptance Criteria**:
- [ ] QB attributes implemented with tiered fallbacks (TIER_1/2/3/4)
- [ ] Era fairness adjustment applied (confidence penalties by tier)
- [ ] Sanity check histograms (no crazy 99s, normal distribution around 75-90)
- [ ] Unit tests: `tests/test_qb_attributes.py` with ≥20 test cases (edge cases, tier selection, range validation)
- [ ] qualification_score vs OVR correlation: r ≥ 0.6

**Estimated Effort**: 5-7 days (recipes + era logic + tests + debugging)

---

## **Milestone 6: Archetypes & Modifiers** ⭕

**Status**: ⭕ **Deferred to v0.2** (Not needed for v0.1 basic ratings)

**Outcome**: Style-based diversity in ratings (deep threat WR vs possession WR).

**Deliverables** (v0.2):
- Cluster players by play style (k-means on normalized stats)
- Apply ±3 attribute nudges based on archetype (e.g., "deep threat" +3 SPD, -2 RTE)
- Store archetype label in `players.parquet` (archetype_primary, archetype_confidence)
- Optional: LLM labeling for archetype names

**Rationale for Deferral**: Basic attribute mapping (M5) provides 90% of value. Archetypes are polish for v1.0.

---

## **Milestone 6.5: Manual Override System** ⭕

**Status**: ⭕ **Deferred to v0.2** (Can manually edit YAML for v0.1)

**Outcome**: CLI tool for adding/applying manual overrides with provenance tracking.

**Deliverables** (v0.2):
- Implement `scripts/override.py` CLI with commands:
  - `add`: Add single override (player_id, attribute, value, reason, curator, confidence)
  - `list`: Show all overrides for a player
  - `remove`: Remove an override
  - `apply`: Apply overrides from YAML to snapshot, create new snapshot variant
  - `audit`: Detect stale overrides, drift, fairness violations
- Validation rules: range check (40-99), era fairness (±10 from position mean), delta check (±15 from base_value)
- Provenance tracking: Every override records WHO, WHEN, WHY, base_value, confidence
- Override application workflow: Base snapshot → override layer → final snapshot

**Rationale for Deferral**: YAML structure exists (`qb_legends.yaml`). For v0.1, manually edit YAML. Build CLI for v0.2 when scaling to all positions.

---

## **Milestone 7: Adapter & Export (Madden 26 CSV - QB Only for v0.1)** ⭕

**Status**: ⭕ **Not Started** (Depends on M5 Attributes)

**Outcome**: Game-ready Madden 26 QB roster CSV.

**Deliverables**:
- ⭕ Implement `adapters/madden_26.yaml` with QB field mappings:
  ```yaml
  fields:
    SPD: {from: speed, scale: linear, clamp: [40,99]}
    ACC: {from: acceleration}
    THP: {from: throw_power}
    DAC: {from: deep_accuracy}
    MAC: {from: medium_accuracy}
    SAC: {from: short_accuracy}
    AWR: {from: awareness}
    # ... 15-20 total QB attributes
  ```
- ⭕ Implement `pipeline/05_export/export.py`:
  - Load `frcs_ratings.parquet` (long-form) → pivot to wide-form (player × attributes)
  - Apply adapter mappings (FRCS → Madden 26 field names)
  - Add position-specific defaults (e.g., long_snap_accuracy: 75 for non-LS)
  - Save to `data/exports/madden26/<DATE>/qb_roster.csv`
- ⭕ Generate `data/exports/madden26/<DATE>/export_manifest.json`:
  - Adapter version, FRCS version, snapshot date, git commit hash
  - Player count (QB: 180), attribute coverage (15 attributes mapped)

**Acceptance Criteria**:
- [ ] CSV has 180 rows (1 per qualified QB)
- [ ] All Madden 26 QB attributes present (SPD, ACC, THP, DAC, MAC, SAC, AWR, etc.)
- [ ] All values in range 40-99 (no violations)
- [ ] CSV validates against Madden 26 schema (if available)
- [ ] Manifest includes git hash for reproducibility

**Estimated Effort**: 2-3 days

---

## **Milestone 8: Validation & Audits** ⭕

**Status**: ⭕ **Not Started** (Depends on M5 Attributes, M7 Export)

**Outcome**: Confidence that ratings make sense and are era-fair.

**Deliverables**:
- ⭕ Implement `pipeline/validation/era_fairness_audit.py` with 5 metrics from design Section 11.2:
  1. **Era Distribution Balance**: Mean OVR by era_bucket (±5 points max variance)
  2. **Source Tier Diversity**: % of attributes by tier (TIER_1: 20%, TIER_2: 15%, TIER_3: 40%, TIER_4: 25%)
  3. **Qualification Score vs OVR Correlation**: Pearson r = 0.6-0.8
  4. **Golden Player Spot Checks**: 30 hand-picked QB legends (Otto Graham 95-99, Mahomes 98-100, etc.)
  5. **Outlier Detection**: Flag TIER_4 with value >95, pre-1970 with OVR <75, 2016+ with avg tier >2.5
- ⭕ Implement `pipeline/validation/spot_checks.py`:
  - Load golden player list (30 QBs from manual curation)
  - Compare actual OVR vs expected range
  - Generate spot-check report (CSV with player_id, expected_range, actual_ovr, delta, pass/fail)
- ⭕ Generate validation report: `data/snapshots/<DATE>/validation_report.json`

**Acceptance Criteria**:
- [ ] All 5 era fairness metrics pass
- [ ] Golden player spot checks: ≥90% within expected range (≤3 outliers allowed)
- [ ] No TIER_4 players with OVR >95 (unless manually curated)
- [ ] No pre-1970 players with OVR <75 (era bonus applied correctly)
- [ ] Validation report saved with pass/fail summary

**Estimated Effort**: 2-3 days

---

## **Milestone 9: AI/ML Enhancements (Optional Upgrade Path)** ⭕

**Status**: ⭕ **Deferred to v1.0** (Rule-based system sufficient for v0.1)

**Outcome**: Smarter, less hand-tuned ratings using ML models.

**Deliverables** (v1.0):
- Train LightGBM/CatBoost on recent Madden ratings (supervised learning for QB accuracy, coverage, etc.)
- Implement imputation models for missing combine data (predict 40-time from height/weight/position)
- Integrate LLM for archetype labeling and mapping explanation
- Hybrid rules + ML: Use ML predictions for TIER_2, rules for TIER_1/4

**Rationale for Deferral**: Rule-based system (M5) is explainable and deterministic. ML adds precision but not fundamentally new capabilities. Focus on v0.1 → v0.2 scale-out first.

---

## **Milestone 10: Scale to All Positions (v0.2)** ⭕

**Status**: ⭕ **Post-v0.1** (Expand after QB-only validation)

**Outcome**: Full ~3,500 player roster with all positions.

**Deliverables**:
- Implement attribute mappers for all positions:
  - `pipeline/03_attributes/rb.py` (RB: speed, elusiveness, trucking, catching)
  - `pipeline/03_attributes/wr.py` (WR: speed, route running, catching, release)
  - `pipeline/03_attributes/te.py` (TE: blocking, receiving, speed)
  - `pipeline/03_attributes/ol.py` (OL: run block, pass block, strength, awareness)
  - `pipeline/03_attributes/dl_edge.py` (DL/EDGE: power moves, finesse moves, pursuit)
  - `pipeline/03_attributes/lb.py` (LB: tackling, coverage, pursuit, play recognition)
  - `pipeline/03_attributes/db.py` (DB: man coverage, zone coverage, speed, press)
  - `pipeline/03_attributes/k_p_ret.py` (K/P/RET: kick power, kick accuracy, hang time)
- Expand `config/era_attribute_recipes.yaml` with position-specific recipes (~40 attributes × 9 positions)
- Run validation audits for all positions (era fairness, golden players, outliers)
- Export full Madden 26 roster CSV (~3,500 players)

**Estimated Effort**: 20-30 days (40+ attributes × 9 positions with testing)

---

# 🎯 Recommended Roadmap (Deliverable-Based)

**v0.1 (QB-Only Roster - MVP):**
- ✅ M1: Repo structure (Done)
- ✅ M2: Player index (Done)
- ⭕ M2.5: FRCS models (2-3 days)
- ❌ M3: 4-path qualification + validation (3-5 days) **CRITICAL REFACTOR**
- ⭕ M4: Apply quotas (1-2 days)
- ⭕ M5: QB attributes (5-7 days)
- ⭕ M7: Export (2-3 days)
- ⭕ M8: Validation (2-3 days)

**Total v0.1 Effort: 15-23 days**

**v0.2 (All Positions):**
- M5 (expanded): All position attributes (20-30 days)
- M6: Archetypes (3-5 days)
- M6.5: Manual override CLI (2-3 days)
- M10: Full roster export (2-3 days)

**v1.0 (Polish & AI):**
- M9: AI/ML enhancements (10-15 days)
- Full validation suite (Great Expectations, CI/CD)
- Comprehensive documentation

---

# 📋 Next Steps (Prioritized)

1. **Audit Player Index** (2 hours):
   - Verify 26 manually curated QBs from `qb_legends.yaml` exist in `players_index_full.csv`
   - Check for missing notable players (James Harrison TODO in build script)
   - Document any gaps for manual curation

2. **Implement M2.5 (FRCS Models)** (2-3 days):
   - Write Pydantic models in `frcs/models.py`
   - Implement validation logic in `frcs/validation.py`
   - Write unit tests in `tests/test_frcs_validation.py`
   - Generate JSON schema (`frcs/schema_v1.json`)

3. **Refactor M3 (Qualification)** (3-5 days):
   - Create `pipeline/02_qualification/qualify.py` with 4-path boolean logic
   - Implement validation test cases (7 required tests from design Section 6.1)
   - Run against `players_index_full.csv` → output `qualified_players.parquet`
   - Verify pool size: 3,000-4,000 players

4. **Draft Config Files** (parallel with M2.5/M3):
   - `config/eras.yaml`: 6 era buckets with year ranges
   - `config/weights.yaml`: Position quotas, peak/career weights
   - `config/era_attribute_recipes.yaml`: QB recipes (stub for M5)

5. **Implement M4 (Quotas)** (1-2 days):
   - Create `pipeline/rank.py` with RankScore logic
   - Apply quotas to qualified pool
   - Output `roster_pool.parquet` with ~3,500 players

6. **Implement M5 (QB Attributes)** (5-7 days):
   - Write QB attribute recipes in `config/era_attribute_recipes.yaml`
   - Implement `pipeline/03_attributes/qb.py` with tiered fallbacks
   - Run validation: correlation check, era fairness, sanity histograms
   - Output `frcs_ratings.parquet`

7. **Implement M7 (Export)** (2-3 days):
   - Populate `adapters/madden_26.yaml` with QB mappings
   - Implement `pipeline/05_export/export.py`
   - Generate QB roster CSV (180 players)

8. **Implement M8 (Validation)** (2-3 days):
   - Write era fairness audit (5 metrics)
   - Run golden player spot checks (30 QBs)
   - Generate validation report

---

**Critical Decision Point**: Before starting M2.5, confirm:
- Are you ready to commit to QB-only for v0.1? (Deferring other positions to v0.2)
- Do you want to pause and audit the player index first? (Check for missing players)
- Should we prioritize fixing M3 (qualification refactor) over M2.5 (FRCS models)?
