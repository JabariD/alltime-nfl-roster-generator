Perfect — let’s break this monster into **bite-sized milestones** so you’re not stuck in “analysis paralysis.” Think of these as **checkpoints** that you can realistically hit in order, each one delivering something tangible you can look at.

---

# 🏈 Milestones for All-Time Madden Legends Roster

## **Milestone 1: Environment & Repo Skeleton**

✅ Outcome: You have a working repo with folders, env, and tests stubbed.

* Install VS Code + Python 3.11 + `uv` (or poetry).
* Clone/init repo with the structure we outlined.
* Add `.pre-commit-config.yaml` (ruff, black, mypy, nbstripout).
* Add empty `models.py`, `adapters/madden_26.yaml`, and `scripts/build_players_index.py`.
* Run `pytest` → passes with just placeholder tests.
  👉 Now you’ve got scaffolding.

---

## **Milestone 2: Player Index (the pool)**

✅ Outcome: You have `data/raw/players_index.csv` with all NFL players and IDs.

* Implement `scripts/build_players_index.py` to load nflverse datasets via nflreadr.
* Combine `load_players()`, `load_rosters()`, `load_draft_picks()`, and `load_combine()`.
* Save CSV with: `player_id, full_name, first_year, last_year, primary_pos, teams, draft_info`.
* Sanity check: \~27k+ rows, Tom Brady, Peyton Manning, Jerry Rice all present.
* Data spans 2002+ with comprehensive biographical and positional data.
  👉 Now you can browse the pool.

---

## **Milestone 3: Boolean Legend Qualification (3-Path System)**

✅ Outcome: ~3,500 qualified players selected via boolean 3-path criteria (PASS 1).

* **CRITICAL:** This implements the 3-path boolean qualification from design.md Section 6.1:
  * PATH 1 (Peak Dominance): ≥3 Pro Bowls in ≤8 seasons OR ≥1 All-Pro OR HOF
  * PATH 2 (Sustained Excellence): ≥12 seasons + ≥1 Pro Bowl OR ≥15 seasons
  * PATH 3 (Positional Impact): Draft pick ≤10 + ≥8 seasons OR top-5 career stats + ≥6 seasons
* Implement `pipeline/02_qualification/qualify.py` with boolean pass/fail logic (NOT weighted percentile).
* Also compute `qualification_score` metric (numeric) for validation purposes (should correlate with final OVR).
* Output `snapshots/<DATE>/qualified_players.parquet` with: `player_id, position, qualified (bool), qualification_score, path_satisfied`.

**VALIDATION REQUIREMENTS (MUST PASS BEFORE MILESTONE 4):**
* Patrick Mahomes MUST qualify (6 PB in 7 seasons, 3 All-Pros → PATH 1)
* Justin Jefferson MUST qualify (4 PB in 4 seasons, 2 All-Pros → PATH 1)
* Quenton Nelson MUST qualify (7 PB in 7 seasons, 3 All-Pros → PATH 1)
* Random backup players (e.g., <2 seasons, no honors) MUST NOT qualify
* Expected output: ~3,500 qualified players

**CURRENT IMPLEMENTATION ISSUE:**
* Existing `pipeline/legend_scores.py` uses weighted percentile scoring (favors longevity over peak)
* This DOES NOT match the design - needs complete refactor to boolean 3-path logic
* Do NOT proceed to Milestone 4/5 until validation passes

👉 Now you have the qualified pool for attribute mapping (next milestone).

---

## **Milestone 4: Apply Position Quotas**

✅ Outcome: A balanced roster pool with quotas enforced (~3,500 total).

* **DEPENDS ON:** Milestone 3 qualification must pass validation first.
* Add position quotas to `config/weights.yaml` (QB 180, RB 420, WR 560, TE 220, OL 760, DL/EDGE/LB 1,020, DB 660, K/P/LS 240).
* Within qualified pool, apply quotas using RankScore (peak/career/honors) for tiebreaking.
* If a position has more qualified players than quota → rank by RankScore and take top-X.
* If a position has fewer qualified players than quota → take all qualified, note shortfall.
* Output `snapshots/<DATE>/roster_pool.parquet` with: `player_id, position, qualified, rank_score, roster_slot`.
* Save selection manifest showing who made quota vs. who was cut.

👉 Now you have a balanced "All-Time Legends Pool" ready for attribute mapping.

---

## **Milestone 5: FRCS Attribute Mapping (PASS 2 - Qualified Players Only)**

✅ Outcome: First pass Madden-style FRCS ratings (rules only) for ~3,500 qualified players.

* **EFFICIENCY NOTE:** Only compute attributes for qualified players from Milestone 3 (~3,500), NOT all 27,000 NFL players.
* Implement mappers in `pipeline/03_attributes/` for 1–2 positions (start with QB only for v0.1).
* Use era percentiles → 40–99 mapping for each FRCS attribute (speed, throw_power, deep_accuracy, etc.).
* Output `snapshots/<DATE>/frcs_ratings.parquet` with columns: `player_id, attribute_key, value_0_99, source_tier, metrics_used`.
* Sanity check with histograms (no crazy 99s, distributions look good).
* **VALIDATION:** Check that qualification_score from Milestone 3 correlates with computed OVR from attributes.

**PHASED APPROACH:**
* v0.1 (Phase 1): QB attributes only → validate end-to-end pipeline
* v0.2 (Phase 2): All positions → full roster
* v1.0 (Phase 3): Add enrichment (archetypes, equipment)

👉 Now you can open Patrick Mahomes and see his FRCS ratings (throw_power: 98, deep_accuracy: 95, etc.).

---

## **Milestone 6: Archetypes & Modifiers**

✅ Outcome: Style-based diversity in ratings.

* Cluster WRs (e.g., deep threat vs possession).
* Apply ±3 nudges in attributes.
* Store archetype label in `players.parquet`.
  👉 Now rosters feel “alive” instead of cookie-cutter.

---

## **Milestone 7: Adapter & Export (Madden 26 CSV)**

✅ Outcome: Game-ready CSV.

* Implement `pipeline/export.py` with `adapters/madden_26.yaml`.
* Translate FRCS → Madden 26 fields.
* Save to `data/exports/madden26/<DATE>/roster.csv`.
  👉 Now you can drop into a roster tool.

---

## **Milestone 8: Validation & Audits**

✅ Outcome: Confidence your numbers make sense.

* Add pytest unit tests for mapping.
* Add outlier detector (flagging weird cases).
* Add era fairness check.
  👉 Now you trust the outputs.

---

## **Milestone 9: AI/ML Enhancements (Optional Upgrade Path)**

✅ Outcome: Smarter, less hand-tuned ratings.

* Train LightGBM/CatBoost on recent Madden ratings (QB accuracy, OL blocking, DB coverage).
* Add imputation models for missing combine data.
* Integrate LLM to label archetypes and explain mappings.
  👉 Now the system blends rules + ML seamlessly.

---

# 🎯 Suggested Roadmap (time-based)

**UPDATED TO REFLECT TWO-PASS ARCHITECTURE:**

* **Week 1:** Milestones 1–2 → repo + player index CSV (~27,000 NFL players).
* **Week 2:** Milestone 3 → Boolean 3-path qualification (PASS 1) + validation (MUST PASS).
* **Week 3:** Milestones 4–5 → Apply quotas + FRCS attribute mapping (PASS 2, QB only for v0.1).
* **Week 4:** Milestones 6–7 → Archetypes + Madden 26 export (QB roster only).
* **Beyond:** Milestones 8–9 → Full validation + all positions + AI upgrades.

**KEY INSIGHT:**
- Weeks 1-2 process ALL 27,000 players (cheap operations: identity, honors, boolean checks)
- Weeks 3-4 only process ~3,500 qualified players (expensive operations: stats normalization, attribute mapping)
- This two-pass approach saves significant computation time

---

👉 Question for you: do you want me to **draft the code for Milestone 2 (build the player index script)** so you can immediately grab the master pool of \~27k+ players from nflverse-data? That's the key unlock before you can do anything else.

**Note:** Using nflverse-data instead of PFR scraping respects data usage policies while providing comprehensive, regularly-updated NFL datasets including player biographical info, rosters, combine data, and draft history.
