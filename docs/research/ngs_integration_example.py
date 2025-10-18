#!/usr/bin/env python3
"""Example implementation: NGS data integration into player index.

This is a reference implementation showing how to integrate Next Gen Stats
into the existing player index build process.

DO NOT RUN THIS FILE DIRECTLY - it's a template for integration into:
  - pipeline/ingest/ngs_loader.py (new module)
  - pipeline/ingest/players_index.py (modify merge_player_datasets)
  - scripts/build_players_index.py (modify build_output_schema)
"""

import logging
from typing import Any, Dict

import nfl_data_py as nfl
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================================
# MODULE 1: pipeline/ingest/ngs_loader.py (NEW FILE)
# ============================================================================


def load_ngs_career_stats(
    year_range: range, verbose: bool = False
) -> pd.DataFrame:
    """Load and aggregate Next Gen Stats across player careers.

    Loads passing, rushing, and receiving NGS data for specified years and
    aggregates metrics using weighted averages (weighted by attempts/targets).

    Args:
        year_range: Range of years to load (e.g., range(2016, 2025))
        verbose: Enable debug logging

    Returns:
        DataFrame with player_gsis_id and aggregated NGS metrics:
          - ngs_pass_* (9 columns): Passing metrics
          - ngs_rush_* (4 columns): Rushing metrics
          - ngs_rec_* (6 columns): Receiving metrics

    Example:
        >>> ngs_stats = load_ngs_career_stats(range(2016, 2025))
        >>> ngs_stats.columns
        ['player_gsis_id', 'ngs_pass_xcomp', 'ngs_pass_time_to_throw', ...]
    """
    years = list(year_range)
    logger.info(f"Loading NGS data for {min(years)}-{max(years)}...")

    # Load all three stat types (nightly cache after first load)
    logger.info("  - Loading passing NGS...")
    passing = nfl.import_ngs_data("passing", years)

    logger.info("  - Loading rushing NGS...")
    rushing = nfl.import_ngs_data("rushing", years)

    logger.info("  - Loading receiving NGS...")
    receiving = nfl.import_ngs_data("receiving", years)

    # Filter to season aggregates (week 0 = season totals)
    passing_agg = passing[passing["week"] == 0].copy()
    rushing_agg = rushing[rushing["week"] == 0].copy()
    receiving_agg = receiving[receiving["week"] == 0].copy()

    if verbose:
        logger.debug(f"Passing seasons: {len(passing_agg)}")
        logger.debug(f"Rushing seasons: {len(rushing_agg)}")
        logger.debug(f"Receiving seasons: {len(receiving_agg)}")

    # Aggregate passing metrics (weighted by attempts)
    ngs_passing = _aggregate_passing_ngs(passing_agg)

    # Aggregate rushing metrics (weighted by attempts)
    ngs_rushing = _aggregate_rushing_ngs(rushing_agg)

    # Aggregate receiving metrics (weighted by targets)
    ngs_receiving = _aggregate_receiving_ngs(receiving_agg)

    # Merge all three stat types (outer join to preserve all players)
    ngs_combined = ngs_passing.merge(
        ngs_rushing, on="player_gsis_id", how="outer"
    )
    ngs_combined = ngs_combined.merge(
        ngs_receiving, on="player_gsis_id", how="outer"
    )

    logger.info(
        f"NGS data loaded: {len(ngs_combined)} players with NGS metrics"
    )

    return ngs_combined


def _aggregate_passing_ngs(passing_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate passing NGS metrics across seasons.

    Weighted by attempts to avoid skewing from small-sample seasons.
    """

    def weighted_agg(group: pd.DataFrame) -> pd.Series:
        total_attempts = group["attempts"].sum()
        total_completions = group["completions"].sum()

        if total_attempts == 0:
            return pd.Series(
                {
                    "ngs_pass_xcomp": None,
                    "ngs_pass_time_to_throw": None,
                    "ngs_pass_aggressiveness": None,
                    "ngs_pass_air_yards": None,
                    "ngs_pass_air_yards_intended": None,
                    "ngs_pass_air_to_sticks": None,
                    "ngs_pass_max_air_distance": None,
                    "ngs_pass_passer_rating": None,
                    "ngs_pass_attempts_total": 0,
                }
            )

        return pd.Series(
            {
                # Completion % above expected (key skill metric)
                "ngs_pass_xcomp": (
                    group["completion_percentage_above_expectation"]
                    * group["attempts"]
                ).sum()
                / total_attempts,
                # Time to throw (awareness/decision-making)
                "ngs_pass_time_to_throw": (
                    group["avg_time_to_throw"] * group["attempts"]
                ).sum()
                / total_attempts,
                # Aggressiveness (risk-taking)
                "ngs_pass_aggressiveness": (
                    group["aggressiveness"] * group["attempts"]
                ).sum()
                / total_attempts,
                # Completed air yards (throw power/deep ball)
                "ngs_pass_air_yards": (
                    group["avg_completed_air_yards"] * group["completions"]
                ).sum()
                / total_completions
                if total_completions > 0
                else None,
                # Intended air yards (route tree depth)
                "ngs_pass_air_yards_intended": (
                    group["avg_intended_air_yards"] * group["attempts"]
                ).sum()
                / total_attempts,
                # Air yards to sticks (situational decision-making)
                "ngs_pass_air_to_sticks": (
                    group["avg_air_yards_to_sticks"] * group["attempts"]
                ).sum()
                / total_attempts,
                # Max air distance (arm strength)
                "ngs_pass_max_air_distance": group[
                    "max_completed_air_distance"
                ].max(),
                # Passer rating (overall)
                "ngs_pass_passer_rating": (
                    group["passer_rating"] * group["attempts"]
                ).sum()
                / total_attempts,
                # Total attempts (sample size)
                "ngs_pass_attempts_total": total_attempts,
            }
        )

    return passing_df.groupby("player_gsis_id").apply(weighted_agg).reset_index()


def _aggregate_rushing_ngs(rushing_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate rushing NGS metrics across seasons.

    Weighted by attempts to avoid skewing from small-sample seasons.
    """

    def weighted_agg(group: pd.DataFrame) -> pd.Series:
        total_attempts = group["rush_attempts"].sum()

        if total_attempts == 0:
            return pd.Series(
                {
                    "ngs_rush_yards_over_exp": None,
                    "ngs_rush_efficiency": None,
                    "ngs_rush_time_to_los": None,
                    "ngs_rush_vs_eight_defenders": None,
                    "ngs_rush_attempts_total": 0,
                }
            )

        return pd.Series(
            {
                # Yards over expected per attempt (vision/patience)
                "ngs_rush_yards_over_exp": (
                    group["rush_yards_over_expected_per_att"]
                    * group["rush_attempts"]
                ).sum()
                / total_attempts,
                # Efficiency (overall effectiveness)
                "ngs_rush_efficiency": (
                    group["efficiency"] * group["rush_attempts"]
                ).sum()
                / total_attempts,
                # Time to line of scrimmage (decisiveness/speed)
                "ngs_rush_time_to_los": (
                    group["avg_time_to_los"] * group["rush_attempts"]
                ).sum()
                / total_attempts,
                # % attempts vs 8+ defenders (difficulty context)
                "ngs_rush_vs_eight_defenders": (
                    group["percent_attempts_gte_eight_defenders"]
                    * group["rush_attempts"]
                ).sum()
                / total_attempts,
                # Total attempts (sample size)
                "ngs_rush_attempts_total": total_attempts,
            }
        )

    return rushing_df.groupby("player_gsis_id").apply(weighted_agg).reset_index()


def _aggregate_receiving_ngs(receiving_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate receiving NGS metrics across seasons.

    Weighted by targets to avoid skewing from small-sample seasons.
    """

    def weighted_agg(group: pd.DataFrame) -> pd.Series:
        total_targets = group["targets"].sum()
        total_receptions = group["receptions"].sum()

        if total_targets == 0:
            return pd.Series(
                {
                    "ngs_rec_separation": None,
                    "ngs_rec_yac_over_exp": None,
                    "ngs_rec_cushion": None,
                    "ngs_rec_air_yards": None,
                    "ngs_rec_catch_pct": None,
                    "ngs_rec_air_yards_share": None,
                    "ngs_rec_targets_total": 0,
                }
            )

        return pd.Series(
            {
                # Separation at catch (route running + speed)
                "ngs_rec_separation": (
                    group["avg_separation"] * group["targets"]
                ).sum()
                / total_targets,
                # YAC over expected (elusiveness after catch)
                "ngs_rec_yac_over_exp": (
                    group["avg_yac_above_expectation"] * group["receptions"]
                ).sum()
                / total_receptions
                if total_receptions > 0
                else None,
                # Cushion at snap (coverage context)
                "ngs_rec_cushion": (
                    group["avg_cushion"] * group["targets"]
                ).sum()
                / total_targets,
                # Intended air yards (route depth/deep threat)
                "ngs_rec_air_yards": (
                    group["avg_intended_air_yards"] * group["targets"]
                ).sum()
                / total_targets,
                # Catch percentage (hands/concentration)
                "ngs_rec_catch_pct": (
                    group["catch_percentage"] * group["targets"]
                ).sum()
                / total_targets,
                # Share of intended air yards (target quality/alpha)
                "ngs_rec_air_yards_share": (
                    group["percent_share_of_intended_air_yards"]
                    * group["targets"]
                ).sum()
                / total_targets,
                # Total targets (sample size)
                "ngs_rec_targets_total": total_targets,
            }
        )

    return (
        receiving_df.groupby("player_gsis_id").apply(weighted_agg).reset_index()
    )


# ============================================================================
# MODULE 2: Modify pipeline/ingest/aggregators.py (or players_index.py)
# ============================================================================


def merge_player_datasets(
    players: pd.DataFrame,
    career_stats: pd.DataFrame,
    playoff_stats: pd.DataFrame,
    draft: pd.DataFrame,
    combine: pd.DataFrame,
) -> pd.DataFrame:
    """Merge all player datasets into comprehensive DataFrame.

    Args:
        players: Base player biographical data
        career_stats: Aggregated career statistics
        playoff_stats: Aggregated playoff statistics
        draft: Draft history and honors
        combine: Physical measurements

    Returns:
        Enhanced players DataFrame with all merged data including NGS
    """
    logger.info("Merging player datasets...")

    # Start with base player data
    enhanced_players = players.copy()

    # Merge career stats
    logger.info("Merging career statistics...")
    enhanced_players = enhanced_players.merge(
        career_stats, left_on="gsis_id", right_on="player_id", how="left"
    )

    # Merge playoff stats
    logger.info("Merging playoff statistics...")
    enhanced_players = enhanced_players.merge(
        playoff_stats, left_on="gsis_id", right_on="player_id", how="left"
    )

    # Merge draft data (existing code...)
    logger.info("Merging draft and honors data...")
    # ... existing draft merge logic ...

    # Merge combine data (existing code...)
    logger.info("Merging combine and physical data...")
    # ... existing combine merge logic ...

    # -------------------------------------------------------------------------
    # NEW: Merge Next Gen Stats
    # -------------------------------------------------------------------------
    logger.info("Merging Next Gen Stats...")
    try:
        # Import here to avoid circular dependency
        from pipeline.ingest.ngs_loader import load_ngs_career_stats

        ngs_stats = load_ngs_career_stats(range(2016, 2025))

        enhanced_players = enhanced_players.merge(
            ngs_stats, left_on="gsis_id", right_on="player_gsis_id", how="left"
        )

        # Log coverage
        ngs_passing_count = (
            enhanced_players["ngs_pass_attempts_total"].notna().sum()
        )
        ngs_rushing_count = (
            enhanced_players["ngs_rush_attempts_total"].notna().sum()
        )
        ngs_receiving_count = (
            enhanced_players["ngs_rec_targets_total"].notna().sum()
        )

        logger.info(f"  Players with NGS passing data: {ngs_passing_count}")
        logger.info(f"  Players with NGS rushing data: {ngs_rushing_count}")
        logger.info(
            f"  Players with NGS receiving data: {ngs_receiving_count}"
        )

        # Validate join rate (expect 20-30% for passing)
        total_players = len(enhanced_players)
        pass_join_rate = ngs_passing_count / total_players
        if pass_join_rate < 0.05:
            logger.warning(
                f"Low NGS passing join rate: {pass_join_rate:.1%} "
                f"(expected >5%) - check player_gsis_id mapping"
            )

    except Exception as e:
        logger.error(f"Failed to load NGS data: {e}")
        logger.warning("Continuing without NGS data...")

    return enhanced_players


# ============================================================================
# MODULE 3: Modify scripts/build_players_index.py (build_output_schema)
# ============================================================================


def build_output_schema(enhanced_players: pd.DataFrame) -> pd.DataFrame:
    """Build the final output schema for players_index.csv.

    Maps enhanced player data to standardized output columns.

    Args:
        enhanced_players: DataFrame with all merged player data

    Returns:
        DataFrame with standardized schema including NGS columns
    """
    logger.info("Building output schema...")

    return pd.DataFrame(
        {
            # ================================================================
            # EXISTING COLUMNS (identity, career, stats, physical)
            # ================================================================
            "player_id": enhanced_players["gsis_id"],
            "full_name": enhanced_players["display_name"],
            "primary_pos": enhanced_players["position"],
            "college": enhanced_players["college_name"],
            # ... all existing columns ...
            # ================================================================
            # NEW: Next Gen Stats (Passing) - 9 columns
            # ================================================================
            "ngs_pass_xcomp": enhanced_players.get("ngs_pass_xcomp", pd.NA),
            "ngs_pass_time_to_throw": enhanced_players.get(
                "ngs_pass_time_to_throw", pd.NA
            ),
            "ngs_pass_aggressiveness": enhanced_players.get(
                "ngs_pass_aggressiveness", pd.NA
            ),
            "ngs_pass_air_yards": enhanced_players.get(
                "ngs_pass_air_yards", pd.NA
            ),
            "ngs_pass_air_yards_intended": enhanced_players.get(
                "ngs_pass_air_yards_intended", pd.NA
            ),
            "ngs_pass_air_to_sticks": enhanced_players.get(
                "ngs_pass_air_to_sticks", pd.NA
            ),
            "ngs_pass_max_air_distance": enhanced_players.get(
                "ngs_pass_max_air_distance", pd.NA
            ),
            "ngs_pass_passer_rating": enhanced_players.get(
                "ngs_pass_passer_rating", pd.NA
            ),
            "ngs_pass_attempts": enhanced_players.get(
                "ngs_pass_attempts_total", 0
            )
            .fillna(0)
            .astype(int),
            # ================================================================
            # NEW: Next Gen Stats (Rushing) - 5 columns
            # ================================================================
            "ngs_rush_yards_over_exp": enhanced_players.get(
                "ngs_rush_yards_over_exp", pd.NA
            ),
            "ngs_rush_efficiency": enhanced_players.get(
                "ngs_rush_efficiency", pd.NA
            ),
            "ngs_rush_time_to_los": enhanced_players.get(
                "ngs_rush_time_to_los", pd.NA
            ),
            "ngs_rush_vs_eight_defenders": enhanced_players.get(
                "ngs_rush_vs_eight_defenders", pd.NA
            ),
            "ngs_rush_attempts": enhanced_players.get(
                "ngs_rush_attempts_total", 0
            )
            .fillna(0)
            .astype(int),
            # ================================================================
            # NEW: Next Gen Stats (Receiving) - 7 columns
            # ================================================================
            "ngs_rec_separation": enhanced_players.get(
                "ngs_rec_separation", pd.NA
            ),
            "ngs_rec_yac_over_exp": enhanced_players.get(
                "ngs_rec_yac_over_exp", pd.NA
            ),
            "ngs_rec_cushion": enhanced_players.get("ngs_rec_cushion", pd.NA),
            "ngs_rec_air_yards": enhanced_players.get(
                "ngs_rec_air_yards", pd.NA
            ),
            "ngs_rec_catch_pct": enhanced_players.get(
                "ngs_rec_catch_pct", pd.NA
            ),
            "ngs_rec_air_yards_share": enhanced_players.get(
                "ngs_rec_air_yards_share", pd.NA
            ),
            "ngs_rec_targets": enhanced_players.get("ngs_rec_targets_total", 0)
            .fillna(0)
            .astype(int),
        }
    )


# ============================================================================
# TESTING / VALIDATION
# ============================================================================


def validate_ngs_integration() -> None:
    """Validate NGS integration with sample data.

    Run this after integration to verify data quality.
    """
    import nfl_data_py as nfl

    logger.info("=== NGS Integration Validation ===")

    # Test 1: Load sample data
    logger.info("Test 1: Loading sample NGS data...")
    passing = nfl.import_ngs_data("passing", [2023])
    logger.info(f"  ✓ Loaded {len(passing)} passing records")
    logger.info(
        f"  ✓ Players: {passing['player_gsis_id'].nunique()}"
    )

    # Test 2: Check schema
    logger.info("Test 2: Validating schema...")
    expected_cols = [
        "player_gsis_id",
        "completion_percentage_above_expectation",
        "avg_time_to_throw",
        "aggressiveness",
    ]
    missing = set(expected_cols) - set(passing.columns)
    if missing:
        logger.error(f"  ✗ Missing columns: {missing}")
    else:
        logger.info(f"  ✓ All expected columns present")

    # Test 3: Check data quality
    logger.info("Test 3: Checking data quality...")
    season_agg = passing[passing["week"] == 0]
    null_xcomp = (
        season_agg["completion_percentage_above_expectation"].isna().sum()
    )
    logger.info(f"  ✓ Season aggregates: {len(season_agg)} records")
    logger.info(f"  ✓ Null xCOMP: {null_xcomp} ({null_xcomp/len(season_agg):.1%})")

    # Test 4: Spot check known player
    logger.info("Test 4: Spot checking known player...")
    mahomes = season_agg[
        season_agg["player_display_name"].str.contains("Mahomes", na=False)
    ]
    if len(mahomes) > 0:
        logger.info(f"  ✓ Found Patrick Mahomes:")
        logger.info(
            f"    - Attempts: {mahomes.iloc[0]['attempts']}"
        )
        logger.info(
            f"    - xCOMP: {mahomes.iloc[0]['completion_percentage_above_expectation']:.2f}"
        )
    else:
        logger.warning("  ⚠ Patrick Mahomes not found in 2023 data")

    logger.info("=== Validation Complete ===")


# ============================================================================
# USAGE EXAMPLES
# ============================================================================


def example_usage() -> None:
    """Example usage of NGS integration."""

    # Example 1: Load NGS career stats
    ngs_stats = load_ngs_career_stats(range(2016, 2025))
    print(f"Loaded NGS stats for {len(ngs_stats)} players")
    print(f"Columns: {list(ngs_stats.columns)}")

    # Example 2: Check a specific player
    mahomes_id = "00-0033873"  # Patrick Mahomes
    mahomes_ngs = ngs_stats[ngs_stats["player_gsis_id"] == mahomes_id]
    if not mahomes_ngs.empty:
        print(f"\nPatrick Mahomes NGS stats:")
        print(f"  xCOMP: {mahomes_ngs.iloc[0]['ngs_pass_xcomp']:.2f}")
        print(
            f"  Time to throw: {mahomes_ngs.iloc[0]['ngs_pass_time_to_throw']:.2f}s"
        )
        print(
            f"  Aggressiveness: {mahomes_ngs.iloc[0]['ngs_pass_aggressiveness']:.1f}%"
        )

    # Example 3: Position-specific stats
    qb_ngs = ngs_stats[ngs_stats["ngs_pass_attempts_total"] > 0]
    rb_ngs = ngs_stats[ngs_stats["ngs_rush_attempts_total"] > 0]
    wr_ngs = ngs_stats[ngs_stats["ngs_rec_targets_total"] > 0]

    print(f"\nPlayers with NGS data:")
    print(f"  QBs: {len(qb_ngs)}")
    print(f"  RBs: {len(rb_ngs)}")
    print(f"  WRs: {len(wr_ngs)}")


if __name__ == "__main__":
    # Don't run directly - this is a reference implementation
    print(
        "This is a reference implementation. Do not run directly."
    )
    print(
        "Copy functions to appropriate modules as described in comments."
    )
