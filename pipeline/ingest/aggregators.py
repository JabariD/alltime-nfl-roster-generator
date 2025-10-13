"""Pure aggregation functions for career and playoff statistics.

All functions are vectorized (no row iteration) and type-annotated.
Extracted from original build_players_index.py script for modularity.
"""

import logging
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


def aggregate_career_stats(seasonal_data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate career statistics from seasonal data.

    Computes total games, yards, TDs across all regular seasons.
    Uses vectorized groupby operations for performance.

    Args:
        seasonal_data: DataFrame with seasonal statistics (1+ rows per player)

    Returns:
        DataFrame with one row per player containing:
        - player_id
        - first_year, last_year, career_seasons
        - total_career_games
        - career_passing_yards, career_rushing_yards, career_receiving_yards
        - career_passing_tds, career_rushing_tds, career_receiving_tds
        - career_tds (sum of all TD types)

    Raises:
        ValueError: If seasonal_data missing required columns
    """
    logger.info("Aggregating career statistics...")

    required_cols = ["player_id", "season"]
    missing = [col for col in required_cols if col not in seasonal_data.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Define aggregation rules
    stat_columns: Dict[str, Any] = {
        "games": "sum",
        "passing_yards": "sum",
        "rushing_yards": "sum",
        "receiving_yards": "sum",
        "passing_tds": "sum",
        "rushing_tds": "sum",
        "receiving_tds": "sum",
        "season": ["min", "max", "count"],
    }

    # Vectorized aggregation
    career_stats = seasonal_data.groupby("player_id").agg(stat_columns).reset_index()

    # Flatten multi-level column names
    career_stats.columns = [
        "player_id",
        "total_career_games",
        "career_passing_yards",
        "career_rushing_yards",
        "career_receiving_yards",
        "career_passing_tds",
        "career_rushing_tds",
        "career_receiving_tds",
        "first_year",
        "last_year",
        "career_seasons",
    ]

    # Calculate total TDs (vectorized)
    career_stats["career_tds"] = (
        career_stats["career_passing_tds"]
        + career_stats["career_rushing_tds"]
        + career_stats["career_receiving_tds"]
    )

    # Fill NaN with 0 for numeric columns
    numeric_cols = career_stats.select_dtypes(include=["number"]).columns
    career_stats[numeric_cols] = career_stats[numeric_cols].fillna(0)

    logger.info(f"Aggregated stats for {len(career_stats)} players")
    return career_stats


def aggregate_playoff_stats(playoff_data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate playoff statistics from postseason data.

    Separate from regular season to enable clutch/legacy scoring.
    Uses vectorized groupby operations for performance.

    Args:
        playoff_data: DataFrame with playoff seasonal statistics

    Returns:
        DataFrame with one row per player containing:
        - player_id
        - playoff_games
        - playoff_passing_yards, playoff_rushing_yards, playoff_receiving_yards
        - playoff_passing_tds, playoff_rushing_tds, playoff_receiving_tds
        - playoff_tds (sum of all playoff TD types)

    Raises:
        ValueError: If playoff_data missing required columns
    """
    logger.info("Aggregating playoff statistics...")

    if playoff_data.empty:
        logger.warning("Empty playoff data, returning empty DataFrame")
        return pd.DataFrame(
            columns=[
                "player_id",
                "playoff_games",
                "playoff_passing_yards",
                "playoff_rushing_yards",
                "playoff_receiving_yards",
                "playoff_passing_tds",
                "playoff_rushing_tds",
                "playoff_receiving_tds",
                "playoff_tds",
            ]
        )

    required_cols = ["player_id"]
    missing = [col for col in required_cols if col not in playoff_data.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Define aggregation rules
    stat_columns: Dict[str, str] = {
        "games": "sum",
        "passing_yards": "sum",
        "rushing_yards": "sum",
        "receiving_yards": "sum",
        "passing_tds": "sum",
        "rushing_tds": "sum",
        "receiving_tds": "sum",
    }

    # Vectorized aggregation
    playoff_stats = playoff_data.groupby("player_id").agg(stat_columns).reset_index()

    # Rename columns with playoff_ prefix
    playoff_stats.columns = [
        "player_id",
        "playoff_games",
        "playoff_passing_yards",
        "playoff_rushing_yards",
        "playoff_receiving_yards",
        "playoff_passing_tds",
        "playoff_rushing_tds",
        "playoff_receiving_tds",
    ]

    # Calculate total playoff TDs (vectorized)
    playoff_stats["playoff_tds"] = (
        playoff_stats["playoff_passing_tds"]
        + playoff_stats["playoff_rushing_tds"]
        + playoff_stats["playoff_receiving_tds"]
    )

    # Fill NaN with 0 for numeric columns
    numeric_cols = playoff_stats.select_dtypes(include=["number"]).columns
    playoff_stats[numeric_cols] = playoff_stats[numeric_cols].fillna(0)

    logger.info(f"Aggregated playoff stats for {len(playoff_stats)} players")
    return playoff_stats


def merge_player_datasets(
    players: pd.DataFrame,
    career_stats: pd.DataFrame,
    playoff_stats: pd.DataFrame,
    draft: pd.DataFrame,
    combine: pd.DataFrame,
) -> pd.DataFrame:
    """Merge all player datasets into comprehensive DataFrame.

    Performs left joins to preserve all players even with missing stats.
    Uses vectorized pandas merge operations.

    Args:
        players: Base player biographical data
        career_stats: Aggregated career statistics
        playoff_stats: Aggregated playoff statistics
        draft: Draft and honors data
        combine: NFL Combine measurements

    Returns:
        Enhanced players DataFrame with all merged data columns

    Raises:
        ValueError: If players DataFrame is empty or missing gsis_id
    """
    logger.info("Merging player datasets...")

    if players.empty:
        raise ValueError("Players DataFrame is empty")

    if "gsis_id" not in players.columns:
        raise ValueError("Players DataFrame missing gsis_id column")

    # Start with base player data
    enhanced = players.copy()

    # Merge career stats (left join to preserve all players)
    if not career_stats.empty:
        logger.info("Merging career statistics...")
        enhanced = enhanced.merge(
            career_stats, left_on="gsis_id", right_on="player_id", how="left"
        )

    # Merge playoff stats (left join)
    if not playoff_stats.empty:
        logger.info("Merging playoff statistics...")
        enhanced = enhanced.merge(
            playoff_stats, left_on="gsis_id", right_on="player_id", how="left"
        )

    # Merge draft data and honors (left join)
    if not draft.empty:
        logger.info("Merging draft and honors data...")

        # Select relevant draft columns
        draft_columns = [
            "gsis_id",
            "season",
            "to",
            "probowls",
            "allpro",
            "hof",
            "pick",
            "games",
            "pass_yards",
            "rush_yards",
            "rec_yards",
            "pass_tds",
            "rush_tds",
            "rec_tds",
            "seasons_started",
            "def_solo_tackles",
            "def_sacks",
            "def_ints",
        ]

        # Filter to existing columns
        available_draft_cols = [col for col in draft_columns if col in draft.columns]
        draft_subset = draft[available_draft_cols].copy()

        # Rename season column to avoid conflicts
        if "season" in draft_subset.columns:
            draft_subset = draft_subset.rename(
                columns={
                    "season": "draft_season",
                    "games": "draft_games",
                    "pass_yards": "draft_pass_yards",
                    "rush_yards": "draft_rush_yards",
                    "rec_yards": "draft_rec_yards",
                    "pass_tds": "draft_pass_tds",
                    "rush_tds": "draft_rush_tds",
                    "rec_tds": "draft_rec_tds",
                }
            )

        enhanced = enhanced.merge(draft_subset, on="gsis_id", how="left")

    # Merge combine data (left join on pfr_id)
    if not combine.empty and "pfr_id" in enhanced.columns:
        logger.info("Merging combine and physical data...")

        # Select relevant combine columns
        combine_columns = [
            "pfr_id",
            "ht",
            "wt",
            "forty",
            "bench",
            "vertical",
            "broad_jump",
            "cone",
            "shuttle",
        ]

        # Filter to existing columns
        available_combine_cols = [
            col for col in combine_columns if col in combine.columns
        ]
        combine_subset = combine[available_combine_cols].copy()

        # Deduplicate by pfr_id (keep first record)
        combine_subset = combine_subset[
            combine_subset["pfr_id"].notna()
        ].drop_duplicates(subset=["pfr_id"])

        enhanced = enhanced.merge(
            combine_subset, left_on="pfr_id", right_on="pfr_id", how="left"
        )

    logger.info(
        f"Merge complete: {len(enhanced)} players with "
        f"{len(enhanced.columns)} columns"
    )
    return enhanced
