"""Next Gen Stats loader with career aggregation and schema normalization.

This module loads NFL Next Gen Stats (NGS) data from nflverse (2016+) and
aggregates it to player-level metrics. NGS provides advanced tracking metrics:
- Passing: completion above expectation, time to throw, aggressiveness
- Rushing: yards over expected, efficiency, time to line of scrimmage
- Receiving: separation, YAC above expectation, catch percentage

All functions are vectorized and type-annotated for performance and maintainability.
"""

import logging
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from pipeline.ingest.ingest_types import NFLVerseLoadError

logger = logging.getLogger(__name__)

# Minimum sample sizes for quality filtering
MIN_PASSING_ATTEMPTS = 50
MIN_RUSHING_ATTEMPTS = 30
MIN_RECEIVING_TARGETS = 20

# Column mapping: NGS raw -> standardized ngs_ prefix
PASSING_COLUMNS = {
    "completion_percentage_above_expectation": "ngs_xcomp",
    "avg_time_to_throw": "ngs_time_to_throw",
    "aggressiveness": "ngs_aggressiveness",
    "avg_completed_air_yards": "ngs_avg_air_yards",
    "max_completed_air_distance": "ngs_max_air_distance",
}

RUSHING_COLUMNS = {
    "rush_yards_over_expected_per_att": "ngs_ryoe_per_att",
    "efficiency": "ngs_rush_efficiency",
    "avg_time_to_los": "ngs_time_to_los",
    "percent_attempts_gte_eight_defenders": "ngs_loaded_box_pct",
}

RECEIVING_COLUMNS = {
    "avg_separation": "ngs_avg_separation",
    "avg_yac_above_expectation": "ngs_yac_above_exp",
    "catch_percentage": "ngs_catch_pct",
    "avg_intended_air_yards": "ngs_avg_target_depth",
    "avg_cushion": "ngs_avg_cushion",
}


def load_raw_ngs_data(
    stat_type: str, years: list[int], nfl_module: Any
) -> pd.DataFrame:
    """Load raw NGS data from nflverse for a specific stat type.

    Args:
        stat_type: One of 'passing', 'rushing', 'receiving'
        years: List of seasons to load (e.g., [2016, 2017, ...])
        nfl_module: nfl_data_py module instance

    Returns:
        Raw DataFrame from nflverse (all weeks, all players)

    Raises:
        NFLVerseLoadError: If load fails or stat_type invalid
    """
    valid_types = ["passing", "rushing", "receiving"]
    if stat_type not in valid_types:
        raise NFLVerseLoadError(
            f"Invalid stat_type '{stat_type}'. Must be one of {valid_types}"
        )

    try:
        logger.info(f"Loading NGS {stat_type} data for {min(years)}-{max(years)}...")
        data = nfl_module.import_ngs_data(stat_type=stat_type, years=years)
        logger.info(f"Loaded {len(data)} NGS {stat_type} records")
        return data

    except Exception as e:
        raise NFLVerseLoadError(
            f"Failed to load NGS {stat_type} data: {e}"
        ) from e


def filter_season_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to season aggregate records (week == 0).

    NGS data includes weekly records and season totals. We use season totals
    to avoid double-counting and to get official aggregations.

    Args:
        df: Raw NGS DataFrame with 'week' column

    Returns:
        Filtered DataFrame with only season aggregates

    Raises:
        ValueError: If 'week' column missing
    """
    if "week" not in df.columns:
        raise ValueError("NGS data missing 'week' column")

    if df.empty:
        logger.warning("Empty NGS DataFrame, returning as-is")
        return df

    season_totals = df[df["week"] == 0].copy()
    logger.debug(
        f"Filtered to {len(season_totals)} season aggregates "
        f"from {len(df)} total records"
    )
    return season_totals


def apply_minimum_thresholds(
    df: pd.DataFrame, stat_type: str
) -> pd.DataFrame:
    """Filter to players meeting minimum sample size thresholds.

    Removes players with insufficient attempts/targets to produce reliable metrics.

    Args:
        df: NGS DataFrame with volume columns
        stat_type: One of 'passing', 'rushing', 'receiving'

    Returns:
        Filtered DataFrame with players above threshold

    Raises:
        ValueError: If required volume column missing for stat_type
    """
    if df.empty:
        return df

    if stat_type == "passing":
        volume_col = "attempts"
        threshold = MIN_PASSING_ATTEMPTS
    elif stat_type == "rushing":
        volume_col = "rush_attempts"
        threshold = MIN_RUSHING_ATTEMPTS
    elif stat_type == "receiving":
        volume_col = "targets"
        threshold = MIN_RECEIVING_TARGETS
    else:
        raise ValueError(f"Unknown stat_type: {stat_type}")

    if volume_col not in df.columns:
        raise ValueError(
            f"NGS {stat_type} data missing '{volume_col}' column"
        )

    before_count = len(df)
    filtered = df[df[volume_col] >= threshold].copy()
    logger.debug(
        f"Filtered to {len(filtered)} players with {threshold}+ {volume_col} "
        f"(removed {before_count - len(filtered)})"
    )
    return filtered


def aggregate_career_ngs_stats(
    df: pd.DataFrame, stat_type: str
) -> pd.DataFrame:
    """Aggregate NGS stats across career using weighted averages.

    Uses volume-weighted averages (by attempts/carries/targets) to properly
    account for varying sample sizes across seasons.

    Args:
        df: NGS DataFrame with season totals (one row per player-season)
        stat_type: One of 'passing', 'rushing', 'receiving'

    Returns:
        DataFrame with one row per player containing:
        - player_gsis_id (join key)
        - ngs_* metrics (career weighted averages)
        - ngs_{stat_type}_attempts (total volume)
        - ngs_seasons_covered (number of seasons with data)

    Raises:
        ValueError: If stat_type invalid or required columns missing
    """
    if df.empty:
        logger.warning(f"Empty NGS {stat_type} data, returning empty DataFrame")
        return pd.DataFrame(columns=["player_gsis_id"])

    # Determine volume column for weighting
    if stat_type == "passing":
        volume_col = "attempts"
        column_map = PASSING_COLUMNS
    elif stat_type == "rushing":
        volume_col = "rush_attempts"
        column_map = RUSHING_COLUMNS
    elif stat_type == "receiving":
        volume_col = "targets"
        column_map = RECEIVING_COLUMNS
    else:
        raise ValueError(f"Invalid stat_type: {stat_type}")

    # Check required columns exist
    required = ["player_gsis_id", volume_col, "season"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Filter to columns we'll actually use (avoid propagating unnecessary data)
    stat_cols = [col for col in column_map.keys() if col in df.columns]
    if not stat_cols:
        logger.warning(
            f"No mapped stat columns found in {stat_type} data. "
            f"Expected: {list(column_map.keys())}"
        )
        return pd.DataFrame(columns=["player_gsis_id"])

    # Group by player and compute weighted averages
    player_count = df["player_gsis_id"].nunique()
    logger.debug(f"Aggregating {stat_type} stats for {player_count} players...")

    # Create weighted sums for each stat
    weighted_stats = df.groupby("player_gsis_id").apply(
        lambda group: pd.Series(
            {
                **{
                    col: (
                        (group[col] * group[volume_col]).sum()
                        / group[volume_col].sum()
                        if group[volume_col].sum() > 0
                        else None
                    )
                    for col in stat_cols
                },
                f"ngs_{stat_type}_attempts": group[volume_col].sum(),
                "ngs_seasons_covered": group["season"].nunique(),
            }
        ),
        include_groups=False,
    ).reset_index()

    # Rename columns to standardized ngs_ prefix
    rename_dict = {
        col: column_map[col] for col in stat_cols if col in column_map
    }
    aggregated = weighted_stats.rename(columns=rename_dict)

    logger.info(
        f"Aggregated {stat_type} stats for {len(aggregated)} players "
        f"(avg {aggregated['ngs_seasons_covered'].mean():.1f} seasons)"
    )

    return aggregated


def merge_ngs_stat_types(
    passing: pd.DataFrame,
    rushing: pd.DataFrame,
    receiving: pd.DataFrame,
) -> pd.DataFrame:
    """Merge all three NGS stat types into unified player-level DataFrame.

    Uses outer joins to preserve all players across stat types (QB may only
    have passing, WR only receiving, etc.).

    Args:
        passing: Aggregated NGS passing stats
        rushing: Aggregated NGS rushing stats
        receiving: Aggregated NGS receiving stats

    Returns:
        DataFrame with one row per player containing all available NGS metrics

    Raises:
        ValueError: If any input DataFrame missing player_gsis_id
    """
    logger.info("Merging NGS stat types...")

    # Start with empty base
    merged = pd.DataFrame(columns=["player_gsis_id"])

    # Merge each stat type if non-empty
    for stat_type, df in [
        ("passing", passing),
        ("rushing", rushing),
        ("receiving", receiving),
    ]:
        if df.empty:
            logger.debug(f"Skipping empty {stat_type} data")
            continue

        if "player_gsis_id" not in df.columns:
            raise ValueError(f"{stat_type} data missing player_gsis_id")

        if merged.empty:
            merged = df.copy()
        else:
            merged = merged.merge(df, on="player_gsis_id", how="outer")

        logger.debug(f"Merged {stat_type}: {len(merged)} total players")

    # Add overall metadata column
    if not merged.empty:
        # Count total seasons across all stat types (use max)
        season_cols = [col for col in merged.columns if "ngs_seasons_covered" in col]
        if season_cols:
            merged["ngs_seasons_covered"] = merged[season_cols].max(axis=1)
            # Drop individual season counts
            merged = merged.drop(columns=season_cols)

    logger.info(f"Final merged NGS data: {len(merged)} players")
    return merged


def load_ngs_data(
    years: list[int],
    nfl_module: Any | None = None,
) -> pd.DataFrame:
    """Load and aggregate Next Gen Stats data for all stat types.

    Main public API for NGS data loading. Handles all three stat types,
    filters to quality samples, aggregates careers, and merges into unified schema.

    Args:
        years: List of seasons to load (e.g., list(range(2016, 2025)))
        nfl_module: Optional nfl_data_py module (for testing/injection)

    Returns:
        DataFrame with one row per player containing:
        - player_gsis_id (join key to main player index)
        - ngs_* metrics (all available NGS stats)
        - ngs_*_attempts (volume for each stat type)
        - ngs_seasons_covered (seasons with NGS data)

    Raises:
        NFLVerseLoadError: If nfl_data_py not available or loads fail

    Example:
        >>> ngs_data = load_ngs_data(list(range(2016, 2025)))
        >>> ngs_data.columns
        ['player_gsis_id', 'ngs_xcomp', 'ngs_time_to_throw', ...]
        >>> len(ngs_data)
        1234  # players with NGS data
    """
    # Import nfl_data_py if not injected (normal usage)
    if nfl_module is None:
        try:
            import nfl_data_py as nfl  # type: ignore[import-untyped]

            nfl_module = nfl
        except ImportError as e:
            raise NFLVerseLoadError(
                f"nfl-data-py not installed: {e}"
            ) from e

    logger.info(
        f"Loading NGS data for {len(years)} seasons ({min(years)}-{max(years)})..."
    )

    # Process each stat type independently
    stat_types = ["passing", "rushing", "receiving"]
    aggregated_data = {}

    for stat_type in stat_types:
        try:
            # Load raw data
            raw = load_raw_ngs_data(stat_type, years, nfl_module)

            # Filter to season aggregates
            season_totals = filter_season_aggregates(raw)

            # Apply minimum thresholds
            quality_samples = apply_minimum_thresholds(season_totals, stat_type)

            # Aggregate across career
            aggregated = aggregate_career_ngs_stats(quality_samples, stat_type)

            aggregated_data[stat_type] = aggregated

        except NFLVerseLoadError:
            # Re-raise NFLVerseLoadError (critical failure)
            raise
        except Exception as e:
            # Log and continue for other stat types (graceful degradation)
            logger.warning(
                f"Failed to process NGS {stat_type} data: {e}. Continuing..."
            )
            aggregated_data[stat_type] = pd.DataFrame(columns=["player_gsis_id"])

    # Merge all stat types
    merged = merge_ngs_stat_types(
        aggregated_data["passing"],
        aggregated_data["rushing"],
        aggregated_data["receiving"],
    )

    # Log coverage statistics
    if not merged.empty:
        avg_seasons = merged.get("ngs_seasons_covered", pd.Series([0])).mean()
        logger.info(
            f"NGS data loaded: {len(merged)} players with data, "
            f"avg {avg_seasons:.1f} seasons"
        )

        # Log stat type coverage
        passing_count = (
            merged["ngs_passing_attempts"].notna().sum()
            if "ngs_passing_attempts" in merged.columns
            else 0
        )
        rushing_count = (
            merged["ngs_rushing_attempts"].notna().sum()
            if "ngs_rushing_attempts" in merged.columns
            else 0
        )
        receiving_count = (
            merged["ngs_receiving_attempts"].notna().sum()
            if "ngs_receiving_attempts" in merged.columns
            else 0
        )

        logger.info(
            f"  Passing: {passing_count} players | "
            f"Rushing: {rushing_count} players | "
            f"Receiving: {receiving_count} players"
        )
    else:
        logger.warning("No NGS data loaded - all stat types empty")

    return merged
