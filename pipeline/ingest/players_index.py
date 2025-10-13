"""CLI orchestration for building comprehensive player index.

Coordinates all ingest modules to produce:
- players_index.csv (master player list with stats)
- player_data_profiles.parquet (data availability metadata)
- ingest_manifest.json (run metadata)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import click
import pandas as pd

from pipeline.ingest.aggregators import (
    aggregate_career_stats,
    aggregate_playoff_stats,
    merge_player_datasets,
)
from pipeline.ingest.data_profiler import generate_player_data_profiles
from pipeline.ingest.era_bucketer import assign_era_bucket
from pipeline.ingest.nflverse_loader import NFLVerseLoader, test_nflverse_connection

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging with appropriate level and handlers.

    Args:
        verbose: Enable debug-level logging if True
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("build_players_index.log", mode="a"),
        ],
    )


def build_output_schema(enhanced_players: pd.DataFrame) -> pd.DataFrame:
    """Build the final output schema for players_index.csv.

    Maps enhanced player data to standardized output columns.
    Uses fallback logic for missing data (seasonal → draft → bio).

    Args:
        enhanced_players: DataFrame with all merged player data

    Returns:
        DataFrame with standardized schema matching design spec
    """
    logger.info("Building output schema...")

    return pd.DataFrame(
        {
            # Identity
            "player_id": enhanced_players["gsis_id"],
            "full_name": enhanced_players["display_name"],
            "primary_pos": enhanced_players["position"],
            "college": enhanced_players["college_name"],
            "birth_date": enhanced_players["birth_date"],
            # Career span (fallback chain: seasonal → draft → bio)
            "first_year": enhanced_players["rookie_season"].fillna(
                enhanced_players["first_year"].fillna(
                    enhanced_players.get("draft_season", pd.NA)
                )
            ),
            "last_year": enhanced_players["last_season"].fillna(
                enhanced_players["last_year"].fillna(enhanced_players.get("to", pd.NA))
            ),
            "career_seasons": enhanced_players["career_seasons"]
            .fillna(enhanced_players.get("seasons_started", 0).fillna(0))
            .astype(int),
            "total_career_games": enhanced_players["total_career_games"]
            .fillna(enhanced_players.get("draft_games", 0).fillna(0))
            .astype(int),
            # Offensive stats - use seasonal data first, then draft data
            "career_passing_yards": enhanced_players["career_passing_yards"]
            .fillna(enhanced_players.get("draft_pass_yards", 0).fillna(0))
            .astype(int),
            "career_rushing_yards": enhanced_players["career_rushing_yards"]
            .fillna(enhanced_players.get("draft_rush_yards", 0).fillna(0))
            .astype(int),
            "career_receiving_yards": enhanced_players["career_receiving_yards"]
            .fillna(enhanced_players.get("draft_rec_yards", 0).fillna(0))
            .astype(int),
            # Touchdown stats
            "career_passing_tds": enhanced_players["career_passing_tds"]
            .fillna(enhanced_players.get("draft_pass_tds", 0).fillna(0))
            .astype(int),
            "career_rushing_tds": enhanced_players["career_rushing_tds"]
            .fillna(enhanced_players.get("draft_rush_tds", 0).fillna(0))
            .astype(int),
            "career_receiving_tds": enhanced_players["career_receiving_tds"]
            .fillna(enhanced_players.get("draft_rec_tds", 0).fillna(0))
            .astype(int),
            "career_tds": enhanced_players["career_tds"]
            .fillna(
                enhanced_players.get("draft_pass_tds", 0).fillna(0)
                + enhanced_players.get("draft_rush_tds", 0).fillna(0)
                + enhanced_players.get("draft_rec_tds", 0).fillna(0)
            )
            .astype(int),
            # Playoff stats
            "playoff_games": enhanced_players["playoff_games"].fillna(0).astype(int),
            "playoff_passing_yards": enhanced_players["playoff_passing_yards"]
            .fillna(0)
            .astype(int),
            "playoff_rushing_yards": enhanced_players["playoff_rushing_yards"]
            .fillna(0)
            .astype(int),
            "playoff_receiving_yards": enhanced_players["playoff_receiving_yards"]
            .fillna(0)
            .astype(int),
            "playoff_passing_tds": enhanced_players["playoff_passing_tds"]
            .fillna(0)
            .astype(int),
            "playoff_rushing_tds": enhanced_players["playoff_rushing_tds"]
            .fillna(0)
            .astype(int),
            "playoff_receiving_tds": enhanced_players["playoff_receiving_tds"]
            .fillna(0)
            .astype(int),
            "playoff_tds": enhanced_players["playoff_tds"].fillna(0).astype(int),
            # Defensive stats
            "def_solo_tackles": enhanced_players.get("def_solo_tackles", 0)
            .fillna(0)
            .astype(int),
            "def_sacks": enhanced_players.get("def_sacks", 0.0).fillna(0.0),
            "def_ints": enhanced_players.get("def_ints", 0).fillna(0).astype(int),
            # Draft and honors
            "draft_pick": enhanced_players.get("pick", 999).fillna(999).astype(int),
            "pro_bowls": enhanced_players.get("probowls", 0).fillna(0).astype(int),
            "all_pros": enhanced_players.get("allpro", 0).fillna(0).astype(int),
            "hof_flag": enhanced_players.get("hof", False).fillna(False).astype(bool),
            # Physical/Combine data
            "height_in": enhanced_players["height"].fillna(
                enhanced_players.get("ht", pd.NA)
            ),
            "weight_lb": enhanced_players["weight"].fillna(
                enhanced_players.get("wt", pd.NA)
            ),
            "forty_time": enhanced_players.get("forty", pd.NA),
            "bench_press": enhanced_players.get("bench", pd.NA),
            "vertical_jump": enhanced_players.get("vertical", pd.NA),
            "broad_jump": enhanced_players.get("broad_jump", pd.NA),
            "three_cone": enhanced_players.get("cone", pd.NA),
            "twenty_shuttle": enhanced_players.get("shuttle", pd.NA),
        }
    )


def apply_filtering(final_index: pd.DataFrame, full_build: bool) -> pd.DataFrame:
    """Apply filtering criteria to select relevant players.

    Uses inclusive OR logic to capture all players with meaningful data.

    Args:
        final_index: DataFrame with standardized schema
        full_build: Whether this is a full build or test

    Returns:
        Filtered DataFrame
    """
    logger.info("Applying inclusive filtering for comprehensive coverage...")

    # Define inclusion criteria (OR logic)
    criteria = [
        # Players with meaningful career data
        final_index["total_career_games"] > 0,
        final_index["career_seasons"] > 0,
        final_index["pro_bowls"] > 0,
        final_index["all_pros"] > 0,
        final_index["hof_flag"],
        # Players with offensive stats
        final_index["career_passing_yards"] > 0,
        final_index["career_rushing_yards"] > 0,
        final_index["career_receiving_yards"] > 0,
        # Players with defensive stats
        final_index["def_solo_tackles"] > 0,
        final_index["def_sacks"] > 0,
        final_index["def_ints"] > 0,
        # Drafted players
        final_index["draft_pick"] <= 300,
    ]

    # Combine with OR logic
    combined_filter = pd.concat([c for c in criteria], axis=1).any(axis=1)
    filtered_index = final_index[combined_filter].copy()

    # Limit for test builds
    if not full_build:
        filtered_index = filtered_index.head(100)
        logger.info("Limited to first 100 players for test build")

    return filtered_index


def generate_manifest(
    output_dir: Path, players_count: int, profiles_count: int, config: Dict[str, Any]
) -> None:
    """Generate ingest manifest with run metadata.

    Args:
        output_dir: Directory containing output files
        players_count: Number of players in index
        profiles_count: Number of profiles generated
        config: Configuration dictionary
    """
    manifest = {
        "ingest_version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "outputs": {
            "players_index": str(output_dir / "players_index.csv"),
            "player_data_profiles": str(output_dir / "player_data_profiles.parquet"),
        },
        "counts": {"players": players_count, "profiles": profiles_count},
        "config": config,
    }

    manifest_path = output_dir / "ingest_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Manifest saved: {manifest_path}")


@click.command()
@click.option(
    "--out",
    "-o",
    type=click.Path(),
    default="data/raw",
    help="Output directory for players_index.csv and profiles",
)
@click.option(
    "--test-only",
    "-t",
    is_flag=True,
    help="Test nflverse connection only, skip index building",
)
@click.option(
    "--full",
    "-f",
    is_flag=True,
    help="Build complete historical dataset (1970-2024) vs recent sample",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable debug-level logging for detailed output",
)
def build_players_index(
    out: str, test_only: bool, full: bool, verbose: bool
) -> None:
    """Build comprehensive NFL player index from nflverse data sources.

    Creates:
    - players_index.csv: Master player list with stats
    - player_data_profiles.parquet: Data availability metadata (FRCS 4.3)
    - ingest_manifest.json: Run metadata

    Args:
        out: Output directory path
        test_only: Only test data connection, don't build index
        full: Build complete historical dataset vs recent sample
        verbose: Enable debug logging
    """
    setup_logging(verbose)
    output_dir = Path(out)
    output_dir.mkdir(parents=True, exist_ok=True)

    scope = "FULL BUILD" if full else "Test Sample"
    logger.info(f"=== nflverse-data Player Index Builder ({scope}) ===")

    # Test connection first
    if not test_nflverse_connection():
        logger.error("Connection test failed. Check nfl-data-py installation.")
        return

    if test_only:
        logger.info("Test-only mode complete.")
        return

    # Build player index
    try:
        logger.info("Initializing NFLVerse loader...")
        loader = NFLVerseLoader(verbose=verbose)

        # Load all datasets
        logger.info("Loading datasets...")
        players, seasonal, playoff, draft, combine = loader.load_all_datasets(full)

        # Aggregate statistics
        career_stats = aggregate_career_stats(seasonal)
        playoff_stats = aggregate_playoff_stats(playoff)

        # Merge datasets
        enhanced_players = merge_player_datasets(
            players, career_stats, playoff_stats, draft, combine
        )

        # Build output schema
        final_index = build_output_schema(enhanced_players)

        # Apply filtering
        final_index = apply_filtering(final_index, full)

        # Assign era buckets
        final_index = assign_era_bucket(
            final_index, config_path=Path("config/eras.yaml")
        )

        # Generate data profiles
        # Need to add era_bucket to enhanced_players for profiling
        enhanced_players = enhanced_players.merge(
            final_index[["player_id", "era_bucket"]],
            left_on="gsis_id",
            right_on="player_id",
            how="left",
        )

        profiles = generate_player_data_profiles(enhanced_players)

        # Save outputs
        players_path = output_dir / "players_index.csv"
        profiles_path = output_dir / "player_data_profiles.parquet"

        final_index.to_csv(players_path, index=False)
        logger.info(f"Players index saved: {players_path} ({len(final_index)} players)")

        profiles.to_parquet(profiles_path, index=False)
        logger.info(f"Data profiles saved: {profiles_path} ({len(profiles)} profiles)")

        # Generate manifest
        config = {
            "full_build": full,
            "verbose": verbose,
            "era_config": "config/eras.yaml",
        }
        generate_manifest(output_dir, len(final_index), len(profiles), config)

        logger.info("✅ Player index build complete!")

    except Exception as e:
        logger.error(f"❌ Failed to build player index: {e}")
        import traceback

        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    build_players_index()
