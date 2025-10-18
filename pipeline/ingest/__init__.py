"""Ingest pipeline for building comprehensive player index.

This package provides modules for loading, aggregating, and profiling NFL player
data from nflverse sources. The main entry point is build_players_index() which
orchestrates the full pipeline.

Main exports:
    build_players_index: CLI command to build players_index.csv and profiles
    NFLVerseLoader: Type-safe data loader class
    aggregate_career_stats: Vectorized career stats aggregation
    aggregate_playoff_stats: Vectorized playoff stats aggregation
    assign_era_bucket: Era bucket assignment for cross-era normalization
    generate_player_data_profiles: Data availability profiling (FRCS 4.3)
"""

from pipeline.ingest.aggregators import (
    aggregate_career_stats,
    aggregate_playoff_stats,
    merge_player_datasets,
)
from pipeline.ingest.data_profiler import generate_player_data_profiles
from pipeline.ingest.era_bucketer import (
    assign_era_bucket,
    get_era_bonus,
    get_era_metadata,
)
from pipeline.ingest.nflverse_loader import NFLVerseLoader, test_nflverse_connection
from pipeline.ingest.players_index import build_players_index
from pipeline.ingest.ingest_types import (
    CareerStats,
    CombineData,
    DraftInfo,
    EraBucket,
    NFLVerseLoadError,
    PlayerRow,
    PlayoffStats,
    SourceTier,
)

__all__ = [
    # Main CLI
    "build_players_index",
    # Loader
    "NFLVerseLoader",
    "test_nflverse_connection",
    # Aggregators
    "aggregate_career_stats",
    "aggregate_playoff_stats",
    "merge_player_datasets",
    # Era bucketing
    "assign_era_bucket",
    "get_era_metadata",
    "get_era_bonus",
    # Data profiling
    "generate_player_data_profiles",
    # Types
    "SourceTier",
    "EraBucket",
    "PlayerRow",
    "CareerStats",
    "PlayoffStats",
    "CombineData",
    "DraftInfo",
    "NFLVerseLoadError",
]
