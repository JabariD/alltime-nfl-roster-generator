"""Type definitions for the ingest pipeline.

This module provides type-safe data structures for player data throughout the
ingestion process. All types are designed to work with pandas DataFrames while
providing strong type hints for mypy validation.
"""

from enum import Enum
from typing import Optional, TypedDict


class SourceTier(str, Enum):
    """Data source quality tiers for attribute mapping.

    Tiers represent measurement precision, not player quality:
    - TIER_1: Direct measurements (combine data)
    - TIER_2: Rich derived stats (NextGen, EPA, advanced metrics)
    - TIER_3: Basic stats (yards, TDs, completions)
    - TIER_4: Honors proxy (All-Pro, awards, positional averages)
    """

    TIER_1 = "TIER_1"
    TIER_2 = "TIER_2"
    TIER_3 = "TIER_3"
    TIER_4 = "TIER_4"


class EraBucket(str, Enum):
    """Historical era classifications for cross-era normalization.

    Eras defined by significant rule changes and data availability.
    See config/eras.yaml for detailed boundaries and characteristics.
    """

    DEAD_BALL = "dead_ball"           # 1920-1945
    POST_WWII = "post_wwii"            # 1946-1977
    PASSING_REVOLUTION = "passing_revolution"  # 1978-1993
    SALARY_CAP = "salary_cap"          # 1994-2003
    MODERN_PASS_HAPPY = "modern_pass_happy"    # 2004-2015
    ANALYTICS_ERA = "analytics_era"    # 2016-present


class PlayerRow(TypedDict, total=False):
    """Player biographical and identity data from nflverse.

    Maps directly to nflverse load_players() output.
    All fields optional to handle incomplete historical data.
    """

    # Identity
    gsis_id: str  # Primary key
    pfr_id: Optional[str]
    esb_id: Optional[str]
    display_name: str

    # Position and bio
    position: str
    college_name: Optional[str]
    birth_date: Optional[str]
    height: Optional[float]  # inches
    weight: Optional[float]  # pounds

    # Career span
    rookie_season: Optional[int]
    last_season: Optional[int]

    # Status
    status: Optional[str]


class CareerStats(TypedDict, total=False):
    """Aggregated career statistics from seasonal data.

    All offensive and defensive stats summed across career.
    Used for TIER_3 attribute mapping.
    """

    player_id: str

    # Career span
    first_year: int
    last_year: int
    career_seasons: int
    total_career_games: int

    # Offensive stats
    career_passing_yards: int
    career_rushing_yards: int
    career_receiving_yards: int
    career_passing_tds: int
    career_rushing_tds: int
    career_receiving_tds: int
    career_tds: int

    # Defensive stats (when available)
    def_solo_tackles: Optional[int]
    def_sacks: Optional[float]
    def_ints: Optional[int]


class PlayoffStats(TypedDict, total=False):
    """Aggregated playoff statistics from postseason data.

    Separate from regular season to enable clutch/legacy scoring.
    """

    player_id: str

    playoff_games: int
    playoff_passing_yards: int
    playoff_rushing_yards: int
    playoff_receiving_yards: int
    playoff_passing_tds: int
    playoff_rushing_tds: int
    playoff_receiving_tds: int
    playoff_tds: int


class CombineData(TypedDict, total=False):
    """NFL Combine physical measurements.

    TIER_1 data for speed/agility attributes.
    Only available for players who attended combine (1987+).
    """

    pfr_id: str

    # Measurables
    ht: Optional[float]  # height in inches
    wt: Optional[float]  # weight in pounds

    # Speed/agility
    forty: Optional[float]  # 40-yard dash (seconds)
    bench: Optional[int]    # bench press reps
    vertical: Optional[float]  # vertical jump (inches)
    broad_jump: Optional[int]  # broad jump (inches)
    cone: Optional[float]      # 3-cone drill (seconds)
    shuttle: Optional[float]   # 20-yard shuttle (seconds)


class DraftInfo(TypedDict, total=False):
    """Draft history and honors data.

    Used for both qualification (4-path criteria) and TIER_4 mapping.
    """

    gsis_id: str

    # Draft
    draft_season: Optional[int]
    pick: Optional[int]

    # Honors
    probowls: int
    allpro: int
    hof: bool

    # Career stats (from draft dataset as fallback)
    to: Optional[int]  # last year played
    seasons_started: Optional[int]


class NFLVerseLoadError(Exception):
    """Raised when nflverse data loading fails."""

    pass
