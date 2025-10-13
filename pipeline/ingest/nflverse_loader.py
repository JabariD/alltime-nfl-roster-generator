"""Type-safe NFLVerse data loader with error handling.

This module provides a clean abstraction over nflverse-data API calls with:
- Type-annotated return values
- Consistent error handling
- Progress logging
- Safe fallback for historical data gaps
"""

import logging
from typing import List, Optional, Tuple

import pandas as pd

from pipeline.ingest.types import NFLVerseLoadError

logger = logging.getLogger(__name__)


class NFLVerseLoader:
    """Type-safe loader for nflverse datasets with error handling."""

    def __init__(self, verbose: bool = False):
        """Initialize loader with nfl_data_py module.

        Args:
            verbose: Enable debug-level logging

        Raises:
            NFLVerseLoadError: If nfl_data_py not installed
        """
        try:
            import nfl_data_py as nfl

            self.nfl = nfl
            self.verbose = verbose
        except ImportError as e:
            raise NFLVerseLoadError(f"nfl-data-py not installed: {e}") from e

    def load_players(self) -> pd.DataFrame:
        """Load complete player biographical data.

        Returns:
            DataFrame with player identity, position, physicals, career span

        Raises:
            NFLVerseLoadError: If load fails
        """
        try:
            logger.info("Loading players data...")
            players = self.nfl.import_players()
            logger.info(f"Loaded {len(players)} total players")

            if self.verbose and not players.empty:
                sample = players.head(3)
                logger.debug("Sample players:")
                for _, player in sample.iterrows():
                    name = player.get("display_name", "N/A")
                    position = player.get("position", "N/A")
                    logger.debug(f"  - {name} ({position})")

            return players

        except Exception as e:
            raise NFLVerseLoadError(f"Failed to load players: {e}") from e

    def load_seasonal_data(
        self, years: List[int], season_type: str = "REG"
    ) -> pd.DataFrame:
        """Load seasonal player statistics with safe fallback.

        Handles pre-1999 data gaps by falling back to 1999+ if needed.

        Args:
            years: List of years to load
            season_type: 'REG' for regular season, 'POST' for playoffs

        Returns:
            DataFrame with seasonal stats (yards, TDs, games, etc.)

        Raises:
            NFLVerseLoadError: If load fails even with fallback
        """
        season_name = "regular season" if season_type == "REG" else "playoff"

        try:
            logger.info(f"Loading {season_name} data ({min(years)}-{max(years)})...")
            data = self.nfl.import_seasonal_data(years=years, s_type=season_type)
            logger.info(f"Loaded {len(data)} {season_name} records")
            return data

        except Exception as e:
            # Try fallback to 1999+ if loading older data
            if min(years) <= 1998:
                logger.warning(
                    f"Failed to load from {min(years)}, falling back to 1999+: {e}"
                )
                fallback_years = [y for y in years if y >= 1999]
                if not fallback_years:
                    raise NFLVerseLoadError(
                        f"No valid fallback years for {season_type}"
                    ) from e

                try:
                    data = self.nfl.import_seasonal_data(
                        years=fallback_years, s_type=season_type
                    )
                    logger.info(
                        f"Loaded {len(data)} {season_name} records (1999+ fallback)"
                    )
                    return data
                except Exception as fallback_e:
                    raise NFLVerseLoadError(
                        f"Fallback load failed: {fallback_e}"
                    ) from fallback_e
            else:
                raise NFLVerseLoadError(
                    f"Failed to load {season_type} data: {e}"
                ) from e

    def load_combine(self, years: List[int]) -> pd.DataFrame:
        """Load NFL Combine physical measurements.

        Args:
            years: List of years to load (typically 1987+)

        Returns:
            DataFrame with 40-time, bench, vertical, etc.

        Raises:
            NFLVerseLoadError: If load fails
        """
        try:
            logger.info(f"Loading combine data ({min(years)}-{max(years)})...")
            data = self.nfl.import_combine_data(years=years)
            logger.info(f"Loaded {len(data)} combine records")
            return data

        except Exception as e:
            raise NFLVerseLoadError(f"Failed to load combine data: {e}") from e

    def load_draft_picks(self, years: List[int]) -> pd.DataFrame:
        """Load draft history and honors.

        Args:
            years: List of draft years to load

        Returns:
            DataFrame with draft pick, Pro Bowls, All-Pro, HOF, career stats

        Raises:
            NFLVerseLoadError: If load fails
        """
        try:
            logger.info(f"Loading draft data ({min(years)}-{max(years)})...")
            data = self.nfl.import_draft_picks(years=years)
            logger.info(f"Loaded {len(data)} draft records")
            return data

        except Exception as e:
            raise NFLVerseLoadError(f"Failed to load draft data: {e}") from e

    def load_nextgen_stats(
        self, stat_type: str = "passing", years: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """Load Next Gen Stats (2016+ advanced metrics).

        Args:
            stat_type: One of 'passing', 'rushing', 'receiving'
            years: List of years to load (defaults to all available)

        Returns:
            DataFrame with advanced metrics (separation, speed, EPA, etc.)

        Raises:
            NFLVerseLoadError: If load fails
        """
        try:
            logger.info(f"Loading NextGen {stat_type} stats...")
            data = self.nfl.import_nextgen_stats(stat_type=stat_type, years=years)
            logger.info(f"Loaded {len(data)} NextGen {stat_type} records")
            return data

        except Exception as e:
            raise NFLVerseLoadError(
                f"Failed to load NextGen {stat_type} stats: {e}"
            ) from e

    def load_all_datasets(
        self, full_build: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all required datasets with appropriate year ranges.

        Args:
            full_build: If True, load complete historical data (1970-2024).
                       If False, load recent sample for testing (2022-2024).

        Returns:
            Tuple of (players, seasonal, playoff, draft, combine) DataFrames

        Raises:
            NFLVerseLoadError: If any dataset fails to load
        """
        # Define year ranges based on build scope
        if full_build:
            logger.info("Loading complete historical datasets...")
            year_ranges = {
                "seasonal": (1970, 2024),
                "draft": (1970, 2024),
                "combine": (1987, 2024),
            }
        else:
            logger.info("Loading recent datasets for testing...")
            year_ranges = {
                "seasonal": (2022, 2024),
                "draft": (1970, 2024),  # Always load full draft history
                "combine": (2020, 2024),
            }

        # Load all datasets
        players = self.load_players()

        seasonal_years = list(range(*year_ranges["seasonal"]))
        seasonal = self.load_seasonal_data(seasonal_years, "REG")
        playoff = self.load_seasonal_data(seasonal_years, "POST")

        draft_years = list(range(*year_ranges["draft"]))
        draft = self.load_draft_picks(draft_years)

        combine_years = list(range(*year_ranges["combine"]))
        combine = self.load_combine(combine_years)

        return players, seasonal, playoff, draft, combine


def test_nflverse_connection() -> bool:
    """Test connection to nflverse data sources.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        loader = NFLVerseLoader(verbose=True)

        # Load sample data
        logger.info("Testing nflverse-data connection...")
        players = loader.load_players()

        if players.empty:
            logger.error("Loaded empty players dataset")
            return False

        # Test other datasets
        rosters = loader.nfl.import_seasonal_rosters(years=[2023])
        logger.info(f"Rosters data loaded: {len(rosters)} records")

        combine = loader.load_combine(years=[2023])
        logger.info(f"Combine data loaded: {len(combine)} records")

        draft = loader.load_draft_picks(years=[2023])
        logger.info(f"Draft data loaded: {len(draft)} records")

        logger.info("✅ nflverse-data connection test successful!")
        return True

    except NFLVerseLoadError as e:
        logger.error(f"❌ nflverse connection test failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error testing nflverse: {e}")
        return False
