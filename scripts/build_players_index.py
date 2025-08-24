#!/usr/bin/env python3
"""Build comprehensive player index from nflverse-data.

This module creates a comprehensive NFL player database by aggregating data from
multiple nflverse datasets. It produces a standardized CSV output suitable for
downstream analysis and rating calculations.

Data Sources:
    - Player biographical data and career spans
    - Season-by-season statistics (1999-2024+)
    - Draft history and honors (Pro Bowls, All-Pro, Hall of Fame)
    - NFL Combine physical measurements
    - Team affiliations by season

Output Schema:
    The resulting CSV contains player identity, career statistics, playoff
    performance, defensive metrics, honors, and physical measurements.

Limitations:
    - Pre-1974 data coverage is limited due to nflverse data availability
    - Historical legends from earlier eras require manual curation and research
    - Some players may be missing from automated datasets and need manual review
    
Data Quality Notes:
    - TODO: Data quality may need inspection (e.g., James Harrison was not included
      in the index despite being a notable player)
    - Older player records may have incomplete statistical coverage
    - Physical measurements only available for combine participants (1987+)

Usage:
    python build_players_index.py --out data/raw/players_index.csv --full

Website: https://nflreadr.nflverse.com/index.html
"""

# System imports
import logging
from pathlib import Path
from typing import Tuple

# Third-party imports
import click


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging with appropriate level and handlers.
    
    Args:
        verbose: Enable debug-level logging if True
        
    Returns:
        Configured logger instance
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("build_players_index.log", mode="a")
        ]
    )
    return logging.getLogger(__name__)


def _load_sample_data(nfl_module, logger: logging.Logger) -> Tuple[bool, dict]:
    """Load sample data from each nflverse dataset.
    
    Args:
        nfl_module: The nfl_data_py module
        logger: Logger instance
        
    Returns:
        Tuple of (success_flag, data_dict)
    """
    data = {}
    
    # Load players data
    logger.info("Loading players data (sample)...")
    players = nfl_module.import_players()
    logger.info(f"Players data loaded: {len(players)} records")
    data['players'] = players
    
    if not players.empty:
        sample_players = players.head(3)
        logger.info("Sample players:")
        for _, player in sample_players.iterrows():
            name = player.get('display_name', 'N/A')
            position = player.get('position', 'N/A')
            logger.info(f"  - {name} ({position})")
    
    # Load other datasets
    datasets = [
        ('rosters', lambda: nfl_module.import_seasonal_rosters(years=[2023])),
        ('combine', lambda: nfl_module.import_combine_data(years=[2023])),
        ('draft', lambda: nfl_module.import_draft_picks(years=[2023]))
    ]
    
    for name, loader in datasets:
        logger.info(f"Loading {name} data (sample)...")
        dataset = loader()
        logger.info(f"{name.title()} data loaded: {len(dataset)} records")
        data[name] = dataset
    
    return True, data


def test_nflverse_connection(logger: logging.Logger) -> bool:
    """Test connection to nflverse data sources.
    
    Args:
        logger: Logger instance for output
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        import nfl_data_py as nfl
        
        logger.info("Testing nflverse-data connection...")
        success, _ = _load_sample_data(nfl, logger)
        
        if success:
            logger.info("✅ nflverse-data connection test successful!")
        
        return success
        
    except ImportError as e:
        logger.error(f"❌ nfl-data-py not installed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing nflverse connection: {e}")
        return False


def _load_nflverse_datasets(nfl_module, logger: logging.Logger, full_build: bool) -> dict:
    """Load all required nflverse datasets based on build scope.
    
    Args:
        nfl_module: The nfl_data_py module
        logger: Logger instance
        full_build: Whether to load complete historical data
        
    Returns:
        Dictionary containing all loaded datasets
    """
    import pandas as pd
    
    datasets = {}
    
    # Always load complete player data
    logger.info("Loading players data...")
    datasets['players'] = nfl_module.import_players()
    logger.info(f"Loaded {len(datasets['players'])} total players")
    
    if full_build:
        logger.info("Loading complete historical datasets...")
        year_ranges = {
            'seasonal': (1970, 2024),
            'draft': (1970, 2024),
            'combine': (1987, 2024)
        }
    else:
        logger.info("Loading recent datasets for testing...")
        year_ranges = {
            'seasonal': (2022, 2024),
            'draft': (1970, 2024),  # Always load full draft history
            'combine': (2020, 2024)
        }
    
    # Load seasonal data
    seasonal_years = list(range(*year_ranges['seasonal']))
    datasets['seasonal'] = _load_seasonal_data_safe(nfl_module, seasonal_years, 'REG', logger)
    datasets['playoff'] = _load_seasonal_data_safe(nfl_module, seasonal_years, 'POST', logger)
    
    # Load draft and combine data
    draft_years = list(range(*year_ranges['draft']))
    datasets['draft'] = nfl_module.import_draft_picks(years=draft_years)
    logger.info(f"Loaded {len(datasets['draft'])} draft records")
    
    combine_years = list(range(*year_ranges['combine']))
    datasets['combine'] = nfl_module.import_combine_data(years=combine_years)
    logger.info(f"Loaded {len(datasets['combine'])} combine records")
    
    return datasets


def _load_seasonal_data_safe(nfl_module, years: list, season_type: str, logger: logging.Logger):
    """Safely load seasonal data with fallback for older years.
    
    Args:
        nfl_module: The nfl_data_py module
        years: List of years to load
        season_type: 'REG' for regular season, 'POST' for playoffs
        logger: Logger instance
        
    Returns:
        Loaded seasonal data DataFrame
    """
    season_name = "regular season" if season_type == 'REG' else "playoff"
    logger.info(f"Loading {season_name} data ({min(years)}-{max(years)})...")
    
    try:
        data = nfl_module.import_seasonal_data(years=years, s_type=season_type)
    except Exception as e:
        if min(years) <= 1998:
            logger.warning(f"Failed to load from {min(years)}, falling back to 1999: {e}")
            fallback_years = [y for y in years if y >= 1999]
            data = nfl_module.import_seasonal_data(years=fallback_years, s_type=season_type)
        else:
            raise
    
    logger.info(f"Loaded {len(data)} {season_name} records")
    return data


def build_comprehensive_index(logger: logging.Logger, output_path: Path, full_build: bool = False) -> bool:
    """Build comprehensive player index with all available stats and physical attributes.
    
    Args:
        logger: Logger instance
        output_path: Path to save the output CSV
        full_build: If True, build complete historical dataset
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import nfl_data_py as nfl
        
        scope = "FULL" if full_build else "test sample"
        logger.info(f"Building comprehensive player index ({scope})...")
        
        # Load all datasets
        datasets = _load_nflverse_datasets(nfl, logger, full_build)
        
        # Process and aggregate all statistics
        career_stats = _aggregate_career_stats(datasets['seasonal'], logger)
        playoff_stats = _aggregate_playoff_stats(datasets['playoff'], logger)
        
        # Merge all datasets into comprehensive player data
        enhanced_players = _merge_player_datasets(datasets, career_stats, playoff_stats, logger)
        
        # Create final comprehensive schema
        final_index = _build_output_schema(enhanced_players, logger)
        
        # Apply filtering and limits
        final_index = _filter_players(final_index, enhanced_players, full_build, logger)
        
        # Save and report results
        _save_and_report_results(final_index, output_path, logger)
        
        return True
        
    except Exception as e:
        logger.error(f"Error building comprehensive index: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False


def _aggregate_career_stats(seasonal_data, logger: logging.Logger):
    """Aggregate career statistics from seasonal data.
    
    Args:
        seasonal_data: DataFrame containing seasonal statistics
        logger: Logger instance
        
    Returns:
        DataFrame with aggregated career statistics
    """
    import pandas as pd
    
    logger.info("Aggregating career statistics...")
    
    stat_columns = {
        'games': 'sum',
        'passing_yards': 'sum',
        'rushing_yards': 'sum', 
        'receiving_yards': 'sum',
        'passing_tds': 'sum',
        'rushing_tds': 'sum',
        'receiving_tds': 'sum',
        'season': ['min', 'max', 'count']
    }
    
    career_stats = seasonal_data.groupby('player_id').agg(stat_columns).reset_index()
    
    # Flatten column names
    career_stats.columns = [
        'player_id', 'total_career_games', 'career_passing_yards',
        'career_rushing_yards', 'career_receiving_yards', 
        'career_passing_tds', 'career_rushing_tds', 'career_receiving_tds',
        'first_year', 'last_year', 'career_seasons'
    ]
    
    # Calculate total TDs
    career_stats['career_tds'] = (
        career_stats['career_passing_tds'] + 
        career_stats['career_rushing_tds'] + 
        career_stats['career_receiving_tds']
    )
    
    return career_stats


def _aggregate_playoff_stats(playoff_data, logger: logging.Logger):
    """Aggregate playoff statistics from playoff data.
    
    Args:
        playoff_data: DataFrame containing playoff statistics
        logger: Logger instance
        
    Returns:
        DataFrame with aggregated playoff statistics
    """
    logger.info("Aggregating playoff statistics...")
    
    stat_columns = {
        'games': 'sum',
        'passing_yards': 'sum',
        'rushing_yards': 'sum', 
        'receiving_yards': 'sum',
        'passing_tds': 'sum',
        'rushing_tds': 'sum',
        'receiving_tds': 'sum'
    }
    
    playoff_stats = playoff_data.groupby('player_id').agg(stat_columns).reset_index()
    
    # Rename columns with playoff_ prefix
    playoff_stats.columns = [
        'player_id', 'playoff_games', 'playoff_passing_yards',
        'playoff_rushing_yards', 'playoff_receiving_yards', 
        'playoff_passing_tds', 'playoff_rushing_tds', 'playoff_receiving_tds'
    ]
    
    # Calculate total playoff TDs
    playoff_stats['playoff_tds'] = (
        playoff_stats['playoff_passing_tds'] + 
        playoff_stats['playoff_rushing_tds'] + 
        playoff_stats['playoff_receiving_tds']
    )
    
    return playoff_stats


def _merge_player_datasets(datasets: dict, career_stats, playoff_stats, logger: logging.Logger):
    """Merge all player datasets into comprehensive DataFrame.
    
    Args:
        datasets: Dictionary containing all loaded datasets
        career_stats: Aggregated career statistics
        playoff_stats: Aggregated playoff statistics
        logger: Logger instance
        
    Returns:
        Enhanced players DataFrame with all merged data
    """
    logger.info("Merging player datasets...")
    
    # Start with base player data
    enhanced_players = datasets['players'].copy()
    
    # Merge career stats
    logger.info("Merging career statistics...")
    enhanced_players = enhanced_players.merge(
        career_stats, left_on='gsis_id', right_on='player_id', how='left'
    )
    
    # Merge playoff stats
    logger.info("Merging playoff statistics...")
    enhanced_players = enhanced_players.merge(
        playoff_stats, left_on='gsis_id', right_on='player_id', how='left'
    )
    
    # Merge draft data
    logger.info("Merging draft and honors data...")
    draft_columns = [
        'gsis_id', 'season', 'to', 'probowls', 'allpro', 'hof', 'pick',
        'games', 'pass_yards', 'rush_yards', 'rec_yards', 
        'pass_tds', 'rush_tds', 'rec_tds', 'seasons_started',
        'def_solo_tackles', 'def_sacks', 'def_ints'
    ]
    
    draft_subset = datasets['draft'][draft_columns].copy()
    draft_subset = draft_subset.rename(columns={
        'season': 'draft_season', 
        'games': 'draft_games',
        'pass_yards': 'draft_pass_yards', 
        'rush_yards': 'draft_rush_yards', 
        'rec_yards': 'draft_rec_yards',
        'pass_tds': 'draft_pass_tds', 
        'rush_tds': 'draft_rush_tds', 
        'rec_tds': 'draft_rec_tds'
    })
    
    enhanced_players = enhanced_players.merge(draft_subset, on='gsis_id', how='left')
    
    # Merge combine data
    logger.info("Merging combine and physical data...")
    combine_columns = [
        'pfr_id', 'ht', 'wt', 'forty', 'bench', 'vertical', 
        'broad_jump', 'cone', 'shuttle'
    ]
    
    combine_subset = datasets['combine'][combine_columns].copy()
    combine_subset = combine_subset[
        combine_subset['pfr_id'].notna()
    ].drop_duplicates(subset=['pfr_id'])
    
    enhanced_players = enhanced_players.merge(
        combine_subset, left_on='pfr_id', right_on='pfr_id', how='left'
    )
    
    return enhanced_players


def _build_output_schema(enhanced_players, logger: logging.Logger):
    """Build the final output schema from enhanced player data.
    
    Args:
        enhanced_players: DataFrame with all merged player data
        logger: Logger instance
        
    Returns:
        DataFrame with standardized output schema
    """
    import pandas as pd
    
    logger.info("Building comprehensive output schema...")
    
    return pd.DataFrame({
        # Identity
        'player_id': enhanced_players['gsis_id'],
        'full_name': enhanced_players['display_name'],
        'primary_pos': enhanced_players['position'], 
        'college': enhanced_players['college_name'],
        'birth_date': enhanced_players['birth_date'],
        
        # Career span (use seasonal data first, then draft data, then player bio)
        'first_year': enhanced_players['rookie_season'].fillna(
            enhanced_players['first_year'].fillna(enhanced_players['draft_season'])
        ),
        'last_year': enhanced_players['last_season'].fillna(
            enhanced_players['last_year'].fillna(enhanced_players['to'])
        ),
        'career_seasons': enhanced_players['career_seasons'].fillna(
            enhanced_players['seasons_started'].fillna(0)
        ).astype(int),
        'total_career_games': enhanced_players['total_career_games'].fillna(
            enhanced_players['draft_games'].fillna(0)
        ).astype(int),
        
        # Offensive stats - use seasonal data first, then draft data
        'career_passing_yards': enhanced_players['career_passing_yards'].fillna(
            enhanced_players['draft_pass_yards'].fillna(0)
        ).astype(int),
        'career_rushing_yards': enhanced_players['career_rushing_yards'].fillna(
            enhanced_players['draft_rush_yards'].fillna(0)
        ).astype(int),
        'career_receiving_yards': enhanced_players['career_receiving_yards'].fillna(
            enhanced_players['draft_rec_yards'].fillna(0)
        ).astype(int),
        
        # Touchdown stats
        'career_passing_tds': enhanced_players['career_passing_tds'].fillna(
            enhanced_players['draft_pass_tds'].fillna(0)
        ).astype(int),
        'career_rushing_tds': enhanced_players['career_rushing_tds'].fillna(
            enhanced_players['draft_rush_tds'].fillna(0)
        ).astype(int),
        'career_receiving_tds': enhanced_players['career_receiving_tds'].fillna(
            enhanced_players['draft_rec_tds'].fillna(0)
        ).astype(int),
        'career_tds': enhanced_players['career_tds'].fillna(
            (enhanced_players['draft_pass_tds'].fillna(0) + 
             enhanced_players['draft_rush_tds'].fillna(0) + 
             enhanced_players['draft_rec_tds'].fillna(0))
        ).astype(int),
        
        # Playoff stats
        'playoff_games': enhanced_players['playoff_games'].fillna(0).astype(int),
        'playoff_passing_yards': enhanced_players['playoff_passing_yards'].fillna(0).astype(int),
        'playoff_rushing_yards': enhanced_players['playoff_rushing_yards'].fillna(0).astype(int),
        'playoff_receiving_yards': enhanced_players['playoff_receiving_yards'].fillna(0).astype(int),
        'playoff_passing_tds': enhanced_players['playoff_passing_tds'].fillna(0).astype(int),
        'playoff_rushing_tds': enhanced_players['playoff_rushing_tds'].fillna(0).astype(int),
        'playoff_receiving_tds': enhanced_players['playoff_receiving_tds'].fillna(0).astype(int),
        'playoff_tds': enhanced_players['playoff_tds'].fillna(0).astype(int),
        
        # Defensive stats  
        'def_solo_tackles': enhanced_players['def_solo_tackles'].fillna(0).astype(int),
        'def_sacks': enhanced_players['def_sacks'].fillna(0),
        'def_ints': enhanced_players['def_ints'].fillna(0).astype(int),
        
        # Draft and honors
        'draft_pick': enhanced_players['draft_pick'].fillna(999).astype(int),
        'pro_bowls': enhanced_players['probowls'].fillna(0).astype(int),
        'all_pros': enhanced_players['allpro'].fillna(0).astype(int),
        'hof_flag': enhanced_players['hof'].fillna(False).astype(bool),
        
        # Physical/Combine data
        'height_in': enhanced_players['height'].fillna(enhanced_players['ht']),
        'weight_lb': enhanced_players['weight'].fillna(enhanced_players['wt']),
        'forty_time': enhanced_players['forty'],
        'bench_press': enhanced_players['bench'],
        'vertical_jump': enhanced_players['vertical'], 
        'broad_jump': enhanced_players['broad_jump'],
        'three_cone': enhanced_players['cone'],
        'twenty_shuttle': enhanced_players['shuttle']
    })


def _filter_players(final_index, enhanced_players, full_build: bool, logger: logging.Logger):
    """Apply filtering criteria to select relevant players.
    
    Args:
        final_index: DataFrame with standardized schema
        enhanced_players: Original enhanced DataFrame for additional filtering
        full_build: Whether this is a full build or test
        logger: Logger instance
        
    Returns:
        Filtered DataFrame
    """
    logger.info("Applying inclusive filtering for comprehensive coverage...")
    
    # Define inclusion criteria
    criteria = [
        # Players with meaningful career data
        final_index['total_career_games'] > 0,
        final_index['career_seasons'] > 0,
        final_index['pro_bowls'] > 0,
        final_index['all_pros'] > 0,
        final_index['hof_flag'] == True,
        
        # Players with offensive stats
        final_index['career_passing_yards'] > 0,
        final_index['career_rushing_yards'] > 0,
        final_index['career_receiving_yards'] > 0,
        
        # Players with defensive stats
        final_index['def_solo_tackles'] > 0,
        final_index['def_sacks'] > 0,
        final_index['def_ints'] > 0,
        
        # Drafted players
        enhanced_players['draft_pick'].fillna(999) <= 300,
        
        # Players with NFL experience from draft data
        ((enhanced_players['draft_games'].fillna(0) > 0) & 
         ((enhanced_players['draft_pass_yards'].fillna(0) > 0) |
          (enhanced_players['draft_rush_yards'].fillna(0) > 0) |
          (enhanced_players['draft_rec_yards'].fillna(0) > 0) |
          (enhanced_players['def_solo_tackles'].fillna(0) > 0) |
          (enhanced_players['def_sacks'].fillna(0) > 0) |
          (enhanced_players['def_ints'].fillna(0) > 0)))
    ]
    
    # Combine all criteria with OR logic
    import pandas as pd
    combined_filter = pd.concat([c for c in criteria], axis=1).any(axis=1)
    filtered_index = final_index[combined_filter].copy()
    
    # Limit for test builds
    if not full_build:
        filtered_index = filtered_index.head(100)
        logger.info("Limited to first 100 players for test build")
    
    return filtered_index


def _save_and_report_results(final_index, output_path: Path, logger: logging.Logger):
    """Save results and generate comprehensive report.
    
    Args:
        final_index: Final DataFrame to save
        output_path: Path to save CSV file
        logger: Logger instance
    """
    import pandas as pd
    
    # Save to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_index.to_csv(output_path, index=False)
    
    logger.info(f"Comprehensive player index saved: {len(final_index)} players → {output_path}")
    
    # Show sample entries
    logger.info("Sample entries with comprehensive schema:")
    for _, player in final_index.head(5).iterrows():
        height_str = f"{player['height_in']}\"" if pd.notna(player['height_in']) else "N/A"
        weight_str = f"{player['weight_lb']}lb" if pd.notna(player['weight_lb']) else "N/A"
        forty_str = f"{player['forty_time']}s" if pd.notna(player['forty_time']) else "N/A"
        
        logger.info(
            f"  - {player['full_name']} ({player['primary_pos']}): "
            f"{player['career_seasons']} seasons, {player['total_career_games']} games, "
            f"{player['career_tds']} TDs, {player['pro_bowls']} Pro Bowls | "
            f"Playoffs: {player['playoff_games']} games, {player['playoff_tds']} TDs | "
            f"Defense: {player['def_solo_tackles']} tackles, {player['def_sacks']} sacks | "
            f"{height_str}, {weight_str}, 40yd: {forty_str}"
        )
    
    # Generate summary statistics
    _generate_summary_report(final_index, logger)
    
    logger.info("✅ Comprehensive player index built successfully!")


def _generate_summary_report(final_index, logger: logging.Logger):
    """Generate comprehensive summary statistics.
    
    Args:
        final_index: Final DataFrame with results
        logger: Logger instance
    """
    import pandas as pd
    
    logger.info("Comprehensive index summary:")
    logger.info(f"  Total players: {len(final_index):,}")
    logger.info(f"  With >10 games: {len(final_index[final_index['total_career_games'] > 10]):,}")
    logger.info(f"  With Pro Bowls: {len(final_index[final_index['pro_bowls'] > 0]):,}")
    logger.info(f"  With offensive TDs: {len(final_index[final_index['career_tds'] > 0]):,}")
    logger.info(f"  With playoff experience: {len(final_index[final_index['playoff_games'] > 0]):,}")
    logger.info(f"  With playoff TDs: {len(final_index[final_index['playoff_tds'] > 0]):,}")
    
    defensive_filter = (
        (final_index['def_solo_tackles'] > 0) | 
        (final_index['def_sacks'] > 0) | 
        (final_index['def_ints'] > 0)
    )
    logger.info(f"  With defensive stats: {len(final_index[defensive_filter]):,}")
    logger.info(f"  With combine data: {len(final_index[pd.notna(final_index['forty_time'])]):,}")
    logger.info(f"  Hall of Fame: {len(final_index[final_index['hof_flag'] == True]):,}")
    
    # Position breakdown
    logger.info("  === POSITION BREAKDOWN ===")
    pos_counts = final_index['primary_pos'].value_counts().head(15)
    for pos, count in pos_counts.items():
        logger.info(f"    {pos}: {count:,}")
    
    # Stats coverage
    logger.info("  === STATS COVERAGE ===")
    stat_categories = [
        ('passing yards', 'career_passing_yards'),
        ('rushing yards', 'career_rushing_yards'),
        ('receiving yards', 'career_receiving_yards'),
        ('tackles', 'def_solo_tackles'),
        ('sacks', 'def_sacks'),
        ('interceptions', 'def_ints')
    ]
    
    for name, column in stat_categories:
        count = len(final_index[final_index[column] > 0])
        logger.info(f"  Players with {name}: {count:,}")
    
    # Data quality validation
    logger.info("=== DATA QUALITY VALIDATION ===")
    
    null_names = final_index['full_name'].isna().sum()
    if null_names > 0:
        logger.warning(f"  ⚠️  {null_names} players missing names")
    else:
        logger.info("  ✅ All players have names")
    
    missing_positions = final_index['primary_pos'].isna().sum()
    if missing_positions > 0:
        logger.warning(f"  ⚠️  {missing_positions} players missing positions")
    else:
        logger.info("  ✅ All players have positions")
    
    # Year range validation
    min_year = final_index['first_year'].min()
    max_year = final_index['last_year'].max()
    logger.info(f"  Career year range: {min_year} - {max_year}")


@click.command()
@click.option('--out', '-o', 
              type=click.Path(), 
              default='data/raw/players_index.csv',
              help='Output CSV file path for the comprehensive player index')
@click.option('--test-only', '-t',
              is_flag=True,
              help='Test nflverse connection only, skip index building')
@click.option('--full', '-f',
              is_flag=True,
              help='Build complete historical dataset (1970-2023) vs recent sample')
@click.option('--verbose', '-v', 
              is_flag=True,
              help='Enable debug-level logging for detailed output')
def main(out: str, test_only: bool, full: bool, verbose: bool) -> None:
    """Build comprehensive NFL player index from nflverse data sources.
    
    Creates a standardized CSV containing player biographical data, career
    statistics, playoff performance, honors, and physical measurements.
    
    Args:
        out: Output CSV file path
        test_only: Only test data connection, don't build index
        full: Build complete historical dataset vs recent sample
        verbose: Enable debug logging
    """
    logger = setup_logging(verbose)
    output_path = Path(out)
    
    scope = "FULL BUILD" if full else "Test Sample"
    logger.info(f"=== nflverse-data Player Index Builder ({scope}) ===")
    
    # Test connection first
    if not test_nflverse_connection(logger):
        logger.error("Connection test failed. Check nfl-data-py installation.")
        return
    
    if test_only:
        logger.info("Test-only mode complete.")
        return
    
    # Build player index
    success = build_comprehensive_index(logger, output_path, full_build=full)
    
    if success:
        result_type = "Full" if full else "Sample"
        logger.info(f"✅ {result_type} comprehensive player index built successfully!")
    else:
        logger.error("❌ Failed to build comprehensive player index")


if __name__ == "__main__":
    main()