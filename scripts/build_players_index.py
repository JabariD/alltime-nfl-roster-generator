#!/usr/bin/env python3
"""
Build comprehensive player index from nflverse-data.

Uses nflverse datasets via nfl-data-py to create comprehensive player database
with all available stats and physical attributes for downstream analysis.
Respects data usage policies and provides regularly-updated, high-quality NFL data.

Website: https://nflreadr.nflverse.com/index.html

DATA SOURCES:
=============

Available datasets (nfl-data-py capabilities):
- import_players(): Biographical data + career spans (rookie_season, last_season)
- import_seasonal_data(): Season-by-season statistics 
  * Coverage: 1999-2023+ confirmed
  * Stats: passing_yards, rushing_yards, receiving_yards, TDs, defensive stats
- import_draft_picks(): Draft history + honors data  
  * probowls, allpro, hof columns + career stat summaries
- import_combine_data(): Physical measurements and athletic testing
- import_seasonal_rosters(): Team affiliations by season

OUTPUT SCHEMA (data/raw/players_index.csv):
===========================================

- player_id (nflverse gsis_id), full_name, primary_pos, college, birth_date
- career_span: first_year, last_year, career_seasons, total_career_games
- offensive_stats: career_passing_yards, career_rushing_yards, career_receiving_yards
- scoring: career_passing_tds, career_rushing_tds, career_receiving_tds, career_tds
- playoff_stats: playoff_games, playoff_passing_yards, playoff_rushing_yards, playoff_receiving_yards
- playoff_scoring: playoff_passing_tds, playoff_rushing_tds, playoff_receiving_tds, playoff_tds
- defensive_stats: def_solo_tackles, def_sacks, def_ints
- honors: pro_bowls, all_pros, hof_flag, draft_pick
- physical: height_in, weight_lb, forty_time, bench_press, vertical_jump, broad_jump, three_cone, twenty_shuttle

Purpose: Provide raw comprehensive data for downstream analysis and scoring.
"""

import logging
from pathlib import Path

import click


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the script."""
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


def test_nflverse_connection(logger: logging.Logger) -> bool:
    """Test connection to nflverse data and show sample data."""
    try:
        import nfl_data_py as nfl
        
        logger.info("Testing nflverse-data connection...")
        
        # Test 1: Load a small sample of player data
        logger.info("Loading players data (sample)...")
        players = nfl.import_players()
        logger.info(f"Players data loaded: {len(players)} records")
        logger.info(f"Players columns: {list(players.columns)}")
        
        # Show sample of players data
        if not players.empty:
            sample_players = players.head(3)
            logger.info("Sample players:")
            for _, player in sample_players.iterrows():
                logger.info(f"  - {player.get('display_name', 'N/A')} ({player.get('position', 'N/A')})")
        
        # Test 2: Load roster data for most recent season
        logger.info("Loading seasonal roster data (sample)...")
        rosters = nfl.import_seasonal_rosters(years=[2023])  # Load just 2023
        logger.info(f"Seasonal rosters data loaded: {len(rosters)} records")
        logger.info(f"Rosters columns: {list(rosters.columns)}")
        
        # Test 3: Check combine data availability
        logger.info("Loading combine data (sample)...")
        combine = nfl.import_combine_data(years=[2023])  # Just recent year
        logger.info(f"Combine data loaded: {len(combine)} records")
        logger.info(f"Combine columns: {list(combine.columns)}")
        
        # Test 4: Check draft picks
        logger.info("Loading draft picks (sample)...")
        draft = nfl.import_draft_picks(years=[2023])  # Just recent year
        logger.info(f"Draft data loaded: {len(draft)} records")
        logger.info(f"Draft columns: {list(draft.columns)}")
        
        logger.info("✅ nflverse-data connection test successful!")
        return True
        
    except ImportError as e:
        logger.error(f"❌ nfl-data-py not installed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing nflverse connection: {e}")
        return False


def build_comprehensive_index(logger: logging.Logger, output_path: Path, full_build: bool = False) -> bool:
    """Build comprehensive player index with all available stats and physical attributes.
    
    Args:
        logger: Logger instance
        output_path: Path to save the output CSV
        full_build: If True, build complete historical dataset. If False, limited scope for testing.
    """
    try:
        import nfl_data_py as nfl
        import pandas as pd
        
        if full_build:
            logger.info("Building FULL comprehensive player index (complete historical dataset)...")
            
            # Load complete datasets
            logger.info("Loading players data...")
            players = nfl.import_players()
            logger.info(f"Loaded {len(players)} total players")
            
            logger.info("Loading complete seasonal data (1970-2023)...")
            try:
                # Try to get maximum historical coverage
                seasonal_data = nfl.import_seasonal_data(years=list(range(1970, 2024)), s_type='REG')
            except Exception as e:
                logger.warning(f"Failed to load from 1970, falling back to 1999: {e}")
                seasonal_data = nfl.import_seasonal_data(years=list(range(1999, 2024)), s_type='REG')
            logger.info(f"Loaded {len(seasonal_data)} regular season records")
            
            logger.info("Loading complete playoff data (1970-2023)...")
            try:
                playoff_data = nfl.import_seasonal_data(years=list(range(1970, 2024)), s_type='POST')
            except Exception as e:
                logger.warning(f"Failed to load playoff data from 1970, falling back to 1999: {e}")
                playoff_data = nfl.import_seasonal_data(years=list(range(1999, 2024)), s_type='POST')
            logger.info(f"Loaded {len(playoff_data)} playoff records")
            
            logger.info("Loading complete draft data (1970+)...")
            draft_data = nfl.import_draft_picks(years=list(range(1970, 2024)))
            logger.info(f"Loaded {len(draft_data)} draft records")
            
            logger.info("Loading complete combine data...")
            combine_data = nfl.import_combine_data(years=list(range(1987, 2024)))
            logger.info(f"Loaded {len(combine_data)} combine records")
        else:
            logger.info("Building comprehensive player index (test scope: recent years only)...")
            
            # Load datasets with limited scope for testing
            logger.info("Loading players data...")
            players = nfl.import_players()
            logger.info(f"Loaded {len(players)} total players")
            
            logger.info("Loading recent seasonal data (2022-2023)...")
            seasonal_data = nfl.import_seasonal_data(years=[2022, 2023], s_type='REG')
            logger.info(f"Loaded {len(seasonal_data)} regular season records")
            
            logger.info("Loading recent playoff data (2022-2023)...")
            playoff_data = nfl.import_seasonal_data(years=[2022, 2023], s_type='POST')
            logger.info(f"Loaded {len(playoff_data)} playoff records")
            
            logger.info("Loading recent draft data (including historical for testing)...")
            draft_data = nfl.import_draft_picks(years=list(range(1970, 2024)))
            logger.info(f"Loaded {len(draft_data)} draft records")
            
            logger.info("Loading combine data...")
            combine_data = nfl.import_combine_data(years=list(range(2020, 2024)))
            logger.info(f"Loaded {len(combine_data)} combine records")
        
        # Build career stats from limited seasonal data
        logger.info("Aggregating career statistics...")
        career_stats = seasonal_data.groupby('player_id').agg({
            'games': 'sum',
            'passing_yards': 'sum',
            'rushing_yards': 'sum', 
            'receiving_yards': 'sum',
            'passing_tds': 'sum',
            'rushing_tds': 'sum',
            'receiving_tds': 'sum',
            'season': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten column names
        career_stats.columns = ['player_id', 'total_career_games', 'career_passing_yards',
                               'career_rushing_yards', 'career_receiving_yards', 
                               'career_passing_tds', 'career_rushing_tds', 'career_receiving_tds',
                               'first_year', 'last_year', 'career_seasons']
        
        # Calculate total TDs
        career_stats['career_tds'] = (career_stats['career_passing_tds'] + 
                                     career_stats['career_rushing_tds'] + 
                                     career_stats['career_receiving_tds'])
        
        # Build playoff stats from playoff data
        logger.info("Aggregating playoff statistics...")
        playoff_stats = playoff_data.groupby('player_id').agg({
            'games': 'sum',
            'passing_yards': 'sum',
            'rushing_yards': 'sum', 
            'receiving_yards': 'sum',
            'passing_tds': 'sum',
            'rushing_tds': 'sum',
            'receiving_tds': 'sum'
        }).reset_index()
        
        # Rename playoff columns with playoff_ prefix
        playoff_stats.columns = ['player_id', 'playoff_games', 'playoff_passing_yards',
                               'playoff_rushing_yards', 'playoff_receiving_yards', 
                               'playoff_passing_tds', 'playoff_rushing_tds', 'playoff_receiving_tds']
        
        # Calculate total playoff TDs
        playoff_stats['playoff_tds'] = (playoff_stats['playoff_passing_tds'] + 
                                       playoff_stats['playoff_rushing_tds'] + 
                                       playoff_stats['playoff_receiving_tds'])
        
        # Merge players with career stats
        logger.info("Merging player identity and career data...")
        enhanced_players = players.merge(career_stats, left_on='gsis_id', right_on='player_id', how='left')
        
        # Merge with playoff stats
        logger.info("Merging playoff data...")
        enhanced_players = enhanced_players.merge(playoff_stats, left_on='gsis_id', right_on='player_id', how='left')
        
        # Merge with draft data for honors and defensive stats  
        logger.info("Merging draft/honors data...")
        draft_subset = draft_data[['gsis_id', 'season', 'to', 'probowls', 'allpro', 'hof', 'pick',
                                  'games', 'pass_yards', 'rush_yards', 'rec_yards', 
                                  'pass_tds', 'rush_tds', 'rec_tds', 'seasons_started',
                                  'def_solo_tackles', 'def_sacks', 'def_ints']].copy()
        # Rename draft columns to avoid conflicts
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
        
        # Merge with combine data for physical measurements
        logger.info("Merging combine/physical data...")
        # Use pfr_id as the join key for combine data since it's more reliable
        # Filter out null pfr_id values to avoid cross-join duplicates
        combine_subset = combine_data[['pfr_id', 'ht', 'wt', 'forty', 'bench', 'vertical', 
                                     'broad_jump', 'cone', 'shuttle']].copy()
        combine_subset = combine_subset[combine_subset['pfr_id'].notna()].drop_duplicates(subset=['pfr_id'])
        enhanced_players = enhanced_players.merge(combine_subset, left_on='pfr_id', right_on='pfr_id', how='left')
        
        # Create final comprehensive schema
        logger.info("Building comprehensive output schema...")
        final_index = pd.DataFrame({
            # Identity
            'player_id': enhanced_players['gsis_id'],
            'full_name': enhanced_players['display_name'],
            'primary_pos': enhanced_players['position'], 
            'college': enhanced_players['college_name'],
            'birth_date': enhanced_players['birth_date'],
            
            # Career span (use seasonal data first, then draft data, then player bio data)
            'first_year': enhanced_players['rookie_season'].fillna(
                enhanced_players['first_year'].fillna(enhanced_players['draft_season'])  # Use draft year as fallback
            ),
            'last_year': enhanced_players['last_season'].fillna(
                enhanced_players['last_year'].fillna(enhanced_players['to'])  # Use draft 'to' year as fallback
            ),
            'career_seasons': enhanced_players['career_seasons'].fillna(
                enhanced_players['seasons_started'].fillna(0)  # Use draft seasons_started as fallback
            ).astype(int),
            'total_career_games': enhanced_players['total_career_games'].fillna(
                enhanced_players['draft_games'].fillna(0)  # Use draft games as fallback
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
            
            # Touchdown stats (individual categories)
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
            'draft_pick': enhanced_players['draft_pick'].fillna(999).astype(int),  # Use players dataset draft_pick, 999 = undrafted
            'pro_bowls': enhanced_players['probowls'].fillna(0).astype(int),
            'all_pros': enhanced_players['allpro'].fillna(0).astype(int),
            'hof_flag': enhanced_players['hof'].fillna(False).astype(bool),
            
            # Physical/Combine Data (use from players table first, then combine data)
            'height_in': enhanced_players['height'].fillna(enhanced_players['ht']),
            'weight_lb': enhanced_players['weight'].fillna(enhanced_players['wt']),
            'forty_time': enhanced_players['forty'],
            'bench_press': enhanced_players['bench'],
            'vertical_jump': enhanced_players['vertical'], 
            'broad_jump': enhanced_players['broad_jump'],
            'three_cone': enhanced_players['cone'],
            'twenty_shuttle': enhanced_players['shuttle']
        })
        
        # Filter to players with some career activity
        logger.info("Filtering for players with career data...")
        
        # Apply inclusive filtering for comprehensive player database
        logger.info("Applying inclusive filtering for comprehensive coverage...")
        
        final_index = final_index[
            # Include any player with meaningful career data
            (final_index['total_career_games'] > 0) |           # From seasonal data
            (final_index['career_seasons'] > 0) | 
            (final_index['pro_bowls'] > 0) |
            (final_index['all_pros'] > 0) |
            (final_index['hof_flag'] == True) |
            
            # Include players with any offensive stats
            (final_index['career_passing_yards'] > 0) |
            (final_index['career_rushing_yards'] > 0) |
            (final_index['career_receiving_yards'] > 0) |
            
            # Include players with any defensive stats
            (final_index['def_solo_tackles'] > 0) |
            (final_index['def_sacks'] > 0) |
            (final_index['def_ints'] > 0) |
            
            # Include drafted players (reasonable NFL experience threshold)
            (enhanced_players['draft_pick'].fillna(999) <= 300) |  # Drafted players
            
            # Include players with NFL experience from draft data
            ((enhanced_players['draft_games'].fillna(0) > 0) & 
             ((enhanced_players['draft_pass_yards'].fillna(0) > 0) |
              (enhanced_players['draft_rush_yards'].fillna(0) > 0) |
              (enhanced_players['draft_rec_yards'].fillna(0) > 0) |
              (enhanced_players['def_solo_tackles'].fillna(0) > 0) |
              (enhanced_players['def_sacks'].fillna(0) > 0) |
              (enhanced_players['def_ints'].fillna(0) > 0)))
        ].copy()
        
        # Limit to first 100 players only for testing (not full build)
        if not full_build:
            final_index = final_index.head(100)
        
        # Save to CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_index.to_csv(output_path, index=False)
        
        logger.info(f"Comprehensive player index saved: {len(final_index)} players → {output_path}")
        
        # Show sample entries with comprehensive data
        logger.info("Sample entries with comprehensive schema:")
        for _, player in final_index.head(5).iterrows():
            height_str = f"{player['height_in']}\"" if pd.notna(player['height_in']) else "N/A"
            weight_str = f"{player['weight_lb']}lb" if pd.notna(player['weight_lb']) else "N/A"
            forty_str = f"{player['forty_time']}s" if pd.notna(player['forty_time']) else "N/A"
            
            logger.info(f"  - {player['full_name']} ({player['primary_pos']}): "
                       f"{player['career_seasons']} seasons, {player['total_career_games']} games, "
                       f"{player['career_tds']} TDs, {player['pro_bowls']} Pro Bowls | "
                       f"Playoffs: {player['playoff_games']} games, {player['playoff_tds']} TDs | "
                       f"Defense: {player['def_solo_tackles']} tackles, {player['def_sacks']} sacks | "
                       f"{height_str}, {weight_str}, 40yd: {forty_str}")
            
        # Summary stats with comprehensive breakdown
        logger.info("Comprehensive index summary:")
        logger.info(f"  Total players: {len(final_index):,}")
        logger.info(f"  With >10 games: {len(final_index[final_index['total_career_games'] > 10]):,}")
        logger.info(f"  With Pro Bowls: {len(final_index[final_index['pro_bowls'] > 0]):,}")
        logger.info(f"  With offensive TDs: {len(final_index[final_index['career_tds'] > 0]):,}")
        logger.info(f"  With playoff experience: {len(final_index[final_index['playoff_games'] > 0]):,}")
        logger.info(f"  With playoff TDs: {len(final_index[final_index['playoff_tds'] > 0]):,}")
        logger.info(f"  With defensive stats: {len(final_index[(final_index['def_solo_tackles'] > 0) | (final_index['def_sacks'] > 0) | (final_index['def_ints'] > 0)]):,}")
        logger.info(f"  With combine data: {len(final_index[pd.notna(final_index['forty_time'])]):,}")
        logger.info(f"  Hall of Fame: {len(final_index[final_index['hof_flag'] == True]):,}")
        
        # Position breakdown 
        logger.info("  === POSITION BREAKDOWN ===")
        pos_counts = final_index['primary_pos'].value_counts().sort_values(ascending=False)
        for pos, count in pos_counts.head(15).items():
            logger.info(f"    {pos}: {count:,}")
        
        # Show stats coverage by category
        logger.info("  === STATS COVERAGE ===")
        logger.info(f"  Players with passing yards: {len(final_index[final_index['career_passing_yards'] > 0]):,}")
        logger.info(f"  Players with rushing yards: {len(final_index[final_index['career_rushing_yards'] > 0]):,}")
        logger.info(f"  Players with receiving yards: {len(final_index[final_index['career_receiving_yards'] > 0]):,}")
        logger.info(f"  Players with tackles: {len(final_index[final_index['def_solo_tackles'] > 0]):,}")
        logger.info(f"  Players with sacks: {len(final_index[final_index['def_sacks'] > 0]):,}")
        logger.info(f"  Players with interceptions: {len(final_index[final_index['def_ints'] > 0]):,}")
        
        # Validate data quality
        logger.info("=== DATA QUALITY VALIDATION ===")
        total_null_names = final_index['full_name'].isna().sum()
        if total_null_names > 0:
            logger.warning(f"  ⚠️  {total_null_names} players missing names")
        else:
            logger.info(f"  ✅ All players have names")
        
        missing_positions = final_index['primary_pos'].isna().sum()
        if missing_positions > 0:
            logger.warning(f"  ⚠️  {missing_positions} players missing positions")
        else:
            logger.info(f"  ✅ All players have positions")
            
        # Check for reasonable year ranges
        min_year = final_index['first_year'].min()
        max_year = final_index['last_year'].max()
        logger.info(f"  Career year range: {min_year} - {max_year}")
        
        logger.info("✅ Comprehensive player index built successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"Error building enhanced index: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False


@click.command()
@click.option('--out', '-o', 
              type=click.Path(), 
              default='data/raw/players_index.csv',
              help='Output CSV file path')
@click.option('--test-only', '-t',
              is_flag=True,
              help='Only test connection, don\'t build index')
@click.option('--full', '-f',
              is_flag=True,
              help='Build complete historical dataset (1999-2023), not just recent test data')
@click.option('--verbose', '-v', 
              is_flag=True,
              help='Enable verbose logging')
def main(out: str, test_only: bool, full: bool, verbose: bool) -> None:
    """Build player index from nflverse-data."""
    logger = setup_logging(verbose)
    output_path = Path(out)
    
    if full:
        logger.info("=== nflverse-data Player Index Builder (FULL BUILD) ===")
    else:
        logger.info("=== nflverse-data Player Index Builder (Test Sample) ===")
    
    # Test connection first
    if not test_nflverse_connection(logger):
        logger.error("Connection test failed. Check nfl-data-py installation.")
        return
    
    if test_only:
        logger.info("Test-only mode complete.")
        return
    
    # Build player index
    if build_comprehensive_index(logger, output_path, full_build=full):
        if full:
            logger.info("✅ Full comprehensive player index built successfully!")
        else:
            logger.info("✅ Sample comprehensive player index built successfully!")
    else:
        logger.error("❌ Failed to build comprehensive player index")


if __name__ == "__main__":
    main()