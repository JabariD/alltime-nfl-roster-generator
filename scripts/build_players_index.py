#!/usr/bin/env python3
"""
Build master player index from nflverse-data with legend identification capabilities.

Uses nflverse datasets via nfl-data-py to create comprehensive player database
suitable for filtering NFL legends (Milestone 2 → Milestone 3 pipeline).
Respects data usage policies and provides regularly-updated, high-quality NFL data.

Website: https://nflreadr.nflverse.com/index.html

LEGEND IDENTIFICATION RESEARCH FINDINGS:
========================================

Data Sources Available (nfl-data-py capabilities confirmed):
- import_players(): Biographical data + career spans (rookie_season, last_season) 
  * Historical coverage: 1975+ for legends (Brady, Rice, Montana confirmed)
- import_seasonal_data(): Season-by-season statistics with 'games' column
  * Coverage: 2000-2023+ confirmed, ~580 players/year in recent seasons
  * Stats: passing_yards, rushing_yards, receiving_yards, TDs, advanced metrics
- import_draft_picks(): Draft history + honors data  
  * probowls, allpro, hof columns for legend identification
  * Career stat summaries included
- import_seasonal_rosters(): Team affiliations by season

Player Index Output Design:
- Provides comprehensive data for downstream legend filtering (see design.md)
- Multi-dataset aggregation: players + seasonal_data + draft_picks + rosters
- Career-span calculations using actual rookie_season/last_season (not placeholders)
- Position-agnostic data collection suitable for all NFL positions

Current Implementation: Proof-of-concept with 2023 active players only
Next Phase: Full historical aggregation across all available seasons (2000+)

Enhanced Output Schema (data/raw/players_index.csv):
- player_id (nflverse gsis_id), full_name, primary_pos, college, birth_date
- career_span: first_year, last_year, career_seasons, total_career_games
- tier1_stats: career_passing_yards, career_rushing_yards, career_receiving_yards, career_tds
- tier2_stats: def_solo_tackles, def_sacks, def_ints, draft_pick  
- honors: pro_bowls, all_pros, hof_flag
- physical: height_in, weight_lb, forty_time, bench_press, vertical_jump, broad_jump, three_cone, twenty_shuttle

Data Quality: ~27k total players → filter to ~8-10k legend candidates
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


def build_sample_index(logger: logging.Logger, output_path: Path) -> bool:
    """Build enhanced player index with legend identification data (limited scope for testing)."""
    try:
        import nfl_data_py as nfl
        import pandas as pd
        
        logger.info("Building enhanced player index (test scope: recent years only)...")
        
        # Load datasets with limited scope for testing
        logger.info("Loading players data...")
        players = nfl.import_players()
        logger.info(f"Loaded {len(players)} total players")
        
        logger.info("Loading recent seasonal data (2022-2023)...")
        seasonal_data = nfl.import_seasonal_data(years=[2022, 2023], s_type='REG')
        logger.info(f"Loaded {len(seasonal_data)} seasonal records")
        
        logger.info("Loading recent draft data...")
        draft_data = nfl.import_draft_picks(years=list(range(2020, 2024)))
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
        
        # Merge players with career stats
        logger.info("Merging player identity and career data...")
        enhanced_players = players.merge(career_stats, left_on='gsis_id', right_on='player_id', how='left')
        
        # Merge with draft data for honors and defensive stats  
        logger.info("Merging draft/honors data...")
        draft_subset = draft_data[['gsis_id', 'probowls', 'allpro', 'hof', 'pick',
                                  'def_solo_tackles', 'def_sacks', 'def_ints']].copy()
        enhanced_players = enhanced_players.merge(draft_subset, on='gsis_id', how='left')
        
        # Merge with combine data for physical measurements
        logger.info("Merging combine/physical data...")
        # Use pfr_id as the join key for combine data since it's more reliable
        combine_subset = combine_data[['pfr_id', 'ht', 'wt', 'forty', 'bench', 'vertical', 
                                     'broad_jump', 'cone', 'shuttle']].copy()
        enhanced_players = enhanced_players.merge(combine_subset, left_on='pfr_id', right_on='pfr_id', how='left')
        
        # Create final enhanced schema
        logger.info("Building enhanced output schema...")
        final_index = pd.DataFrame({
            # Identity
            'player_id': enhanced_players['gsis_id'],
            'full_name': enhanced_players['display_name'],
            'primary_pos': enhanced_players['position'], 
            'college': enhanced_players['college_name'],
            'birth_date': enhanced_players['birth_date'],
            
            # Career span (use actual rookie/last season when available)
            'first_year': enhanced_players['rookie_season'].fillna(enhanced_players['first_year']),
            'last_year': enhanced_players['last_season'].fillna(enhanced_players['last_year']),
            'career_seasons': enhanced_players['career_seasons'].fillna(0).astype(int),
            'total_career_games': enhanced_players['total_career_games'].fillna(0).astype(int),
            
            # Tier 1 stats (skill positions)
            'career_passing_yards': enhanced_players['career_passing_yards'].fillna(0).astype(int),
            'career_rushing_yards': enhanced_players['career_rushing_yards'].fillna(0).astype(int),
            'career_receiving_yards': enhanced_players['career_receiving_yards'].fillna(0).astype(int),
            'career_tds': enhanced_players['career_tds'].fillna(0).astype(int),
            
            # Tier 2 stats (defensive)  
            'def_solo_tackles': enhanced_players['def_solo_tackles'].fillna(0).astype(int),
            'def_sacks': enhanced_players['def_sacks'].fillna(0),
            'def_ints': enhanced_players['def_ints'].fillna(0).astype(int),
            'draft_pick': enhanced_players['pick'].fillna(999).astype(int),  # 999 = undrafted
            
            # Honors
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
        
        # Filter to players with some career activity (for testing)
        logger.info("Filtering for players with career data...")
        final_index = final_index[
            (final_index['total_career_games'] > 0) |
            (final_index['career_seasons'] > 0) | 
            (final_index['pro_bowls'] > 0) |
            (final_index['hof_flag'] == True)
        ].copy()
        
        # Limit to first 100 players for testing
        final_index = final_index.head(100)
        
        # Save to CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_index.to_csv(output_path, index=False)
        
        logger.info(f"Enhanced player index saved: {len(final_index)} players → {output_path}")
        
        # Show sample entries with enhanced data
        logger.info("Sample entries with enhanced schema:")
        for _, player in final_index.head(5).iterrows():
            height_str = f"{player['height_in']}\"" if pd.notna(player['height_in']) else "N/A"
            weight_str = f"{player['weight_lb']}lb" if pd.notna(player['weight_lb']) else "N/A"
            forty_str = f"{player['forty_time']}s" if pd.notna(player['forty_time']) else "N/A"
            
            logger.info(f"  - {player['full_name']} ({player['primary_pos']}): "
                       f"{player['career_seasons']} seasons, {player['total_career_games']} games, "
                       f"{player['career_tds']} TDs, {player['pro_bowls']} Pro Bowls | "
                       f"{height_str}, {weight_str}, 40yd: {forty_str}")
            
        # Summary stats
        logger.info("Enhanced index summary:")
        logger.info(f"  Total players: {len(final_index):,}")
        logger.info(f"  With >10 games: {len(final_index[final_index['total_career_games'] > 10]):,}")
        logger.info(f"  With Pro Bowls: {len(final_index[final_index['pro_bowls'] > 0]):,}")
        logger.info(f"  With TDs: {len(final_index[final_index['career_tds'] > 0]):,}")
        logger.info(f"  With combine data: {len(final_index[pd.notna(final_index['forty_time'])]):,}")
        logger.info(f"  Hall of Fame: {len(final_index[final_index['hof_flag'] == True]):,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error building enhanced index: {e}")
        return False


@click.command()
@click.option('--out', '-o', 
              type=click.Path(), 
              default='data/raw/players_index.csv',
              help='Output CSV file path')
@click.option('--test-only', '-t',
              is_flag=True,
              help='Only test connection, don\'t build index')
@click.option('--verbose', '-v', 
              is_flag=True,
              help='Enable verbose logging')
def main(out: str, test_only: bool, verbose: bool) -> None:
    """Build player index from nflverse-data (proof of concept)."""
    logger = setup_logging(verbose)
    output_path = Path(out)
    
    logger.info("=== nflverse-data Player Index Builder (Proof of Concept) ===")
    
    # Test connection first
    if not test_nflverse_connection(logger):
        logger.error("Connection test failed. Check nfl-data-py installation.")
        return
    
    if test_only:
        logger.info("Test-only mode complete.")
        return
    
    # Build sample index
    if build_sample_index(logger, output_path):
        logger.info("✅ Sample player index built successfully!")
    else:
        logger.error("❌ Failed to build player index")


if __name__ == "__main__":
    main()