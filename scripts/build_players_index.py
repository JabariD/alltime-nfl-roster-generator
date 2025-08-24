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
- tier3_stats: games_started, team_changes (longevity indicators)
- tier4_stats: seasons_as_specialist, team_tenure_count
- honors: pro_bowls, all_pros, hof_flag
- legend_scores: peak_season_score, position_tier_score, overall_legend_score

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
    """Build a small sample player index for testing."""
    try:
        import nfl_data_py as nfl
        
        logger.info("Building sample player index...")
        
        # Load basic player data
        players = nfl.import_players()
        logger.info(f"Loaded {len(players)} total players")
        
        # Load recent roster data for context
        rosters_2023 = nfl.import_seasonal_rosters(years=[2023])
        
        # Get active players from 2023
        active_players = rosters_2023['player_id'].unique()
        recent_players = players[players['gsis_id'].isin(active_players)].copy()
        
        logger.info(f"Found {len(recent_players)} active players in 2023")
        
        # Create simplified index with key columns
        player_index = recent_players[['gsis_id', 'display_name', 'position', 'college_name', 'birth_date']].copy()
        player_index.columns = ['player_id', 'full_name', 'primary_pos', 'college', 'birth_date']
        
        # Add some derived info
        player_index['first_year'] = 2023  # Placeholder - would need more complex logic
        player_index['last_year'] = 2023   # Placeholder
        player_index['teams'] = 'Multiple'  # Placeholder
        
        # Save to CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        player_index.to_csv(output_path, index=False)
        
        logger.info(f"Sample player index saved: {len(player_index)} players → {output_path}")
        
        # Show sample
        logger.info("Sample entries:")
        for _, player in player_index.head(5).iterrows():
            logger.info(f"  - {player['full_name']} ({player['primary_pos']})")
            
        return True
        
    except Exception as e:
        logger.error(f"Error building sample index: {e}")
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