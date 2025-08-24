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
  * Historical coverage: 1974+ for legends (Brady, Rice, Montana confirmed)
- import_seasonal_data(): Season-by-season statistics with 'games' column
  * Coverage: 1999-2023+ confirmed, ~580 players/year in recent seasons
  * Stats: passing_yards, rushing_yards, receiving_yards, TDs, advanced metrics
- import_draft_picks(): Draft history + honors data  
  * probowls, allpro, hof columns for legend identification
  * Career stat summaries included
- import_seasonal_rosters(): Team affiliations by season

HISTORICAL COVERAGE LIMITATION:
===============================

**MISSING EARLY LEGENDS (Pre-1974):**
Current nflverse data does NOT include legendary players from NFL's early eras:
- Jim Thorpe (1915-1928) - Pro Football Hall of Fame, first NFL president
- Red Grange (1925-1934) - "The Galloping Ghost," brought legitimacy to pro football  
- Sammy Baugh (1937-1952) - Revolutionary passer, punter, defensive back
- Otto Graham (1946-1955) - Browns dynasty quarterback, 7 championship games
- Jim Brown (1957-1965) - Arguably greatest RB ever, 9 seasons/8 rushing titles
- Johnny Unitas (1956-1973) - "Johnny U," passing pioneer
- Gale Sayers (1965-1971) - "Kansas Comet," electric runner cut short by injury
- Dick Butkus (1965-1973) - Prototype middle linebacker
- Joe Namath (1965-1977) - Super Bowl III guarantee, cultural icon
- Roger Staubach (1969-1979) - "Captain America," clutch performer (at data boundary)

**PLANNED HYBRID APPROACH:**
Phase 1 (Current): Modern Era Legends (1974+) via nflverse data
Phase 2 (Future): Historical Legends Supplement (Pre-1974) via:
- AI/LLM curation of canonical early legends lists
- Manual research of Hall of Fame inductees by era
- Integration with Pro Football Reference historical data
- Estimated ~200-300 additional legendary players from early eras

**IMPACT ON MILESTONE 3:**
- Legend filtering will initially work on ~24k modern players (1974+)  
- Historical legends will be added as separate curated dataset
- Final "All-Time Legends Roster" will merge both modern + historical data
- Clear provenance tracking for data source (nflverse vs curated historical)

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
- playoff_stats: playoff_games, playoff_passing_yards, playoff_rushing_yards, playoff_receiving_yards, playoff_tds
- legend_qualification: playoff_performance_bonus (clutch factor for legend identification)
- tier2_stats: def_solo_tackles, def_sacks, def_ints, draft_pick  
- honors: pro_bowls, all_pros, hof_flag
- physical: height_in, weight_lb, forty_time, bench_press, vertical_jump, broad_jump, three_cone, twenty_shuttle

Data Quality: 
- Modern Era (1974+): ~24k players → filter to ~6-8k modern legend candidates
- Historical Era (Pre-1974): ~200-300 curated legendary players (future supplement)
- Combined All-Time: ~24-25k total → ~6-8k final legend candidates
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


def build_sample_index(logger: logging.Logger, output_path: Path, full_build: bool = False) -> bool:
    """Build enhanced player index with legend identification data.
    
    Args:
        logger: Logger instance
        output_path: Path to save the output CSV
        full_build: If True, build complete historical dataset. If False, limited scope for testing.
    """
    try:
        import nfl_data_py as nfl
        import pandas as pd
        
        if full_build:
            logger.info("Building FULL enhanced player index (complete historical dataset)...")
            
            # Load complete datasets
            logger.info("Loading players data...")
            players = nfl.import_players()
            logger.info(f"Loaded {len(players)} total players")
            
            logger.info("Loading complete seasonal data (1999-2023)...")
            seasonal_data = nfl.import_seasonal_data(years=list(range(1999, 2024)), s_type='REG')
            logger.info(f"Loaded {len(seasonal_data)} regular season records")
            
            logger.info("Loading complete playoff data (1999-2023)...")
            playoff_data = nfl.import_seasonal_data(years=list(range(1999, 2024)), s_type='POST')
            logger.info(f"Loaded {len(playoff_data)} playoff records")
            
            logger.info("Loading complete draft data...")
            draft_data = nfl.import_draft_picks(years=list(range(1980, 2024)))
            logger.info(f"Loaded {len(draft_data)} draft records")
            
            logger.info("Loading complete combine data...")
            combine_data = nfl.import_combine_data(years=list(range(1987, 2024)))
            logger.info(f"Loaded {len(combine_data)} combine records")
        else:
            logger.info("Building enhanced player index (test scope: recent years only)...")
            
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
            
            # Playoff stats
            'playoff_games': enhanced_players['playoff_games'].fillna(0).astype(int),
            'playoff_passing_yards': enhanced_players['playoff_passing_yards'].fillna(0).astype(int),
            'playoff_rushing_yards': enhanced_players['playoff_rushing_yards'].fillna(0).astype(int),
            'playoff_receiving_yards': enhanced_players['playoff_receiving_yards'].fillna(0).astype(int),
            'playoff_tds': enhanced_players['playoff_tds'].fillna(0).astype(int),
            
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
        
        # Calculate playoff performance bonus (clutch factor) for legend qualification
        logger.info("Calculating playoff performance bonus...")
        
        # Playoff bonus calculation:
        # - Base bonus for playoff appearances (games > 0)
        # - TDs per game bonus (playoff TDs / playoff games if games > 0)  
        # - High stakes games bonus (games >= 10 for sustained playoff success)
        playoff_bonus = pd.Series(0.0, index=final_index.index)
        
        # Players with playoff experience get base bonus
        playoff_players = final_index['playoff_games'] > 0
        playoff_bonus[playoff_players] += 5.0
        
        # TD efficiency bonus (capped at 10 points)
        td_efficiency = final_index['playoff_tds'] / final_index['playoff_games'].replace(0, 1)
        playoff_bonus += (td_efficiency * 2.0).clip(upper=10.0)
        
        # Sustained playoff success bonus  
        high_stakes = final_index['playoff_games'] >= 10
        playoff_bonus[high_stakes] += 10.0
        
        final_index['playoff_performance_bonus'] = playoff_bonus.round(1)
        
        # Filter to players with some career activity
        logger.info("Filtering for players with career data...")
        final_index = final_index[
            (final_index['total_career_games'] > 0) |
            (final_index['career_seasons'] > 0) | 
            (final_index['pro_bowls'] > 0) |
            (final_index['hof_flag'] == True)
        ].copy()
        
        # Limit to first 100 players only for testing (not full build)
        if not full_build:
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
                       f"Playoffs: {player['playoff_games']} games, {player['playoff_tds']} TDs "
                       f"(bonus: {player['playoff_performance_bonus']}) | "
                       f"{height_str}, {weight_str}, 40yd: {forty_str}")
            
        # Summary stats
        logger.info("Enhanced index summary:")
        logger.info(f"  Total players: {len(final_index):,}")
        logger.info(f"  With >10 games: {len(final_index[final_index['total_career_games'] > 10]):,}")
        logger.info(f"  With Pro Bowls: {len(final_index[final_index['pro_bowls'] > 0]):,}")
        logger.info(f"  With TDs: {len(final_index[final_index['career_tds'] > 0]):,}")
        logger.info(f"  With playoff experience: {len(final_index[final_index['playoff_games'] > 0]):,}")
        logger.info(f"  With playoff TDs: {len(final_index[final_index['playoff_tds'] > 0]):,}")
        logger.info(f"  With high playoff bonus (>10): {len(final_index[final_index['playoff_performance_bonus'] > 10]):,}")
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
    if build_sample_index(logger, output_path, full_build=full):
        if full:
            logger.info("✅ Full historical player index built successfully!")
        else:
            logger.info("✅ Sample player index built successfully!")
    else:
        logger.error("❌ Failed to build player index")


if __name__ == "__main__":
    main()