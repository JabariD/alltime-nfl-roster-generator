#!/usr/bin/env python3
"""
Position-specific legend scoring for NFL players.

Calculates legend_score for each player-position combination based on:
- Physical attributes (height, weight, speed, combine metrics)
- Draft position and career honors
- Position-specific weighting factors

Usage:
    python pipeline/legend_scores.py --input data/raw/players_index_full.csv \
        --output data/snapshots/2025-08-24/legend_scores.csv
"""

import logging
from pathlib import Path

import click
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Position-specific scoring weights
POSITION_WEIGHTS = {
    'QB': {
        'height': 0.01,  # Less important for QBs
        'weight': 0.01,  # Much less important
        'forty_time': 0.01,  # Speed not crucial for QBs
        'career_passing_yards': 0.10,  # Important but championships matter more
        'career_passing_tds': 0.12,    # Important QB stat
        'playoff_passing_yards': 0.18,  # Clutch performance heavily weighted
        'playoff_passing_tds': 0.35,    # Championship moments - MOST important
        'career_seasons': 0.05,         # Longevity but don't penalize shorter careers
        'draft_pick': 0.02,  # Much reduced - Brady was pick 199!
        'pro_bowls': 0.08,   # Reduced to make room for playoffs
        'all_pros': 0.12,    # Elite recognition  
        'hof_flag': 0.09     # HOF status important but not everything
    },
    'RB': {
        'height': 0.03,
        'weight': 0.08,
        'forty_time': 0.15,  # Speed important for RBs
        'career_rushing_yards': 0.25,  # Primary RB stat
        'career_rushing_tds': 0.20,    # Key scoring metric
        'career_receiving_yards': 0.10, # Modern RB requirement
        'career_receiving_tds': 0.05,   # Pass-catching ability
        'playoff_rushing_yards': 0.04,  # Clutch performance
        'career_seasons': 0.05,         # Longevity matters
        'draft_pick': 0.05,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.06
    },
    'WR': {
        'height': 0.06,
        'weight': 0.02,
        'forty_time': 0.15,   # Speed crucial for WRs
        'vertical_jump': 0.06,
        'career_receiving_yards': 0.25,  # Primary WR stat
        'career_receiving_tds': 0.20,    # Key scoring metric
        'playoff_receiving_yards': 0.06, # Clutch performance
        'playoff_receiving_tds': 0.04,   # Clutch scoring
        'career_seasons': 0.05,          # Longevity
        'draft_pick': 0.06,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.06
    },
    'TE': {
        'height': 0.15,
        'weight': 0.12,
        'forty_time': 0.10,
        'career_receiving_yards': 0.20,  # Primary TE stat
        'career_receiving_tds': 0.15,    # Key scoring metric
        'playoff_receiving_yards': 0.05, # Clutch performance
        'career_seasons': 0.05,          # Longevity
        'draft_pick': 0.08,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.06
    },
    'OL': {
        'height': 0.20,
        'weight': 0.20,
        'bench_press': 0.12,
        'career_seasons': 0.15,   # Longevity crucial for OL
        'total_career_games': 0.10, # Durability
        'draft_pick': 0.08,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.08
    },
    'DL': {
        'height': 0.10,
        'weight': 0.15,
        'forty_time': 0.10,
        'bench_press': 0.10,
        'def_sacks': 0.25,        # Primary DL stat
        'def_solo_tackles': 0.15, # Defensive production
        'career_seasons': 0.05,   # Longevity
        'draft_pick': 0.06,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.07
    },
    'LB': {
        'height': 0.08,
        'weight': 0.12,
        'forty_time': 0.15,
        'vertical_jump': 0.08,
        'def_solo_tackles': 0.25, # Primary LB stat
        'def_sacks': 0.15,        # Pass rush ability
        'def_ints': 0.08,         # Coverage ability
        'career_seasons': 0.05,   # Longevity
        'draft_pick': 0.06,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.06
    },
    'DB': {
        'height': 0.08,
        'weight': 0.04,
        'forty_time': 0.25,       # Speed crucial for DBs
        'vertical_jump': 0.12,
        'def_ints': 0.25,         # Primary DB stat
        'def_solo_tackles': 0.12, # Run support
        'career_seasons': 0.05,   # Longevity
        'draft_pick': 0.06,
        'pro_bowls': 0.12,
        'all_pros': 0.15,
        'hof_flag': 0.06
    },
    'K': {
        'height': 0.05,
        'weight': 0.05,
        'draft_pick': 0.20,
        'pro_bowls': 0.30,
        'all_pros': 0.30,
        'hof_flag': 0.10
    },
    'P': {
        'height': 0.05,
        'weight': 0.05,
        'draft_pick': 0.20,
        'pro_bowls': 0.30,
        'all_pros': 0.30,
        'hof_flag': 0.10
    }
}

# Position groupings for players with ambiguous positions
POSITION_MAPPINGS = {
    'S': 'DB',
    'CB': 'DB',
    'SS': 'DB',
    'FS': 'DB',
    'SAF': 'DB',  # Safety -> DB
    'OLB': 'LB',
    'ILB': 'LB',
    'MLB': 'LB',
    'DE': 'DL',
    'DT': 'DL',
    'NT': 'DL',
    'C': 'OL',
    'G': 'OL',
    'T': 'OL',
    'OG': 'OL',
    'OT': 'OL',
    'FB': 'RB',
    'HB': 'RB',
    'LS': 'P'  # Long snapper -> Punter
}


def normalize_position(pos: str) -> str:
    """Normalize position to standard groups."""
    if pos in POSITION_MAPPINGS:
        return POSITION_MAPPINGS[pos]
    return pos


def percentile_score(
    value: float, values: pd.Series, higher_better: bool = True
) -> float:
    """Convert a value to its percentile within a series (0-100)."""
    if pd.isna(value) or values.empty:
        return 50.0  # Neutral score for missing data
    
    if higher_better:
        return (values < value).mean() * 100
    else:
        return (values > value).mean() * 100


def calculate_attribute_scores(
    row: pd.Series, position_data: pd.DataFrame
) -> dict[str, float]:
    """Calculate percentile scores for all attributes within position group."""
    scores = {}
    
    # Physical attributes
    if not pd.isna(row.get('height_in')):
        scores['height'] = percentile_score(
            row['height_in'], position_data['height_in'], True
        )
    
    if not pd.isna(row.get('weight_lb')):
        scores['weight'] = percentile_score(
            row['weight_lb'], position_data['weight_lb'], True
        )
    
    # Combine metrics (lower times are better)
    if not pd.isna(row.get('forty_time')):
        scores['forty_time'] = percentile_score(
            row['forty_time'], position_data['forty_time'], False
        )
    
    if not pd.isna(row.get('vertical_jump')):
        scores['vertical_jump'] = percentile_score(
            row['vertical_jump'], position_data['vertical_jump'], True
        )
    
    if not pd.isna(row.get('bench_press')):
        scores['bench_press'] = percentile_score(
            row['bench_press'], position_data['bench_press'], True
        )
    
    # Draft position (lower pick number is better)
    # Draft position (lower pick number is better)
    if not pd.isna(row.get('draft_pick')) and row['draft_pick'] != 999:
        # 999 = undrafted
        scores['draft_pick'] = percentile_score(
            row['draft_pick'], position_data['draft_pick'], False
        )
    else:
        scores['draft_pick'] = 10.0  # Low score for undrafted
    
    # Offensive stats
    if not pd.isna(row.get('career_passing_yards')):
        scores['career_passing_yards'] = percentile_score(
            row['career_passing_yards'], position_data['career_passing_yards'], True
        )
    
    if not pd.isna(row.get('career_passing_tds')):
        scores['career_passing_tds'] = percentile_score(
            row['career_passing_tds'], position_data['career_passing_tds'], True
        )
    
    if not pd.isna(row.get('career_rushing_yards')):
        scores['career_rushing_yards'] = percentile_score(
            row['career_rushing_yards'], position_data['career_rushing_yards'], True
        )
    
    if not pd.isna(row.get('career_rushing_tds')):
        scores['career_rushing_tds'] = percentile_score(
            row['career_rushing_tds'], position_data['career_rushing_tds'], True
        )
    
    if not pd.isna(row.get('career_receiving_yards')):
        scores['career_receiving_yards'] = percentile_score(
            row['career_receiving_yards'], position_data['career_receiving_yards'], True
        )
    
    if not pd.isna(row.get('career_receiving_tds')):
        scores['career_receiving_tds'] = percentile_score(
            row['career_receiving_tds'], position_data['career_receiving_tds'], True
        )
    
    # Playoff performance
    if not pd.isna(row.get('playoff_passing_yards')):
        scores['playoff_passing_yards'] = percentile_score(
            row['playoff_passing_yards'], position_data['playoff_passing_yards'], True
        )
    
    if not pd.isna(row.get('playoff_passing_tds')):
        scores['playoff_passing_tds'] = percentile_score(
            row['playoff_passing_tds'], position_data['playoff_passing_tds'], True
        )
    
    if not pd.isna(row.get('playoff_rushing_yards')):
        scores['playoff_rushing_yards'] = percentile_score(
            row['playoff_rushing_yards'], position_data['playoff_rushing_yards'], True
        )
    
    if not pd.isna(row.get('playoff_receiving_yards')):
        scores['playoff_receiving_yards'] = percentile_score(
            row['playoff_receiving_yards'], position_data['playoff_receiving_yards'], True
        )
    
    if not pd.isna(row.get('playoff_receiving_tds')):
        scores['playoff_receiving_tds'] = percentile_score(
            row['playoff_receiving_tds'], position_data['playoff_receiving_tds'], True
        )
    
    # Defensive stats
    if not pd.isna(row.get('def_solo_tackles')):
        scores['def_solo_tackles'] = percentile_score(
            row['def_solo_tackles'], position_data['def_solo_tackles'], True
        )
    
    if not pd.isna(row.get('def_sacks')):
        scores['def_sacks'] = percentile_score(
            row['def_sacks'], position_data['def_sacks'], True
        )
    
    if not pd.isna(row.get('def_ints')):
        scores['def_ints'] = percentile_score(
            row['def_ints'], position_data['def_ints'], True
        )
    
    # Career longevity
    if not pd.isna(row.get('career_seasons')):
        scores['career_seasons'] = percentile_score(
            row['career_seasons'], position_data['career_seasons'], True
        )
    
    if not pd.isna(row.get('total_career_games')):
        scores['total_career_games'] = percentile_score(
            row['total_career_games'], position_data['total_career_games'], True
        )
    
    # Career honors
    scores['pro_bowls'] = percentile_score(
        row.get('pro_bowls', 0), position_data['pro_bowls'], True
    )
    scores['all_pros'] = percentile_score(
        row.get('all_pros', 0), position_data['all_pros'], True
    )
    scores['hof_flag'] = 100.0 if row.get('hof_flag', False) else 0.0
    
    return scores


def calculate_legend_score(attribute_scores: dict[str, float], position: str) -> float:
    """Calculate weighted legend score for a position."""
    if position not in POSITION_WEIGHTS:
        logger.warning(f"Unknown position: {position}, using default weights")
        position = 'LB'  # Default fallback
    
    weights = POSITION_WEIGHTS[position]
    total_score = 0.0
    total_weight = 0.0
    
    for attribute, weight in weights.items():
        if attribute in attribute_scores:
            total_score += attribute_scores[attribute] * weight
            total_weight += weight
        else:
            # Missing attribute gets neutral score
            total_score += 50.0 * weight
            total_weight += weight
    
    if total_weight == 0:
        return 50.0
    
    return total_score / total_weight


def process_players(df: pd.DataFrame, min_position_players: int = 3) -> pd.DataFrame:
    """Process all players and calculate legend scores."""
    results = []
    
    # Group players by position for percentile calculations
    df['normalized_pos'] = df['primary_pos'].apply(normalize_position)
    
    for position in df['normalized_pos'].unique():
        if pd.isna(position):
            continue
            
        logger.info(f"Processing {position} players...")
        position_df = df[df['normalized_pos'] == position].copy()
        
        # Skip positions with too few players
        if len(position_df) < min_position_players:
            logger.warning(f"Skipping {position} - only {len(position_df)} players")
            continue
        
        for _, row in position_df.iterrows():
            try:
                # Calculate attribute percentile scores within position
                attribute_scores = calculate_attribute_scores(row, position_df)
                
                # Calculate weighted legend score
                legend_score = calculate_legend_score(attribute_scores, position)
                
                results.append({
                    'player_id': row['player_id'],
                    'full_name': row['full_name'],
                    'position': position,
                    'legend_score': round(legend_score, 2),
                    'source_tier': 2  # Rules-based calculation
                })
                
            except Exception as e:
                logger.error(
                    f"Error processing {row['player_id']} ({row['full_name']}): {e}"
                )
                continue
    
    return pd.DataFrame(results)


@click.command()
@click.option('--input', 'input_file', required=True, type=click.Path(exists=True),
              help='Input CSV file with player data')
@click.option('--output', 'output_file', required=True, type=click.Path(),
              help='Output CSV file for legend scores')
@click.option('--min-games', default=16, type=int,
              help='Minimum career games to be eligible (default: 16 for all players)')
@click.option('--dry-run', is_flag=True,
              help='Show what would be processed without writing output')
@click.option('--verbose', is_flag=True,
              help='Enable verbose logging')
def main(
    input_file: str, output_file: str, min_games: int, dry_run: bool, verbose: bool
):
    """Calculate position-specific legend scores for NFL players."""
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"Loading player data from {input_file}")
    df = pd.read_csv(input_file)
    
    # Basic eligibility filtering
    logger.info(f"Loaded {len(df)} total players")
    if min_games > 0:
        df_eligible = df[df['total_career_games'] >= min_games].copy()
        logger.info(f"After filtering for {min_games}+ games: {len(df_eligible)} players")
    else:
        df_eligible = df.copy()
        logger.info("Processing ALL players (no minimum games filter)")
    
    if dry_run:
        logger.info("DRY RUN: Would process legend scores for eligible players")
        for pos in df_eligible['primary_pos'].value_counts().head(10).items():
            logger.info(f"  {pos[0]}: {pos[1]} players")
        return
    
    # Calculate legend scores
    logger.info("Calculating position-specific legend scores...")
    results_df = process_players(df_eligible)
    
    if results_df.empty:
        logger.error("No legend scores calculated!")
        return
    
    # Create output directory
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save results
    logger.info(f"Saving {len(results_df)} legend scores to {output_file}")
    results_df.to_csv(output_file, index=False)
    
    # Summary stats
    logger.info("Legend Score Summary:")
    logger.info(f"  Total players scored: {len(results_df)}")
    logger.info(f"  Positions covered: {results_df['position'].nunique()}")
    logger.info(
        f"  Score range: {results_df['legend_score'].min():.1f} - "
        f"{results_df['legend_score'].max():.1f}"
    )
    logger.info(f"  Mean score: {results_df['legend_score'].mean():.1f}")
    
    # Top scorers by position
    logger.info("\nTop scorers by position:")
    for pos in results_df['position'].unique():
        pos_df = results_df[results_df['position'] == pos]
        top_player = pos_df.loc[pos_df['legend_score'].idxmax()]
        logger.info(
            f"  {pos}: {top_player['full_name']} "
            f"({top_player['legend_score']:.1f})"
        )


if __name__ == '__main__':
    main()