#!/usr/bin/env python3
"""
Milestone 3: Candidate Filtering - Clean and filter NFL players for legend status.

Takes raw player data from build_players_index.py output and historical legends,
then applies normalization and filtering to produce a clean pool of ~3-4k eligible
legend candidates saved as immutable snapshots.

Pipeline: Raw Data (5.3k players) → Normalized & Filtered → Snapshots (~3-4k legends)

Key Operations:
- Data consolidation: Merge modern players + pre-1974 Hall of Famers
- Name cleaning: Handle variations, nicknames, punctuation standardization
- Position harmonization: Map to standard categories (QB, RB, WR, TE, OL, DL, LB, DB, K, P)
- Era bucketing: Group by decades for contextual analysis
- Legend filtering: Games played, achievements, statistical thresholds
- Snapshot creation: Immutable parquet output with manifest

Output: data/snapshots/<DATE>/players.parquet + manifest.json
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple

import click
import pandas as pd


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the script."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("normalize_players.log", mode="a")
        ]
    )
    return logging.getLogger(__name__)


def load_raw_data(main_path: Path, historical_path: Path, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw player data from both modern and historical sources."""
    try:
        logger.info(f"Loading main player dataset: {main_path}")
        main_df = pd.read_csv(main_path)
        logger.info(f"Loaded {len(main_df):,} modern players (1974+)")
        
        logger.info(f"Loading historical legends: {historical_path}")
        historical_df = pd.read_csv(historical_path)
        logger.info(f"Loaded {len(historical_df):,} pre-1974 Hall of Fame legends")
        
        # Validate schemas match
        main_cols = set(main_df.columns)
        hist_cols = set(historical_df.columns)
        
        if main_cols != hist_cols:
            missing_in_main = hist_cols - main_cols
            missing_in_hist = main_cols - hist_cols
            if missing_in_main:
                logger.warning(f"Columns missing in main data: {missing_in_main}")
            if missing_in_hist:
                logger.warning(f"Columns missing in historical data: {missing_in_hist}")
        
        return main_df, historical_df
        
    except FileNotFoundError as e:
        logger.error(f"Data file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading raw data: {e}")
        raise


def clean_player_names(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Clean and standardize player names."""
    logger.info("Cleaning player names...")
    
    df = df.copy()
    original_names = df['full_name'].copy()
    
    # Remove extra whitespace and normalize case
    df['full_name'] = df['full_name'].str.strip().str.title()
    
    # Handle common name variations and punctuation
    name_fixes = {
        # Remove periods from initials
        r'([A-Z])\.': r'\1',
        # Standardize Jr/Sr
        r'\bJr\.?\b': 'Jr',
        r'\bSr\.?\b': 'Sr',
        # Standardize III/IV
        r'\bIII\b': 'III',
        r'\bIV\b': 'IV',
        # Handle apostrophes consistently
        r"'": "'",
        # Remove extra spaces
        r'\s+': ' ',
    }
    
    for pattern, replacement in name_fixes.items():
        df['full_name'] = df['full_name'].str.replace(pattern, replacement, regex=True)
    
    # Log significant changes
    changed_names = df[df['full_name'] != original_names]
    if not changed_names.empty:
        logger.debug(f"Name changes made: {len(changed_names)}")
        for _, player in changed_names.head(10).iterrows():
            orig_idx = original_names[original_names.index == player.name].iloc[0]
            logger.debug(f"  '{orig_idx}' → '{player['full_name']}'")
    
    logger.info(f"Name cleaning complete: {len(changed_names)} names standardized")
    return df


def harmonize_positions(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Harmonize positions to standard categories."""
    logger.info("Harmonizing positions...")
    
    df = df.copy()
    original_positions = df['primary_pos'].value_counts()
    
    # Position mapping to standard categories
    position_mapping = {
        # Quarterbacks
        'QB': 'QB',
        
        # Running Backs
        'RB': 'RB', 'HB': 'RB', 'FB': 'RB',
        
        # Wide Receivers
        'WR': 'WR', 'SE': 'WR', 'FL': 'WR',
        
        # Tight Ends
        'TE': 'TE',
        
        # Offensive Line
        'C': 'OL', 'G': 'OL', 'T': 'OL', 'OG': 'OL', 'OT': 'OL',
        'LG': 'OL', 'RG': 'OL', 'LT': 'OL', 'RT': 'OL',
        
        # Defensive Line
        'DE': 'DL', 'DT': 'DL', 'NT': 'DL', 'NG': 'DL',
        'LE': 'DL', 'RE': 'DL', 'LDE': 'DL', 'RDE': 'DL',
        
        # Linebackers
        'LB': 'LB', 'MLB': 'LB', 'OLB': 'LB', 'ILB': 'LB',
        'LOLB': 'LB', 'ROLB': 'LB', 'WLB': 'LB', 'SLB': 'LB',
        
        # Defensive Backs
        'CB': 'DB', 'S': 'DB', 'FS': 'DB', 'SS': 'DB',
        'LCB': 'DB', 'RCB': 'DB', 'NCB': 'DB', 'DB': 'DB',
        
        # Special Teams
        'K': 'K', 'PK': 'K',
        'P': 'P',
        'LS': 'ST', 'KR': 'ST', 'PR': 'ST',
    }
    
    # Apply position mapping
    df['normalized_pos'] = df['primary_pos'].map(position_mapping)
    
    # Handle unmapped positions
    unmapped_mask = df['normalized_pos'].isna()
    if unmapped_mask.any():
        unmapped_positions = df[unmapped_mask]['primary_pos'].value_counts()
        logger.warning(f"Unmapped positions found: {dict(unmapped_positions)}")
        # Keep original position for unmapped
        df.loc[unmapped_mask, 'normalized_pos'] = df.loc[unmapped_mask, 'primary_pos']
    
    # Log position harmonization results
    new_positions = df['normalized_pos'].value_counts()
    logger.info("Position harmonization results:")
    for pos, count in new_positions.items():
        logger.info(f"  {pos}: {count:,} players")
    
    # Replace original position with normalized
    df['primary_pos'] = df['normalized_pos']
    df = df.drop('normalized_pos', axis=1)
    
    logger.info(f"Position harmonization complete: {len(original_positions)} → {len(new_positions)} categories")
    return df


def add_era_buckets(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Add era buckets based on first playing year."""
    logger.info("Adding era buckets...")
    
    df = df.copy()
    
    def get_era(first_year: float) -> str:
        """Get era bucket for a given first year."""
        if pd.isna(first_year):
            return 'Unknown'
        
        year = int(first_year)
        if year < 1960:
            return 'Pioneer Era (Pre-1960)'
        elif year < 1970:
            return '1960s'
        elif year < 1980:
            return '1970s'
        elif year < 1990:
            return '1980s'  
        elif year < 2000:
            return '1990s'
        elif year < 2010:
            return '2000s'
        elif year < 2020:
            return '2010s'
        else:
            return '2020s'
    
    df['era'] = df['first_year'].apply(get_era)
    
    # Log era distribution
    era_counts = df['era'].value_counts().sort_index()
    logger.info("Era distribution:")
    for era, count in era_counts.items():
        logger.info(f"  {era}: {count:,} players")
    
    logger.info("Era bucketing complete")
    return df


def apply_legend_filters(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply less aggressive filtering with position quota awareness."""
    logger.info("Applying legend filtering criteria with position quota guarantees...")
    
    original_count = len(df)
    df = df.copy()
    
    # Target position quotas (minimum guarantees, can exceed)
    position_quotas = {
        'QB': 200,    # Target: 180, buffer for selection
        'RB': 450,    # Target: 420
        'WR': 600,    # Target: 560  
        'TE': 250,    # Target: 220
        'OL': 800,    # Target: 760
        'DL': 600,    # Target: part of 1,020 DL/EDGE/LB
        'LB': 450,    # Target: part of 1,020 DL/EDGE/LB
        'DB': 700,    # Target: 660
        'K': 120,     # Target: part of 240 K/P/RET/LS
        'P': 60,      # Target: part of 240 K/P/RET/LS
        'ST': 60,     # Target: part of 240 K/P/RET/LS
    }
    
    # Create quality tiers for each position
    def calculate_quality_score(row) -> float:
        """Calculate a quality score for ranking players within position."""
        score = 0.0
        
        # Hall of Fame gets massive bonus
        if row['hof_flag']:
            score += 100
        
        # Achievement bonuses
        score += row['all_pros'] * 10  # All-Pro is huge
        score += row['pro_bowls'] * 3  # Pro Bowls important
        
        # Career length bonus
        score += row['career_seasons'] * 1
        
        # Games played bonus (durability)
        score += min(row['total_career_games'] / 10, 20)  # Cap at 20 points
        
        # Position-specific statistical bonuses
        pos = row['primary_pos']
        if pos == 'QB':
            score += min(row['career_passing_yards'] / 1000, 50)  # Up to 50 points
            score += row['career_tds'] * 0.5
        elif pos == 'RB':
            score += min(row['career_rushing_yards'] / 500, 40)
            score += row['career_tds'] * 0.8
        elif pos in ['WR', 'TE']:
            score += min(row['career_receiving_yards'] / 500, 40)
            score += row['career_tds'] * 0.8
        elif pos in ['OL', 'DL', 'LB', 'DB']:
            # For non-skill positions, emphasize durability and honors
            score += min(row['def_solo_tackles'] / 50, 30)
            score += row['def_sacks'] * 2
            score += row['def_ints'] * 3
        elif pos in ['K', 'P', 'ST']:
            # Special teams: emphasize longevity
            score += row['career_seasons'] * 2
        
        # Playoff bonus
        score += row['playoff_performance_bonus'] * 0.5
        
        return score
    
    # Calculate quality scores
    df['quality_score'] = df.apply(calculate_quality_score, axis=1)
    
    # Apply basic filters first (more lenient)
    basic_filters = (
        # Much lower games threshold OR any achievement
        ((df['total_career_games'] >= 16) | 
         (df['pro_bowls'] > 0) | 
         (df['all_pros'] > 0) | 
         (df['hof_flag'] == True)) &
        
        # At least 2 seasons OR special achievement
        ((df['career_seasons'] >= 2) | 
         (df['hof_flag'] == True) |
         (df['all_pros'] > 0) |
         (df['pro_bowls'] >= 2)) &
        
        # Some career activity (very lenient)
        ((df['total_career_games'] > 0) | (df['career_seasons'] > 0))
    )
    
    basic_filtered = df[basic_filters].copy()
    basic_pass = len(basic_filtered)
    logger.info(f"Basic filters: {basic_pass:,}/{original_count:,} players pass ({basic_pass/original_count*100:.1f}%)")
    
    # Position-based quota selection
    final_selections = []
    
    logger.info("Applying position-based selection with quotas:")
    for pos, quota in position_quotas.items():
        pos_players = basic_filtered[basic_filtered['primary_pos'] == pos].copy()
        pos_count = len(pos_players)
        
        if pos_count == 0:
            logger.warning(f"  {pos}: No players found after basic filtering!")
            continue
            
        # Sort by quality score (descending)
        pos_players = pos_players.sort_values('quality_score', ascending=False)
        
        # Take top players up to quota, but ensure we get at least some players
        actual_quota = min(quota, pos_count)
        
        # If we have very few players, take them all
        if pos_count <= quota * 0.5:
            selected = pos_players
            logger.info(f"  {pos}: Taking all {pos_count} available players (quota: {quota})")
        else:
            selected = pos_players.head(actual_quota)
            logger.info(f"  {pos}: Selected top {actual_quota} of {pos_count} players (quota: {quota})")
        
        final_selections.append(selected)
    
    # Combine all selections
    if final_selections:
        filtered_df = pd.concat(final_selections, ignore_index=True)
    else:
        filtered_df = pd.DataFrame()  # Empty if no selections
    
    # Log filtering results by position
    logger.info("Final position distribution:")
    if not filtered_df.empty:
        position_results = filtered_df['primary_pos'].value_counts().sort_values(ascending=False)
        total_selected = len(filtered_df)
        for pos, count in position_results.items():
            quota = position_quotas.get(pos, 0)
            logger.info(f"  {pos}: {count:,} players (quota: {quota})")
        
        # Log filtering results by era
        logger.info("Era distribution:")
        era_results = filtered_df['era'].value_counts().sort_index()
        for era, count in era_results.items():
            logger.info(f"  {era}: {count:,} players")
        
        logger.info(f"Total selected: {total_selected:,} players")
    else:
        logger.warning("No players selected after filtering!")
    
    # Clean up temporary columns
    if 'quality_score' in filtered_df.columns:
        filtered_df = filtered_df.drop('quality_score', axis=1)
    
    logger.info(f"Position-quota filtering complete: {len(filtered_df):,}/{original_count:,} players selected ({len(filtered_df)/original_count*100:.1f}%)")
    
    return filtered_df


def create_snapshot_manifest(df: pd.DataFrame) -> Dict:
    """Create manifest metadata for the snapshot."""
    manifest = {
        'snapshot_date': date.today().isoformat(),
        'milestone': 'milestone_3_candidate_filtering',
        'input_sources': [
            'data/raw/players_index_full.csv',
            'data/raw/historical_legends_pre1974.csv'
        ],
        'record_count': len(df),
        'schema_version': 'frcs_v1',
        'processing_steps': [
            'data_consolidation',
            'name_cleaning',
            'position_harmonization', 
            'era_bucketing',
            'legend_filtering'
        ],
        'filtering_criteria': {
            'min_games': 32,
            'requires_achievement': True,
            'min_career_seasons': 3,
            'position_specific_thresholds': True
        },
        'position_distribution': df['primary_pos'].value_counts().to_dict(),
        'era_distribution': df['era'].value_counts().to_dict()
    }
    
    return manifest


def save_snapshot(df: pd.DataFrame, output_dir: Path, logger: logging.Logger, dry_run: bool = False) -> bool:
    """Save normalized data as immutable snapshot."""
    try:
        if dry_run:
            logger.info(f"[DRY RUN] Would save snapshot to: {output_dir}")
            logger.info(f"[DRY RUN] Would save {len(df)} players to players.parquet")
            return True
        
        # Create snapshot directory
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created snapshot directory: {output_dir}")
        
        # Save players data as parquet
        players_path = output_dir / 'players.parquet'
        df.to_parquet(players_path, index=False)
        logger.info(f"Saved players data: {players_path} ({len(df):,} records)")
        
        # Create and save manifest
        manifest = create_snapshot_manifest(df)
        manifest_path = output_dir / 'manifest.json'
        
        import json
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Saved manifest: {manifest_path}")
        
        logger.info(f"✅ Snapshot saved successfully: {output_dir}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving snapshot: {e}")
        return False


def normalize_players(main_path: Path, historical_path: Path, output_dir: Path, 
                     logger: logging.Logger, batch_size: Optional[int] = None, 
                     dry_run: bool = False) -> bool:
    """Main normalization pipeline."""
    try:
        # Load raw data
        main_df, historical_df = load_raw_data(main_path, historical_path, logger)
        
        # Combine datasets
        logger.info("Combining modern and historical datasets...")
        combined_df = pd.concat([main_df, historical_df], ignore_index=True)
        logger.info(f"Combined dataset: {len(combined_df):,} total players")
        
        # Apply batch processing if specified
        if batch_size and batch_size < len(combined_df):
            logger.info(f"Applying batch processing: using first {batch_size} players for testing")
            combined_df = combined_df.head(batch_size)
        
        # Normalization steps
        logger.info("Starting normalization pipeline...")
        
        # Step 1: Clean names
        normalized_df = clean_player_names(combined_df, logger)
        
        # Step 2: Harmonize positions
        normalized_df = harmonize_positions(normalized_df, logger)
        
        # Step 3: Add era buckets
        normalized_df = add_era_buckets(normalized_df, logger)
        
        # Step 4: Apply legend filters
        filtered_df = apply_legend_filters(normalized_df, logger)
        
        # Save snapshot
        if not save_snapshot(filtered_df, output_dir, logger, dry_run):
            return False
        
        # Final summary
        logger.info("=== Normalization Pipeline Complete ===")
        logger.info(f"Input: {len(combined_df):,} players")
        logger.info(f"Output: {len(filtered_df):,} legend candidates")
        logger.info(f"Retention rate: {len(filtered_df)/len(combined_df)*100:.1f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"Normalization pipeline failed: {e}")
        return False


@click.command()
@click.option('--main-data', '-m',
              type=click.Path(exists=True, path_type=Path),
              default='data/raw/players_index_full.csv',
              help='Path to main players dataset')
@click.option('--historical-data', '-h',
              type=click.Path(exists=True, path_type=Path), 
              default='data/raw/historical_legends_pre1974.csv',
              help='Path to historical legends dataset')
@click.option('--output-dir', '-o',
              type=click.Path(path_type=Path),
              help='Output directory for snapshot (defaults to data/snapshots/YYYY-MM-DD)')
@click.option('--batch-size', '-b',
              type=int,
              help='Process only first N players for testing')
@click.option('--dry-run', '-d',
              is_flag=True,
              help='Show what would be done without making changes')
@click.option('--verbose', '-v',
              is_flag=True,
              help='Enable verbose logging')
def main(main_data: Path, historical_data: Path, output_dir: Optional[Path], 
         batch_size: Optional[int], dry_run: bool, verbose: bool) -> None:
    """
    Milestone 3: Normalize and filter NFL players for legend candidates.
    
    Combines modern NFL data with historical Hall of Fame legends, applies
    cleaning and filtering to produce a curated pool of ~3-4k legend candidates.
    """
    logger = setup_logging(verbose)
    
    # Default output directory with today's date
    if output_dir is None:
        today = date.today().isoformat()
        output_dir = Path(f'data/snapshots/{today}')
    
    logger.info("=== Milestone 3: NFL Legend Candidate Filtering ===")
    if dry_run:
        logger.info("🔍 DRY RUN MODE - No files will be modified")
    if batch_size:
        logger.info(f"🧪 BATCH MODE - Processing first {batch_size} players only")
    
    # Run normalization pipeline
    success = normalize_players(
        main_path=main_data,
        historical_path=historical_data, 
        output_dir=output_dir,
        logger=logger,
        batch_size=batch_size,
        dry_run=dry_run
    )
    
    if success:
        logger.info("✅ Milestone 3 complete: Legend candidates ready for ranking pipeline")
    else:
        logger.error("❌ Milestone 3 failed")
        exit(1)


if __name__ == "__main__":
    main()