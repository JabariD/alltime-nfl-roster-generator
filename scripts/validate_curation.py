#!/usr/bin/env python3
"""
Validate manual curation YAML files against player data and schema rules.

Usage:
    python scripts/validate_curation.py --player-data data/raw/players_index_full.csv \
        --curation-dir data/manual_curation/ --verbose
"""

import logging
from pathlib import Path
from collections import Counter

import click
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Expected target counts
POSITION_TARGETS = {
    'WR': 64, 'RB': 64, 'LB': 64, 'DB': 64,
    'QB': 32, 'TE': 32, 'DL': 32, 'OL': 32, 'K': 32, 'P': 32
}


def validate_yaml_structure(yaml_data: dict, filename: str) -> list[str]:
    """Validate YAML structure against schema."""
    errors = []
    
    # Check required top-level fields
    required_fields = ['position', 'target_count', 'legends']
    for field in required_fields:
        if field not in yaml_data:
            errors.append(f"{filename}: Missing required field '{field}'")
    
    if 'legends' not in yaml_data:
        return errors  # Can't validate legends without the field
    
    # Validate each legend entry
    for i, legend in enumerate(yaml_data['legends']):
        legend_id = f"{filename}:legends[{i}]"
        
        # Required fields for each legend
        required_legend_fields = ['player_id', 'full_name', 'legend_score', 'tier']
        for field in required_legend_fields:
            if field not in legend:
                errors.append(f"{legend_id}: Missing required field '{field}'")
        
        # Validate score range
        if 'legend_score' in legend:
            score = legend['legend_score']
            if not isinstance(score, int) or score < 98 or score > 100:
                errors.append(f"{legend_id}: legend_score must be integer 98-100, got {score}")
        
        # Validate tier
        if 'tier' in legend:
            tier = legend['tier']
            if not isinstance(tier, int) or tier < 1 or tier > 3:
                errors.append(f"{legend_id}: tier must be integer 1-3, got {tier}")
    
    return errors


def validate_player_ids(legends_data: dict, player_df: pd.DataFrame) -> list[str]:
    """Validate that all player_ids exist in the dataset."""
    errors = []
    valid_player_ids = set(player_df['player_id'].astype(str))
    
    for filename, data in legends_data.items():
        for i, legend in enumerate(data.get('legends', [])):
            player_id = str(legend.get('player_id', ''))
            if player_id not in valid_player_ids:
                errors.append(f"{filename}:legends[{i}]: Unknown player_id '{player_id}'")
    
    return errors


def validate_no_duplicates(legends_data: dict) -> list[str]:
    """Validate no duplicate player_ids across all files."""
    errors = []
    player_id_counter = Counter()
    player_id_sources = {}
    
    for filename, data in legends_data.items():
        for i, legend in enumerate(data.get('legends', [])):
            player_id = legend.get('player_id')
            if player_id:
                player_id_counter[player_id] += 1
                if player_id not in player_id_sources:
                    player_id_sources[player_id] = []
                player_id_sources[player_id].append(f"{filename}:legends[{i}]")
    
    # Report duplicates
    for player_id, count in player_id_counter.items():
        if count > 1:
            sources = ", ".join(player_id_sources[player_id])
            errors.append(f"Duplicate player_id '{player_id}' found in: {sources}")
    
    return errors


def validate_target_counts(legends_data: dict) -> list[str]:
    """Validate each position meets its target count."""
    errors = []
    
    for filename, data in legends_data.items():
        position = data.get('position')
        target_count = data.get('target_count')
        actual_count = len(data.get('legends', []))
        expected_count = POSITION_TARGETS.get(position)
        
        if expected_count and target_count != expected_count:
            errors.append(f"{filename}: target_count is {target_count}, expected {expected_count} for position {position}")
        
        if target_count and actual_count != target_count:
            errors.append(f"{filename}: Has {actual_count} legends, target_count is {target_count}")
    
    return errors


@click.command()
@click.option('--player-data', required=True, type=click.Path(exists=True),
              help='Player data CSV file')
@click.option('--curation-dir', required=True, type=click.Path(exists=True),
              help='Directory containing curation YAML files')
@click.option('--verbose', is_flag=True,
              help='Enable verbose logging')
def main(player_data: str, curation_dir: str, verbose: bool):
    """Validate manual curation files."""
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load player data
    logger.info(f"Loading player data from {player_data}")
    player_df = pd.read_csv(player_data)
    logger.info(f"Loaded {len(player_df)} players")
    
    # Load all curation YAML files
    curation_path = Path(curation_dir)
    legends_data = {}
    
    yaml_files = list(curation_path.glob("*_legends.yaml"))
    if not yaml_files:
        logger.warning(f"No *_legends.yaml files found in {curation_dir}")
        return
    
    logger.info(f"Found {len(yaml_files)} curation files")
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                legends_data[yaml_file.name] = data
        except Exception as e:
            logger.error(f"Error loading {yaml_file}: {e}")
            continue
    
    if not legends_data:
        logger.error("No valid curation files loaded")
        return
    
    # Run validation checks
    all_errors = []
    
    logger.info("Validating YAML structure...")
    for filename, data in legends_data.items():
        errors = validate_yaml_structure(data, filename)
        all_errors.extend(errors)
    
    logger.info("Validating player IDs...")
    errors = validate_player_ids(legends_data, player_df)
    all_errors.extend(errors)
    
    logger.info("Checking for duplicate player IDs...")
    errors = validate_no_duplicates(legends_data)
    all_errors.extend(errors)
    
    logger.info("Validating target counts...")
    errors = validate_target_counts(legends_data)
    all_errors.extend(errors)
    
    # Report results
    if all_errors:
        logger.error(f"Found {len(all_errors)} validation errors:")
        for error in all_errors:
            logger.error(f"  {error}")
        return 1
    else:
        logger.info("✅ All validation checks passed!")
        
        # Summary statistics
        total_legends = sum(len(data.get('legends', [])) for data in legends_data.values())
        positions = [data.get('position') for data in legends_data.values()]
        
        logger.info(f"Summary:")
        logger.info(f"  Total manual legends: {total_legends}")
        logger.info(f"  Positions covered: {len(set(positions))}")
        
        # Score distribution
        all_scores = []
        for data in legends_data.values():
            for legend in data.get('legends', []):
                if 'legend_score' in legend:
                    all_scores.append(legend['legend_score'])
        
        if all_scores:
            score_counter = Counter(all_scores)
            logger.info(f"  Score distribution: {dict(score_counter)}")
        
        return 0


if __name__ == '__main__':
    exit(main())