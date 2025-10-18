"""Era bucket assignment for cross-era normalization.

Assigns players to historical eras based on career midpoint.
Uses vectorized pandas operations for performance.
"""

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from pipeline.ingest.ingest_types import EraBucket

logger = logging.getLogger(__name__)


def load_era_config(config_path: Path) -> Dict[str, Any]:
    """Load era definitions from YAML config.

    Args:
        config_path: Path to eras.yaml config file

    Returns:
        Dictionary with era definitions and boundaries

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Era config not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if "eras" not in config:
        raise ValueError("Config missing 'eras' key")

    return config


def assign_era_bucket(
    df: pd.DataFrame, config_path: Path = Path("config/eras.yaml")
) -> pd.DataFrame:
    """Assign era_bucket to players based on career midpoint.

    Uses vectorized binning to assign each player to an era based on the
    midpoint year of their career. Players are assigned to the era where
    their career midpoint falls within the era's year range.

    Args:
        df: DataFrame with first_year and last_year columns
        config_path: Path to eras.yaml config (default: config/eras.yaml)

    Returns:
        DataFrame with added era_bucket column (EraBucket enum value)

    Raises:
        ValueError: If df missing required columns or invalid data
    """
    logger.info("Assigning era buckets to players...")

    # Validate input
    required_cols = ["first_year", "last_year"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    # Load era config
    config = load_era_config(config_path)
    eras = config["eras"]

    # Calculate career midpoint (vectorized)
    df = df.copy()
    df["career_midpoint"] = (
        (df["first_year"] + df["last_year"]) / 2
    ).round().astype("Int64")

    # Build era boundaries for binning
    era_boundaries = []
    era_labels = []

    for era_key in [
        "dead_ball",
        "post_wwii",
        "passing_revolution",
        "salary_cap",
        "modern_pass_happy",
        "analytics_era",
    ]:
        if era_key not in eras:
            logger.warning(f"Era '{era_key}' not found in config, skipping")
            continue

        era_def = eras[era_key]
        start_year, end_year = era_def["years"]

        era_boundaries.append((start_year, end_year))
        era_labels.append(era_key)

    # Assign era bucket using pd.cut with custom bins
    # For pd.cut: bins define edges, labels define intervals between edges
    # Need N+1 edges for N labels
    bins = [float('-inf')]  # Lower bound for first era

    for start, end in era_boundaries:
        bins.append(end)

    # Use pd.cut for vectorized binning
    df["era_bucket"] = pd.cut(
        df["career_midpoint"],
        bins=bins,
        labels=era_labels,
        include_lowest=True,
        right=True,
    )

    # Convert to string for consistency
    df["era_bucket"] = df["era_bucket"].astype(str)

    # Handle missing/invalid midpoints
    invalid_mask = df["career_midpoint"].isna()
    if invalid_mask.any():
        logger.warning(
            f"Found {invalid_mask.sum()} players with invalid career dates, "
            f"assigning to 'analytics_era' as default"
        )
        df.loc[invalid_mask, "era_bucket"] = EraBucket.ANALYTICS_ERA.value

    # Log era distribution
    era_counts = df["era_bucket"].value_counts().sort_index()
    logger.info("Era bucket distribution:")
    for era, count in era_counts.items():
        logger.info(f"  {era}: {count} players")

    # Drop temporary column
    df = df.drop(columns=["career_midpoint"])

    return df


def get_era_metadata(
    era_bucket: str, config_path: Path = Path("config/eras.yaml")
) -> Dict[str, Any]:
    """Get metadata for a specific era.

    Args:
        era_bucket: Era bucket key (e.g., "analytics_era")
        config_path: Path to eras.yaml config

    Returns:
        Dictionary with era metadata (name, years, data_richness, etc.)

    Raises:
        ValueError: If era_bucket not found in config
    """
    config = load_era_config(config_path)

    if era_bucket not in config["eras"]:
        raise ValueError(f"Era bucket '{era_bucket}' not found in config")

    return config["eras"][era_bucket]


def get_era_bonus(
    era_bucket: str, config_path: Path = Path("config/eras.yaml")
) -> int:
    """Get era fairness bonus for a specific era.

    Args:
        era_bucket: Era bucket key
        config_path: Path to eras.yaml config

    Returns:
        Bonus value (0-5) to offset data scarcity

    Raises:
        ValueError: If era_bucket not found in config
    """
    config = load_era_config(config_path)

    if "era_bonuses" not in config:
        raise ValueError("Config missing 'era_bonuses' key")

    if era_bucket not in config["era_bonuses"]:
        logger.warning(f"No era bonus defined for '{era_bucket}', using 0")
        return 0

    return config["era_bonuses"][era_bucket]


def get_tier_penalty(
    source_tier: str, config_path: Path = Path("config/eras.yaml")
) -> int:
    """Get confidence penalty for a source tier.

    Args:
        source_tier: Source tier key (e.g., "TIER_1")
        config_path: Path to eras.yaml config

    Returns:
        Penalty value (0-6) reflecting measurement uncertainty

    Raises:
        ValueError: If source_tier not found in config
    """
    config = load_era_config(config_path)

    if "tier_confidence_penalties" not in config:
        raise ValueError("Config missing 'tier_confidence_penalties' key")

    if source_tier not in config["tier_confidence_penalties"]:
        logger.warning(f"No tier penalty defined for '{source_tier}', using 0")
        return 0

    return config["tier_confidence_penalties"][source_tier]
