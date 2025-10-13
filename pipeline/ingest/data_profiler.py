"""Player data profiler for era-aware attribute mapping.

Generates metadata tracking data availability per player (FRCS Section 4.3).
Used for tiered fallback logic in attribute mapping (PASS 2).
"""

import logging
from typing import List

import pandas as pd

from pipeline.ingest.types import SourceTier

logger = logging.getLogger(__name__)


def generate_player_data_profiles(enhanced_players: pd.DataFrame) -> pd.DataFrame:
    """Generate player data profiles for era-aware attribute mapping.

    Analyzes available data for each player and computes:
    - available_tiers: List of TIER_1/2/3/4 available for this player
    - has_combine: Boolean for combine data presence
    - has_nextgen: Boolean for NextGen stats (placeholder for future)
    - has_advanced_stats: Boolean for advanced metrics (placeholder)
    - has_basic_stats: Boolean for basic box score stats
    - has_honors: Boolean for Pro Bowl/All-Pro/Awards/HOF
    - data_richness_score: Float 0.0-1.0 for auditing

    Args:
        enhanced_players: DataFrame with merged player data from aggregators

    Returns:
        DataFrame matching FRCS Section 4.3 schema:
        - player_id
        - era_bucket
        - available_tiers (list of strings)
        - has_combine (bool)
        - has_nextgen (bool)
        - has_advanced_stats (bool)
        - has_basic_stats (bool)
        - has_honors (bool)
        - data_richness_score (float)

    Raises:
        ValueError: If enhanced_players missing required columns
    """
    logger.info("Generating player data profiles...")

    # Validate input
    required_cols = ["gsis_id", "era_bucket"]
    missing = [col for col in required_cols if col not in enhanced_players.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    profiles = pd.DataFrame()
    profiles["player_id"] = enhanced_players["gsis_id"]
    profiles["era_bucket"] = enhanced_players["era_bucket"]

    # TIER_1: Combine data (40-time, bench, vertical, broad, 3-cone, shuttle)
    combine_cols = ["forty", "bench", "vertical", "broad_jump", "cone", "shuttle"]
    has_combine_mask = pd.Series(False, index=enhanced_players.index)

    for col in combine_cols:
        if col in enhanced_players.columns:
            has_combine_mask |= enhanced_players[col].notna()

    profiles["has_combine"] = has_combine_mask

    # TIER_2: NextGen stats (placeholder - not yet available in base data)
    # In future: check for separation, route_tracking, speed_mph columns
    profiles["has_nextgen"] = False  # TODO: Add when NextGen data integrated

    # TIER_2: Advanced stats (EPA, PACR, target share, air yards)
    # Currently not in base enhanced_players - placeholder for future
    profiles["has_advanced_stats"] = False  # TODO: Add when advanced stats integrated

    # TIER_3: Basic stats (yards, TDs, completions, attempts)
    basic_stat_cols = [
        "career_passing_yards",
        "career_rushing_yards",
        "career_receiving_yards",
        "career_passing_tds",
        "career_rushing_tds",
        "career_receiving_tds",
        "total_career_games",
    ]

    has_basic_stats_mask = pd.Series(False, index=enhanced_players.index)

    for col in basic_stat_cols:
        if col in enhanced_players.columns:
            # Consider stats present if value > 0 (not just non-null)
            has_basic_stats_mask |= (enhanced_players[col].fillna(0) > 0)

    profiles["has_basic_stats"] = has_basic_stats_mask

    # TIER_4: Honors (Pro Bowl, All-Pro, awards, HOF)
    honors_cols = ["probowls", "allpro", "hof"]
    has_honors_mask = pd.Series(False, index=enhanced_players.index)

    for col in honors_cols:
        if col in enhanced_players.columns:
            if col == "hof":
                # HOF is boolean
                has_honors_mask |= enhanced_players[col].fillna(False)
            else:
                # Pro Bowls and All-Pro are counts
                has_honors_mask |= (enhanced_players[col].fillna(0) > 0)

    profiles["has_honors"] = has_honors_mask

    # Compute available_tiers (list of TIER_X strings)
    def compute_tiers(row: pd.Series) -> List[str]:
        """Compute available tiers for a single player."""
        tiers = []

        if row["has_combine"]:
            tiers.append(SourceTier.TIER_1.value)

        if row["has_nextgen"] or row["has_advanced_stats"]:
            tiers.append(SourceTier.TIER_2.value)

        if row["has_basic_stats"]:
            tiers.append(SourceTier.TIER_3.value)

        if row["has_honors"]:
            tiers.append(SourceTier.TIER_4.value)

        # Fallback: if no tiers available, default to TIER_4
        if not tiers:
            tiers.append(SourceTier.TIER_4.value)

        return tiers

    # Vectorized tier computation
    profiles["available_tiers"] = profiles.apply(compute_tiers, axis=1)

    # Compute data_richness_score (0.0-1.0)
    # Weighted by tier quality: TIER_1=0.4, TIER_2=0.3, TIER_3=0.2, TIER_4=0.1
    tier_weights = {
        SourceTier.TIER_1.value: 0.4,
        SourceTier.TIER_2.value: 0.3,
        SourceTier.TIER_3.value: 0.2,
        SourceTier.TIER_4.value: 0.1,
    }

    def compute_richness_score(tiers: List[str]) -> float:
        """Compute weighted data richness score."""
        score = sum(tier_weights.get(tier, 0.0) for tier in tiers)
        # Normalize to 0-1 (max possible score = 1.0 with all tiers)
        return min(score, 1.0)

    profiles["data_richness_score"] = profiles["available_tiers"].apply(
        compute_richness_score
    )

    # Log summary statistics
    logger.info(f"Generated profiles for {len(profiles)} players")
    logger.info(
        f"  Players with TIER_1 (combine): {profiles['has_combine'].sum()}"
    )
    logger.info(
        f"  Players with TIER_2 (advanced): {profiles['has_advanced_stats'].sum()}"
    )
    logger.info(
        f"  Players with TIER_3 (basic stats): {profiles['has_basic_stats'].sum()}"
    )
    logger.info(
        f"  Players with TIER_4 (honors): {profiles['has_honors'].sum()}"
    )

    # Log richness distribution
    mean_richness = profiles["data_richness_score"].mean()
    logger.info(f"  Mean data richness score: {mean_richness:.3f}")

    # Era-specific richness
    era_richness = profiles.groupby("era_bucket")["data_richness_score"].mean()
    logger.info("  Data richness by era:")
    for era, score in era_richness.items():
        logger.info(f"    {era}: {score:.3f}")

    return profiles
