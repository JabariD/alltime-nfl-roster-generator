"""Unit tests for Next Gen Stats loader module.

Tests cover:
- Raw data loading
- Season aggregate filtering
- Minimum threshold filtering
- Career aggregation (weighted averages)
- Column renaming
- Stat type merging
- Error handling and edge cases
"""

from unittest.mock import Mock

import pandas as pd
import pytest

from pipeline.ingest.ngs_loader import (
    MIN_PASSING_ATTEMPTS,
    aggregate_career_ngs_stats,
    apply_minimum_thresholds,
    filter_season_aggregates,
    load_ngs_data,
    load_raw_ngs_data,
    merge_ngs_stat_types,
)
from pipeline.ingest.types import NFLVerseLoadError


class TestLoadRawNGSData:
    """Test raw NGS data loading."""

    def test_valid_stat_type(self) -> None:
        """Should load data for valid stat types."""
        mock_nfl = Mock()
        mock_nfl.import_ngs_data.return_value = pd.DataFrame(
            {"player_gsis_id": ["P1"], "week": [0], "attempts": [100]}
        )

        result = load_raw_ngs_data("passing", [2023], mock_nfl)

        assert len(result) == 1
        mock_nfl.import_ngs_data.assert_called_once_with(
            stat_type="passing", years=[2023]
        )

    def test_invalid_stat_type(self) -> None:
        """Should raise error for invalid stat type."""
        mock_nfl = Mock()

        with pytest.raises(NFLVerseLoadError, match="Invalid stat_type"):
            load_raw_ngs_data("invalid_type", [2023], mock_nfl)

    def test_nfl_module_error(self) -> None:
        """Should raise NFLVerseLoadError when nfl module fails."""
        mock_nfl = Mock()
        mock_nfl.import_ngs_data.side_effect = Exception("API error")

        with pytest.raises(NFLVerseLoadError, match="Failed to load"):
            load_raw_ngs_data("passing", [2023], mock_nfl)


class TestFilterSeasonAggregates:
    """Test season aggregate filtering (week == 0)."""

    def test_filters_to_week_zero(self) -> None:
        """Should filter to only season totals (week 0)."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P1", "P2"],
                "week": [0, 1, 0],
                "attempts": [100, 20, 50],
            }
        )

        result = filter_season_aggregates(df)

        assert len(result) == 2
        assert all(result["week"] == 0)
        assert set(result["player_gsis_id"]) == {"P1", "P2"}

    def test_empty_dataframe(self) -> None:
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame({"week": []})

        result = filter_season_aggregates(df)

        assert result.empty

    def test_missing_week_column(self) -> None:
        """Should raise error if week column missing."""
        df = pd.DataFrame({"player_gsis_id": ["P1"]})

        with pytest.raises(ValueError, match="missing 'week' column"):
            filter_season_aggregates(df)


class TestApplyMinimumThresholds:
    """Test minimum sample size filtering."""

    def test_passing_threshold(self) -> None:
        """Should filter to players with 50+ attempts."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P2", "P3"],
                "attempts": [100, 49, 50],
                "season": [2023, 2023, 2023],
            }
        )

        result = apply_minimum_thresholds(df, "passing")

        assert len(result) == 2
        assert set(result["player_gsis_id"]) == {"P1", "P3"}
        assert all(result["attempts"] >= MIN_PASSING_ATTEMPTS)

    def test_rushing_threshold(self) -> None:
        """Should filter to players with 30+ carries."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P2"],
                "carries": [50, 20],
                "season": [2023, 2023],
            }
        )

        result = apply_minimum_thresholds(df, "rushing")

        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"

    def test_receiving_threshold(self) -> None:
        """Should filter to players with 20+ targets."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P2"],
                "targets": [25, 15],
                "season": [2023, 2023],
            }
        )

        result = apply_minimum_thresholds(df, "receiving")

        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"

    def test_empty_dataframe(self) -> None:
        """Should return empty DataFrame for empty input."""
        df = pd.DataFrame({"attempts": []})

        result = apply_minimum_thresholds(df, "passing")

        assert result.empty

    def test_missing_volume_column(self) -> None:
        """Should raise error if volume column missing."""
        df = pd.DataFrame({"player_gsis_id": ["P1"]})

        with pytest.raises(ValueError, match="missing 'attempts' column"):
            apply_minimum_thresholds(df, "passing")

    def test_invalid_stat_type(self) -> None:
        """Should raise error for unknown stat type."""
        df = pd.DataFrame({"player_gsis_id": ["P1"]})

        with pytest.raises(ValueError, match="Unknown stat_type"):
            apply_minimum_thresholds(df, "invalid")


class TestAggregateCareerNGSStats:
    """Test career-level weighted aggregation."""

    def test_weighted_average_single_season(self) -> None:
        """Should return season stats for single season."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "season": [2023],
                "attempts": [100],
                "completion_percentage_above_expectation": [5.0],
                "avg_time_to_throw": [2.5],
            }
        )

        result = aggregate_career_ngs_stats(df, "passing")

        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"
        assert result.iloc[0]["ngs_xcomp"] == 5.0
        assert result.iloc[0]["ngs_time_to_throw"] == 2.5
        assert result.iloc[0]["ngs_passing_attempts"] == 100
        assert result.iloc[0]["ngs_seasons_covered"] == 1

    def test_weighted_average_multiple_seasons(self) -> None:
        """Should compute weighted average across seasons."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P1"],
                "season": [2022, 2023],
                "carries": [100, 200],  # weights
                "rush_yards_over_expected_per_att": [0.5, 1.0],
                "efficiency": [0.6, 0.8],
            }
        )

        result = aggregate_career_ngs_stats(df, "rushing")

        # Weighted avg for ryoe: (0.5*100 + 1.0*200) / 300 = 250/300 = 0.833
        # Weighted avg for efficiency: (0.6*100 + 0.8*200) / 300 = 220/300 = 0.733
        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"
        assert abs(result.iloc[0]["ngs_ryoe_per_att"] - 0.833) < 0.01
        assert abs(result.iloc[0]["ngs_rush_efficiency"] - 0.733) < 0.01
        assert result.iloc[0]["ngs_rushing_attempts"] == 300
        assert result.iloc[0]["ngs_seasons_covered"] == 2

    def test_multiple_players(self) -> None:
        """Should aggregate separately for each player."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P2"],
                "season": [2023, 2023],
                "targets": [50, 75],
                "avg_separation": [2.5, 3.0],
                "catch_percentage": [0.7, 0.8],
            }
        )

        result = aggregate_career_ngs_stats(df, "receiving")

        assert len(result) == 2
        assert set(result["player_gsis_id"]) == {"P1", "P2"}

    def test_empty_dataframe(self) -> None:
        """Should return empty DataFrame for empty input."""
        df = pd.DataFrame(
            {"player_gsis_id": [], "season": [], "attempts": []}
        )

        result = aggregate_career_ngs_stats(df, "passing")

        assert result.empty
        assert "player_gsis_id" in result.columns

    def test_missing_stat_columns(self) -> None:
        """Should handle missing stat columns gracefully."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "season": [2023],
                "attempts": [100],
                # No actual stat columns
            }
        )

        result = aggregate_career_ngs_stats(df, "passing")

        assert result.empty

    def test_invalid_stat_type(self) -> None:
        """Should raise error for invalid stat type."""
        df = pd.DataFrame({"player_gsis_id": ["P1"], "season": [2023]})

        with pytest.raises(ValueError, match="Invalid stat_type"):
            aggregate_career_ngs_stats(df, "invalid")

    def test_column_renaming(self) -> None:
        """Should rename columns to ngs_ prefix."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "season": [2023],
                "attempts": [100],
                "completion_percentage_above_expectation": [5.0],
            }
        )

        result = aggregate_career_ngs_stats(df, "passing")

        assert "ngs_xcomp" in result.columns
        assert "completion_percentage_above_expectation" not in result.columns


class TestMergeNGSStatTypes:
    """Test merging of passing, rushing, receiving stats."""

    def test_merge_all_three_stat_types(self) -> None:
        """Should merge all three stat types with outer join."""
        passing = pd.DataFrame(
            {"player_gsis_id": ["P1", "P2"], "ngs_xcomp": [5.0, 3.0]}
        )
        rushing = pd.DataFrame(
            {"player_gsis_id": ["P2", "P3"], "ngs_ryoe_per_att": [0.5, 0.8]}
        )
        receiving = pd.DataFrame(
            {"player_gsis_id": ["P1", "P3"], "ngs_avg_separation": [2.5, 3.0]}
        )

        result = merge_ngs_stat_types(passing, rushing, receiving)

        # Should have all 3 players
        assert len(result) == 3
        assert set(result["player_gsis_id"]) == {"P1", "P2", "P3"}

        # Check data integrity
        p1 = result[result["player_gsis_id"] == "P1"].iloc[0]
        assert p1["ngs_xcomp"] == 5.0
        assert pd.isna(p1["ngs_ryoe_per_att"])
        assert p1["ngs_avg_separation"] == 2.5

    def test_empty_stat_types(self) -> None:
        """Should handle empty DataFrames for some stat types."""
        passing = pd.DataFrame(
            {"player_gsis_id": ["P1"], "ngs_xcomp": [5.0]}
        )
        rushing = pd.DataFrame({"player_gsis_id": []})
        receiving = pd.DataFrame({"player_gsis_id": []})

        result = merge_ngs_stat_types(passing, rushing, receiving)

        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"

    def test_all_empty(self) -> None:
        """Should return empty DataFrame if all inputs empty."""
        empty = pd.DataFrame({"player_gsis_id": []})

        result = merge_ngs_stat_types(empty, empty, empty)

        assert "player_gsis_id" in result.columns

    def test_missing_player_gsis_id(self) -> None:
        """Should raise error if player_gsis_id missing."""
        passing = pd.DataFrame({"some_column": [1, 2]})
        rushing = pd.DataFrame({"player_gsis_id": []})
        receiving = pd.DataFrame({"player_gsis_id": []})

        with pytest.raises(ValueError, match="missing player_gsis_id"):
            merge_ngs_stat_types(passing, rushing, receiving)

    def test_seasons_covered_aggregation(self) -> None:
        """Should use max seasons_covered across stat types."""
        passing = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "ngs_xcomp": [5.0],
                "ngs_seasons_covered": [3],
            }
        )
        rushing = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "ngs_ryoe_per_att": [0.5],
                "ngs_seasons_covered": [5],
            }
        )
        receiving = pd.DataFrame({"player_gsis_id": []})

        result = merge_ngs_stat_types(passing, rushing, receiving)

        # Should take max (5)
        assert result.iloc[0]["ngs_seasons_covered"] == 5


class TestLoadNGSData:
    """Test main NGS data loading orchestration."""

    def test_successful_load(self) -> None:
        """Should load and merge all three stat types."""
        # Mock nfl_data_py module
        mock_nfl = Mock()
        mock_nfl.import_ngs_data.side_effect = [
            # Passing data
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    "attempts": [100],
                    "completion_percentage_above_expectation": [5.0],
                }
            ),
            # Rushing data
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    "carries": [50],
                    "rush_yards_over_expected_per_att": [0.5],
                }
            ),
            # Receiving data
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    "targets": [30],
                    "avg_separation": [2.5],
                }
            ),
        ]

        result = load_ngs_data([2023], nfl_module=mock_nfl)

        assert len(result) == 1
        assert result.iloc[0]["player_gsis_id"] == "P1"
        assert "ngs_xcomp" in result.columns
        assert "ngs_ryoe_per_att" in result.columns
        assert "ngs_avg_separation" in result.columns

    def test_graceful_degradation_on_stat_type_failure(self) -> None:
        """Should continue if one stat type fails during processing."""
        mock_nfl = Mock()
        mock_nfl.import_ngs_data.side_effect = [
            # Passing succeeds
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    "attempts": [100],
                    "completion_percentage_above_expectation": [5.0],
                }
            ),
            # Rushing succeeds but has no carries column (will fail in thresholds)
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    # Missing 'carries' column - will fail validation
                }
            ),
            # Receiving succeeds
            pd.DataFrame(
                {
                    "player_gsis_id": ["P1"],
                    "week": [0],
                    "season": [2023],
                    "targets": [30],
                    "avg_separation": [2.5],
                }
            ),
        ]

        result = load_ngs_data([2023], nfl_module=mock_nfl)

        # Should have passing and receiving data, but not rushing
        assert len(result) == 1
        assert "ngs_xcomp" in result.columns
        assert "ngs_avg_separation" in result.columns
        assert "ngs_ryoe_per_att" not in result.columns

    def test_empty_result_all_stat_types_fail(self) -> None:
        """Should return empty DataFrame if all stat types fail."""
        mock_nfl = Mock()
        # All stat types return empty DataFrames
        mock_nfl.import_ngs_data.return_value = pd.DataFrame(
            {"player_gsis_id": [], "week": [], "season": []}
        )

        result = load_ngs_data([2023], nfl_module=mock_nfl)

        # Should have player_gsis_id column but no data
        assert "player_gsis_id" in result.columns


class TestEdgeCases:
    """Test edge cases and data quality scenarios."""

    def test_zero_volume_handling(self) -> None:
        """Should handle zero volume gracefully (avoid division by zero)."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1"],
                "season": [2023],
                "attempts": [0],  # Zero attempts
                "completion_percentage_above_expectation": [5.0],
            }
        )

        result = aggregate_career_ngs_stats(df, "passing")

        # Should return None for weighted average when volume is zero
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["ngs_xcomp"])

    def test_mixed_quality_seasons(self) -> None:
        """Should weight heavily toward high-volume seasons."""
        df = pd.DataFrame(
            {
                "player_gsis_id": ["P1", "P1"],
                "season": [2022, 2023],
                "attempts": [10, 990],  # 1% vs 99% weight
                "completion_percentage_above_expectation": [10.0, 0.0],
            }
        )

        result = aggregate_career_ngs_stats(df, "passing")

        # Should be heavily weighted toward 2023 (0.0)
        # (10.0 * 10 + 0.0 * 990) / 1000 = 100/1000 = 0.1
        assert abs(result.iloc[0]["ngs_xcomp"] - 0.1) < 0.01
