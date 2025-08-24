import pytest
import pandas as pd
import numpy as np
from pipeline.legend_scores import (
    normalize_position,
    percentile_score,
    calculate_attribute_scores,
    calculate_legend_score,
    process_players,
    POSITION_WEIGHTS
)


class TestNormalizePosition:
    def test_standard_positions(self):
        assert normalize_position('QB') == 'QB'
        assert normalize_position('RB') == 'RB'
        assert normalize_position('WR') == 'WR'
    
    def test_position_mappings(self):
        assert normalize_position('S') == 'DB'
        assert normalize_position('CB') == 'DB'
        assert normalize_position('OLB') == 'LB'
        assert normalize_position('DE') == 'DL'
        assert normalize_position('C') == 'OL'
        assert normalize_position('FB') == 'RB'
    
    def test_unknown_position(self):
        # Should return the position as-is if not in mappings
        assert normalize_position('UNKNOWN') == 'UNKNOWN'


class TestPercentileScore:
    def test_percentile_calculation_higher_better(self):
        values = pd.Series([10, 20, 30, 40, 50])
        
        # Value at 50th percentile
        assert percentile_score(30, values, True) == 40.0  # 2/5 * 100
        
        # Value at top
        assert percentile_score(60, values, True) == 100.0
        
        # Value at bottom
        assert percentile_score(5, values, True) == 0.0
    
    def test_percentile_calculation_lower_better(self):
        values = pd.Series([4.3, 4.4, 4.5, 4.6, 4.7])  # 40-yard times
        
        # Faster time should score higher
        assert percentile_score(4.3, values, False) == 80.0  # 4/5 * 100
        assert percentile_score(4.7, values, False) == 0.0
    
    def test_missing_value(self):
        values = pd.Series([10, 20, 30])
        assert percentile_score(np.nan, values, True) == 50.0
    
    def test_empty_series(self):
        values = pd.Series([])
        assert percentile_score(25, values, True) == 50.0


class TestCalculateAttributeScores:
    def create_sample_position_data(self):
        return pd.DataFrame({
            'height_in': [70, 72, 74, 76, 78],
            'weight_lb': [200, 220, 240, 260, 280],
            'forty_time': [4.3, 4.4, 4.5, 4.6, 4.7],
            'vertical_jump': [30, 32, 34, 36, 38],
            'bench_press': [15, 20, 25, 30, 35],
            'draft_pick': [10, 50, 100, 150, 200],
            'pro_bowls': [0, 1, 2, 3, 4],
            'all_pros': [0, 0, 1, 1, 2],
            'hof_flag': [False, False, False, True, True]
        })
    
    def test_attribute_scores_calculation(self):
        position_data = self.create_sample_position_data()
        
        # Test player with middle values
        player_row = pd.Series({
            'height_in': 74,
            'weight_lb': 240,
            'forty_time': 4.5,
            'vertical_jump': 34,
            'bench_press': 25,
            'draft_pick': 100,
            'pro_bowls': 2,
            'all_pros': 1,
            'hof_flag': False
        })
        
        scores = calculate_attribute_scores(player_row, position_data)
        
        # Should be roughly middle percentiles
        assert 30 <= scores['height'] <= 70
        assert 30 <= scores['weight'] <= 70
        assert 30 <= scores['forty_time'] <= 70
        assert scores['hof_flag'] == 0.0
    
    def test_hall_of_fame_player(self):
        position_data = self.create_sample_position_data()
        
        hof_player = pd.Series({
            'height_in': 76,
            'weight_lb': 260,
            'forty_time': 4.3,
            'pro_bowls': 4,
            'all_pros': 2,
            'hof_flag': True
        })
        
        scores = calculate_attribute_scores(hof_player, position_data)
        
        assert scores['hof_flag'] == 100.0
        assert scores['forty_time'] == 80.0  # 4/5 values are >= 4.3
        assert scores['pro_bowls'] == 80.0   # 4/5 values are < 4
    
    def test_undrafted_player(self):
        position_data = self.create_sample_position_data()
        
        undrafted_player = pd.Series({
            'draft_pick': 999,  # Undrafted marker
            'height_in': 72,
            'pro_bowls': 0,
            'all_pros': 0,
            'hof_flag': False
        })
        
        scores = calculate_attribute_scores(undrafted_player, position_data)
        assert scores['draft_pick'] == 10.0  # Low score for undrafted


class TestCalculateLegendScore:
    def test_qb_legend_score(self):
        # QB with good physical and career attributes
        attribute_scores = {
            'height': 80.0,      # Tall QB
            'forty_time': 60.0,  # Decent mobility
            'draft_pick': 90.0,  # High draft pick
            'pro_bowls': 95.0,   # Many Pro Bowls
            'all_pros': 90.0,    # Multiple All-Pros
            'hof_flag': 100.0    # HOF
        }
        
        score = calculate_legend_score(attribute_scores, 'QB')
        
        # Should be a high score given the weights
        assert score > 80.0
        assert score <= 100.0
    
    def test_wr_legend_score(self):
        # Speed-focused WR
        attribute_scores = {
            'height': 60.0,
            'forty_time': 95.0,     # Very fast
            'vertical_jump': 90.0,  # High vertical
            'draft_pick': 80.0,
            'pro_bowls': 85.0,
            'all_pros': 80.0,
            'hof_flag': 0.0
        }
        
        score = calculate_legend_score(attribute_scores, 'WR')
        
        # Speed and athleticism should be heavily weighted for WR
        assert score > 70.0
    
    def test_ol_legend_score(self):
        # Big, strong OL
        attribute_scores = {
            'height': 95.0,      # Very tall
            'weight': 90.0,      # Heavy
            'bench_press': 95.0, # Very strong
            'draft_pick': 70.0,
            'pro_bowls': 80.0,
            'all_pros': 75.0,
            'hof_flag': 0.0
        }
        
        score = calculate_legend_score(attribute_scores, 'OL')
        
        # Size and strength matter most for OL
        assert score > 75.0
    
    def test_unknown_position_fallback(self):
        attribute_scores = {
            'height': 50.0,
            'forty_time': 50.0
        }
        
        # Should not crash on unknown position
        score = calculate_legend_score(attribute_scores, 'UNKNOWN_POS')
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_missing_attributes(self):
        # Player with only some attributes
        attribute_scores = {
            'pro_bowls': 80.0,
            'hof_flag': 100.0
        }
        
        score = calculate_legend_score(attribute_scores, 'QB')
        
        # Should handle missing attributes gracefully
        assert isinstance(score, float)
        assert 0 <= score <= 100


class TestProcessPlayers:
    def create_sample_dataframe(self):
        return pd.DataFrame({
            'player_id': ['00-001', '00-002', '00-003', '00-004', '00-005'],
            'full_name': ['Tom Brady', 'Jerry Rice', 'Joe Montana', 'Randy Moss', 'Lawrence Taylor'],
            'primary_pos': ['QB', 'WR', 'QB', 'WR', 'LB'],
            'height_in': [76, 74, 74, 76, 75],
            'weight_lb': [225, 200, 200, 210, 245],
            'forty_time': [5.3, 4.5, np.nan, 4.2, 4.6],
            'draft_pick': [199, 16, 82, 21, 2],
            'pro_bowls': [15, 13, 8, 6, 10],
            'all_pros': [3, 10, 3, 4, 8],
            'hof_flag': [True, True, True, True, True],
            'total_career_games': [335, 303, 192, 218, 184]
        })
    
    def test_process_players_basic(self):
        df = self.create_sample_dataframe()
        results = process_players(df, min_position_players=1)
        
        # Should return results for all players
        assert len(results) == 5
        
        # Check required columns
        expected_columns = ['player_id', 'full_name', 'position', 'legend_score', 'source_tier']
        assert all(col in results.columns for col in expected_columns)
        
        # All scores should be between 0-100
        assert results['legend_score'].min() >= 0
        assert results['legend_score'].max() <= 100
        
        # All should have source_tier = 2 (rules-based)
        assert all(results['source_tier'] == 2)
    
    def test_position_normalization_in_process(self):
        df = pd.DataFrame({
            'player_id': ['00-001', '00-002'],
            'full_name': ['Ed Reed', 'Ray Lewis'],
            'primary_pos': ['S', 'MLB'],  # Should normalize to DB, LB
            'height_in': [72, 73],
            'weight_lb': [200, 250],
            'pro_bowls': [9, 13],
            'all_pros': [5, 7],
            'hof_flag': [True, True],
            'total_career_games': [174, 228]
        })
        
        results = process_players(df, min_position_players=1)
        
        # Should normalize positions
        positions = set(results['position'].tolist())
        assert 'DB' in positions
        assert 'LB' in positions
        assert 'S' not in positions  # Should be normalized
        assert 'MLB' not in positions  # Should be normalized


if __name__ == '__main__':
    pytest.main([__file__])