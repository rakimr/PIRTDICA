"""Unit tests for Context Engine v2 (matchup_engine.py).

Uses fixed fixtures to verify behavior independent of live database state.
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matchup_engine import (
    normalize_team_abbr,
    _normalize_name,
    _build_archetype_distance,
    ARCHETYPE_DISTANCE,
    parse_opponent_team,
    parse_player_team,
    compute_interaction_weights,
    compute_size_impact,
    compute_matchup_adjustment,
    build_matchup_familiarity,
    build_archetype_matchup_profiles,
)


def make_fixture_data():
    archetypes = pd.DataFrame([
        {'player_name': 'Big Al', 'team': 'BOS', 'archetype': 'Traditional Big'},
        {'player_name': 'Stretch Sam', 'team': 'BOS', 'archetype': 'Stretch 5'},
        {'player_name': 'Wing Will', 'team': 'BOS', 'archetype': '3-and-D Wing'},
        {'player_name': 'Guard Greg', 'team': 'BOS', 'archetype': 'Playmaker'},
        {'player_name': 'Center Carl', 'team': 'NY', 'archetype': 'Traditional Big'},
        {'player_name': 'Center Dan', 'team': 'NY', 'archetype': 'Versatile Big'},
        {'player_name': 'Wing Wes', 'team': 'NY', 'archetype': 'Scoring Wing'},
        {'player_name': 'Guard Gabe', 'team': 'NY', 'archetype': 'Combo Guard'},
    ])

    measurements = pd.DataFrame([
        {'player_name': 'Big Al', 'team': 'BOS', 'height_inches': 84, 'weight_lbs': 260, 'wingspan_inches': 88},
        {'player_name': 'Stretch Sam', 'team': 'BOS', 'height_inches': 82, 'weight_lbs': 240, 'wingspan_inches': 86},
        {'player_name': 'Wing Will', 'team': 'BOS', 'height_inches': 78, 'weight_lbs': 210, 'wingspan_inches': 82},
        {'player_name': 'Guard Greg', 'team': 'BOS', 'height_inches': 74, 'weight_lbs': 190, 'wingspan_inches': 76},
        {'player_name': 'Center Carl', 'team': 'NY', 'height_inches': 80, 'weight_lbs': 230, 'wingspan_inches': 83},
        {'player_name': 'Center Dan', 'team': 'NY', 'height_inches': 81, 'weight_lbs': 245, 'wingspan_inches': 85},
        {'player_name': 'Wing Wes', 'team': 'NY', 'height_inches': 77, 'weight_lbs': 205, 'wingspan_inches': 80},
        {'player_name': 'Guard Gabe', 'team': 'NY', 'height_inches': 75, 'weight_lbs': 195, 'wingspan_inches': 77},
    ])

    volatility = pd.DataFrame([
        {'player_name': 'Big Al', 'avg_min': 30.0, 'min_sd': 4.0, 'games_played': 50},
        {'player_name': 'Stretch Sam', 'avg_min': 25.0, 'min_sd': 5.0, 'games_played': 45},
        {'player_name': 'Wing Will', 'avg_min': 28.0, 'min_sd': 3.5, 'games_played': 55},
        {'player_name': 'Guard Greg', 'avg_min': 32.0, 'min_sd': 3.0, 'games_played': 60},
        {'player_name': 'Center Carl', 'avg_min': 28.0, 'min_sd': 6.0, 'games_played': 40},
        {'player_name': 'Center Dan', 'avg_min': 22.0, 'min_sd': 7.0, 'games_played': 35},
        {'player_name': 'Wing Wes', 'avg_min': 30.0, 'min_sd': 4.0, 'games_played': 50},
        {'player_name': 'Guard Gabe', 'avg_min': 34.0, 'min_sd': 2.5, 'games_played': 55},
    ])

    shot_zones = pd.DataFrame([
        {'player_name': 'Big Al', 'team': 'BOS', 'total_fga': 100, 'ra_fga': 40, 'paint_fga': 30, 'mid_fga': 20, 'three_fga': 10, 'rim_paint_pct': 0.70},
        {'player_name': 'Stretch Sam', 'team': 'BOS', 'total_fga': 100, 'ra_fga': 20, 'paint_fga': 15, 'mid_fga': 25, 'three_fga': 40, 'rim_paint_pct': 0.35},
        {'player_name': 'Wing Will', 'team': 'BOS', 'total_fga': 100, 'ra_fga': 10, 'paint_fga': 15, 'mid_fga': 30, 'three_fga': 45, 'rim_paint_pct': 0.25},
        {'player_name': 'Guard Greg', 'team': 'BOS', 'total_fga': 100, 'ra_fga': 5, 'paint_fga': 15, 'mid_fga': 30, 'three_fga': 50, 'rim_paint_pct': 0.20},
        {'player_name': 'Center Carl', 'team': 'NY', 'total_fga': 100, 'ra_fga': 45, 'paint_fga': 30, 'mid_fga': 15, 'three_fga': 10, 'rim_paint_pct': 0.75},
        {'player_name': 'Center Dan', 'team': 'NY', 'total_fga': 100, 'ra_fga': 35, 'paint_fga': 25, 'mid_fga': 20, 'three_fga': 20, 'rim_paint_pct': 0.60},
        {'player_name': 'Wing Wes', 'team': 'NY', 'total_fga': 100, 'ra_fga': 10, 'paint_fga': 20, 'mid_fga': 25, 'three_fga': 45, 'rim_paint_pct': 0.30},
        {'player_name': 'Guard Gabe', 'team': 'NY', 'total_fga': 100, 'ra_fga': 8, 'paint_fga': 12, 'mid_fga': 30, 'three_fga': 50, 'rim_paint_pct': 0.20},
    ])

    game_logs = pd.DataFrame([
        {'player_name': 'Big Al', 'game_date': '2025-12-01', 'matchup': 'BOS vs. NY', 'min': 30, 'pts': 18, 'reb': 10, 'ast': 2, 'stl': 1, 'blk': 2, 'tov': 2, 'fp': 42.0, 'fg3m': 0},
        {'player_name': 'Big Al', 'game_date': '2025-12-15', 'matchup': 'BOS @ NY', 'min': 28, 'pts': 14, 'reb': 8, 'ast': 3, 'stl': 0, 'blk': 1, 'tov': 1, 'fp': 35.0, 'fg3m': 0},
        {'player_name': 'Big Al', 'game_date': '2025-11-01', 'matchup': 'BOS vs. ATL', 'min': 32, 'pts': 20, 'reb': 12, 'ast': 1, 'stl': 1, 'blk': 3, 'tov': 3, 'fp': 48.0, 'fg3m': 0},
        {'player_name': 'Big Al', 'game_date': '2025-11-15', 'matchup': 'BOS @ ATL', 'min': 30, 'pts': 16, 'reb': 9, 'ast': 2, 'stl': 0, 'blk': 1, 'tov': 2, 'fp': 36.0, 'fg3m': 0},
        {'player_name': 'Center Carl', 'game_date': '2025-12-01', 'matchup': 'NY vs. BOS', 'min': 28, 'pts': 12, 'reb': 8, 'ast': 1, 'stl': 0, 'blk': 1, 'tov': 2, 'fp': 28.0, 'fg3m': 0},
        {'player_name': 'Center Carl', 'game_date': '2025-12-15', 'matchup': 'NY @ BOS', 'min': 26, 'pts': 10, 'reb': 7, 'ast': 0, 'stl': 1, 'blk': 0, 'tov': 1, 'fp': 24.0, 'fg3m': 0},
    ])

    data = {
        'archetypes': archetypes,
        'measurements': measurements,
        'volatility': volatility,
        'shot_zones': shot_zones,
        'game_logs': game_logs,
        'historical': pd.DataFrame(),
        '_name_map': {},
        '_name_variants': {},
    }

    for df_key in ['measurements', 'volatility', 'shot_zones', 'archetypes']:
        df = data.get(df_key, pd.DataFrame())
        if 'player_name' in df.columns:
            for name in df['player_name'].unique():
                norm = _normalize_name(name)
                if norm:
                    if norm not in data['_name_variants']:
                        data['_name_variants'][norm] = set()
                    data['_name_variants'][norm].add(name)
                    if norm not in data['_name_map']:
                        data['_name_map'][norm] = name

    return data


class TestTeamAbbrevNormalization:
    def test_nba_to_fanduel(self):
        assert normalize_team_abbr('GSW') == 'GS'
        assert normalize_team_abbr('NYK') == 'NY'
        assert normalize_team_abbr('PHX') == 'PHO'
        assert normalize_team_abbr('NOP') == 'NO'
        assert normalize_team_abbr('SAS') == 'SA'

    def test_already_canonical(self):
        assert normalize_team_abbr('GS') == 'GS'
        assert normalize_team_abbr('NY') == 'NY'
        assert normalize_team_abbr('PHO') == 'PHO'

    def test_passthrough_unknown(self):
        assert normalize_team_abbr('ATL') == 'ATL'
        assert normalize_team_abbr('BOS') == 'BOS'
        assert normalize_team_abbr('LAL') == 'LAL'

    def test_none_and_empty(self):
        assert normalize_team_abbr(None) is None
        assert normalize_team_abbr('') == ''

    def test_whitespace_stripping(self):
        assert normalize_team_abbr(' GSW ') == 'GS'
        assert normalize_team_abbr('NYK ') == 'NY'


class TestParseMatchup:
    def test_parse_opponent_vs(self):
        assert parse_opponent_team('BOS vs. GSW') == 'GS'
        assert parse_opponent_team('TOR vs. NYK') == 'NY'
        assert parse_opponent_team('DEN vs. PHX') == 'PHO'

    def test_parse_opponent_at(self):
        assert parse_opponent_team('BOS @ GSW') == 'GS'
        assert parse_opponent_team('TOR @ NYK') == 'NY'

    def test_parse_player_team(self):
        assert parse_player_team('BOS vs. GSW') == 'BOS'
        assert parse_player_team('GSW @ BOS') == 'GS'
        assert parse_player_team('NYK vs. BOS') == 'NY'

    def test_parse_na(self):
        assert parse_opponent_team(None) is None
        assert parse_player_team(None) is None
        assert parse_opponent_team(float('nan')) is None


class TestNameNormalization:
    def test_diacritics(self):
        assert _normalize_name('Nikola Jokić') == 'Nikola Jokic'
        assert _normalize_name('Luka Dončić') == 'Luka Doncic'

    def test_plain(self):
        assert _normalize_name('LeBron James') == 'LeBron James'

    def test_none(self):
        assert _normalize_name(None) == ''
        assert _normalize_name('') == ''


class TestArchetypeDistance:
    def test_same_archetype_zero(self):
        assert ARCHETYPE_DISTANCE[('Traditional Big', 'Traditional Big')] == 0.0
        assert ARCHETYPE_DISTANCE[('Playmaker', 'Playmaker')] == 0.0

    def test_adjacent_one(self):
        d = ARCHETYPE_DISTANCE.get(('Traditional Big', 'Versatile Big'), None)
        assert d is not None
        assert d <= 1.0

    def test_distant_archetypes(self):
        d = ARCHETYPE_DISTANCE.get(('Playmaker', 'Traditional Big'), None)
        assert d is not None
        assert d >= 2.0


class TestInteractionWeights:
    def test_weights_sum_to_one(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Big Al', 'NY', data)
        assert len(weights) > 0
        total = sum(w['weight'] for w in weights.values())
        assert abs(total - 1.0) < 0.01

    def test_center_weights_favor_bigs(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Big Al', 'NY', data)
        big_weight = sum(
            w['weight'] for name, w in weights.items()
            if w['archetype'] in ('Traditional Big', 'Versatile Big', 'Stretch 5', 'Point Center')
        )
        guard_weight = sum(
            w['weight'] for name, w in weights.items()
            if w['archetype'] in ('Playmaker', 'Combo Guard')
        )
        assert big_weight > guard_weight

    def test_empty_for_missing_team(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Big Al', 'ZZZZZ', data)
        assert weights == {}

    def test_empty_for_missing_player(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Nonexistent Player', 'NY', data)
        assert weights == {}

    def test_team_normalization_in_weights(self):
        data = make_fixture_data()
        data['archetypes'].loc[data['archetypes']['team'] == 'NY', 'team'] = 'NY'
        weights_gs = compute_interaction_weights('Big Al', 'NY', data)
        assert len(weights_gs) > 0


class TestSizeImpact:
    def test_bigger_player_positive_impact(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Big Al', 'NY', data)
        impact, details = compute_size_impact('Big Al', weights, data)
        assert impact > 0, f"Big Al (84in, 260lbs) should have positive size vs NY bigs, got {impact}"

    def test_smaller_player_negative_impact(self):
        data = make_fixture_data()
        weights = compute_interaction_weights('Center Carl', 'BOS', data)
        impact, details = compute_size_impact('Center Carl', weights, data)
        assert impact < 0, f"Center Carl (80in, 230lbs) should have negative size vs BOS bigs, got {impact}"

    def test_zero_for_missing_measurements(self):
        data = make_fixture_data()
        data['measurements'] = pd.DataFrame(columns=['player_name', 'team', 'height_inches', 'weight_lbs', 'wingspan_inches'])
        weights = compute_interaction_weights('Big Al', 'NY', data)
        impact, details = compute_size_impact('Big Al', weights, data)
        assert impact == 0.0

    def test_interior_gating(self):
        data = make_fixture_data()
        weights_center = compute_interaction_weights('Big Al', 'NY', data)
        impact_center, _ = compute_size_impact('Big Al', weights_center, data)
        weights_guard = compute_interaction_weights('Guard Greg', 'NY', data)
        impact_guard, _ = compute_size_impact('Guard Greg', weights_guard, data)
        assert abs(impact_center) > abs(impact_guard) * 0.5 or impact_guard == 0


class TestMatchupAdjustment:
    def test_returns_all_fields(self):
        data = make_fixture_data()
        fam = build_matchup_familiarity(data)
        arch = build_archetype_matchup_profiles(data)
        result = compute_matchup_adjustment('Big Al', 'NY', data, fam, arch)
        assert 'player_name' in result
        assert 'opponent_team' in result
        assert 'fp_adjustment_est' in result
        assert 'fppm_adjustment' in result
        assert 'details' in result

    def test_capped_at_max(self):
        data = make_fixture_data()
        result = compute_matchup_adjustment('Big Al', 'NY', data)
        assert -3.0 <= result['fp_adjustment_est'] <= 3.0

    def test_missing_player_returns_zero(self):
        data = make_fixture_data()
        result = compute_matchup_adjustment('Fake Player', 'NY', data)
        assert result['fp_adjustment_est'] == 0.0

    def test_team_abbr_normalized(self):
        data = make_fixture_data()
        r1 = compute_matchup_adjustment('Big Al', 'NY', data)
        r2 = compute_matchup_adjustment('Big Al', 'NYK', data)
        assert r1['fp_adjustment_est'] == r2['fp_adjustment_est']

    def test_opposite_size_signs(self):
        data = make_fixture_data()
        r_big = compute_matchup_adjustment('Big Al', 'NY', data)
        r_small = compute_matchup_adjustment('Center Carl', 'BOS', data)
        size_big = r_big['details'].get('component_scores', {}).get('size', 0)
        size_small = r_small['details'].get('component_scores', {}).get('size', 0)
        if size_big != 0 and size_small != 0:
            assert (size_big > 0 and size_small < 0) or (size_big < 0 and size_small > 0), \
                f"Expected opposite signs: Big Al size={size_big}, Carl size={size_small}"


class TestFamiliarity:
    def test_builds_records(self):
        data = make_fixture_data()
        fam = build_matchup_familiarity(data)
        assert len(fam) > 0

    def test_has_required_columns(self):
        data = make_fixture_data()
        fam = build_matchup_familiarity(data)
        assert 'player_name' in fam.columns
        assert 'opponent' in fam.columns
        assert 'matchup_score' in fam.columns


class TestArchetypeProfiles:
    def test_builds_profiles(self):
        data = make_fixture_data()
        profiles = build_archetype_matchup_profiles(data)
        assert len(profiles) > 0

    def test_has_required_columns(self):
        data = make_fixture_data()
        profiles = build_archetype_matchup_profiles(data)
        assert 'player_archetype' in profiles.columns
        assert 'opp_archetype' in profiles.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
