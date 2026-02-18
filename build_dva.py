import sqlite3
import pandas as pd
import numpy as np

DB = "dfs_nba.db"

STAT_COLS = ['pts', 'reb', 'ast', 'stl', 'blk', 'fg3m', 'tov']
FD_WEIGHTS = {'pts': 1.0, 'reb': 1.2, 'ast': 1.5, 'stl': 3.0, 'blk': 3.0, 'fg3m': 3.0, 'tov': -1.0}

TEAM_ABBREV_MAP = {
    'NYK': 'NY', 'NOP': 'NO', 'SAS': 'SA', 'PHX': 'PHO', 'PHO': 'PHO',
    'GSW': 'GS', 'BRK': 'BKN', 'BKN': 'BKN',
}

def normalize_team(raw):
    raw = raw.strip().upper()
    return TEAM_ABBREV_MAP.get(raw, raw)

def extract_opponent(matchup, player_team_hint=None):
    if ' vs. ' in matchup:
        parts = matchup.split(' vs. ')
        home_team = normalize_team(parts[0])
        away_team = normalize_team(parts[1])
        if player_team_hint:
            return away_team if normalize_team(player_team_hint) == home_team else home_team
        return away_team
    elif ' @ ' in matchup:
        parts = matchup.split(' @ ')
        away_team = normalize_team(parts[0])
        home_team = normalize_team(parts[1])
        if player_team_hint:
            return home_team if normalize_team(player_team_hint) == away_team else away_team
        return home_team
    return None

SHRINKAGE_MIN_N = 15
SHRINKAGE_FULL_N = 50
ROLLING_WINDOW_DAYS = 30
ROLLING_BLEND_FULL_SEASON = 0.5
ROLLING_BLEND_RECENT = 0.5

def shrink_toward_zero(value, sample_n):
    if sample_n >= SHRINKAGE_FULL_N:
        return value
    if sample_n <= SHRINKAGE_MIN_N:
        shrink_factor = sample_n / SHRINKAGE_FULL_N
    else:
        shrink_factor = sample_n / SHRINKAGE_FULL_N
    return value * shrink_factor

def compute_team_arch_stats(df_subset, stat_cols):
    return df_subset.groupby(['opp_team', 'archetype']).agg(
        fp_pm=('fp_pm', 'mean'),
        **{f'{s}_pm': (f'{s}_pm', 'mean') for s in stat_cols},
        sample_n=('fp_pm', 'count')
    ).reset_index()

def build_dva():
    print("=" * 60)
    print("DEFENSE VS ARCHETYPE (DVA) — Phase 1")
    print("=" * 60)

    conn = sqlite3.connect(DB)

    df = pd.read_sql_query('''
        SELECT g.player_name, g.matchup, g.min, g.pts, g.reb, g.ast,
               g.stl, g.blk, g.fg3m, g.tov, g.fp, g.game_date,
               COALESCE(a.base_archetype, a.archetype) as archetype
        FROM player_game_logs g
        JOIN player_archetypes a ON g.player_name = a.player_name
        WHERE g.min > 5
    ''', conn)

    print(f"1. Loaded {len(df)} game logs with archetypes ({df.player_name.nunique()} players)")

    df['opp_team'] = df.apply(lambda r: extract_opponent(r['matchup']), axis=1)
    df = df.dropna(subset=['opp_team'])
    df['game_date'] = pd.to_datetime(df['game_date'])
    print(f"   Parsed opponents for {len(df)} rows across {df.opp_team.nunique()} teams")

    for stat in STAT_COLS:
        df[f'{stat}_pm'] = df[stat] / df['min']
    df['fp_pm'] = df['fp'] / df['min']

    cutoff_date = df['game_date'].max() - pd.Timedelta(days=ROLLING_WINDOW_DAYS)
    df_recent = df[df['game_date'] >= cutoff_date]
    recent_games = len(df_recent)
    print(f"   Rolling window: {recent_games} games in last {ROLLING_WINDOW_DAYS} days (cutoff: {cutoff_date.strftime('%Y-%m-%d')})")

    print("2. Computing league-average baselines per archetype...")
    league_avg = df.groupby('archetype').agg(
        fp_pm=('fp_pm', 'mean'),
        **{f'{s}_pm': (f'{s}_pm', 'mean') for s in STAT_COLS},
        sample_n=('fp_pm', 'count')
    ).reset_index()
    league_avg.columns = ['archetype', 'lg_fp_pm'] + [f'lg_{s}_pm' for s in STAT_COLS] + ['lg_sample_n']

    for _, row in league_avg.iterrows():
        print(f"   {row['archetype']}: FP/min={row['lg_fp_pm']:.4f}, n={int(row['lg_sample_n'])}")

    print("3. Computing team-vs-archetype rates (full season)...")
    team_arch_full = compute_team_arch_stats(df, STAT_COLS)

    print(f"   Computing team-vs-archetype rates (last {ROLLING_WINDOW_DAYS} days)...")
    team_arch_recent = compute_team_arch_stats(df_recent, STAT_COLS)

    team_arch = team_arch_full.merge(
        team_arch_recent, on=['opp_team', 'archetype'], how='left', suffixes=('_full', '_recent')
    )

    blend_cols = ['fp_pm'] + [f'{s}_pm' for s in STAT_COLS]
    for col in blend_cols:
        full_col = f'{col}_full'
        recent_col = f'{col}_recent'
        team_arch[col] = np.where(
            team_arch[recent_col].notna() & (team_arch['sample_n_recent'].fillna(0) >= 5),
            team_arch[full_col] * ROLLING_BLEND_FULL_SEASON + team_arch[recent_col] * ROLLING_BLEND_RECENT,
            team_arch[full_col]
        )

    team_arch['sample_n'] = team_arch['sample_n_full']
    team_arch['recent_n'] = team_arch['sample_n_recent'].fillna(0).astype(int)
    team_arch = team_arch[['opp_team', 'archetype', 'fp_pm'] + [f'{s}_pm' for s in STAT_COLS] + ['sample_n', 'recent_n']]

    all_teams = sorted(team_arch['opp_team'].unique())
    all_archetypes = sorted(team_arch['archetype'].unique())
    full_index = pd.MultiIndex.from_product([all_teams, all_archetypes], names=['opp_team', 'archetype'])
    team_arch = team_arch.set_index(['opp_team', 'archetype']).reindex(full_index).reset_index()

    fill_vals = {'sample_n': 0, 'recent_n': 0}
    for col in ['fp_pm'] + [f'{s}_pm' for s in STAT_COLS]:
        fill_vals[col] = 0
    team_arch = team_arch.fillna(fill_vals)

    lg_map = league_avg.set_index('archetype')
    for _, row in lg_map.iterrows():
        arch = row.name
        mask = (team_arch['archetype'] == arch) & (team_arch['sample_n'] == 0)
        team_arch.loc[mask, 'fp_pm'] = row['lg_fp_pm']
        for s in STAT_COLS:
            team_arch.loc[mask, f'{s}_pm'] = row[f'lg_{s}_pm']

    missing_filled = int((team_arch['sample_n'] == 0).sum())
    if missing_filled > 0:
        print(f"   Filled {missing_filled} missing team-archetype combos with league averages (neutral)")

    dva = team_arch.merge(league_avg, on='archetype', suffixes=('', '_lg'))

    dva['fp_pm_diff'] = dva['fp_pm'] - dva['lg_fp_pm']
    for s in STAT_COLS:
        dva[f'{s}_pm_diff'] = dva[f'{s}_pm'] - dva[f'lg_{s}_pm']

    print("4. Computing archetype stat profiles (Phase 2)...")
    profiles = {}
    for arch in df['archetype'].unique():
        arch_data = df[df['archetype'] == arch]
        total_fp_pm = arch_data['fp_pm'].mean()
        profile = {}
        for s in STAT_COLS:
            stat_fp_contribution = arch_data[f'{s}_pm'].mean() * abs(FD_WEIGHTS[s])
            profile[s] = stat_fp_contribution
        total = sum(profile.values())
        for s in STAT_COLS:
            profile[s] = round(profile[s] / total * 100, 1) if total > 0 else 0
        profiles[arch] = profile
        stats_str = ', '.join(f'{s}={profile[s]}%' for s in STAT_COLS)
        print(f"   {arch}: {stats_str}")

    profile_rows = []
    for arch, prof in profiles.items():
        row = {'archetype': arch}
        for s in STAT_COLS:
            row[f'{s}_pct'] = prof[s]
        profile_rows.append(row)
    profile_df = pd.DataFrame(profile_rows)

    print("5. Computing DVS multipliers (Phase 3) with sample-size shrinkage...")
    dvs_rows = []
    for _, row in dva.iterrows():
        arch = row['archetype']
        team = row['opp_team']
        n = row['sample_n']
        if arch not in profiles:
            continue
        prof = profiles[arch]
        multiplier_raw = 0.0
        components = {}
        for s in STAT_COLS:
            weight = prof[s] / 100.0
            leak = row[f'{s}_pm_diff'] / row[f'lg_{s}_pm'] if row[f'lg_{s}_pm'] != 0 else 0
            contribution = weight * leak
            multiplier_raw += contribution
            components[s] = round(contribution * 100, 2)

        multiplier = shrink_toward_zero(multiplier_raw, n)

        dvs_rows.append({
            'opp_team': team,
            'archetype': arch,
            'dvs_multiplier': round(multiplier * 100, 2),
            'dvs_raw': round(multiplier_raw * 100, 2),
            'sample_n_used': int(n),
            **{f'{s}_component': components[s] for s in STAT_COLS}
        })

    dvs_df = pd.DataFrame(dvs_rows)
    dva = dva.merge(dvs_df, on=['opp_team', 'archetype'], how='left')

    print("6. Saving to database...")
    cols_to_save = ['opp_team', 'archetype', 'fp_pm', 'fp_pm_diff', 'sample_n', 'recent_n']
    for s in STAT_COLS:
        cols_to_save.extend([f'{s}_pm', f'{s}_pm_diff'])
    cols_to_save.extend(['dvs_multiplier', 'dvs_raw', 'sample_n_used'])
    for s in STAT_COLS:
        cols_to_save.append(f'{s}_component')

    dva_save = dva[cols_to_save].copy()
    dva_save.to_sql('dva_stats', conn, if_exists='replace', index=False)
    print(f"   Saved {len(dva_save)} DVA rows to dva_stats table")

    shrunk_count = (dva_save['dvs_raw'].abs() > dva_save['dvs_multiplier'].abs() + 0.01).sum()
    print(f"   Shrinkage applied to {shrunk_count} matchups (small sample sizes)")

    profile_df.to_sql('archetype_profiles', conn, if_exists='replace', index=False)
    print(f"   Saved {len(profile_df)} archetype profiles to archetype_profiles table")

    print("\n7. Sample DVA results (biggest advantages):")
    top = dva.nlargest(10, 'fp_pm_diff')[['opp_team', 'archetype', 'fp_pm', 'fp_pm_diff', 'sample_n', 'dvs_multiplier']]
    for _, row in top.iterrows():
        print(f"   {row['archetype']:25s} vs {row['opp_team']:3s}: "
              f"+{row['fp_pm_diff']:.4f} FP/min (n={int(row['sample_n'])}), "
              f"DVS={row['dvs_multiplier']:+.2f}%")

    print("\n8. Sample DVA results (biggest vulnerabilities — defenses get torched):")
    bottom = dva.nsmallest(10, 'fp_pm_diff')[['opp_team', 'archetype', 'fp_pm', 'fp_pm_diff', 'sample_n', 'dvs_multiplier']]
    for _, row in bottom.iterrows():
        print(f"   {row['archetype']:25s} vs {row['opp_team']:3s}: "
              f"{row['fp_pm_diff']:.4f} FP/min (n={int(row['sample_n'])}), "
              f"DVS={row['dvs_multiplier']:+.2f}%")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    build_dva()
