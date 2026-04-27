"""
Generate daily article content for the PIRTDICA Articles page.
Usage: python generate_article.py [YYYY-MM-DD]
       Defaults to today's date.
Reads HIGH confidence picks from prop_recommendations.csv, builds analysis text
using Claude AI (with template fallback), generates header image, and saves
everything to PostgreSQL.
"""
import os
import sys
import json
import time
import pandas as pd
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _safe_float(val, default=0):
    try:
        v = float(val)
        if pd.isna(v):
            return default
        return v
    except (ValueError, TypeError):
        return default


def build_game_label(player_name, player_team, opponent, dfs_df):
    player_row = dfs_df[dfs_df['player_name'] == player_name]
    if player_row.empty:
        player_row = dfs_df[dfs_df['team'] == player_team]
    if player_row.empty:
        return f"vs {opponent}"
    location = str(player_row.iloc[0].get('location', '')).lower()
    if location == 'away':
        return f"{player_team} @ {opponent}"
    return f"{opponent} @ {player_team}"


STAT_LABELS = {
    'PTS': 'points', 'REB': 'rebounds', 'AST': 'assists',
    'STL': 'steals', 'BLK': 'blocks', '3PM': 'threes', 'TO': 'turnovers',
}

STAT_LOG_COL = {
    'PTS': 'pts', 'REB': 'reb', 'AST': 'ast',
    'STL': 'stl', 'BLK': 'blk', '3PM': 'fg3m', 'TO': 'tov',
}


def _get_recent_games(player_name, stat, n=5):
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        col = STAT_LOG_COL.get(stat, stat.lower())
        rows = conn.execute(
            f"SELECT game_date, matchup, {col}, min FROM player_game_logs "
            f"WHERE player_name = ? ORDER BY game_date DESC LIMIT ?",
            (player_name, n)
        ).fetchall()
        conn.close()
        return [{'date': r[0], 'matchup': r[1], 'val': r[2], 'min': r[3]} for r in rows]
    except Exception:
        return []


def _get_matchup_history(player_name, opponent):
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        rows = conn.execute(
            "SELECT vs_fp_avg, season_fp_avg, fp_diff, matchup_score, games_vs "
            "FROM matchup_history WHERE player_name = ? AND opponent = ?",
            (player_name, opponent)
        ).fetchall()
        conn.close()
        if rows:
            return {'vs_fp_avg': rows[0][0], 'season_fp_avg': rows[0][1],
                    'fp_diff': rows[0][2], 'score': rows[0][3], 'games': rows[0][4]}
    except Exception:
        pass
    return None


def _parse_factors(factors_text):
    if not factors_text or str(factors_text) == 'nan':
        return {}
    parsed = {}
    for part in str(factors_text).split(';'):
        part = part.strip()
        if not part:
            continue
        if 'DVA' in part:
            parsed['dva'] = part
        elif 'DVP' in part:
            parsed['dvp'] = part
        elif 'Recent' in part or 'trend' in part.lower():
            parsed['trend'] = part
        elif 'Pace' in part:
            parsed['pace'] = part
        elif 'Total' in part and 'total' not in parsed:
            parsed['total'] = part
        elif 'Usage' in part or 'usage' in part:
            parsed['usage'] = part
        elif 'Min' in part and 'proj' in part.lower():
            parsed['minutes'] = part
        elif 'Teammate' in part or 'OUT' in part:
            parsed['injury'] = part
        elif 'H2H' in part:
            parsed['h2h'] = part
        elif 'ShotZone' in part:
            parsed['shotzone'] = part
        elif 'Redistrib' in part:
            parsed['redistrib'] = part
        elif 'RES' in part:
            parsed['rebound_env'] = part
        elif 'Share' in part:
            parsed['share'] = part
        elif 'Size' in part:
            parsed['size'] = part
        elif 'ROLE CHANGE' in part:
            parsed['role_change'] = part
        elif 'Blowout' in part:
            parsed['blowout'] = part
        elif 'Playmaking' in part or 'P&R' in part:
            parsed['playmaking'] = part
        else:
            parsed.setdefault('other', [])
            parsed['other'].append(part)
    return parsed


def build_analysis_text_template(row, dfs_df):
    player = row['player']
    last_name = player.split()[-1] if ' ' in player else player
    stat = row['stat']
    stat_label = STAT_LABELS.get(stat, stat.lower())
    team = row['team']
    opponent = row['opponent']
    archetype = row.get('archetype', '')
    book_line = _safe_float(row.get('book_line', 0))
    projected = _safe_float(row.get('projected_value', row.get('adjusted_avg', 0)))
    player_avg = _safe_float(row.get('player_avg', 0))
    vs_book_edge = _safe_float(row.get('vs_book_edge', 0))
    dva_edge = _safe_float(row.get('dva_edge', 0))
    dvp_edge = _safe_float(row.get('dvp_edge', 0))
    hit_rate = _safe_float(row.get('hit_rate', 0))
    last5_avg = _safe_float(row.get('last5_avg', 0))
    cv = _safe_float(row.get('cv', 0))
    recommendation = row.get('recommendation', '')
    projection_factors = row.get('projection_factors', '')
    projected_min = _safe_float(row.get('projected_min', 0))
    composite_score = _safe_float(row.get('composite_score', 0))
    blend = row.get('blend', '')
    usage_boost = _safe_float(row.get('usage_boost', 0))

    call = "OVER" if "OVER" in str(recommendation).upper() else "UNDER"
    edge_sign = "+" if vs_book_edge > 0 else ""
    edge_str = f"{edge_sign}{vs_book_edge:.1f}%"

    dfs_row = dfs_df[dfs_df['player_name'] == player]
    salary = int(dfs_row.iloc[0]['salary']) if len(dfs_row) else int(_safe_float(row.get('salary', 0)))
    implied_total = float(dfs_row.iloc[0].get('implied_total', 0)) if len(dfs_row) else 0

    template_team_is_b2b = False
    template_load_mgmt = False
    if len(dfs_row):
        _tr = dfs_row.iloc[0]
        try:
            template_team_is_b2b = bool(_tr.get('team_is_b2b', False)) and not pd.isna(_tr.get('team_is_b2b', False))
        except Exception:
            template_team_is_b2b = False
        try:
            template_load_mgmt = bool(_tr.get('load_mgmt_risk', False)) and not pd.isna(_tr.get('load_mgmt_risk', False))
        except Exception:
            template_load_mgmt = False

    recent_games = _get_recent_games(player, stat)
    matchup_hist = _get_matchup_history(player, opponent)
    factors = _parse_factors(projection_factors)

    paragraphs = []

    trending_up = last5_avg > player_avg if last5_avg and player_avg else None
    if recent_games and len(recent_games) >= 3:
        game_strs = []
        for g in recent_games[:3]:
            from datetime import datetime as _dt
            try:
                gd = _dt.strptime(g['date'], '%Y-%m-%d')
                date_str = gd.strftime('%b %-d')
            except Exception:
                date_str = g['date']
            opp_short = g['matchup'].split()[-1] if g['matchup'] else '?'
            game_strs.append(f"{int(g['val'])} {stat_label} against {opp_short} ({date_str})")

        if call == "OVER" and trending_up:
            opener = f"{player} has been on a tear lately."
        elif call == "OVER" and not trending_up:
            opener = f"The numbers say {player} is due for a bounce-back."
        elif call == "UNDER" and not trending_up:
            opener = f"{player} has been cooling off, and tonight's matchup doesn't help."
        else:
            opener = f"The market has {player} set at {book_line} {stat_label} tonight, and the data tells an interesting story."

        recent_line = f"{opener} In his last three games: {', '.join(game_strs)}."
        if last5_avg > 0:
            trend_word = "up" if trending_up else "down"
            recent_line += f" His 5-game rolling average sits at {last5_avg:.1f}, trending {trend_word} from his season mark of {player_avg:.1f}."
        paragraphs.append(recent_line)
    else:
        opener = f"{player} ({archetype}) faces {opponent} tonight"
        if implied_total and implied_total > 0:
            opener += f" in a game with a {implied_total:.1f} implied total"
        opener += f", and the books have his {stat_label} line set at {book_line}."
        paragraphs.append(opener)

    matchup_parts = []
    if archetype:
        if call == "OVER":
            if dva_edge > 0.5:
                matchup_parts.append(
                    f"As a {archetype}, {last_name} draws a favorable archetype matchup against {opponent}'s defense. "
                    f"The DVA model flags a {dva_edge:+.1f} edge, meaning {opponent} has historically struggled to contain this player profile"
                )
            elif dva_edge < -0.5:
                matchup_parts.append(
                    f"The archetype matchup is slightly negative (DVA {dva_edge:+.1f}), but other factors override it"
                )
        else:
            if dva_edge < -0.5:
                matchup_parts.append(
                    f"{opponent}'s defense has been particularly effective against {archetype}s this season (DVA {dva_edge:+.1f}), "
                    f"suggesting {last_name} may have a harder time finding his rhythm in this one"
                )

    if abs(dvp_edge) > 0.5:
        if dvp_edge > 0:
            matchup_parts.append(
                f"positionally, {opponent} ranks as a {stat_label}-friendly matchup (DVP {dvp_edge:+.1f})"
            )
        else:
            matchup_parts.append(
                f"positionally, {opponent} has been stingy in this category (DVP {dvp_edge:+.1f})"
            )

    if matchup_parts:
        combined = matchup_parts[0]
        for mp in matchup_parts[1:]:
            if combined[-1] != '.':
                combined += ". " + mp[0].upper() + mp[1:]
            else:
                combined += " " + mp[0].upper() + mp[1:]
        if combined[-1] != '.':
            combined += "."
        paragraphs.append(combined)

    if template_team_is_b2b:
        if call == "OVER" and template_load_mgmt:
            paragraphs.append(
                f"**Fatigue caveat:** {team} is on the second night of a back-to-back. "
                f"{last_name} is a high-minutes star, so load management is in play and our model has already trimmed his projected minutes. "
                f"This is the main risk on the OVER."
            )
        elif call == "OVER":
            paragraphs.append(
                f"One yellow flag: {team} is on the second night of a back-to-back. "
                f"Expect a small efficiency drag (~2-3% on shooting) league-wide on B2Bs."
            )
        elif call == "UNDER":
            paragraphs.append(
                f"Bonus context for the UNDER: {team} is on the second night of a back-to-back. "
                f"Tired legs typically mean fewer minutes for stars and lower shooting efficiency."
            )

    context_parts = []

    if 'role_change' in factors:
        rc_text = factors['role_change']
        if '↑' in rc_text:
            import re as _rc_re
            rc_match = _rc_re.search(r'\((\d+)→(\d+) min\)', rc_text)
            if rc_match:
                old_min, new_min = rc_match.group(1), rc_match.group(2)
                context_parts.append(
                    f"This is a role change situation. {last_name} has jumped from {old_min} to {new_min} minutes per game recently, "
                    f"and our model weights his recent production more heavily as a result"
                )
        elif '↓' in rc_text:
            import re as _rc_re
            rc_match = _rc_re.search(r'\((\d+)→(\d+) min\)', rc_text)
            if rc_match:
                old_min, new_min = rc_match.group(1), rc_match.group(2)
                context_parts.append(
                    f"his minutes have dropped significantly from {old_min} to {new_min} per game, "
                    f"indicating a reduced role that the model accounts for"
                )

    if 'injury' in factors:
        inj_text = factors['injury']
        if '+' in inj_text:
            context_parts.append(
                f"There's a teammate absence factor here. {inj_text.replace('Teammate ', '').capitalize()}, "
                f"which should open up additional opportunities for {last_name}"
            )

    if usage_boost and usage_boost > 3:
        context_parts.append(
            f"his projected usage is elevated ({usage_boost:.1f}% boost), suggesting an expanded role tonight"
        )

    if implied_total and implied_total > 115:
        context_parts.append(
            f"the game environment is conducive with a {implied_total:.1f} implied team total"
        )
    elif implied_total and implied_total < 105:
        context_parts.append(
            f"the implied team total of {implied_total:.1f} points to a lower-scoring affair"
        )

    if 'pace' in factors:
        pace_text = factors['pace']
        try:
            import re as _re
            pace_val = _re.search(r'(\d+\.\d+)x', pace_text)
            if pace_val:
                pv = float(pace_val.group(1))
                if pv > 1.01:
                    context_parts.append("an uptick in pace adds possessions")
                elif pv < 0.99:
                    context_parts.append("a slower pace environment could limit possessions")
        except Exception:
            pass

    if 'blowout' in factors:
        context_parts.append(f"the blowout risk caps his upside slightly ({factors['blowout']})")

    if context_parts:
        intro = "Beyond the matchup data, " if matchup_parts else "The game context matters here. "
        paragraphs.append(intro + ", ".join(context_parts) + ".")

    if matchup_hist and matchup_hist.get('games', 0) >= 2:
        fp_diff = matchup_hist.get('fp_diff', 0)
        games_played = matchup_hist.get('games', 0)
        if fp_diff > 2:
            paragraphs.append(
                f"History backs this up: in {games_played} meetings against {opponent} this season, "
                f"{last_name} has averaged {fp_diff:+.1f} fantasy points above his season baseline."
            )
        elif fp_diff < -2:
            paragraphs.append(
                f"It's worth noting that {last_name} has underperformed against {opponent} this season, "
                f"averaging {abs(fp_diff):.1f} fantasy points below his baseline across {games_played} meetings."
            )

    reliability = ""
    if hit_rate > 65:
        reliability = f"**This is one of the more reliable plays on the board.** The {call} has cleared at a {hit_rate:.0f}% rate this season"
    elif hit_rate > 55:
        reliability = f"The {call} has cleared at a solid {hit_rate:.0f}% rate this season"
    elif hit_rate > 0 and hit_rate < 40:
        reliability = f"The historical hit rate is low ({hit_rate:.0f}%), but the current projection data and matchup context override the backward-looking numbers"

    if cv > 0 and cv < 0.25:
        if reliability:
            reliability += f", and {last_name} has been remarkably consistent (CV: {cv:.2f})"
        else:
            reliability = f"{last_name} has been remarkably consistent in this category (CV: {cv:.2f})"
    elif cv > 0.6:
        if reliability:
            reliability += f". The variance is elevated (CV: {cv:.2f}), so size your position accordingly"
        else:
            reliability = f"The variance here is elevated (CV: {cv:.2f}), so this is a higher-risk, higher-reward play"

    if reliability:
        paragraphs.append(reliability + ".")

    opening_line_t = _safe_float(row.get('opening_line', 0))
    current_line_t = _safe_float(row.get('current_line', 0))
    line_drift_t = _safe_float(row.get('line_drift', 0))
    line_snapshots_t = int(_safe_float(row.get('line_snapshots', 0)))
    if opening_line_t and line_snapshots_t > 1 and abs(line_drift_t) >= 0.5:
        if not current_line_t:
            current_line_t = book_line or (opening_line_t + line_drift_t)
        direction_t = "up" if line_drift_t > 0 else "down"
        aligns_t = (line_drift_t > 0 and call == 'OVER') or (line_drift_t < 0 and call == 'UNDER')
        tell_t = "sharp money agrees with our pick" if aligns_t else "the market is moving against this pick (yellow flag)"
        paragraphs.append(
            f"Line movement: Vegas opened at {opening_line_t:.1f} and moved {direction_t} to "
            f"{current_line_t:.1f} ({line_drift_t:+.1f}) across {line_snapshots_t} snapshots // {tell_t}."
        )

    paragraphs.append(
        f"**The Call: {call} {book_line} {stat}** // We project {last_name} at {projected:.1f} {stat_label} tonight "
        f"({edge_str} edge vs. the book). Composite score: {composite_score:.1f}."
    )

    return "\n\n".join(paragraphs)


def _build_pick_context(row, dfs_df):
    player = row['player']
    stat = row['stat']
    stat_label = STAT_LABELS.get(stat, stat.lower())
    team = row['team']
    opponent = row['opponent']
    archetype = row.get('archetype', '')
    book_line = _safe_float(row.get('book_line', 0))
    projected = _safe_float(row.get('projected_value', row.get('adjusted_avg', 0)))
    player_avg = _safe_float(row.get('player_avg', 0))
    vs_book_edge = _safe_float(row.get('vs_book_edge', 0))
    dva_edge = _safe_float(row.get('dva_edge', 0))
    dvp_edge = _safe_float(row.get('dvp_edge', 0))
    hit_rate = _safe_float(row.get('hit_rate', 0))
    last5_avg = _safe_float(row.get('last5_avg', 0))
    cv = _safe_float(row.get('cv', 0))
    recommendation = row.get('recommendation', '')
    projected_min = _safe_float(row.get('projected_min', 0))
    composite_score = _safe_float(row.get('composite_score', 0))
    usage_boost = _safe_float(row.get('usage_boost', 0))
    opportunity_index = _safe_float(row.get('opportunity_index', 0))
    opportunity_spike = row.get('opportunity_spike', False)
    out_player_details = row.get('out_player_details', '')
    confidence_reasons = row.get('confidence_reasons', '')
    projection_factors = row.get('projection_factors', '')
    pace_factor = _safe_float(row.get('pace_factor', 0))
    total_factor = _safe_float(row.get('total_factor', 0))
    blend = row.get('blend', '')
    opening_line = _safe_float(row.get('opening_line', 0))
    current_line = _safe_float(row.get('current_line', 0))
    line_drift = _safe_float(row.get('line_drift', 0))
    line_drift_pct = _safe_float(row.get('line_drift_pct', 0))
    line_snapshots = int(_safe_float(row.get('line_snapshots', 0)))
    last_hour_drift = _safe_float(row.get('last_hour_drift', 0))
    last_hour_from = _safe_float(row.get('last_hour_from', 0))
    last_hour_minutes = _safe_float(row.get('last_hour_minutes', 0))
    raw_move_pattern = row.get('move_pattern')
    if raw_move_pattern is None or (isinstance(raw_move_pattern, float) and pd.isna(raw_move_pattern)):
        move_pattern = None
    else:
        move_pattern = str(raw_move_pattern)
    largest_swing = _safe_float(row.get('largest_swing', 0))
    largest_swing_share = _safe_float(row.get('largest_swing_share', 0))

    call = "OVER" if "OVER" in str(recommendation).upper() else "UNDER"

    dfs_row = dfs_df[dfs_df['player_name'] == player]
    salary = int(dfs_row.iloc[0]['salary']) if len(dfs_row) else int(_safe_float(row.get('salary', 0)))
    implied_total = float(dfs_row.iloc[0].get('implied_total', 0)) if len(dfs_row) else 0

    team_is_b2b = False
    team_is_3in4 = False
    team_days_rest = None
    load_mgmt_risk = False
    opp_is_b2b = False
    opp_days_rest = None
    rest_diff = None
    if len(dfs_row):
        _r = dfs_row.iloc[0]
        team_is_b2b = bool(_r.get('team_is_b2b', False)) if not pd.isna(_r.get('team_is_b2b', False)) else False
        team_is_3in4 = bool(_r.get('team_is_3in4', False)) if not pd.isna(_r.get('team_is_3in4', False)) else False
        load_mgmt_risk = bool(_r.get('load_mgmt_risk', False)) if not pd.isna(_r.get('load_mgmt_risk', False)) else False
        rdr = _r.get('team_days_rest')
        team_days_rest = int(rdr) if rdr is not None and not pd.isna(rdr) else None
        opp_is_b2b = bool(_r.get('opp_is_b2b', False)) if not pd.isna(_r.get('opp_is_b2b', False)) else False
        odr = _r.get('opp_days_rest')
        opp_days_rest = int(odr) if odr is not None and not pd.isna(odr) else None
        rad = _r.get('rest_advantage_days')
        rest_diff = int(rad) if rad is not None and not pd.isna(rad) else None

    recent_games = _get_recent_games(player, stat)
    matchup_hist = _get_matchup_history(player, opponent)

    game_label = build_game_label(player, team, opponent, dfs_df)

    ctx = {
        'player': player,
        'stat': stat,
        'stat_label': stat_label,
        'team': team,
        'opponent': opponent,
        'game': game_label,
        'archetype': archetype,
        'book_line': book_line,
        'projected': round(projected, 1),
        'player_avg': round(player_avg, 1),
        'vs_book_edge': round(vs_book_edge, 1),
        'dva_edge': round(dva_edge, 2),
        'dvp_edge': round(dvp_edge, 2),
        'hit_rate': round(hit_rate, 1),
        'last5_avg': round(last5_avg, 1),
        'cv': round(cv, 3),
        'call': call,
        'projected_min': round(projected_min, 1),
        'composite_score': round(composite_score, 1),
        'salary': salary,
        'implied_total': round(implied_total, 1),
        'usage_boost': round(usage_boost, 1),
        'blend': str(blend),
        'confidence_reasons': str(confidence_reasons),
        'projection_factors': str(projection_factors),
    }

    if opportunity_index and opportunity_index > 0:
        ctx['opportunity_index'] = round(opportunity_index, 2)
    if opportunity_spike and str(opportunity_spike).lower() == 'true':
        ctx['opportunity_spike'] = True
    if out_player_details and str(out_player_details) not in ('', 'nan'):
        try:
            import ast
            details = ast.literal_eval(str(out_player_details))
            ctx['out_players'] = [
                f"{p['name']} ({p['archetype']}, {p['usg']:.1f}% USG, {p['mpg']:.1f} MPG)"
                for p in details
            ]
        except Exception:
            ctx['out_player_details_raw'] = str(out_player_details)[:300]

    if pace_factor and pace_factor > 0:
        ctx['pace_factor'] = round(pace_factor, 3)
    if total_factor and total_factor > 0:
        ctx['total_factor'] = round(total_factor, 3)

    if team_is_b2b:
        if load_mgmt_risk:
            ctx['b2b_signal'] = (
                f"{team} on second night of back-to-back // "
                f"{player} is a high-minutes star, watch for load management or trimmed minutes"
            )
        else:
            ctx['b2b_signal'] = (
                f"{team} on second night of back-to-back // "
                f"expect a small efficiency hit from fatigue (~2-3% on shooting)"
            )
    elif team_is_3in4:
        ctx['b2b_signal'] = f"{team} playing 3 games in 4 nights // mild fatigue context"
    if rest_diff is not None and rest_diff >= 2 and not team_is_b2b:
        if opp_is_b2b:
            ctx['rest_advantage'] = (
                f"Rest mismatch in {team}'s favor // {team} on {team_days_rest} days rest, "
                f"{opponent} on second night of B2B (diff +{rest_diff} days)"
            )
        else:
            ctx['rest_advantage'] = (
                f"Rest mismatch in {team}'s favor // {team} on {team_days_rest} days rest "
                f"vs {opponent} on {opp_days_rest} (diff +{rest_diff} days)"
            )
    elif rest_diff is not None and rest_diff <= -2 and not opp_is_b2b:
        ctx['rest_disadvantage'] = (
            f"Rest mismatch against {team} // {team} on {team_days_rest} days rest "
            f"vs {opponent} on {opp_days_rest} (diff {rest_diff} days)"
        )
    elif team_days_rest is not None and team_days_rest >= 3 and not team_is_b2b:
        ctx['rest_advantage'] = f"{team} coming off {team_days_rest} days rest"

    if opening_line and line_snapshots and line_snapshots > 1:
        if not current_line:
            current_line = book_line or (opening_line + line_drift)
        ctx['opening_line'] = round(opening_line, 1)
        ctx['current_line'] = round(current_line, 1)
        ctx['line_drift'] = round(line_drift, 2)
        ctx['line_drift_pct'] = round(line_drift_pct, 2)
        ctx['line_snapshots'] = line_snapshots
        if abs(line_drift) >= 0.5:
            direction = "UP" if line_drift > 0 else "DOWN"
            aligns = (line_drift > 0 and call == 'OVER') or (line_drift < 0 and call == 'UNDER')
            tell = "sharp money agrees with our pick" if aligns else "line moving against our pick (yellow flag)"
            ctx['line_movement_signal'] = (
                f"Line moved {direction} from {opening_line:.1f} to {current_line:.1f} "
                f"({line_drift:+.1f}, {line_drift_pct:+.1f}%) over {line_snapshots} snapshots: {tell}"
            )
        if abs(last_hour_drift) >= 0.5 and last_hour_from and last_hour_minutes:
            ctx['recent_drift'] = round(last_hour_drift, 2)
            ctx['recent_drift_minutes'] = round(last_hour_minutes, 1)
            late_aligns = (last_hour_drift > 0 and call == 'OVER') or (last_hour_drift < 0 and call == 'UNDER')
            late_tell = (
                "late sharp action confirms our pick" if late_aligns
                else "late market moving against our pick (yellow flag)"
            )
            ctx['late_move_signal'] = (
                f"Recent same-day move (last {last_hour_minutes:.0f} min): {last_hour_from:.1f} -> "
                f"{current_line:.1f} ({last_hour_drift:+.1f}) // {late_tell}"
            )
        if move_pattern:
            ctx['move_pattern'] = move_pattern
        if move_pattern == 'sudden_swing' and abs(largest_swing) >= 0.5:
            swing_dir = "UP" if largest_swing > 0 else "DOWN"
            swing_aligns = (largest_swing > 0 and call == 'OVER') or (largest_swing < 0 and call == 'UNDER')
            swing_tell = (
                "sharp action fired in a single tick — strongest market signal of the day"
                if swing_aligns
                else "sharp market just moved hard against our pick (major yellow flag)"
            )
            share_pct = int(round(largest_swing_share * 100))
            ctx['sharp_swing_signal'] = (
                f"Sudden swing detected: line jumped {largest_swing:+.1f} {swing_dir} in a single snapshot "
                f"(that tick = {share_pct}% of the day's total movement, vs steady drift) // {swing_tell}"
            )
        elif move_pattern == 'reversal':
            ctx['reversal_note'] = (
                "Line reversed direction during the day (moved both ways) — "
                "treat total drift as noisier than usual; weight late_move_signal more heavily."
            )

    if recent_games:
        ctx['recent_games'] = [
            {'date': g['date'], 'matchup': g['matchup'], 'val': g['val'], 'min': g['min']}
            for g in recent_games[:5]
        ]
    if matchup_hist:
        ctx['matchup_history'] = matchup_hist

    return ctx


def _build_slate_context(dfs_df, high_rows):
    games = {}
    for _, row in dfs_df.iterrows():
        t = str(row.get('team', ''))
        o = str(row.get('opponent', ''))
        loc = str(row.get('location', '')).lower()
        imp = _safe_float(row.get('implied_total', 0))
        if t and o:
            if loc == 'away':
                key = f"{t} @ {o}"
            else:
                key = f"{o} @ {t}"
            if key not in games:
                games[key] = {'game': key, 'implied_totals': []}
            if imp > 0:
                games[key]['implied_totals'].append(imp)

    game_summaries = []
    for key, g in sorted(games.items()):
        totals = g['implied_totals']
        avg_total = sum(totals) / len(totals) if totals else 0
        game_summaries.append(f"{key} (implied total ~{avg_total:.0f})" if avg_total > 0 else key)

    import sqlite3
    key_absences = []
    try:
        conn = sqlite3.connect("dfs_nba.db")
        injuries = conn.execute(
            "SELECT player_name, status, reason FROM injuries WHERE status IN ('OUT', 'Doubtful') "
            "ORDER BY player_name"
        ).fetchall()
        conn.close()
        for name, status, reason in injuries:
            match = dfs_df[dfs_df['player_name'] == name]
            if not match.empty:
                team = match.iloc[0].get('team', '')
                usg = _safe_float(match.iloc[0].get('usg_pct', 0))
                if usg > 18:
                    reason_str = f" ({reason})" if reason and str(reason) != 'nan' else ""
                    key_absences.append(f"{name} ({team}, {usg:.1f}% USG) // {status}{reason_str}")
    except Exception:
        pass

    if not key_absences:
        out_details_col = high_rows.get('out_player_details')
        if out_details_col is not None:
            seen_out = set()
            for val in out_details_col:
                if not val or str(val) in ('', 'nan'):
                    continue
                try:
                    import ast
                    details = ast.literal_eval(str(val))
                    for p in details:
                        if p['name'] not in seen_out:
                            seen_out.add(p['name'])
                            key_absences.append(
                                f"{p['name']} ({p.get('archetype', '?')}, {p['usg']:.1f}% USG, {p['mpg']:.1f} MPG) // OUT"
                            )
                except Exception:
                    continue

    ctx = f"SLATE: {len(games)} games // {', '.join(game_summaries)}"
    if key_absences:
        ctx += f"\n\nKEY ABSENCES:\n" + "\n".join(f"- {a}" for a in key_absences[:15])

    rest_lines = []
    try:
        if 'team_is_b2b' in dfs_df.columns:
            b2b_teams = sorted({
                str(t) for t, b in zip(dfs_df['team'], dfs_df['team_is_b2b'])
                if t and bool(b) is True
            })
            if b2b_teams:
                rest_lines.append(
                    "B2B (second night): " + ", ".join(b2b_teams)
                    + " // expect star load management and ~2-3% efficiency drag"
                )
        if 'team_is_3in4' in dfs_df.columns:
            threein4 = sorted({
                str(t) for t, b in zip(dfs_df['team'], dfs_df['team_is_3in4'])
                if t and bool(b) is True
            })
            if threein4:
                rest_lines.append("3-in-4 stretch: " + ", ".join(threein4))
        if 'load_mgmt_risk' in dfs_df.columns:
            star_risks = dfs_df[dfs_df['load_mgmt_risk'] == True]
            if not star_risks.empty:
                names = star_risks['player_name'].astype(str).head(8).tolist()
                rest_lines.append(
                    f"Load mgmt watch ({len(star_risks)} stars on B2B): " + ", ".join(names)
                )
    except Exception:
        pass

    if rest_lines:
        ctx += "\n\nREST WATCH:\n" + "\n".join(f"- {r}" for r in rest_lines)
    return ctx


FEW_SHOT_EXAMPLES = """
EXAMPLE ANALYSES (from a gold-standard 80% hit rate slate. Match this quality and tone):

**RUI HACHIMURA // PTS OVER 9.5 (CHI @ LAL)**
The line is almost insultingly low. Hachimura averages 11.6 on the season and the model has him at 16.3 in 35 projected minutes.

Chicago is the best power forward matchup in the league right now with a +3.8 DVP edge, and LAL is missing Hayes, Smart, and Kleber. That pushes Rui into a heavier offensive role with a +4.2 usage boost. He's hit this line in back-to-back meetings with CHI and the 240 game total creates an ideal scoring environment.

**Top composite score on the slate at 72.9.**

**The Call: OVER 9.5 PTS** // We project Hachimura at 16.3 points tonight (+8.7% edge vs. the book). Composite score: 72.9.

**TRE JONES // AST OVER 4.5 (CHI @ LAL)**
Jones runs this Bulls offense and averages 5.5 assists, a full assist above the book line. The model projects 6.8 in 31 minutes, with Ayo Dosunmu OUT redistributing 0.5 extra assists his way.

His playmaking composite index of 3.1 is strong. Both DVA (+0.73) and DVP (+0.79) are positive, and the 240 game total means plenty of possessions. **Hit rate of 64.6% is the second-highest among all HIGH picks today.**

The one caution: last 5 average is 4.6, slightly below his season mark. But the matchup and usage context override that dip.

**The Call: OVER 4.5 AST** // We project Jones at 6.8 assists tonight (+51.1% edge vs. the book). Composite score: 65.2.

**DYLAN HARPER // PTS OVER 10.5 (DEN @ SA)**
Harper's been heating up. Last 5 average of 13.0 already clears the line by 2.5 points.

He's scored in a DEN matchup before (+2.4 H2H edge) and the game environment is elite: second-highest total on the slate at 240, fast pace on both sides. DVA +0.6 confirms his archetype produces well against Denver's coverage scheme.

**The line at 10.5 hasn't caught up to his recent form, which is exactly the inefficiency we're targeting.**

**The Call: OVER 10.5 PTS** // We project Harper at 14.5 points tonight (+38.1% edge vs. the book). Composite score: 61.8.
"""


def _build_full_slate_briefing(props_df, dfs_df):
    import sqlite3

    games = {}
    for _, row in dfs_df.iterrows():
        t = str(row.get('team', ''))
        o = str(row.get('opponent', ''))
        loc = str(row.get('location', '')).lower()
        imp = _safe_float(row.get('implied_total', 0))
        if t and o:
            key = f"{t} @ {o}" if loc == 'away' else f"{o} @ {t}"
            if key not in games:
                games[key] = {'game': key, 'implied_totals': [], 'teams': set()}
            if imp > 0:
                games[key]['implied_totals'].append(imp)
            games[key]['teams'].update([t, o])

    game_env = {}
    for key, g in games.items():
        totals = g['implied_totals']
        avg = sum(totals) / len(totals) if totals else 0
        game_env[key] = round(avg, 1)

    key_absences = []
    try:
        conn = sqlite3.connect("dfs_nba.db")
        injuries = conn.execute(
            "SELECT player_name, status, reason FROM injuries WHERE status IN ('OUT', 'Doubtful') "
            "ORDER BY player_name"
        ).fetchall()
        conn.close()
        for name, status, reason in injuries:
            match = dfs_df[dfs_df['player_name'] == name]
            if not match.empty:
                team = match.iloc[0].get('team', '')
                usg = _safe_float(match.iloc[0].get('usg_pct', 0))
                if usg > 12:
                    reason_str = f" ({reason})" if reason and str(reason) != 'nan' else ""
                    key_absences.append(f"{name} ({team}, {usg:.1f}% USG) // {status}{reason_str}")
    except Exception:
        pass

    prop_lines = []
    for _, row in props_df.iterrows():
        player = row['player']
        stat = row['stat']
        team = row['team']
        opponent = row['opponent']

        game_label = build_game_label(player, team, opponent, dfs_df)

        dfs_row = dfs_df[dfs_df['player_name'] == player]
        implied_total = float(dfs_row.iloc[0].get('implied_total', 0)) if len(dfs_row) else 0

        out_summary = []
        raw_out = row.get('out_player_details', '')
        if raw_out and str(raw_out) not in ('', 'nan'):
            try:
                import ast
                details = ast.literal_eval(str(raw_out))
                out_summary = [f"{p['name']} ({p['usg']:.0f}% USG, {p['mpg']:.0f} MPG)" for p in details]
            except Exception:
                pass

        entry = {
            'player': player,
            'team': team,
            'opponent': opponent,
            'game': game_label,
            'stat': stat,
            'archetype': row.get('archetype', ''),
            'book_line': _safe_float(row.get('book_line', 0)),
            'projected_value': round(_safe_float(row.get('projected_value', 0)), 1),
            'player_avg': round(_safe_float(row.get('player_avg', 0)), 1),
            'last5_avg': round(_safe_float(row.get('last5_avg', 0)), 1),
            'vs_book_edge': round(_safe_float(row.get('vs_book_edge', 0)), 1),
            'dva_edge': round(_safe_float(row.get('dva_edge', 0)), 2),
            'dvp_edge': round(_safe_float(row.get('dvp_edge', 0)), 2),
            'hit_rate': round(_safe_float(row.get('hit_rate', 0)), 1),
            'cv': round(_safe_float(row.get('cv', 0)), 3),
            'composite_score': round(_safe_float(row.get('composite_score', 0)), 1),
            'usage_boost': round(_safe_float(row.get('usage_boost', 0)), 1),
            'opportunity_index': round(_safe_float(row.get('opportunity_index', 0)), 2),
            'opportunity_spike': str(row.get('opportunity_spike', '')).lower() == 'true',
            'projected_min': round(_safe_float(row.get('projected_min', 0)), 1),
            'implied_team_total': round(implied_total, 1),
            'pace_factor': round(_safe_float(row.get('pace_factor', 0)), 3),
            'total_vacated_usage': round(_safe_float(row.get('total_vacated_usage', 0)), 1),
            'total_vacated_minutes': round(_safe_float(row.get('total_vacated_minutes', 0)), 1),
            'confidence': row.get('confidence', ''),
            'gate_fail_count': int(_safe_float(row.get('gate_fail_count', 99))),
            'confidence_reasons': str(row.get('confidence_reasons', '')),
            'projection_factors': str(row.get('projection_factors', '')),
            'recommendation': row.get('recommendation', ''),
        }
        if out_summary:
            entry['out_players'] = out_summary

        recent = _get_recent_games(player, stat)
        if recent:
            entry['recent_games'] = [
                {'date': g['date'], 'vs': g['matchup'], 'val': g['val'], 'min': g['min']}
                for g in recent[:5]
            ]

        matchup = _get_matchup_history(player, opponent)
        if matchup and matchup.get('games', 0) >= 1:
            entry['h2h'] = {
                'fp_diff': round(matchup.get('fp_diff', 0), 1),
                'games': matchup.get('games', 0),
            }

        prop_lines.append(entry)

    briefing = {
        'game_count': len(games),
        'games': [{'game': k, 'implied_total': v} for k, v in sorted(game_env.items())],
        'key_absences': key_absences[:20],
        'prop_lines': prop_lines,
    }
    return briefing


CLAUDE_ANALYST_PATTERNS = """
ANALYTICAL FRAMEWORK (What separates a great pick from a good one, from our 80% hit rate reference slate):

1. USAGE REDISTRIBUTION FROM STAR ABSENCES: When high-usage stars are OUT, their usage/minutes redistribute to teammates. Look for players with high total_vacated_usage (>30%) and opportunity_spike=true. These are the highest-edge plays.

2. DVP/DVA DOUBLE ALIGNMENT: The strongest picks have BOTH Defense vs Position (dvp_edge) AND Defense vs Archetype (dva_edge) supporting the direction. Both >+0.5 for OVER = strong signal. If both are negative, avoid.

3. GAME TOTAL / PACE ENVIRONMENTS: High implied team totals (>115) and game totals (check game implied_total) of 235+ create more possessions and scoring. These environments amplify OVER picks, especially for PTS and AST.

4. LAST-5 AVERAGE CLEARING THE BOOK LINE: For OVER picks, the player's last5_avg should already clear the book_line. This confirms recent form supports the direction. For UNDER picks, last5_avg should be below the line.

5. HIGH COMPOSITE SCORES: Composite score ranks multi-factor quality. Picks above 60 are strong; above 70 are elite.

6. CONTRARIAN DISCIPLINE: Limit UNDER picks to 1-2 max per slate. Only take UNDERs where the projection is clearly below the line AND matchup/game environment supports lower production. NEVER take UNDER when usage_boost > 3.0.

7. CONSISTENCY CHECK: CV (coefficient of variation) below 0.30 = very consistent player. Above 0.50 = volatile (higher risk).

QUALITY GATES (reference, not hard constraints):
- Hit rate >= 58%
- CV <= 0.30 (tight consistency)
- Last-5 clears book line in pick direction
- At least one of DVA/DVP > 0.5 magnitude supporting direction
- Implied team total > 105 for PTS/AST OVER picks
- Block UNDER if usage_boost > 3.0
"""


def build_claude_analyst(props_df, dfs_df):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude API not configured — cannot run Claude Analyst mode")
        return None

    briefing = _build_full_slate_briefing(props_df, dfs_df)
    briefing_json = json.dumps(briefing, indent=2, default=str)
    print(f"Claude Analyst briefing: {len(briefing['prop_lines'])} prop lines across {briefing['game_count']} games ({len(briefing_json)} chars)")

    system_prompt = """You are an elite NBA DFS analyst for PIRTDICA SPORTS CO. You are given the FULL slate data with projections, matchup edges, usage context, injury impacts, game environments, and recent form.

You are competing directly against the sharpest analysts at FanDuel who set these player prop lines. These are highly prepared, well-resourced professionals backed by Vegas-caliber modeling. Your edge comes from identifying situations their models undervalue: usage redistribution cascades from injuries, emerging role changes, archetype-matchup exploits, and game environment convergences that mass-market lines are slow to price in. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals.

YOUR JOB: Independently analyze the entire slate and select 4-8 HIGH confidence prop picks. You are NOT limited to what the statistical model labeled as HIGH. Evaluate ALL prop lines and find the sharpest edges yourself.

PICK SELECTION CRITERIA:
- Look for convergence of multiple positive signals (usage redistribution + matchup alignment + recent form + game environment)
- Prioritize picks where the book line significantly undervalues the player's projected output
- Weight recent form (last5_avg) heavily, it captures momentum the season average misses
- Opportunity Spikes (star absences creating usage vacuums) are the highest-edge situations
- Consider the game environment: high implied totals create more scoring opportunities
- When `b2b_signal` is present, the player's team is on the second night of a back-to-back. Stars on B2B carry load-management risk (trimmed minutes), and shooting efficiency dips ~2-3% league-wide. Be more skeptical of OVER picks for B2B teams; the model already trims minutes for flagged stars but Vegas usually prices this in too.
- When `rest_advantage` is present, the player's team has a 2+ day rest edge over the opponent (especially strong if the opponent is on a B2B). Treat this as a real positive for OVERs on volume players and a yellow flag for UNDERs. When `rest_disadvantage` is present, the OPPONENT has the rest edge — small negative for OVERs.
- Use the analytical framework provided to evaluate each potential pick

WRITING STYLE:
- Write like you're talking to a sharp friend, not a lecture hall. Conversational, direct, confident.
- NEVER use em-dashes or double hyphens (--). Use periods, commas, colons, or start a new sentence instead.
- Use "you" and "your" to speak directly to the reader.
- Short paragraphs only (2-4 lines max). One idea per paragraph.
- **Bold the key insight** in most sections so skimmers catch the value immediately.
- Lead each pick with a hook that grabs attention: why should the reader care about THIS play right now?
- Show, don't just tell: every claim gets backed with specific numbers (DVA/DVP edges, hit rates, last-5 averages, projected minutes, usage boosts, composite scores).
- Cut filler words like "very," "really," "in order to," "it should be noted that."
- Each analysis: 3-4 paragraphs, 150-250 words.
- End each analysis with: **The Call: OVER/UNDER X.X STAT** // We project [Player] at [Value] [stat] tonight ([edge]% edge vs. the book). Composite score: [X].
- Use "//" as a separator instead of dashes.

OUTPUT FORMAT:
Return a JSON object with two keys:
1. "picks": an array of objects, each with:
   - "player": exact player name
   - "team": team abbreviation
   - "opponent": opponent abbreviation
   - "game": game label (e.g., "CLE @ MIA")
   - "stat": stat type (PTS, REB, AST, STL, BLK)
   - "book_line": the book line number
   - "projected": the model's projected value
   - "avg": season average
   - "edge": edge string (e.g., "+17.8%")
   - "pick": "OVER" or "UNDER"
   - "composite_score": the composite score
2. "analyses": an array of objects, each with:
   - "player": exact player name (must match picks)
   - "stat": stat type
   - "call": "OVER" or "UNDER"
   - "archetype": player archetype
   - "team": team
   - "opponent": opponent
   - "analysis": the full analysis text (3-4 paragraphs, 150-250 words, ending with **The Call:** line)

Return ONLY the JSON object, no other text. Order picks by edge strength (strongest first)."""

    user_prompt = f"""Analyze the full slate and select your HIGH confidence picks.

{CLAUDE_ANALYST_PATTERNS}

{FEW_SHOT_EXAMPLES}

HERE IS THE COMPLETE SLATE DATA (every player prop line with full model context):

{briefing_json}

Remember:
- Select 4-8 picks where you see the strongest convergence of signals
- You can pick ANY prop line from the full slate, not just ones the model labeled HIGH
- Each analysis must be data-driven, cite specific numbers, and end with **The Call:** line
- NEVER use em-dashes or double hyphens. Use periods, commas, colons, or "//" instead.
- Return ONLY a JSON object with "picks" and "analyses" keys"""

    try:
        from anthropic import Anthropic

        client = Anthropic(api_key=api_key, base_url=base_url)

        print("Calling Claude Analyst for full-slate pick selection...")
        start_time = time.time()

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        elapsed = time.time() - start_time
        response_text = message.content[0].text
        print(f"Claude Analyst response received in {elapsed:.1f}s ({len(response_text)} chars)")

        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

        result = json.loads(cleaned)

        if not isinstance(result, dict) or 'picks' not in result or 'analyses' not in result:
            print("Claude Analyst returned unexpected format — falling back")
            return None

        picks = result['picks']
        analyses = result['analyses']

        if not isinstance(picks, list) or not isinstance(analyses, list):
            print("Claude Analyst picks/analyses are not arrays — falling back")
            return None

        if len(picks) < 2:
            print(f"Claude Analyst returned only {len(picks)} picks — falling back")
            return None

        analysis_map = {}
        for item in analyses:
            if isinstance(item, dict):
                player = str(item.get('player', '')).strip()
                analysis = str(item.get('analysis', '')).strip()
                if player and analysis and len(analysis) > 50:
                    analysis_map[player] = item

        slate_lookup = {}
        for _, row in props_df.iterrows():
            key = (str(row['player']).strip(), str(row['stat']).strip())
            if key not in slate_lookup:
                slate_lookup[key] = row

        valid_picks = []
        seen_keys = set()
        for p in picks:
            if not isinstance(p, dict):
                continue
            player = str(p.get('player', '')).strip()
            stat = str(p.get('stat', '')).strip()
            if not player or not stat:
                continue
            key = (player, stat)
            if key in seen_keys:
                print(f"  Skipping duplicate: {player} {stat}")
                continue
            if player not in analysis_map:
                print(f"  Skipping {player} {stat}: no analysis")
                continue
            if key not in slate_lookup:
                print(f"  Skipping {player} {stat}: not found in slate data (hallucinated?)")
                continue
            seen_keys.add(key)
            valid_picks.append((p, slate_lookup[key]))

        print(f"Claude Analyst: {len(picks)} raw picks, {len(valid_picks)} validated against slate")

        if len(valid_picks) < 3:
            print("Claude Analyst too few validated picks — falling back")
            return None

        if len(valid_picks) > 8:
            valid_picks = valid_picks[:8]
            print(f"  Trimmed to 8 picks")

        picks_data = []
        for idx, (claude_pick, source_row) in enumerate(valid_picks):
            player = str(claude_pick.get('player', '')).strip()
            call = str(claude_pick.get('pick', 'OVER')).upper()

            book_line = _safe_float(source_row.get('book_line', 0))
            projected = _safe_float(source_row.get('projected_value', source_row.get('adjusted_avg', 0)))
            player_avg = _safe_float(source_row.get('player_avg', 0))
            vs_book_edge = _safe_float(source_row.get('vs_book_edge', 0))
            composite = _safe_float(source_row.get('composite_score', 0))
            edge_sign = "+" if vs_book_edge > 0 else ""

            game_label = build_game_label(
                player, str(source_row.get('team', '')),
                str(source_row.get('opponent', '')), dfs_df
            )

            picks_data.append({
                'rank': idx + 1,
                'player': player,
                'game': game_label,
                'stat': str(source_row.get('stat', '')),
                'avg': round(player_avg, 1),
                'line': round(book_line, 1),
                'projected': round(projected, 1),
                'edge': f"{edge_sign}{vs_book_edge:.1f}%",
                'pick': call,
                'composite_score': round(composite, 1),
                'archetype': str(source_row.get('archetype', '')) or None,
                'dva_edge': round(float(source_row['dva_edge']), 2) if 'dva_edge' in source_row and pd.notna(source_row['dva_edge']) else None,
                'usage_boost': round(float(source_row['usage_boost']), 2) if 'usage_boost' in source_row and pd.notna(source_row['usage_boost']) else None,
            })

        analysis_data = []
        for claude_pick, source_row in valid_picks:
            player = str(claude_pick.get('player', '')).strip()
            a = analysis_map[player]
            analysis_data.append({
                'player': player,
                'stat': str(source_row.get('stat', '')),
                'call': str(claude_pick.get('pick', 'OVER')).upper(),
                'archetype': str(source_row.get('archetype', '')),
                'team': str(source_row.get('team', '')),
                'opponent': str(source_row.get('opponent', '')),
                'analysis': a.get('analysis', ''),
            })

        print(f"Claude Analyst final: {len(picks_data)} picks, {len(analysis_data)} analyses")
        return {'picks_data': picks_data, 'analysis_data': analysis_data}

    except json.JSONDecodeError as e:
        print(f"Claude Analyst JSON parse error: {e}")
        if 'response_text' in dir():
            print(f"Response preview: {response_text[:500]}")
        return None
    except Exception as e:
        error_msg = str(e)
        if "FREE_CLOUD_BUDGET_EXCEEDED" in error_msg:
            print(f"Claude budget exceeded — falling back")
        else:
            print(f"Claude Analyst error: {error_msg}")
        return None


def build_analysis_claude(high_rows, dfs_df, best_available=False):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude API not configured — falling back to template engine")
        return None

    pick_contexts = []
    seen = set()
    for _, row in high_rows.iterrows():
        player = row['player']
        if player in seen:
            continue
        seen.add(player)
        pick_contexts.append(_build_pick_context(row, dfs_df))

    if not pick_contexts:
        return None

    slate_context = _build_slate_context(dfs_df, high_rows)
    picks_json = json.dumps(pick_contexts, indent=2, default=str)

    system_prompt = """You are a sharp NBA DFS analyst writing for PIRTDICA SPORTS CO., a competitive fantasy sports platform. Your audience is sportsbook bettors looking for HIGH confidence prop picks.

You are competing directly against the sharpest analysts at FanDuel who set these player prop lines. These are highly prepared, well-resourced professionals backed by Vegas-caliber modeling. Your edge comes from identifying situations their models undervalue: usage redistribution cascades from injuries, emerging role changes, archetype-matchup exploits, and game environment convergences that mass-market lines are slow to price in. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals.

WRITING STYLE:
- Write like you're talking to a sharp friend, not a lecture hall. Conversational, direct, confident.
- NEVER use em-dashes or double hyphens (--). Use periods, commas, colons, or start a new sentence instead.
- Use "you" and "your" to speak directly to the reader.
- Short paragraphs only (2-4 lines max). One idea per paragraph.
- **Bold the key insight** in most sections so skimmers catch the value immediately.
- Lead each pick with a hook that grabs attention: why should the reader care about THIS play right now?
- Show, don't just tell: every claim gets backed with specific numbers (DVA/DVP edges, hit rates, last-5 averages, projected minutes, usage boosts, composite scores).
- Cut filler words like "very," "really," "in order to," "it should be noted that."
- Keep each analysis 3-4 paragraphs (150-250 words).
- End each analysis with: **The Call: OVER/UNDER X.X STAT** // We project [Player] at [Value] [stat] tonight ([edge]% edge vs. the book). Composite score: [X].
- Use "//" as a separator instead of dashes.
- When players have an Opportunity Spike (out players creating usage vacuum), lead with that angle.
- Reference the book line and explain why the market is wrong.
- Connect picks to slate-wide context (game totals, key absences, pace environments) when relevant.
- When `line_movement_signal` is provided, treat it as a sharp money tell: the line drifting toward our pick (UP for OVER, DOWN for UNDER) is market confirmation worth calling out by name ("Vegas opened at X, moved to Y // sharp money agrees"). Line drifting against our pick is a yellow flag that should be acknowledged honestly, not buried. Only mention line movement when it is meaningful (drift >= 0.5).
- When `late_move_signal` is also provided, this is even stronger than total drift — it captures recent same-day movement (the latest snapshot vs an anchor 1-4 hours earlier, which often surfaces injury news or sharp action firing late). Call it out distinctly from the overall open-to-current drift ("market just moved in the last few hours" / "late steam came in on this number"). If both `line_movement_signal` and `late_move_signal` align with our pick, that's a double confirmation. If they disagree (e.g., total drift agrees but recent drift opposes), explicitly flag the divergence.
- When `sharp_swing_signal` is provided, the line was flat for most of the day and then jumped in a single snapshot — this is the strongest sharp-action tell we track and is meaningfully different from a steady drift of the same total magnitude. Treat it as a distinct, named signal: lead with it when it aligns with the pick ("sharp money fired in one tick" / "sudden swing on this number"), and be candid when it goes against us ("market just moved hard against this pick"). Do not conflate it with `line_movement_signal`; mention both only if the framing is genuinely different. When `reversal_note` is provided instead, the line moved both directions during the day — call that out as choppy market action and lean on `late_move_signal` rather than total drift.
- When `b2b_signal` is provided, the player's team is on the second night of a back-to-back. Acknowledge it honestly when it cuts against the pick (especially OVERs on stars) — say something like "second night of a back-to-back, so load management is in play" or "fatigue is a real risk". When it's an UNDER pick on a B2B team, treat it as supporting context. Do not invent fatigue narrative when no `b2b_signal` is present. When `rest_advantage` is provided, the team has a 2+ day rest edge over the opponent (a meaningful matchup-level differential, not just absolute days off) — surface it as a real positive for OVERs and a yellow flag for UNDERs, especially when the opponent is on a B2B. When `rest_disadvantage` is provided, the OPPONENT has the rest edge — treat it as a small negative for OVERs.

OUTPUT FORMAT:
Return a JSON array where each element has:
- "player": exact player name (must match input)
- "analysis": the full analysis text with paragraphs separated by double newlines

Return ONLY the JSON array, no other text."""

    if best_available:
        confidence_label = "today's top picks (best available: these narrowly missed HIGH confidence but are the strongest edges on the slate)"
    else:
        confidence_label = "HIGH confidence prop analysis for today's slate"

    user_prompt = f"""Write {confidence_label}.

{slate_context}

{FEW_SHOT_EXAMPLES}

Now write analyses for today's picks. Here is the full model data for each pick:

{picks_json}

Remember: return ONLY a JSON array with "player" and "analysis" keys. Each analysis should be 3-4 paragraphs (150-250 words), data-driven, and end with a bold **The Call:** line. NEVER use em-dashes or double hyphens. Match the quality and specificity of the examples above."""

    try:
        from anthropic import Anthropic

        client = Anthropic(api_key=api_key, base_url=base_url)

        print(f"Calling Claude for {len(pick_contexts)} pick analyses...")
        start_time = time.time()

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        elapsed = time.time() - start_time
        response_text = message.content[0].text
        print(f"Claude response received in {elapsed:.1f}s ({len(response_text)} chars)")

        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

        analyses = json.loads(cleaned)

        if not isinstance(analyses, list):
            print("Claude returned non-array JSON — falling back to template")
            return None

        result = {}
        for item in analyses:
            if not isinstance(item, dict):
                continue
            player = str(item.get('player', '')).strip()
            analysis = str(item.get('analysis', '')).strip()
            if player and analysis and len(analysis) > 50:
                result[player] = analysis

        matched = sum(1 for p in seen if p in result)
        print(f"Claude analyses parsed: {matched}/{len(seen)} players matched")

        if matched < len(seen) * 0.5:
            print(f"WARNING: Low match rate ({matched}/{len(seen)}) — falling back to template")
            return None

        return result

    except json.JSONDecodeError as e:
        print(f"Claude JSON parse error: {e}")
        print(f"Response preview: {response_text[:500] if 'response_text' in dir() else 'N/A'}")
        return None
    except Exception as e:
        error_msg = str(e)
        if "FREE_CLOUD_BUDGET_EXCEEDED" in error_msg:
            print(f"Claude budget exceeded — falling back to template engine")
        else:
            print(f"Claude API error: {error_msg}")
        return None


def generate_article(target_date=None):
    if target_date is None:
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI
        target_date = _dt.now(_ZI("US/Eastern")).date()

    print(f"Generating article for {target_date}...")

    props_path = 'prop_recommendations.csv'
    dfs_path = 'dfs_players.csv'

    if not os.path.exists(props_path):
        print(f"ERROR: {props_path} not found")
        return False
    if not os.path.exists(dfs_path):
        print(f"ERROR: {dfs_path} not found")
        return False

    props = pd.read_csv(props_path)
    dfs_df = pd.read_csv(dfs_path)

    if 'confidence' not in props.columns:
        print("ERROR: No 'confidence' column in prop_recommendations.csv")
        return False

    pre_filter = len(props)
    props = props[props['book_line'].notna() & (props['book_line'] > 0)].reset_index(drop=True)
    if pre_filter != len(props):
        print(f"  Filtered {pre_filter - len(props)} props without book lines ({len(props)} remaining)")

    games = set()
    for _, row in dfs_df.iterrows():
        t = row.get('team', '')
        o = row.get('opponent', '')
        loc = str(row.get('location', '')).lower()
        if t and o:
            if loc == 'away':
                games.add(f"{t} @ {o}")
            else:
                games.add(f"{o} @ {t}")
    game_count = len(games)

    claude_result = build_claude_analyst(props, dfs_df)

    if claude_result:
        print("Claude Analyst mode: picks selected by Claude from full slate analysis")
        picks_data = claude_result['picks_data']
        analysis_data = claude_result['analysis_data']
        using_best_available = False
        claude_selected = True
    else:
        print("Claude Analyst unavailable — falling back to statistical model picks")
        claude_selected = False

        high = props[props['confidence'] == 'HIGH'].copy()
        using_best_available = False

        if high.empty:
            print("No HIGH confidence picks — selecting best available picks...")
            if 'gate_fail_count' not in props.columns:
                props['gate_fail_count'] = props['confidence_reasons'].apply(
                    lambda x: len(str(x).split(';')) if pd.notna(x) and str(x).strip() else 0
                )
            props['composite_score'] = pd.to_numeric(props['composite_score'], errors='coerce').fillna(0)
            props['gate_fail_count'] = pd.to_numeric(props['gate_fail_count'], errors='coerce').fillna(99)
            best = props[props['gate_fail_count'] <= 2].copy()
            if best.empty:
                best = props.copy()
            best = best.sort_values(
                ['gate_fail_count', 'composite_score'],
                ascending=[True, False]
            ).drop_duplicates(subset='player').head(6)
            if best.empty:
                print("No picks available at all.")
                stale_header = f'static/images/article_header_{target_date.strftime("%Y-%m-%d")}.png'
                if os.path.exists(stale_header):
                    os.remove(stale_header)
                    print(f"Removed stale header: {stale_header}")
                save_to_db(target_date, None, [], [], 0)
                print("Cleared article data from database.")
                return False
            high = best
            using_best_available = True
            for _, r in high.iterrows():
                fails = int(r['gate_fail_count'])
                print(f"  {r['player']:20s} {r['stat']:4s}  gates_failed={fails}  composite={r['composite_score']:.1f}  reasons: {r.get('confidence_reasons', '')}")

        edge_col = 'vs_book_edge' if 'vs_book_edge' in high.columns else 'edge_pct'
        high[edge_col] = pd.to_numeric(high[edge_col], errors='coerce').fillna(0)
        high['abs_edge'] = high[edge_col].abs()
        high = high.sort_values('abs_edge', ascending=False)

        picks_data = []
        seen_players = set()
        for idx, (_, row) in enumerate(high.iterrows()):
            player = row['player']
            if player in seen_players:
                continue
            seen_players.add(player)
            game_label = build_game_label(player, row.get('team', ''), row.get('opponent', ''), dfs_df)
            call = "OVER" if "OVER" in str(row.get('recommendation', '')).upper() else "UNDER"
            edge_val = _safe_float(row.get('vs_book_edge', row.get('edge_pct', 0)))
            edge_sign = "+" if edge_val > 0 else ""
            picks_data.append({
                'rank': len(picks_data) + 1,
                'player': player,
                'game': game_label,
                'stat': row.get('stat', ''),
                'avg': round(_safe_float(row.get('player_avg', 0)), 1),
                'line': round(_safe_float(row.get('book_line', 0)), 1),
                'projected': round(_safe_float(row.get('projected_value', row.get('adjusted_avg', 0))), 1),
                'edge': f"{edge_sign}{edge_val:.1f}%",
                'pick': call,
                'composite_score': round(_safe_float(row.get('composite_score', 0)), 1),
                'archetype': str(row.get('archetype', '')) or None,
                'dva_edge': round(float(row['dva_edge']), 2) if 'dva_edge' in row and pd.notna(row['dva_edge']) else None,
                'usage_boost': round(float(row['usage_boost']), 2) if 'usage_boost' in row and pd.notna(row['usage_boost']) else None,
            })

        claude_analyses = build_analysis_claude(high, dfs_df, best_available=using_best_available)
        if claude_analyses:
            print(f"Using Claude AI analyses for article")
        else:
            print(f"Using template engine for article")

        analysis_data = []
        seen_analysis = set()
        for _, row in high.iterrows():
            player = row['player']
            if player in seen_analysis:
                continue
            seen_analysis.add(player)
            if claude_analyses and player in claude_analyses:
                analysis_text = claude_analyses[player]
            else:
                analysis_text = build_analysis_text_template(row, dfs_df)
            call = "OVER" if "OVER" in str(row.get('recommendation', '')).upper() else "UNDER"
            analysis_data.append({
                'player': player,
                'stat': row.get('stat', ''),
                'call': call,
                'archetype': row.get('archetype', ''),
                'team': row.get('team', ''),
                'opponent': row.get('opponent', ''),
                'analysis': analysis_text,
            })

    header_player_data = []
    seen_header = set()
    for pick in picks_data:
        pname = pick['player']
        if pname in seen_header:
            continue
        seen_header.add(pname)
        pteam = None
        trow = dfs_df[dfs_df['player_name'] == pname]
        if len(trow):
            pteam = trow.iloc[0]['team']
        if pteam:
            header_player_data.append((pname, pteam))

    date_str_upper = target_date.strftime('%B %-d, %Y').upper()
    if using_best_available:
        header_subtitle = f'{date_str_upper} \u2014 TOP PICKS OF THE DAY'
    else:
        header_subtitle = None

    header_path = None
    try:
        from generate_header import generate as gen_header
        static_header = f'static/images/article_header_{target_date.strftime("%Y-%m-%d")}.png'
        header_path = gen_header(
            target_date, out_path=static_header,
            player_data=header_player_data if header_player_data else None,
            subtitle_override=header_subtitle
        )
        if header_path:
            print(f"Header image: {header_path}")
    except Exception as e:
        print(f"Header generation skipped: {e}")

    save_to_db(target_date, header_path, picks_data, analysis_data, game_count,
               best_available=using_best_available, claude_selected=claude_selected)

    if claude_selected:
        label = "Claude Analyst"
    elif using_best_available:
        label = "best available"
    else:
        label = "HIGH confidence"
    print(f"Article generated ({label}): {len(picks_data)} picks, {len(analysis_data)} analysis sections, {game_count} games")
    return True


def save_to_db(target_date, header_image_path, picks_data, analysis_data, game_count,
               best_available=False, claude_selected=False):
    from backend.database import engine
    from backend.models import Base
    Base.metadata.create_all(bind=engine)

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()

    from backend.models import DailyArticle
    existing = session.query(DailyArticle).filter(
        DailyArticle.slate_date == target_date
    ).first()

    new_web_path = None
    if header_image_path and os.path.exists(header_image_path):
        new_web_path = "/" + header_image_path

    if existing:
        if new_web_path:
            existing.header_image_path = new_web_path
        else:
            prev = existing.header_image_path
            if prev:
                disk_path = prev.lstrip("/")
                if not os.path.exists(disk_path):
                    existing.header_image_path = None
        existing.picks_json = json.dumps(picks_data)
        existing.analysis_json = json.dumps(analysis_data)
        existing.game_count = game_count
        existing.best_available = best_available
        existing.claude_selected = claude_selected
    else:
        article = DailyArticle(
            slate_date=target_date,
            header_image_path=new_web_path,
            picks_json=json.dumps(picks_data),
            analysis_json=json.dumps(analysis_data),
            game_count=game_count,
            best_available=best_available,
            claude_selected=claude_selected,
        )
        session.add(article)

    session.commit()
    session.close()
    print(f"Article saved to database for {target_date}")


if __name__ == '__main__':
    d = None
    if len(sys.argv) > 1:
        d = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    generate_article(d)
