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

from utils.season_phase import is_playoff_window_active


def _safe_float(val, default=0):
    try:
        v = float(val)
        if pd.isna(v):
            return default
        return v
    except (ValueError, TypeError):
        return default


def _get_slate_game_count(max_age_hours=36):
    """Return the number of games on tonight's slate from the game_odds table.

    This is the same odds source the rest of the site reads (game_odds_live),
    so the article's slate size stays consistent with the contest pages even
    when the downstream dfs_players.csv pipeline drops players whose opponent
    could not be resolved.

    Only rows scraped within `max_age_hours` of now are counted so a stale
    odds table left over from a prior slate cannot inflate today's count.
    Returns an int, or None if the table is missing, empty, stale, or cannot
    be read (caller falls back to the dfs-derived count).
    """
    import sqlite3
    try:
        conn = sqlite3.connect('dfs_nba.db')
        try:
            df = pd.read_sql(
                "SELECT away_team, home_team, scraped_at FROM game_odds "
                "WHERE away_team IS NOT NULL AND away_team != '' "
                "AND home_team IS NOT NULL AND home_team != ''",
                conn,
            )
        finally:
            conn.close()

        if df.empty:
            return None

        ts = pd.to_datetime(df['scraped_at'], errors='coerce')
        now = pd.Timestamp.utcnow().tz_localize(None)
        fresh = df[ts.notna() & ((now - ts).dt.total_seconds() <= max_age_hours * 3600)]
        if fresh.empty:
            newest = ts.max()
            print(f"  Slate game count: odds table is stale (newest scraped_at={newest}); using dfs-derived count.")
            return None

        pairs = {
            frozenset((str(r['away_team']).strip(), str(r['home_team']).strip()))
            for _, r in fresh.iterrows()
        }
        return len(pairs)
    except Exception as e:
        print(f"  Slate game count: odds table unavailable ({e}); using dfs-derived count.")
        return None


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
        cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        has_phase = 'season_type' in cols
        sel = f"SELECT game_date, matchup, {col}, min{', season_type' if has_phase else ''} " \
              f"FROM player_game_logs WHERE player_name = ? ORDER BY game_date DESC LIMIT ?"
        rows = conn.execute(sel, (player_name, n)).fetchall()

        playoff_seq = {}
        if has_phase:
            playoff_dates = conn.execute(
                "SELECT game_date FROM player_game_logs "
                "WHERE player_name = ? AND season_type = 'PLAYOFF' "
                "ORDER BY game_date ASC",
                (player_name,)
            ).fetchall()
            for idx, (gd,) in enumerate(playoff_dates, start=1):
                playoff_seq[gd] = idx
        conn.close()
        out = []
        for r in rows:
            phase = (r[4] if has_phase else 'REGULAR') or 'REGULAR'
            if phase == 'PLAYOFF':
                gnum = playoff_seq.get(r[0])
                tag = f'[PLAYOFF G{gnum}]' if gnum else '[PLAYOFF]'
            else:
                tag = '[REG]'
            mu = r[1] or ''
            out.append({
                'date': r[0],
                'matchup': f"{tag} {mu}".strip(),
                'val': r[2],
                'min': r[3],
                'phase': phase,
            })
        return out
    except Exception:
        return []


def _get_regular_season_min_avg(player_name, last_n=20):
    """Average minutes across the player's most recent N regular-season games."""
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        if 'season_type' in cols:
            rows = conn.execute(
                "SELECT min FROM player_game_logs "
                "WHERE player_name = ? AND season_type = 'REGULAR' "
                "ORDER BY game_date DESC LIMIT ?",
                (player_name, last_n)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT min FROM player_game_logs "
                "WHERE player_name = ? "
                "ORDER BY game_date DESC LIMIT ?",
                (player_name, last_n)
            ).fetchall()
        conn.close()
        vals = [r[0] for r in rows if r[0] is not None]
        if not vals:
            return None
        return round(sum(vals) / len(vals), 1)
    except Exception:
        return None


def _get_playoff_recent_games(player_name, stat, n=5):
    """Return only PLAYOFF games for the player, most recent first."""
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        col = STAT_LOG_COL.get(stat, stat.lower())
        cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        if 'season_type' not in cols:
            conn.close()
            return []
        rows = conn.execute(
            f"SELECT game_date, matchup, {col}, min FROM player_game_logs "
            f"WHERE player_name = ? AND season_type = 'PLAYOFF' "
            f"ORDER BY game_date DESC LIMIT ?",
            (player_name, n)
        ).fetchall()
        conn.close()
        return [{'date': r[0], 'matchup': r[1] or '', 'val': r[2], 'min': r[3]} for r in rows]
    except Exception:
        return []


def _get_matchup_history(player_name, opponent, game_date=None):
    """Returns matchup history.

    Base block: regular-season-only player-vs-team aggregate (fp_diff vs
    season baseline). The persisted matchup_history table now holds both
    REGULAR and PLAYOFF partitions; we explicitly select REGULAR so the
    season-baseline diff isn't polluted by a tiny playoff sample.

    Series block (only added when the slate is in the playoff window):
    playoff-only stats vs this opponent, so Claude reasons about the
    current series rather than stale December meetings.
    """
    import sqlite3
    out = None
    try:
        conn = sqlite3.connect("dfs_nba.db")
        mh_cols = [r[1] for r in conn.execute("PRAGMA table_info(matchup_history)").fetchall()]
        if 'season_type' in mh_cols:
            rows = conn.execute(
                "SELECT vs_fp_avg, season_fp_avg, fp_diff, matchup_score, games_vs "
                "FROM matchup_history WHERE player_name = ? AND opponent = ? "
                "AND season_type = 'REGULAR'",
                (player_name, opponent)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT vs_fp_avg, season_fp_avg, fp_diff, matchup_score, games_vs "
                "FROM matchup_history WHERE player_name = ? AND opponent = ?",
                (player_name, opponent)
            ).fetchall()
        if rows:
            out = {'vs_fp_avg': rows[0][0], 'season_fp_avg': rows[0][1],
                   'fp_diff': rows[0][2], 'score': rows[0][3], 'games': rows[0][4]}

        if is_playoff_window_active(game_date):
            from utils.season_phase import current_playoff_window_start
            series_start = current_playoff_window_start(game_date)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
            if 'season_type' in cols:
                if series_start is not None:
                    srows = conn.execute(
                        "SELECT pts, reb, ast, fp, min, game_date FROM player_game_logs "
                        "WHERE player_name = ? AND season_type = 'PLAYOFF' "
                        "AND (matchup LIKE ? OR matchup LIKE ?) "
                        "AND game_date >= ? "
                        "ORDER BY game_date DESC",
                        (player_name, f"% vs. {opponent}", f"% @ {opponent}", str(series_start))
                    ).fetchall()
                else:
                    srows = conn.execute(
                        "SELECT pts, reb, ast, fp, min, game_date FROM player_game_logs "
                        "WHERE player_name = ? AND season_type = 'PLAYOFF' "
                        "AND (matchup LIKE ? OR matchup LIKE ?) "
                        "ORDER BY game_date DESC",
                        (player_name, f"% vs. {opponent}", f"% @ {opponent}")
                    ).fetchall()
                if srows:
                    games = len(srows)
                    avg = lambda i: round(sum((r[i] or 0) for r in srows) / games, 1)
                    series_block = {
                        'games': games,
                        'pts_avg': avg(0),
                        'reb_avg': avg(1),
                        'ast_avg': avg(2),
                        'fp_avg': avg(3),
                        'min_avg': avg(4),
                        'last_min': srows[0][4],
                        'first_min': srows[-1][4],
                        'min_trend': round((srows[0][4] or 0) - (srows[-1][4] or 0), 1),
                    }
                    if out is None:
                        out = {}
                    out['series'] = series_block
        conn.close()
    except Exception:
        pass
    return out


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


def build_analysis_text_template(row, dfs_df, game_date=None):
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
    matchup_hist = _get_matchup_history(player, opponent, game_date=game_date)
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

    template_playoff_active = is_playoff_window_active(game_date)
    series_block = matchup_hist.get('series') if matchup_hist else None

    if template_playoff_active and series_block and series_block.get('games', 0) >= 2:
        s = series_block
        paragraphs.append(
            f"In this series so far ({s['games']} games), {last_name} is averaging "
            f"{s['fp_avg']:.1f} fantasy points on {s['min_avg']:.1f} minutes "
            f"(pts {s['pts_avg']:.1f} / reb {s['reb_avg']:.1f} / ast {s['ast_avg']:.1f}). "
            f"Minutes trend across the series: {s['min_trend']:+.1f}."
        )
    elif matchup_hist and matchup_hist.get('games', 0) >= 2 and not template_playoff_active:
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


def _build_pick_context(row, dfs_df, game_date=None):
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
    matchup_hist = _get_matchup_history(player, opponent, game_date=game_date)

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

**ZONE-LED EXAMPLE (use this structure when shot_diet, opp_def_zones, or zone_matchup_edges are populated):**

**ANTHONY EDWARDS // PTS OVER 26.5 (MIN @ HOU)**
Edwards takes 38% of his shots from three and HOU allows 38.4% from above-the-break (def rank 27/30 // leaky). Combine that with the 22% he takes at the rim where HOU gives up 67.1% (rank 24/30) and you have a shot diet that maps perfectly into Houston's two softest zones.

The model's shot-zone adjustment adds **+1.4 PTS** to his baseline before any usage or pace tailwind, which is how it lands at 28.9 projected. Houston's top defensive coverage is Pick & Roll Ball Handler at the 22nd percentile, which is where Edwards initiates most of his offense. Their pace is fast (101.9, +2.8 vs lg avg) so possessions aren't a constraint either.

**Hit rate of 61% and a last-5 of 28.4 confirm what the matchup already tells us.** FP projection of 49.2 sits well above his season pace of 45.8, so the entire stat line gets a tailwind, not just points.

**The Call: OVER 26.5 PTS** // We project Edwards at 28.9 points tonight (+9.1% edge vs. the book). Composite score: 68.4.
"""


def _get_playoff_summary(player_name, stat):
    """Aggregate stats for a player's playoff games this postseason."""
    import sqlite3
    try:
        conn = sqlite3.connect("dfs_nba.db")
        col = STAT_LOG_COL.get(stat, stat.lower())
        cols = [r[1] for r in conn.execute("PRAGMA table_info(player_game_logs)").fetchall()]
        if 'season_type' not in cols:
            conn.close()
            return None
        rows = conn.execute(
            f"SELECT {col}, min, game_date FROM player_game_logs "
            f"WHERE player_name = ? AND season_type = 'PLAYOFF' "
            f"ORDER BY game_date DESC",
            (player_name,)
        ).fetchall()
        conn.close()
        if not rows:
            return None
        vals = [r[0] for r in rows if r[0] is not None]
        mins = [r[1] for r in rows if r[1] is not None]
        if not vals:
            return None
        return {
            'games': len(rows),
            'avg': round(sum(vals) / len(vals), 1),
            'min_avg': round(sum(mins) / len(mins), 1) if mins else 0,
            'last_min': mins[0] if mins else 0,
            'last_val': vals[0] if vals else 0,
        }
    except Exception:
        return None


def _load_briefing_enrichment_caches():
    """Load shot zones, team defenses, play types, hustle, shot creation, measurements once.

    Returns a dict consumed by `_enrich_pick_blocks` to add scheme/zone context to Claude's
    briefing. Failures are logged but never raise — the briefing degrades gracefully.
    """
    import sqlite3
    cache = {
        'shot_zones': {}, 'team_def_zones': {}, 'shot_creation': {},
        'hustle': {}, 'play_types_off': {}, 'play_types_def': {},
        'measurements': {}, 'pace': {}, 'zone_def_ranks': {},
        'league_avg_pace': None,
    }
    try:
        conn = sqlite3.connect("dfs_nba.db")
        cur = conn.cursor()
        cols = [r[1] for r in cur.execute("PRAGMA table_info(player_shot_zones)")]
        if cols:
            for row in cur.execute("SELECT * FROM player_shot_zones"):
                d = dict(zip(cols, row))
                cache['shot_zones'][d.get('player_name')] = d
        cols = [r[1] for r in cur.execute("PRAGMA table_info(team_defense_shot_zones)")]
        if cols:
            for row in cur.execute("SELECT * FROM team_defense_shot_zones"):
                d = dict(zip(cols, row))
                cache['team_def_zones'][d.get('team')] = d
            for zone in ['ra_fg_pct', 'paint_fg_pct', 'mid_fg_pct', 'corner3_fg_pct', 'atb3_fg_pct']:
                pairs = [(t, d.get(zone)) for t, d in cache['team_def_zones'].items()
                         if d.get(zone) is not None and (d.get(zone) or 0) > 0]
                pairs.sort(key=lambda x: x[1])
                for rank, (t, _) in enumerate(pairs, start=1):
                    cache['zone_def_ranks'].setdefault(t, {})[zone] = rank
        cols = [r[1] for r in cur.execute("PRAGMA table_info(player_shot_creation)")]
        if cols:
            for row in cur.execute("SELECT * FROM player_shot_creation"):
                d = dict(zip(cols, row))
                cache['shot_creation'][d.get('player_name')] = d
        cols = [r[1] for r in cur.execute("PRAGMA table_info(player_hustle_stats)")]
        if cols:
            for row in cur.execute("SELECT * FROM player_hustle_stats"):
                d = dict(zip(cols, row))
                cache['hustle'][d.get('player_name')] = d
        team_abbrev_alias = {
            'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'PHO': 'PHX', 'SA': 'SAS',
            'GSW': 'GSW', 'NOP': 'NOP', 'NYK': 'NYK', 'PHX': 'PHX', 'SAS': 'SAS',
        }
        def _norm(t):
            if not t:
                return t
            return team_abbrev_alias.get(t, t)

        cols = [r[1] for r in cur.execute("PRAGMA table_info(team_play_types)")]
        if cols:
            for row in cur.execute("SELECT * FROM team_play_types"):
                d = dict(zip(cols, row))
                bucket = cache['play_types_off'] if d.get('type_grouping') == 'Offensive' else cache['play_types_def']
                bucket.setdefault(_norm(d.get('team')), []).append(d)
        try:
            paces = list(cur.execute("SELECT team, pace FROM team_pace"))
            for t, p in paces:
                cache['pace'][_norm(t)] = p
            if paces:
                cache['league_avg_pace'] = sum(p for _, p in paces) / len(paces)
        except Exception:
            pass
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(player_measurements)")]
            if cols:
                for row in cur.execute("SELECT * FROM player_measurements"):
                    d = dict(zip(cols, row))
                    cache['measurements'][d.get('player_name')] = d
        except Exception:
            pass
        conn.close()
    except Exception as e:
        print(f"[brief] enrichment cache load failed: {e}")
    return cache


def _zone_rank_label(rank):
    """Map team rank (1=best defense, 30=leakiest) to a human label."""
    if not rank:
        return None
    if rank <= 6:
        return "elite"
    if rank <= 12:
        return "good"
    if rank <= 18:
        return "avg"
    if rank <= 24:
        return "below-avg"
    return "leaky"


def _pace_label(team_pace, league_avg):
    if not team_pace or not league_avg:
        return None
    diff = team_pace - league_avg
    if diff >= 1.5:
        return f"fast ({team_pace:.1f}, +{diff:.1f} vs lg avg)"
    if diff <= -1.5:
        return f"slow ({team_pace:.1f}, {diff:.1f} vs lg avg)"
    return f"avg ({team_pace:.1f})"


def _enrich_pick_blocks(player, team, opponent, stat, caches, entry=None):
    """Build the enrichment blocks (shot_diet, opp_def_zones, etc.) for a single pick.

    Each block is added only when source data clears a minimum sample threshold,
    and is capped to its 2-3 most informative numbers to bound prompt size.
    `entry` is the partially-built prop_line, used to source redistribution context.
    Returns a dict of optional sub-blocks to merge into the prop_line entry.
    """
    out = {}
    _alias = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'PHO': 'PHX', 'SA': 'SAS'}
    nopp = _alias.get(opponent, opponent) if opponent else opponent
    pz = caches['shot_zones'].get(player)
    opp_def = caches['team_def_zones'].get(nopp) or caches['team_def_zones'].get(opponent)

    player_zones = []
    if pz and (pz.get('total_fga') or 0) >= 50:
        def _pct(fgm, fga):
            return round(100.0 * fgm / fga, 1) if fga else None
        zone_rows = [
            ('rim',   pz.get('ra_pct'),    _pct(pz.get('ra_fgm') or 0,    pz.get('ra_fga') or 0)),
            ('paint', pz.get('paint_pct'), _pct(pz.get('paint_fgm') or 0, pz.get('paint_fga') or 0)),
            ('mid',   pz.get('mid_pct'),   _pct(pz.get('mid_fgm') or 0,   pz.get('mid_fga') or 0)),
            ('three', pz.get('three_pct'), _pct(pz.get('three_fgm') or 0, pz.get('three_fga') or 0)),
        ]
        zone_rows = [(z, s, fg) for z, s, fg in zone_rows if s and s >= 8]
        zone_rows.sort(key=lambda r: -(r[1] or 0))
        player_zones = zone_rows[:3]
        if player_zones:
            out['shot_diet'] = {
                'season_fga': pz.get('total_fga'),
                'top_zones': [
                    {'zone': z, 'share_pct': round(s, 1), 'fg_pct': fg}
                    for z, s, fg in player_zones
                ],
            }

    if opp_def and player_zones:
        ranks = caches['zone_def_ranks'].get(nopp) or caches['zone_def_ranks'].get(opponent) or {}
        zone_to_def_key = {
            'rim':   'ra_fg_pct',
            'paint': 'paint_fg_pct',
            'mid':   'mid_fg_pct',
            'three': 'atb3_fg_pct',
        }
        relevant = []
        for z, _share, _fg in player_zones:
            key = zone_to_def_key.get(z)
            if not key:
                continue
            v = opp_def.get(key)
            if not v:
                continue
            r = ranks.get(key)
            relevant.append({
                'zone': z,
                'allowed_fg_pct': round(v, 1),
                'def_rank': f"{r}/30 ({_zone_rank_label(r)})" if r else None,
            })
        if relevant:
            out['opp_def_zones'] = relevant

    if pz and opp_def and (pz.get('total_fga') or 0) >= 50:
        league_avgs = {'ra': 65.0, 'paint': 42.0, 'mid': 41.0, 'three': 36.0}
        edges = []
        zone_map = [
            ('rim', 'ra_pct', 'ra_fg_pct', 'ra'),
            ('paint', 'paint_pct', 'paint_fg_pct', 'paint'),
            ('mid', 'mid_pct', 'mid_fg_pct', 'mid'),
            ('three', 'three_pct', 'atb3_fg_pct', 'three'),
        ]
        ranks = caches['zone_def_ranks'].get(nopp) or caches['zone_def_ranks'].get(opponent) or {}
        for label, share_k, opp_k, lavg_k in zone_map:
            share = pz.get(share_k) or 0
            opp_allowed = opp_def.get(opp_k) or 0
            lavg = league_avgs[lavg_k]
            if share < 8:
                continue
            diff = opp_allowed - lavg
            mag = abs(share * diff)
            if mag <= 0.5:
                continue
            rk = ranks.get(opp_k)
            tone = "easier" if diff > 0 else "tougher"
            base = (f"{label}: takes {share:.0f}% of shots, opp allows {opp_allowed:.1f}% "
                    f"(lg avg {lavg:.0f}")
            if rk:
                base += f", def rank {rk}/30 // {_zone_rank_label(rk)}"
            base += f" // {tone} than avg)"
            edges.append((mag, base))
        edges.sort(key=lambda e: -e[0])
        top = [t for _, t in edges[:2]]
        if top:
            out['zone_matchup_edges'] = top

    sc = caches['shot_creation'].get(player)
    if sc and (sc.get('total_fga') or 0) >= 50 and ((sc.get('cs_pct') or 0) + (sc.get('pu_pct') or 0)) > 0:
        sc_block = {
            'catch_shoot_pct': sc.get('cs_pct'),
            'pull_up_pct': sc.get('pu_pct'),
            'paint_pct': sc.get('paint_pct'),
            'cs_three_share_pct': sc.get('cs_3_share'),
            'pu_three_share_pct': sc.get('pu_3_share'),
        }
        sc_block = {k: v for k, v in sc_block.items() if v}
        if sc_block:
            out['shot_creation'] = sc_block

    if stat in ('STL', 'BLK', 'REB'):
        h = caches['hustle'].get(player)
        if h and (h.get('minutes') or 0) >= 100:
            hb = {
                'deflections_per48': h.get('deflections_per48'),
                'contested_shots_per48': h.get('contested_per48'),
                'contested_2pt_total': h.get('contested_2pt'),
                'box_outs_per48': h.get('box_outs_per48'),
                'screen_ast_per48': h.get('screen_ast_per48'),
            }
            hb = {k: v for k, v in hb.items() if v}
            if hb:
                out['hustle_signals'] = hb

    drivers = {}
    try:
        from analysis.player_value import _shot_zone_efficiency_adjustment, _physical_mismatch_score
        mini_cache = {
            'shot_zones': caches['shot_zones'],
            'team_def_zones': caches['team_def_zones'],
            'measurements': caches['measurements'],
        }
        if stat == 'PTS':
            sz_adj, sz_details = _shot_zone_efficiency_adjustment(player, nopp, mini_cache)
            if not sz_details and nopp != opponent:
                sz_adj, sz_details = _shot_zone_efficiency_adjustment(player, opponent, mini_cache)
            if abs(sz_adj) >= 0.1:
                drivers['shot_zone_adj_pts'] = sz_adj
                if sz_details:
                    top_break = sorted(sz_details.items(), key=lambda kv: -abs(kv[1]))[:3]
                    drivers['shot_zone_adj_breakdown'] = dict(top_break)
        if stat in ('REB', 'BLK'):
            meas = caches['measurements'].get(player)
            if meas and meas.get('position'):
                pm_score, pm_details = _physical_mismatch_score(player, nopp, meas['position'], mini_cache)
                if not pm_details and nopp != opponent:
                    pm_score, pm_details = _physical_mismatch_score(player, opponent, meas['position'], mini_cache)
                if pm_score is not None:
                    drivers['physical_mismatch_score'] = pm_score
                if pm_details:
                    drivers['physical_mismatch_details'] = pm_details
    except Exception:
        pass

    if entry is not None:
        usage_boost = entry.get('usage_boost') or 0
        vacated_usg = entry.get('total_vacated_usage') or 0
        vacated_min = entry.get('total_vacated_minutes') or 0
        if vacated_usg >= 5 or usage_boost >= 1.5:
            redis = {}
            if vacated_usg:
                redis['vacated_usage_pct'] = round(vacated_usg, 1)
            if vacated_min:
                redis['vacated_minutes'] = round(vacated_min, 1)
            if usage_boost:
                redis['player_usage_boost'] = round(usage_boost, 1)
            if vacated_usg > 0 and usage_boost > 0:
                redis['player_share_of_vacated_pct'] = round(100.0 * usage_boost / vacated_usg, 1)
            if redis:
                drivers['opportunity_redistribution'] = redis

    if drivers:
        out['projection_drivers'] = drivers

    return out


def _build_game_scheme_blocks(games_dict, caches):
    """Build per-game scheme blocks: off + def top play types, pace label, recent allowed-by-zone.

    Returns {game_key: scheme_dict_keyed_by_team}. Each team's block has at most
    ~12 numeric fields (3 off + 3 def play types compressed, pace, top-3 leakiest zones).
    """
    out = {}
    pt_off = caches['play_types_off']
    pt_def = caches['play_types_def']
    lg_pace = caches.get('league_avg_pace')
    tdz = caches['team_def_zones']
    ranks = caches['zone_def_ranks']
    _alias = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'PHO': 'PHX', 'SA': 'SAS'}
    def _nt(t):
        return _alias.get(t, t) if t else t

    def _pt_summary(rows, side):
        rows = sorted(rows, key=lambda d: -(d.get('poss_pct') or 0))[:3]
        return [
            {
                'play_type': d.get('play_type_label') or d.get('play_type'),
                'freq_pct': round((d.get('poss_pct') or 0) * 100, 1),
                'ppp': round(d.get('ppp') or 0, 2),
                ('def_percentile' if side == 'def' else 'off_percentile'):
                    round((d.get('percentile') or 0) * 100),
            }
            for d in rows
        ]

    def _team_block(team):
        block = {}
        nt = _nt(team)
        if nt in pt_off:
            block['top_off_play_types'] = _pt_summary(pt_off[nt], 'off')
        if nt in pt_def:
            block['top_def_play_types'] = _pt_summary(pt_def[nt], 'def')
        plabel = _pace_label(caches['pace'].get(nt), lg_pace)
        if plabel:
            block['pace'] = plabel
        td = tdz.get(nt) or tdz.get(team)
        if td:
            zone_keys = [('rim','ra_fg_pct'),('paint','paint_fg_pct'),
                         ('mid','mid_fg_pct'),('corner3','corner3_fg_pct'),
                         ('atb3','atb3_fg_pct')]
            zone_rows = []
            for label, key in zone_keys:
                v = td.get(key)
                r = ranks.get(nt, {}).get(key) or ranks.get(team, {}).get(key)
                if v and r:
                    zone_rows.append({'zone': label, 'allowed_fg_pct': round(v, 1),
                                       'def_rank': f"{r}/30 ({_zone_rank_label(r)})"})
            zone_rows.sort(key=lambda z: -int(z['def_rank'].split('/')[0]))
            if zone_rows:
                block['leakiest_zones_allowed'] = zone_rows[:3]
        return block

    for game_key, g in games_dict.items():
        teams = sorted(g.get('teams', set()))
        team_blocks = {}
        for t in teams:
            tb = _team_block(t)
            if tb:
                team_blocks[t] = tb
        if team_blocks:
            out[game_key] = team_blocks
    return out


def _build_full_slate_briefing(props_df, dfs_df, game_date=None):
    import sqlite3
    from utils.season_phase import is_playoff_window_active
    playoff_mode = is_playoff_window_active(game_date)
    enrichment = _load_briefing_enrichment_caches()

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

        _hr_un = row.get('hit_rate_unweighted')
        _cv_un = row.get('cv_unweighted')
        _l5_un = row.get('last5_avg_unweighted')
        _po_n = row.get('playoff_n')
        _po_w = row.get('playoff_weight_applied')
        conf_unweighted_hit_rate = round(_safe_float(_hr_un), 1) if _hr_un is not None and not pd.isna(_hr_un) else None
        conf_unweighted_cv = round(_safe_float(_cv_un), 3) if _cv_un is not None and not pd.isna(_cv_un) else None
        conf_unweighted_last5 = round(_safe_float(_l5_un), 1) if _l5_un is not None and not pd.isna(_l5_un) else None
        conf_playoff_n = int(_safe_float(_po_n)) if _po_n is not None and not pd.isna(_po_n) else None
        if _po_w is None or (hasattr(pd, 'isna') and pd.isna(_po_w)):
            conf_weight_applied = None
        elif isinstance(_po_w, bool):
            conf_weight_applied = _po_w
        elif isinstance(_po_w, (int, float)):
            conf_weight_applied = bool(_po_w)
        else:
            conf_weight_applied = str(_po_w).strip().lower() in ('true', '1', 'yes', 't')

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

        if len(dfs_row):
            drow = dfs_row.iloc[0]
            fp_block = {
                'fp_per_min': round(_safe_float(drow.get('fp_per_min', 0)), 2),
                'season_fp_pg': round(_safe_float(drow.get('fp_pg', 0)), 1),
                'projected_fp_tonight': round(_safe_float(drow.get('proj_fp', 0)), 1),
                'projected_fp_ceiling': round(_safe_float(drow.get('ceiling', 0)), 1),
                'projected_fp_floor': round(_safe_float(drow.get('floor', 0)), 1),
            }
            fp_block = {k: v for k, v in fp_block.items() if v}
            if 'projected_fp_tonight' in fp_block:
                fp_block['fp_proj'] = fp_block['projected_fp_tonight']
            if fp_block:
                entry['fp_context'] = fp_block

        try:
            enrich_blocks = _enrich_pick_blocks(player, team, opponent, stat, enrichment, entry=entry)
            for k, v in enrich_blocks.items():
                entry[k] = v
        except Exception as _enr_err:
            print(f"[brief] enrichment for {player}/{stat}: {_enr_err}")

        recent = _get_recent_games(player, stat)
        if recent:
            entry['recent_games'] = [
                {'date': g['date'], 'vs': g['matchup'], 'val': g['val'], 'min': g['min']}
                for g in recent[:5]
            ]
            playoff_in_recent = sum(1 for g in recent if g.get('phase') == 'PLAYOFF')
            if playoff_in_recent:
                entry['recent_includes_playoff'] = playoff_in_recent

        if playoff_mode:
            po_summary = _get_playoff_summary(player, stat)
            if po_summary:
                entry['playoff_avg'] = po_summary['avg']
                entry['playoff_games_count'] = po_summary['games']
                entry['playoff_min_avg'] = po_summary['min_avg']
                entry['playoff_last_min'] = po_summary['last_min']
                entry['playoff_last_val'] = po_summary['last_val']
                po_recent = _get_playoff_recent_games(player, stat, n=8)
                if po_recent:
                    entry['playoff_game_log'] = [
                        {'date': g['date'], 'vs': g['matchup'], 'val': g['val'], 'min': g['min']}
                        for g in po_recent
                    ]

        matchup = _get_matchup_history(player, opponent, game_date=game_date)
        if matchup and matchup.get('games', 0) >= 1:
            entry['h2h'] = {
                'fp_diff': round(matchup.get('fp_diff', 0), 1),
                'games': matchup.get('games', 0),
                'note': 'regular_season_only',
            }
        if matchup and matchup.get('series'):
            s = matchup['series']
            entry['series_avg_vs_opponent'] = {
                'games': s['games'],
                'pts_avg': s['pts_avg'],
                'reb_avg': s['reb_avg'],
                'ast_avg': s['ast_avg'],
                'fp_avg': s['fp_avg'],
                'min_avg': s['min_avg'],
            }
            entry['series_minutes_trend'] = s['min_trend']

            season_min_avg = _get_regular_season_min_avg(player)
            if season_min_avg is not None:
                role_delta = round(s['min_avg'] - season_min_avg, 1)
                if role_delta >= 5:
                    role_label = 'expanded'
                elif role_delta <= -5:
                    role_label = 'reduced'
                else:
                    role_label = 'stable'
                entry['series_role_change'] = {
                    'season_min_avg': season_min_avg,
                    'series_min_avg': s['min_avg'],
                    'delta': role_delta,
                    'label': role_label,
                    'role_shifted': role_label != 'stable',
                }

        if playoff_mode:
            entry['hit_rate_unweighted'] = (
                conf_unweighted_hit_rate
                if conf_unweighted_hit_rate is not None
                else entry['hit_rate']
            )
            entry['cv_unweighted'] = (
                conf_unweighted_cv
                if conf_unweighted_cv is not None
                else entry['cv']
            )
            entry['last5_avg_unweighted'] = (
                conf_unweighted_last5
                if conf_unweighted_last5 is not None
                else entry['last5_avg']
            )
            entry['playoff_n'] = (
                conf_playoff_n
                if conf_playoff_n is not None
                else int(entry.get('playoff_games_count', 0) or 0)
            )
            entry['playoff_weight_applied'] = bool(
                conf_weight_applied
                if _po_w is not None and not pd.isna(_po_w)
                else (entry['playoff_n'] >= 3)
            )

        prop_lines.append(entry)

    try:
        scheme_blocks = _build_game_scheme_blocks(games, enrichment)
    except Exception as _sb_err:
        print(f"[brief] game scheme blocks failed: {_sb_err}")
        scheme_blocks = {}

    games_payload = []
    for k, v in sorted(game_env.items()):
        gb = {'game': k, 'implied_total': v}
        if k in scheme_blocks:
            gb['scheme'] = scheme_blocks[k]
        games_payload.append(gb)

    briefing = {
        'playoff_mode': bool(playoff_mode),
        'game_count': len(games),
        'games': games_payload,
        'key_absences': key_absences[:20],
        'prop_lines': prop_lines,
    }
    return briefing


CLAUDE_ANALYST_PATTERNS = """
ANALYTICAL FRAMEWORK (in priority order // lead each analysis with the highest-priority signal that fires for that pick):

1. SHOT-DIET vs OPPONENT-DEFENSE-BY-ZONE (HEADLINE SIGNAL when present): If the pick has `zone_matchup_edges` or `shot_diet` + `opp_def_zones`, lead with it. Cite the player's zone share and the opponent's allowed FG% and rank in that zone (e.g. "takes 36% of shots from mid // MIA allows 49.1% there // def rank 28/30 // leaky"). This is the single most distinguishing piece of context per pick — never bury it.

2. PROJECTION DRIVERS (the math behind the projection): When `projection_drivers` is present, cite it. `shot_zone_adj_pts` is the points the model has added or subtracted from the player's baseline based on shot-zone × opponent-defense alignment. `physical_mismatch_score` (REB/BLK only) shows the player's height/weight/wingspan edge over a positional baseline. Use these to explain WHY the model arrived at `projected_value`.

3. OPPONENT SCHEME (`games[i].scheme[<opp>].top_def_play_types` + `pace`): Find your player's game in the `games` array, then read the opponent team's scheme block. For passers/PnR ball-handlers, look for opponents who are weak vs Pick & Roll Ball Handler or Iso. For spot-up shooters, look for weak vs Spot Up. `def_percentile` is on a 0-100 scale where higher = better defense (so LOW percentile in the play types your player runs = juicy). Cross-reference the player's own team `top_off_play_types` to confirm they actually run the coverage you are exploiting.

4. SHOT-CREATION FIT (`shot_creation`): Catch-and-shoot heavy players need sets and screens — verify their team has a creator on the floor. Pull-up heavy players are scheme-proof but capped by minutes. Match this against the OUT players to see if their creator is on or off.

5. HUSTLE SIGNALS (STL/BLK/REB only, in `hustle_signals`): `deflections_per48` >= 3.5 is elite for STL OVERs. `contested_shots_per48` >= 8 is elite for BLK OVERs. `box_outs_per48` supports defensive REB OVERs.

6. FANTASY-POINT SANITY CHECK (`fp_context`): If `projected_fp_tonight` is near or below `season_fp_pg`, be skeptical of any aggressive stat-line OVER. If projected is well above season, the entire stat line gets a tailwind.

7. USAGE REDISTRIBUTION FROM STAR ABSENCES: When high-usage stars are OUT (`out_players` populated, `total_vacated_usage` > 30, `opportunity_spike=true`), this is the highest-edge category in DFS. Cite specifically WHICH stars are out and HOW that funnels to your player.

8. DVP/DVA DOUBLE ALIGNMENT: The strongest picks have BOTH Defense vs Position (dvp_edge) AND Defense vs Archetype (dva_edge) supporting the direction. Both >+0.5 for OVER = strong signal. If both are negative, avoid.

9. GAME ENVIRONMENT: High implied team totals (>115) and game totals (235+) amplify scoring picks. `b2b_signal` flags fatigue. `rest_advantage`/`rest_disadvantage` shows rest mismatches.

10. RECENT FORM AND HIT RATE (CONFIRMING signals, NOT headlines): `last5_avg` and `hit_rate` are confirmation, not the lede. Two analyses leading with "He hit it 64% of the time and last 5 is X" are indistinguishable. Use these to confirm your zone/scheme thesis, not to replace it.

11. CONTRARIAN DISCIPLINE: Limit UNDER picks to 1-2 max per slate. NEVER take UNDER when usage_boost > 3.0. NEVER take UNDER when zone_matchup_edges and shot_zone_adj_pts both lean OVER.

12. CONSISTENCY CHECK: CV below 0.30 = very consistent. Above 0.50 = volatile.

QUALITY GATES (reference, not hard constraints):
- Hit rate >= 58%
- CV <= 0.30 (tight consistency)
- Last-5 clears book line in pick direction
- At least one of DVA/DVP > 0.5 magnitude supporting direction
- Implied team total > 105 for PTS/AST OVER picks
- Block UNDER if usage_boost > 3.0
"""


def build_claude_analyst(props_df, dfs_df, game_date=None):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude API not configured — cannot run Claude Analyst mode")
        return None

    briefing = _build_full_slate_briefing(props_df, dfs_df, game_date=game_date)
    briefing_json = json.dumps(briefing, indent=2, default=str)
    n_props = len(briefing['prop_lines'])
    avg_chars = (len(briefing_json) // n_props) if n_props else 0
    enrich_keys = ('shot_diet', 'opp_def_zones', 'zone_matchup_edges',
                   'shot_creation', 'hustle_signals',
                   'projection_drivers', 'fp_context')
    enrich_counts = {k: sum(1 for p in briefing['prop_lines'] if k in p) for k in enrich_keys}
    games_with_scheme = sum(1 for g in briefing.get('games', []) if 'scheme' in g)
    print(f"Claude Analyst briefing: {n_props} prop lines across {briefing['game_count']} games "
          f"({len(briefing_json)} chars, ~{avg_chars}/pick)")
    print(f"  per-pick enrichment coverage: {enrich_counts}")
    print(f"  game-level scheme coverage: {games_with_scheme}/{briefing['game_count']} games")

    playoff_mode_active = bool(briefing.get('playoff_mode'))
    playoff_mode_block = """

PLAYOFF MODE ACTIVE
The slate is in the NBA Play-In/Playoffs window. Every prop entry includes playoff-specific fields when available:
- `playoff_avg`, `playoff_games_count`, `playoff_min_avg`, `playoff_last_min`, `playoff_last_val`: this player's stats in this postseason only.
- `playoff_game_log`: the full chronological postseason log for this stat.
- `series_avg_vs_opponent`: pts_avg, reb_avg, ast_avg, fp_avg, min_avg across THIS series only.
- `series_minutes_trend`: numeric delta of minutes from first series game to most recent (positive = trending up).
- `series_role_change`: {season_min_avg, series_min_avg, delta, label} — label is 'expanded'/'reduced'/'stable'. Use to flag rotation shifts.
- `hit_rate_unweighted` / `cv_unweighted`: same metrics computed against regular-season-only data, for comparison with playoff-weighted `hit_rate` / `cv`. Divergence between the two signals a regime change.
- `recent_games[].vs` is prefixed with `[PLAYOFF G{n}]` for playoff games (n = chronological playoff game number for the player) and `[REG]` for regular-season games.
- `recent_includes_playoff` shows how many of the last 5 are playoff games.

Reasoning rules in playoff mode:
1. Postseason rotations tighten dramatically. A player averaging 22 MPG in the regular season but logging 9-12 in the series IS the new baseline. Trust the playoff minutes, not the season MPG.
2. Series-level data (`series_avg_vs_opponent`) overrides any regular-season `h2h` data when ≥2 series games exist. The `h2h.note: regular_season_only` flag is your reminder that head-to-head numbers from December are often irrelevant.
3. Cite playoff games explicitly. If you reference a recent stat line, prefer one tagged `[PLAYOFF G{n}]`. Do not cite a regular-season game (`[REG]`) against a non-current opponent to justify a playoff prop.
4. When `playoff_games_count` >= 3 and `playoff_avg` clearly diverges from `player_avg` or `last5_avg`, weight `playoff_avg` more heavily. Compare `hit_rate` (playoff-weighted) vs `hit_rate_unweighted` (regular-only): meaningful divergence is a regime signal.
5. Watch `series_minutes_trend`: a positive value (minutes climbing across the series) supports OVERs on volume; a negative value supports UNDERs. `series_role_change.label` ('expanded' vs 'reduced') is the cleaner version of this signal.
6. Single-elimination Play-In games carry full intensity — treat their data the same as Playoff games.
"""

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

CONTEXT BLOCKS YOU WILL SEE (use these to make every analysis distinctive — DO NOT default to hit_rate/last5_avg as the headline):

GAME-LEVEL (`games[i].scheme[<team>]` // applies to every player on that team in that game):
- `top_off_play_types` and `top_def_play_types`: the team's top 3 offensive and top 3 defensive coverages by frequency. Each entry has `play_type` (e.g. "Pick & Roll Ball Handler", "Spot Up", "Isolation"), `freq_pct` (% of possessions), `ppp` (points per possession), and a percentile on a 0-100 scale where 100 = league-best. For DEF entries, LOW `def_percentile` (< 30) means leaky in that coverage. For OFF entries, HIGH `off_percentile` means a strong offensive identity worth respecting.
- `pace`: team pace labelled fast/avg/slow vs league average.
- `leakiest_zones_allowed`: the opponent's 3 most-exploitable zones with allowed FG% and "X/30 (label)" rank.

PER-PICK:
- `shot_diet`: `season_fga` plus `top_zones`, the player's 3 most-used shooting zones with `share_pct` and `fg_pct`. Zones below 8% share are dropped as immaterial.
- `opp_def_zones`: only the opponent's allowed FG% + "X/30 (label)" rank in the zones THIS PLAYER actually shoots from. RANK 1 = best defense, RANK 30 = leakiest. Higher rank in your player's zones = juicy.
- `zone_matchup_edges`: pre-computed top 1-2 mismatches as one-line strings. When present, this is the headline of your analysis.
- `projection_drivers`:
    * `shot_zone_adj_pts` + `shot_zone_adj_breakdown` (PTS only): the points the model added or subtracted from the player's baseline due to shot-zone × opponent-defense, with the top-3 contributing zones.
    * `physical_mismatch_score` + `physical_mismatch_details` (REB/BLK only): height/weight/wingspan deltas vs the positional baseline.
    * `opportunity_redistribution` (when usage is being vacated): `vacated_usage_pct`, `vacated_minutes`, `player_usage_boost`, `player_share_of_vacated_pct` (what % of the freed-up usage funnels to THIS player).
- `shot_creation`: catch_shoot_pct, pull_up_pct, paint_pct. Cross-check with `out_players` to see if the player's primary creator is OUT.
- `hustle_signals` (STL/BLK/REB only): deflections_per48, contested_shots_per48, contested_2pt_total, box_outs_per48, screen_ast_per48.
- `fp_context`: season fantasy points per game, fp_per_min, projected fp tonight (with ceiling and floor). If `projected_fp_tonight` clears `season_fp_pg` by 3+, the entire stat line gets a tailwind. If it does not, be more conservative on aggressive OVERs.

WRITING DIRECTIVE: Lead each analysis with a zone, scheme, projection-driver, or usage-redistribution cue when it is present. Hit rate and last-5 average are CONFIRMATION, not the headline. Two analyses that both lead with "Hit rate of X%, last 5 is Y" are a failure of differentiation.

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
   - "stat": stat type (MUST match the stat of the corresponding pick)
   - "call": "OVER" or "UNDER" (MUST match the pick side)
   - "archetype": player archetype
   - "team": team
   - "opponent": opponent
   - "analysis": the full analysis text (3-4 paragraphs, 150-250 words, ending with **The Call:** line)

CRITICAL MATCHING RULES (failing these breaks the published article):
- The "analyses" array MUST have exactly one entry per pick in the "picks" array. Same length, same order.
- Each analysis is uniquely identified by the tuple (player, stat, call). If you write two picks for the same player on different stats (e.g., DeAndre Ayton OVER PTS and DeAndre Ayton UNDER REB), you MUST write two distinct analysis entries with the matching stat and call values, AND each analysis text must argue the correct side. Never reuse the same analysis text for two different picks.
- Never write an UNDER analysis for an OVER pick (or vice versa). The "call" field in each analysis must match the "pick" field of the corresponding pick exactly.

Return ONLY the JSON object, no other text. Order picks by edge strength (strongest first)."""

    if playoff_mode_active:
        system_prompt += playoff_mode_block

    playoff_user_reminder = ""
    if playoff_mode_active:
        playoff_user_reminder = (
            "- PLAYOFF MODE: prefer playoff_avg / series_avg_vs_opponent / playoff_game_log over "
            "regular-season averages. Cite [PLAYOFF G{n}]-tagged games when justifying picks. Do not lean on "
            "regular-season h2h vs the current opponent (see h2h.note: regular_season_only). "
            "If hit_rate (playoff-weighted) and hit_rate_unweighted disagree, treat the playoff-weighted value as the truer signal.\n"
        )

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
{playoff_user_reminder}- Return ONLY a JSON object with "picks" and "analyses" keys"""

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
        analysis_by_player_only = {}
        for item in analyses:
            if isinstance(item, dict):
                player = str(item.get('player', '')).strip()
                stat = str(item.get('stat', '')).strip().upper()
                call = str(item.get('call', '')).strip().upper()
                analysis = str(item.get('analysis', '')).strip()
                if player and analysis and len(analysis) > 50:
                    full_key = (player, stat, call)
                    if full_key in analysis_map:
                        existing = analysis_map[full_key].get('analysis', '')
                        if len(analysis) > len(existing):
                            analysis_map[full_key] = item
                        print(f"  WARN: duplicate Claude analysis for {player} {stat} {call} // kept the longer one")
                    else:
                        analysis_map[full_key] = item
                    analysis_by_player_only.setdefault(player, []).append(item)

        slate_lookup = {}
        for _, row in props_df.iterrows():
            key = (str(row['player']).strip(), str(row['stat']).strip())
            if key not in slate_lookup:
                slate_lookup[key] = row

        def _resolve_analysis(player, stat, call):
            full_key = (player, stat.upper(), call.upper())
            if full_key in analysis_map:
                return analysis_map[full_key]
            candidates = analysis_by_player_only.get(player, [])
            stat_matches = [
                c for c in candidates
                if str(c.get('stat', '')).strip().upper() == stat.upper()
            ]
            if len(stat_matches) == 1:
                return stat_matches[0]
            if len(candidates) == 1:
                return candidates[0]
            return None

        valid_picks = []
        seen_stat_keys = set()
        for p in picks:
            if not isinstance(p, dict):
                continue
            player = str(p.get('player', '')).strip()
            stat = str(p.get('stat', '')).strip()
            call = str(p.get('pick', 'OVER')).strip().upper()
            if not player or not stat:
                continue
            stat_key = (player, stat)
            if stat_key in seen_stat_keys:
                print(f"  Skipping contradictory or duplicate pick for {player} {stat} (already have one side)")
                continue
            resolved = _resolve_analysis(player, stat, call)
            if resolved is None:
                print(f"  Skipping {player} {stat} {call}: no matching analysis (player has "
                      f"{len(analysis_by_player_only.get(player, []))} analyses, none on this stat+side)")
                continue
            slate_key = (player, stat)
            if slate_key not in slate_lookup:
                print(f"  Skipping {player} {stat}: not found in slate data (hallucinated?)")
                continue
            resolved_call = str(resolved.get('call', '')).strip().upper()
            if resolved_call and resolved_call != call:
                print(f"  Skipping {player} {stat} {call}: matched analysis argues {resolved_call}, refusing to publish a contradictory article")
                continue
            seen_stat_keys.add(stat_key)
            valid_picks.append((p, slate_lookup[slate_key], resolved))

        print(f"Claude Analyst: {len(picks)} raw picks, {len(valid_picks)} validated against slate")

        if len(valid_picks) < 3:
            print("Claude Analyst too few validated picks — falling back")
            return None

        if len(valid_picks) > 8:
            valid_picks = valid_picks[:8]
            print(f"  Trimmed to 8 picks")

        picks_data = []
        for idx, (claude_pick, source_row, _resolved) in enumerate(valid_picks):
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
        seen_analysis_text = {}
        for claude_pick, source_row, resolved in valid_picks:
            player = str(claude_pick.get('player', '')).strip()
            call = str(claude_pick.get('pick', 'OVER')).upper()
            stat = str(source_row.get('stat', ''))
            analysis_text = resolved.get('analysis', '')
            text_fingerprint = analysis_text.strip()[:200]
            if text_fingerprint and text_fingerprint in seen_analysis_text:
                prior_player, prior_stat, prior_call = seen_analysis_text[text_fingerprint]
                print(f"  WARN: Claude reused identical analysis text for {player} {stat} {call} "
                      f"(also used for {prior_player} {prior_stat} {prior_call}) // article may be misleading")
            else:
                seen_analysis_text[text_fingerprint] = (player, stat, call)
            analysis_data.append({
                'player': player,
                'stat': stat,
                'call': call,
                'archetype': str(source_row.get('archetype', '')),
                'team': str(source_row.get('team', '')),
                'opponent': str(source_row.get('opponent', '')),
                'analysis': analysis_text,
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


def build_analysis_claude(high_rows, dfs_df, best_available=False, game_date=None):
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
        pick_contexts.append(_build_pick_context(row, dfs_df, game_date=game_date))

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

    narrator_playoff_active = is_playoff_window_active(game_date)
    if narrator_playoff_active:
        system_prompt += """

PLAYOFF MODE ACTIVE
The slate is in the NBA Play-In/Playoffs window. Each pick context includes playoff-specific fields when available:
- `playoff_avg`, `playoff_games_count`, `playoff_min_avg`, `playoff_last_min`, `playoff_last_val`: this player's stats in this postseason only.
- `playoff_game_log`: the full chronological postseason log for this stat.
- `series_avg_vs_opponent`: pts_avg, reb_avg, ast_avg, fp_avg, min_avg across THIS series only.
- `series_minutes_trend`: numeric delta of minutes from first series game to most recent.
- `series_role_change`: {season_min_avg, series_min_avg, delta, label='expanded'/'reduced'/'stable'}.
- `recent_games[].vs` is prefixed `[PLAYOFF G{n}]` (chronological playoff game number) or `[REG]`.
- `h2h.note: regular_season_only` is your reminder that any h2h line is December data, not the current series.

Playoff narrative rules:
1. Cite playoff games explicitly. Never use a regular-season ([REG]) line against a non-current opponent to justify a playoff prop.
2. When `series_role_change.label` is 'reduced' or 'expanded', lead with the rotation shift, not the season averages.
3. Trust playoff minutes (`playoff_min_avg`, `playoff_last_min`) over regular-season MPG when describing volume expectations.
4. Series-level data (`series_avg_vs_opponent`) overrides regular-season h2h once ≥2 series games exist; do not write narratives like "he torched them in November" when a fresh series sample disagrees.
5. Single-elimination Play-In games carry full playoff intensity — treat them the same as Playoff games."""

    if best_available:
        confidence_label = "today's top picks (best available: these narrowly missed HIGH confidence but are the strongest edges on the slate)"
    else:
        confidence_label = "HIGH confidence prop analysis for today's slate"

    playoff_user_reminder = ""
    if narrator_playoff_active:
        playoff_user_reminder = (
            "\nPLAYOFF MODE: prefer playoff_avg / series_avg_vs_opponent / playoff_game_log over "
            "regular-season averages. Cite [PLAYOFF G{n}]-tagged games when justifying picks. Do not lean on "
            "regular-season h2h vs the current opponent (see h2h.note: regular_season_only).\n"
        )

    user_prompt = f"""Write {confidence_label}.

{slate_context}
{playoff_user_reminder}
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

    # Authoritative slate size comes from the game_odds table (the same
    # source the rest of the site reads via game_odds_live). dfs_df drops
    # every player whose opponent could not be resolved, so on nights when
    # the odds scrape lags behind the salary scrape the dfs-derived count
    # collapses to 0 even though there are clearly games on the slate. Use
    # the odds table as the source of truth and only fall back to the
    # dfs-derived count if the odds table is unavailable/empty.
    slate_game_count = _get_slate_game_count()
    if slate_game_count is not None and slate_game_count > game_count:
        print(f"  Slate game count: using odds table ({slate_game_count}) over dfs-derived ({game_count}).")
        game_count = slate_game_count

    claude_result = build_claude_analyst(props, dfs_df, game_date=target_date)

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
                print(f"No picks available — saving placeholder article for {game_count} game(s) on slate.")
                fallback_header = f'static/images/article_header_{target_date.strftime("%Y-%m-%d")}.png'
                try:
                    from generate_header import generate as gen_header
                    gen_header(target_date, out_path=fallback_header, player_data=[])
                except Exception as e:
                    print(f"Fallback header generation failed: {e}")
                header_arg = fallback_header if os.path.exists(fallback_header) else None
                save_to_db(target_date, header_arg, [], [], game_count)
                print(f"Saved placeholder article: {game_count} game(s), 0 picks, header={'yes' if header_arg else 'no'}.")
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

        claude_analyses = build_analysis_claude(high, dfs_df, best_available=using_best_available, game_date=target_date)
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
                analysis_text = build_analysis_text_template(row, dfs_df, game_date=target_date)
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
    try:
        _props_lookup = pd.read_csv('prop_recommendations.csv')
    except Exception:
        _props_lookup = None
    for pick in picks_data:
        pname = pick['player']
        if pname in seen_header:
            continue
        seen_header.add(pname)
        pteam = pick.get('team')
        if not pteam and dfs_df is not None and 'player_name' in dfs_df.columns:
            trow = dfs_df[dfs_df['player_name'] == pname]
            if len(trow):
                pteam = trow.iloc[0]['team']
        if not pteam and _props_lookup is not None and 'player' in _props_lookup.columns:
            prow = _props_lookup[_props_lookup['player'] == pname]
            if len(prow):
                pteam = prow.iloc[0].get('team')
        if not pteam:
            game = str(pick.get('game') or '')
            if '@' in game:
                halves = [h.strip() for h in game.split('@')]
                if halves:
                    pteam = halves[-1]
        if pteam:
            header_player_data.append({
                'player': pname,
                'team': pteam,
                'stat': pick.get('stat'),
                'side': pick.get('pick') or pick.get('side'),
                'line': pick.get('line') or pick.get('book_line'),
                'edge': pick.get('edge'),
            })

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


_OFFICIAL_LOCK_WINDOW_MINUTES = 60
# Grace floor: how many minutes AFTER first tip we still allow the first lock to
# fire. This handles the realistic "wave started at T-5 but DB write landed at
# T+5" race, while refusing a stale 9 PM regen from suddenly being recorded as
# the "official call" for a 6 PM tipoff. If the slate is never locked within
# this window, the grader cleanly falls back to picks_json (legacy path).
_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES = 30
_DFS_DB_PATH = "/home/runner/workspace/dfs_nba.db"


def _first_tipoff_today_et(target_date):
    """Return the earliest tipoff datetime in ET for `target_date`, or None.

    Uses the same `player_salaries.game_time` source as `scheduler_pregame.py`
    so the lock window matches when the pregame waves actually fire (T-60 from
    the first tip). Returns None on any failure or when no game times exist //
    callers must treat that as "no lock yet".
    """
    try:
        import sqlite3
        from zoneinfo import ZoneInfo as _ZI
        et_zone = _ZI("America/New_York")
        if not os.path.exists(_DFS_DB_PATH):
            return None
        conn = sqlite3.connect(_DFS_DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT game_time FROM player_salaries WHERE game_time IS NOT NULL"
        )
        raw = [row[0] for row in cur.fetchall()]
        conn.close()
        parsed = []
        for gt in raw:
            try:
                t = datetime.strptime(gt, "%I:%M%p")
                naive = t.replace(year=target_date.year, month=target_date.month, day=target_date.day)
                parsed.append(naive.replace(tzinfo=et_zone))
            except Exception:
                continue
        if not parsed:
            return None
        return min(parsed)
    except Exception:
        return None


def _should_lock_official_call(target_date, now_et):
    """Return True iff we're inside the slate's lock window.

    Window: from T-LOCK_WINDOW_MINUTES (60 min before first tip) through
    T+GRACE_AFTER_TIP_MINUTES (30 min after first tip). The lock is one-shot
    (caller checks official_picks_json is NULL first). If the window closes
    without a lock firing, the grader cleanly falls back to picks_json so
    we never silently record a stale post-tip regen as the "official call".
    """
    first_tip = _first_tipoff_today_et(target_date)
    if first_tip is None:
        return False
    delta_min = (first_tip - now_et).total_seconds() / 60.0
    # delta_min > 0 → still before tip; delta_min < 0 → past tip
    return -_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES <= delta_min <= _OFFICIAL_LOCK_WINDOW_MINUTES


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
                candidates = [prev, prev.lstrip("/")]
                if not any(os.path.exists(c) for c in candidates):
                    existing.header_image_path = None
        try:
            existing_picks = json.loads(existing.picks_json) if existing.picks_json else []
        except Exception:
            existing_picks = []
        new_picks = picks_data or []
        if existing_picks and not new_picks:
            print(f"PRESERVE PICKS: existing has {len(existing_picks)} picks, new is empty — keeping existing picks AND game_count.")
        else:
            existing.picks_json = json.dumps(new_picks)
            existing.analysis_json = json.dumps(analysis_data)
            existing.game_count = game_count
        existing.best_available = best_available
        existing.claude_selected = claude_selected
        article_row = existing
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
        article_row = article

    # Task #45: Official Call Snapshot — freeze picks_json into
    # official_picks_json the first time we save within the lock window
    # (typically the T-60 pregame wave for the slate's first tipoff). Once
    # locked, subsequent regens (later pregame waves, prop-movement regens)
    # update picks_json only // the official snapshot is immutable until an
    # admin re-snapshots it.
    try:
        from zoneinfo import ZoneInfo as _ZI
        now_et = datetime.now(_ZI("America/New_York"))
        if not article_row.official_picks_json and _should_lock_official_call(target_date, now_et):
            current_picks = article_row.picks_json or json.dumps([])
            try:
                parsed_picks = json.loads(current_picks)
            except Exception:
                parsed_picks = []
            if parsed_picks:
                article_row.official_picks_json = current_picks
                article_row.official_locked_at = now_et
                print(f"[OFFICIAL CALL][source=auto-lock] Locked snapshot for {target_date} at {now_et.strftime('%Y-%m-%d %H:%M ET')} ({len(parsed_picks)} picks)")
    except Exception as e:
        print(f"[OFFICIAL CALL][source=auto-lock] Lock skipped for {target_date}: {e}")

    session.commit()
    session.close()
    print(f"Article saved to database for {target_date}")


if __name__ == '__main__':
    d = None
    if len(sys.argv) > 1:
        d = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    generate_article(d)
