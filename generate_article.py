"""
Generate daily article content for the PIRTDICA Articles page.
Usage: python generate_article.py [YYYY-MM-DD]
       Defaults to today's date.
Reads HIGH confidence picks from prop_recommendations.csv, builds analysis text,
generates header image, and saves everything to PostgreSQL.
"""
import os
import sys
import json
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


def build_analysis_text(row, dfs_df):
    player = row['player']
    stat = row['stat']
    team = row['team']
    opponent = row['opponent']
    archetype = row.get('archetype', '')
    book_line = row.get('book_line', 0)
    projected = row.get('projected_value', row.get('adjusted_avg', 0))
    player_avg = row.get('player_avg', 0)
    vs_book_edge = _safe_float(row.get('vs_book_edge', 0))
    dva_edge = _safe_float(row.get('dva_edge', 0))
    dvp_edge = _safe_float(row.get('dvp_edge', 0))
    hit_rate = _safe_float(row.get('hit_rate', 0))
    last5_avg = _safe_float(row.get('last5_avg', 0))
    cv = _safe_float(row.get('cv', 0))
    recommendation = row.get('recommendation', '')
    projection_factors = row.get('projection_factors', '')
    pace_factor = row.get('pace_factor', 0)
    total_factor = row.get('total_factor', 0)
    projected_min = _safe_float(row.get('projected_min', 0))
    composite_score = _safe_float(row.get('composite_score', 0))
    blend = row.get('blend', '')
    usage_boost = row.get('usage_boost', 0)

    call = "OVER" if "OVER" in str(recommendation).upper() else "UNDER"
    edge_sign = "+" if vs_book_edge > 0 else ""
    edge_str = f"{edge_sign}{vs_book_edge:.1f}%"

    dfs_row = dfs_df[dfs_df['player_name'] == player]
    salary = int(dfs_row.iloc[0]['salary']) if len(dfs_row) else row.get('salary', 0)
    implied_total = dfs_row.iloc[0].get('implied_total', 0) if len(dfs_row) else 0

    paragraphs = []

    matchup_line = f"{player} ({archetype}) faces {opponent} tonight"
    if implied_total and float(implied_total) > 0:
        matchup_line += f" in a game with a {float(implied_total):.1f} implied total"
    matchup_line += "."
    paragraphs.append(matchup_line)

    dva_direction = "favorable" if dva_edge > 0 else "tough"
    dvp_direction = "favorable" if dvp_edge > 0 else "tough"
    if abs(dva_edge) > 0.5 or abs(dvp_edge) > 0.5:
        edge_parts = []
        if abs(dva_edge) > 0.5:
            edge_parts.append(f"DVA edge of {'+' if dva_edge > 0 else ''}{dva_edge:.1f} ({dva_direction} archetype matchup)")
        if abs(dvp_edge) > 0.5:
            edge_parts.append(f"DVP edge of {'+' if dvp_edge > 0 else ''}{dvp_edge:.1f} ({dvp_direction} positional matchup)")
        paragraphs.append(f"The matchup data shows a {' and '.join(edge_parts)} using the {blend} blend.")

    if last5_avg and float(last5_avg) > 0:
        trend_dir = "above" if float(last5_avg) > float(player_avg) else "below"
        paragraphs.append(
            f"Over his last 5 games, {player.split()[1]} is averaging {float(last5_avg):.1f} {stat}, "
            f"trending {trend_dir} his season average of {float(player_avg):.1f}."
        )

    if hit_rate and float(hit_rate) > 0:
        paragraphs.append(
            f"This {call} has a {float(hit_rate):.0f}% historical hit rate with a consistency score (CV) of {float(cv):.2f}."
        )

    factors_text = str(projection_factors) if projection_factors and str(projection_factors) != 'nan' else ''
    if factors_text:
        factor_parts = [f.strip() for f in factors_text.split(';') if f.strip()]
        if factor_parts:
            paragraphs.append("Key projection factors: " + ", ".join(factor_parts) + ".")

    paragraphs.append(
        f"**The Call: {call} {book_line} {stat}** — Projected at {float(projected):.1f} ({edge_str} edge). "
        f"Composite score: {float(composite_score):.1f}."
    )

    return "\n\n".join(paragraphs)


def generate_article(target_date=None):
    if target_date is None:
        target_date = date.today()

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

    high = props[props['confidence'] == 'HIGH'].copy()
    if high.empty:
        print("No HIGH confidence picks found.")
        return False

    edge_col = 'vs_book_edge' if 'vs_book_edge' in high.columns else 'edge_pct'
    high[edge_col] = pd.to_numeric(high[edge_col], errors='coerce').fillna(0)
    high['abs_edge'] = high[edge_col].abs()
    high = high.sort_values('abs_edge', ascending=False)

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
        })

    analysis_data = []
    seen_analysis = set()
    for _, row in high.iterrows():
        player = row['player']
        if player in seen_analysis:
            continue
        seen_analysis.add(player)
        analysis_text = build_analysis_text(row, dfs_df)
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

    header_path = None
    try:
        from generate_header import generate as gen_header
        static_header = f'static/images/article_header_{target_date.strftime("%Y-%m-%d")}.png'
        header_path = gen_header(target_date, out_path=static_header)
        if header_path:
            print(f"Header image: {header_path}")
    except Exception as e:
        print(f"Header generation skipped: {e}")

    save_to_db(target_date, header_path, picks_data, analysis_data, game_count)

    print(f"Article generated: {len(picks_data)} picks, {len(analysis_data)} analysis sections, {game_count} games")
    return True


def save_to_db(target_date, header_image_path, picks_data, analysis_data, game_count):
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

    web_path = None
    if header_image_path and os.path.exists(header_image_path):
        web_path = "/" + header_image_path

    if existing:
        existing.header_image_path = web_path
        existing.picks_json = json.dumps(picks_data)
        existing.analysis_json = json.dumps(analysis_data)
        existing.game_count = game_count
    else:
        article = DailyArticle(
            slate_date=target_date,
            header_image_path=web_path,
            picks_json=json.dumps(picks_data),
            analysis_json=json.dumps(analysis_data),
            game_count=game_count,
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
