"""
WNBA daily article generator for PIRTDICA SPORTS CO.

Mirrors the NBA article subsystem: it selects HIGH-confidence WNBA prop picks,
writes a Claude-authored analysis for each, renders a Pillow header image of the
featured players, and saves everything to the `wnba_daily_articles` Postgres table
so /articles?league=wnba renders the SAME articles.html as the NBA page.

Inputs:
  - wnba_prop_recommendations.csv  (build_wnba_projections.py)
  - wnba_player_stats              (espn_id + position, for header + archetype line)
  - wnba_standings                 (records, for slate context)
Fallback: if Claude is unavailable, a template engine writes data-driven analysis
so the page never goes blank.
"""
import json
import os
import sqlite3
import time
import unicodedata
from datetime import datetime

import pandas as pd

import generate_header

DB = "dfs_nba.db"
POS_LABEL = {"G": "Guard", "F": "Forward", "C": "Center", "G/F": "Wing", "F/C": "Big"}
STAT_FULL = {"PTS": "points", "REB": "rebounds", "AST": "assists", "3PM": "three-pointers"}


def _norm(name):
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _load_recs():
    if not os.path.exists("wnba_prop_recommendations.csv"):
        return pd.DataFrame()
    return pd.read_csv("wnba_prop_recommendations.csv")


def _player_meta():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    meta = {}
    for name, espn_id, pos, team in cur.execute(
            "SELECT player_name, espn_id, position, team FROM wnba_player_stats").fetchall():
        meta[_norm(name)] = {"espn_id": espn_id, "pos": pos or "", "team": team}
    records = {}
    for abbr, wins, losses in cur.execute(
            "SELECT team, wins, losses FROM wnba_standings").fetchall():
        records[abbr] = f"{wins}-{losses}"
    conn.close()
    return meta, records


def _edge_str(edge):
    try:
        e = float(edge)
        return f"+{e:.1f}%" if e >= 0 else f"{e:.1f}%"
    except (TypeError, ValueError):
        return ""


def _build_briefing(recs, meta, records):
    prop_lines = []
    for _, r in recs.iterrows():
        nk = _norm(r["player"])
        pos = meta.get(nk, {}).get("pos", "")
        prop_lines.append({
            "player": r["player"],
            "team": r["team"],
            "team_record": records.get(r["team"], ""),
            "opponent": r["opponent"],
            "opponent_record": records.get(r["opponent"], ""),
            "position": POS_LABEL.get(pos, pos or "Player"),
            "stat": r["stat"],
            "book_line": r["book_line"],
            "projected": r["projected_value"],
            "season_avg": r["player_avg"],
            "last5_avg": r["last5_avg"],
            "edge_pct": r["vs_book_edge"],
            "model_side": r["recommendation"],
            "hit_rate": r["hit_rate"],
            "cv": r["cv"],
            "composite_score": r["composite_score"],
            "confidence": r["confidence"],
        })
    return prop_lines


SYSTEM_PROMPT = """You are an elite WNBA DFS analyst for PIRTDICA SPORTS CO. You are given the full WNBA slate with model projections, matchup edges, recent form, and game environment.

You are competing against the sharpest analysts at FanDuel who set these player prop lines. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals (projection edge + recent form + hit rate + matchup).

YOUR JOB: Independently analyze the slate and select 4-8 HIGH confidence prop picks. You are NOT limited to what the model labeled HIGH; evaluate every prop line and find the sharpest edges yourself.

PICK SELECTION:
- Prioritize picks where the projection meaningfully beats the book line AND the player's hit rate / last-5 form confirms it.
- Lower CV (consistency) is a tailwind. High CV means volatile, so demand a bigger edge.
- Use the matchup: a soft opponent in that stat supports OVERs, a tough one supports UNDERs.

WRITING STYLE:
- Talk to a sharp friend. Conversational, direct, confident.
- NEVER use em-dashes or double hyphens. Use periods, commas, colons, or "//" instead.
- Use "you" and "your". Short paragraphs (2-4 lines). One idea per paragraph.
- **Bold the key insight** in most sections.
- Back every claim with specific numbers (projection, edge, hit rate, last-5, composite score).
- Each analysis: 3-4 paragraphs, 150-250 words.
- End each analysis with: **The Call: OVER/UNDER X.X STAT** // We project [Player] at [Value] [stat] tonight ([edge]% edge vs. the book). Composite score: [X].

OUTPUT FORMAT:
Return a JSON object with two keys:
1. "picks": array of objects with: "player", "team", "opponent", "stat", "book_line", "projected", "avg", "edge" (e.g. "+17.8%"), "pick" ("OVER"/"UNDER"), "composite_score".
2. "analyses": array of objects with: "player", "stat", "call" ("OVER"/"UNDER"), "archetype", "team", "opponent", "analysis".

CRITICAL MATCHING RULES:
- "analyses" MUST have exactly one entry per pick, same order. Each is identified by (player, stat, call). The "call" must match the pick side exactly.
- Never write an UNDER analysis for an OVER pick or vice versa.

Return ONLY the JSON object. Order picks by edge strength (strongest first)."""


def _call_claude(prop_lines, game_count, slate_date):
    api_key = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
    base_url = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")
    if not api_key or not base_url:
        print("Claude not configured // using template fallback.")
        return None
    briefing = {
        "slate_date": str(slate_date),
        "game_count": game_count,
        "prop_lines": prop_lines,
    }
    user_prompt = (
        "Analyze the full WNBA slate and select your HIGH confidence picks.\n\n"
        "HERE IS THE COMPLETE SLATE DATA (every prop line with model context):\n\n"
        f"{json.dumps(briefing, indent=2, default=str)}\n\n"
        "Remember: select 4-8 picks with the strongest convergence of signals, cite "
        "specific numbers, end each analysis with a bold **The Call:** line, and NEVER "
        "use em-dashes. Return ONLY a JSON object with \"picks\" and \"analyses\" keys."
    )
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key, base_url=base_url)
        print("Calling Claude for WNBA pick selection + analysis...")
        t0 = time.time()
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = msg.content[0].text.strip()
        print(f"Claude responded in {time.time() - t0:.1f}s ({len(text)} chars)")
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        result = json.loads(text)
        if not isinstance(result, dict) or "picks" not in result or "analyses" not in result:
            print("Claude returned unexpected format // template fallback.")
            return None
        if not isinstance(result["picks"], list) or len(result["picks"]) < 2:
            print("Claude returned too few picks // template fallback.")
            return None
        return result
    except Exception as e:
        print(f"Claude error ({e}) // template fallback.")
        return None


def _template_result(prop_lines):
    """Data-driven fallback when Claude is unavailable."""
    highs = [p for p in prop_lines if p["confidence"] in ("HIGH", "MEDIUM")][:6]
    if not highs:
        highs = prop_lines[:5]
    picks, analyses = [], []
    for p in highs:
        side = p["model_side"]
        stat_full = STAT_FULL.get(p["stat"], p["stat"].lower())
        edge = p["edge_pct"]
        analysis = (
            f"**{p['player']} is our {side.lower()} look on {stat_full} tonight.** "
            f"The model projects {p['projected']} against a book line of {p['book_line']}, "
            f"a {_edge_str(edge)} edge.\n\n"
            f"He profiles as a {p['position'].lower()} for {p['team']} ({p['team_record']}) "
            f"facing {p['opponent']} ({p['opponent_record']}). Season average sits at "
            f"{p['season_avg']} with a last-5 of {p['last5_avg']}, and the hit rate on this "
            f"side is {p['hit_rate']:.0f}%.\n\n"
            f"Consistency reads at a {p['cv']} coefficient of variation. "
            f"**The Call: {side} {p['book_line']} {p['stat']}** // We project {p['player']} at "
            f"{p['projected']} {p['stat']} tonight ({_edge_str(edge)} edge vs. the book). "
            f"Composite score: {p['composite_score']}."
        )
        picks.append({
            "player": p["player"], "team": p["team"], "opponent": p["opponent"],
            "stat": p["stat"], "book_line": p["book_line"], "projected": p["projected"],
            "avg": p["season_avg"], "edge": _edge_str(edge), "pick": side,
            "composite_score": p["composite_score"],
        })
        analyses.append({
            "player": p["player"], "stat": p["stat"], "call": side,
            "archetype": p["position"], "team": p["team"], "opponent": p["opponent"],
            "analysis": analysis,
        })
    return {"picks": picks, "analyses": analyses}


def _to_template_shapes(result, meta):
    picks_data, analysis_data = [], []
    for i, pk in enumerate(result["picks"], start=1):
        picks_data.append({
            "rank": i,
            "player": pk.get("player", ""),
            "game": f"{pk.get('team','')} vs {pk.get('opponent','')}",
            "stat": pk.get("stat", ""),
            "avg": pk.get("avg", ""),
            "line": pk.get("book_line", ""),
            "projected": pk.get("projected", ""),
            "edge": pk.get("edge") if isinstance(pk.get("edge"), str) else _edge_str(pk.get("edge")),
            "pick": str(pk.get("pick", "OVER")).upper(),
        })
    for a in result["analyses"]:
        nk = _norm(a.get("player", ""))
        pos = meta.get(nk, {}).get("pos", "")
        analysis_data.append({
            "player": a.get("player", ""),
            "archetype": a.get("archetype") or POS_LABEL.get(pos, "Player"),
            "team": a.get("team", ""),
            "opponent": a.get("opponent", ""),
            "stat": a.get("stat", ""),
            "call": str(a.get("call", "OVER")).upper(),
            "analysis": a.get("analysis", ""),
        })
    return picks_data, analysis_data


def _save(slate_date, header_web_path, picks_data, analysis_data, game_count, claude_selected):
    from backend.database import SessionLocal
    from backend import models
    db = SessionLocal()
    try:
        row = db.query(models.WNBADailyArticle).filter(
            models.WNBADailyArticle.slate_date == slate_date).first()
        if row:
            row.header_image_path = header_web_path
            row.picks_json = json.dumps(picks_data)
            row.analysis_json = json.dumps(analysis_data)
            row.game_count = game_count
            row.claude_selected = claude_selected
        else:
            row = models.WNBADailyArticle(
                slate_date=slate_date,
                header_image_path=header_web_path,
                picks_json=json.dumps(picks_data),
                analysis_json=json.dumps(analysis_data),
                game_count=game_count,
                claude_selected=claude_selected,
            )
            db.add(row)
        db.commit()
        print(f"Saved WNBA article for {slate_date}: {len(picks_data)} picks, claude={claude_selected}")
    finally:
        db.close()


def main():
    print("=== WNBA article generation ===")
    recs = _load_recs()
    if recs.empty:
        print("No WNBA prop recommendations // run build_wnba_projections.py first.")
        return
    meta, records = _player_meta()
    slate_date_str = str(recs["game_date"].iloc[0])
    slate_date = datetime.strptime(slate_date_str, "%Y-%m-%d").date()
    game_count = recs[["team", "opponent"]].apply(
        lambda r: frozenset([r["team"], r["opponent"]]), axis=1).nunique()

    prop_lines = _build_briefing(recs, meta, records)
    result = _call_claude(prop_lines, game_count, slate_date)
    claude_selected = result is not None
    if not result:
        result = _template_result(prop_lines)

    picks_data, analysis_data = _to_template_shapes(result, meta)

    # Header image: featured players (dedup, top 6 by order) -> static/images.
    seen, header_players, espn_ids = set(), [], {}
    for pk in picks_data:
        name = pk["player"]
        if name in seen:
            continue
        seen.add(name)
        nk = _norm(name)
        m = meta.get(nk, {})
        header_players.append({
            "player": name, "team": m.get("team", pk["game"].split(" vs ")[0]),
            "stat": pk["stat"], "side": pk["pick"], "line": pk["line"], "edge": pk["edge"],
        })
        if m.get("espn_id"):
            espn_ids[name] = int(m["espn_id"])
        if len(header_players) >= 6:
            break

    out_path = f"static/images/wnba_article_header_{slate_date.isoformat()}.png"
    header_web_path = None
    try:
        sub = f"{slate_date.strftime('%B %-d, %Y').upper()} \u2014 WNBA HIGH CONFIDENCE PICKS"
        generate_header.generate(
            target_date=slate_date, out_path=out_path, player_data=header_players,
            subtitle_override=sub, espn_ids=espn_ids, league="wnba")
        if os.path.exists(out_path):
            header_web_path = "/" + out_path
    except Exception as e:
        print(f"Header generation failed ({e}) // continuing without header.")

    _save(slate_date, header_web_path, picks_data, analysis_data, game_count, claude_selected)
    print("Done.")


if __name__ == "__main__":
    main()
