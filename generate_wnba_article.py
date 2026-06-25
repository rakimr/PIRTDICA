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
# Prop stat -> wnba_dvp.stat key (DVP = how many of this stat the opponent allows
# vs the league average; factor > 1.0 = soft matchup that supports OVERs).
DVP_STAT = {"PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m"}


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


def _zone_rank_label(rank, total):
    """1 = stingiest defense in the league, total = leakiest."""
    if not rank or not total:
        return ""
    if rank <= max(1, total // 3):
        return "stingy"
    if rank >= total - max(1, total // 3) + 1:
        return "leaky"
    return "average"


def _load_enrichment():
    """Load WNBA shot-zone, opponent-defense-by-zone, and DVP context once.

    Mirrors the NBA article enrichment so Claude leads with shot-diet vs zone
    defense and DVP instead of leaning on last-5 / hit-rate. No archetype/DVA or
    usage-redistribution blocks // there is no WNBA tracking source for those.
    """
    caches = {"shot_zones": {}, "team_def": {}, "zone_ranks": {},
              "league_avg": {}, "dvp": {}, "dvp_rank": {}}
    try:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        def _has(table):
            return cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone() is not None

        if _has("wnba_player_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_player_shot_zones"):
                caches["shot_zones"][_norm(row["player_name"])] = dict(row)

        if _has("wnba_team_defense_shot_zones"):
            for row in cur.execute("SELECT * FROM wnba_team_defense_shot_zones"):
                d = dict(row)
                # Player shot zones carry one combined "three" // synthesize the
                # opponent's combined three-point defense from corner3 + atb3.
                c3a, c3m = d.get("corner3_fga") or 0, d.get("corner3_fgm") or 0
                a3a, a3m = d.get("atb3_fga") or 0, d.get("atb3_fgm") or 0
                d["three_fg_pct"] = round(100.0 * (c3m + a3m) / (c3a + a3a), 1) if (c3a + a3a) else None
                d["three_freq"] = (d.get("corner3_freq") or 0) + (d.get("atb3_freq") or 0)
                caches["team_def"][d["team"]] = d

            # Per-zone league ranks: rank 1 = lowest FG% allowed (stingiest).
            zone_keys = ["ra_fg_pct", "paint_fg_pct", "mid_fg_pct", "three_fg_pct"]
            caches["zone_ranks"] = {t: {} for t in caches["team_def"]}
            for k in zone_keys:
                vals = [(t, dv.get(k)) for t, dv in caches["team_def"].items() if dv.get(k) is not None]
                vals.sort(key=lambda x: x[1])
                for i, (t, _v) in enumerate(vals, start=1):
                    caches["zone_ranks"][t][k] = (i, len(vals))

            # Data-driven league averages (attempts-weighted) so "soft vs tough"
            # is judged against the real WNBA baseline, not a hardcoded NBA one.
            for zone, (fgm_k, fga_k) in {
                "rim": ("ra_fgm", "ra_fga"),
                "paint": ("paint_fgm", "paint_fga"),
                "mid": ("mid_fgm", "mid_fga"),
            }.items():
                tm = sum((dv.get(fgm_k) or 0) for dv in caches["team_def"].values())
                ta = sum((dv.get(fga_k) or 0) for dv in caches["team_def"].values())
                caches["league_avg"][zone] = round(100.0 * tm / ta, 1) if ta else None
            tm = sum(((dv.get("corner3_fgm") or 0) + (dv.get("atb3_fgm") or 0)) for dv in caches["team_def"].values())
            ta = sum(((dv.get("corner3_fga") or 0) + (dv.get("atb3_fga") or 0)) for dv in caches["team_def"].values())
            caches["league_avg"]["three"] = round(100.0 * tm / ta, 1) if ta else None

        if _has("wnba_dvp"):
            by_stat = {}
            for team, stat, factor in cur.execute("SELECT team, stat, factor FROM wnba_dvp"):
                caches["dvp"][(team, stat)] = factor
                by_stat.setdefault(stat, []).append((team, factor))
            for stat, rows in by_stat.items():
                rows.sort(key=lambda x: x[1])  # rank 1 = toughest (lowest factor)
                for i, (team, _f) in enumerate(rows, start=1):
                    caches["dvp_rank"][(team, stat)] = (i, len(rows))
        conn.close()
    except Exception as e:
        print(f"[WNBA ARTICLE] enrichment load failed ({e}) // continuing without it.")
    return caches


def _enrich_pick(player, opponent, stat, caches):
    """Build shot_diet, opp_def_zones, zone_matchup_edges, and dvp blocks for one
    pick. Returns only the blocks whose source data clears the sample threshold."""
    out = {}
    pz = caches["shot_zones"].get(_norm(player))
    opp_def = caches["team_def"].get(opponent)
    ranks = caches["zone_ranks"].get(opponent, {})
    lavg = caches["league_avg"]

    # zone label -> (player share key, player fgm/fga keys, opp def fg% key, lavg key)
    zone_map = [
        ("rim",   "ra_pct",    "ra_fgm",    "ra_fga",    "ra_fg_pct",    "rim"),
        ("paint", "paint_pct", "paint_fgm", "paint_fga", "paint_fg_pct", "paint"),
        ("mid",   "mid_pct",   "mid_fgm",   "mid_fga",   "mid_fg_pct",   "mid"),
        ("three", "three_pct", "three_fgm", "three_fga", "three_fg_pct", "three"),
    ]

    player_zones = []
    if pz and (pz.get("total_fga") or 0) >= 30:
        rows = []
        for label, share_k, fgm_k, fga_k, _opp_k, _l in zone_map:
            share = pz.get(share_k)
            if not share or share < 8:
                continue
            fga, fgm = pz.get(fga_k) or 0, pz.get(fgm_k) or 0
            fg = round(100.0 * fgm / fga, 1) if fga else None
            rows.append((label, round(share, 1), fg))
        rows.sort(key=lambda r: -(r[1] or 0))
        player_zones = rows[:3]
        if player_zones:
            out["shot_diet"] = {
                "season_fga": pz.get("total_fga"),
                "top_zones": [{"zone": z, "share_pct": s, "fg_pct": fg} for z, s, fg in player_zones],
            }

    if opp_def and player_zones:
        key_for = {"rim": "ra_fg_pct", "paint": "paint_fg_pct", "mid": "mid_fg_pct", "three": "three_fg_pct"}
        relevant = []
        for z, _s, _fg in player_zones:
            v = opp_def.get(key_for[z])
            if v is None:
                continue
            rk = ranks.get(key_for[z])
            relevant.append({
                "zone": z,
                "allowed_fg_pct": round(v, 1),
                "def_rank": f"{rk[0]}/{rk[1]} ({_zone_rank_label(*rk)})" if rk else None,
            })
        if relevant:
            out["opp_def_zones"] = relevant

    if pz and opp_def and (pz.get("total_fga") or 0) >= 30:
        edges = []
        for label, share_k, _fgm, _fga, opp_k, lavg_k in zone_map:
            share = pz.get(share_k) or 0
            allowed = opp_def.get(opp_k)
            base_avg = lavg.get(lavg_k)
            if share < 8 or allowed is None or base_avg is None:
                continue
            diff = allowed - base_avg
            mag = abs(share * diff)
            if mag <= 5:
                continue
            rk = ranks.get(opp_k)
            tone = "easier" if diff > 0 else "tougher"
            s = (f"{label}: takes {share:.0f}% of shots, opp allows {allowed:.1f}% "
                 f"(lg avg {base_avg:.0f}")
            if rk:
                s += f", def rank {rk[0]}/{rk[1]} // {_zone_rank_label(*rk)}"
            s += f" // {tone} than avg)"
            edges.append((mag, s))
        edges.sort(key=lambda e: -e[0])
        top = [t for _m, t in edges[:2]]
        if top:
            out["zone_matchup_edges"] = top

    dvp_key = DVP_STAT.get(stat)
    if dvp_key:
        factor = caches["dvp"].get((opponent, dvp_key))
        if factor is not None:
            rk = caches["dvp_rank"].get((opponent, dvp_key))
            edge_pct = round((factor - 1.0) * 100, 1)
            label = "soft" if edge_pct > 2 else "tough" if edge_pct < -2 else "neutral"
            out["dvp"] = {
                "stat": stat,
                "opp_allows_vs_avg_pct": edge_pct,
                "factor": round(factor, 3),
                "rank": f"{rk[0]}/{rk[1]} (1=toughest)" if rk else None,
                "read": label,
            }
    return out


def _build_briefing(recs, meta, records, caches=None):
    if caches is None:
        caches = _load_enrichment()
    prop_lines = []
    for _, r in recs.iterrows():
        nk = _norm(r["player"])
        pos = meta.get(nk, {}).get("pos", "")
        entry = {
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
        }
        entry.update(_enrich_pick(r["player"], r["opponent"], r["stat"], caches))
        prop_lines.append(entry)
    return prop_lines


SYSTEM_PROMPT = """You are an elite WNBA DFS analyst for PIRTDICA SPORTS CO. You are given the full WNBA slate with model projections, shot-diet vs opponent-defense-by-zone matchups, DVP (defense vs the stat), recent form, and game environment.

You are competing against the sharpest analysts at FanDuel who set these player prop lines. Respect the lines. Only attack when you have genuine conviction backed by multiple converging signals.

YOUR JOB: Independently analyze the slate and select 4-8 HIGH confidence prop picks. You are NOT limited to what the model labeled HIGH; evaluate every prop line and find the sharpest edges yourself.

ANALYTICAL FRAMEWORK (this is how a sharp WNBA analyst reasons // follow it):
1. SHOT-DIET vs OPPONENT-DEFENSE-BY-ZONE (HEADLINE SIGNAL when present): If the pick has `zone_matchup_edges`, or `shot_diet` + `opp_def_zones`, LEAD with it. Cite the player's zone share and the opponent's allowed FG% and rank in that zone (e.g. "takes 35% of shots from mid // CON allows 38.4% there // def rank 13/15 // leaky"). When a player's high-frequency zones line up with the opponent's leaky zones, that is the strongest case for an OVER. This is the single most distinguishing piece of context per pick // never bury it.
2. DVP (defense vs the stat): `dvp.opp_allows_vs_avg_pct` is how much more/less of this stat the opponent gives up vs the WNBA average (positive = soft = supports OVER, negative = tough = supports UNDER). Cite it: "POR allows +7.1% more rebounds than average (rank 14/15)". Treat a soft/tough DVP as a primary matchup signal, not a footnote.
3. PROJECTION EDGE: the model's projected value vs the book line (`edge_pct`).
4. CONFIRMATION SIGNALS (secondary): hit rate, last-5 form, CV (consistency). Use these to CONFIRM a pick that the shot-diet/DVP/projection signals already support // do NOT lead with last-5 or hit rate. Lower CV is a tailwind; high CV demands a bigger edge.

PICK SELECTION:
- Prioritize picks where shot-diet vs zone defense and/or DVP point the same direction as the projection edge, then confirm with form.
- Do not write an analysis that only cites last-5 and hit rate when zone/DVP context is available. The zone matchup and DVP are why these are PIRTDICA picks.
- A soft opponent in that stat/zone supports OVERs, a tough one supports UNDERs.

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
        # Lead with the matchup context (shot diet vs zone defense + DVP) when we
        # have it, mirroring the Claude framework, so the fallback is not just
        # last-5 / hit-rate either.
        matchup = ""
        zme = p.get("zone_matchup_edges")
        if zme:
            matchup += f"**Matchup edge:** {zme[0]}.\n\n"
        dvp = p.get("dvp")
        if dvp and dvp.get("read") in ("soft", "tough"):
            sign = "+" if dvp["opp_allows_vs_avg_pct"] >= 0 else ""
            matchup += (
                f"{p['opponent']} grades as a {dvp['read']} {stat_full} matchup: they allow "
                f"{sign}{dvp['opp_allows_vs_avg_pct']}% vs the WNBA average "
                f"(DVP rank {dvp.get('rank','')}).\n\n"
            )
        analysis = (
            f"**{p['player']} is our {side.lower()} look on {stat_full} tonight.** "
            f"The model projects {p['projected']} against a book line of {p['book_line']}, "
            f"a {_edge_str(edge)} edge.\n\n"
            f"{matchup}"
            f"She profiles as a {p['position'].lower()} for {p['team']} ({p['team_record']}) "
            f"facing {p['opponent']} ({p['opponent_record']}). Season average sits at "
            f"{p['season_avg']} with a last-5 of {p['last5_avg']}, and the hit rate on this "
            f"side is {p['hit_rate']:.0f}%, confirming the matchup read.\n\n"
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


_OFFICIAL_LOCK_WINDOW_MINUTES = 60
_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES = 30


def _first_tipoff_today_et(slate_date):
    """Return the earliest WNBA tipoff datetime in ET for `slate_date`, or None.

    Uses `wnba_games.commence_time` (stored as a UTC ISO timestamp) for the
    slate, converted to ET. Returns None on any failure or when no game times
    exist // callers must treat that as "no lock yet". Mirrors the NBA helper in
    generate_article.py so the lock window is consistent across both leagues.
    """
    try:
        from zoneinfo import ZoneInfo
        et_zone = ZoneInfo("America/New_York")
        if not os.path.exists(DB):
            return None
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        cur.execute(
            "SELECT commence_time FROM wnba_games WHERE game_date = ? AND commence_time IS NOT NULL",
            (slate_date.isoformat(),),
        )
        raw = [row[0] for row in cur.fetchall()]
        conn.close()
        parsed = []
        for ct in raw:
            try:
                ts = ct.replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                parsed.append(dt.astimezone(et_zone))
            except Exception:
                continue
        if not parsed:
            return None
        return min(parsed)
    except Exception:
        return None


def _should_lock_official_call(slate_date, now_et):
    """Return True iff we're inside the slate's lock window (mirror of NBA).

    Window: from T-60min before first tip through T+30min after. One-shot //
    caller checks official_picks_json is NULL first. If the window closes without
    a lock firing, the grader cleanly falls back to picks_json.
    """
    first_tip = _first_tipoff_today_et(slate_date)
    if first_tip is None:
        return False
    delta_min = (first_tip - now_et).total_seconds() / 60.0
    return -_OFFICIAL_LOCK_GRACE_AFTER_TIP_MINUTES <= delta_min <= _OFFICIAL_LOCK_WINDOW_MINUTES


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

        # Official Call Snapshot — freeze picks_json into official_picks_json the
        # first time we save within the lock window (typically the pregame wave
        # ~60 min before the slate's first tipoff). Once locked, later regens
        # update picks_json only // the official snapshot is immutable until an
        # admin re-snapshots it. Mirrors generate_article.save_to_db.
        try:
            from zoneinfo import ZoneInfo as _ZI
            now_et = datetime.now(_ZI("America/New_York"))
            if not row.official_picks_json and _should_lock_official_call(slate_date, now_et):
                current_picks = row.picks_json or json.dumps([])
                try:
                    parsed_picks = json.loads(current_picks)
                except Exception:
                    parsed_picks = []
                if parsed_picks:
                    row.official_picks_json = current_picks
                    row.official_locked_at = now_et
                    print(f"[OFFICIAL CALL][WNBA][source=auto-lock] Locked snapshot for {slate_date} at {now_et.strftime('%Y-%m-%d %H:%M ET')} ({len(parsed_picks)} picks)")
        except Exception as e:
            print(f"[OFFICIAL CALL][WNBA][source=auto-lock] Lock skipped for {slate_date}: {e}")

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

    # Pick the ACTIVE slate explicitly instead of trusting row order. The recs
    # CSV can carry more than one game_date (e.g. an upcoming slate already has
    # odds), and recs["game_date"].iloc[0] could grab a future date // that would
    # make the official-call lock check the wrong day's tipoff and never fire.
    # Choose the earliest slate that is today-or-later in ET (the active/upcoming
    # slate); if every slate is in the past, fall back to the most recent one.
    from zoneinfo import ZoneInfo as _ZI
    et_today = datetime.now(_ZI("America/New_York")).date()
    slate_dates = sorted({
        datetime.strptime(str(d), "%Y-%m-%d").date()
        for d in recs["game_date"].dropna().unique()
    })
    if len(slate_dates) > 1:
        print(f"[WNBA ARTICLE] recs span {len(slate_dates)} slates: "
              f"{[d.isoformat() for d in slate_dates]} // selecting active slate.")
    upcoming = [d for d in slate_dates if d >= et_today]
    slate_date = upcoming[0] if upcoming else slate_dates[-1]
    slate_date_str = slate_date.isoformat()
    print(f"[WNBA ARTICLE] Active slate: {slate_date_str} (ET today: {et_today})")
    recs = recs[recs["game_date"].astype(str) == slate_date_str].reset_index(drop=True)
    if recs.empty:
        print(f"No WNBA recs for active slate {slate_date_str} after filtering.")
        return
    game_count = recs[["team", "opponent"]].apply(
        lambda r: frozenset([r["team"], r["opponent"]]), axis=1).nunique()

    prop_lines = _build_briefing(recs, meta, records, _load_enrichment())
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
