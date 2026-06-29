"""
WNBA Chart Gallery generator for PIRTDICA SPORTS CO.

Mirrors the NBA value/upside/DVP charts using WNBA model data so
/trends?league=wnba renders the SAME trends.html image charts, just with
WNBA numbers. Charts with no WNBA data source (referee, Synergy play-types,
NBA.com shot-zone tracking, archetype clusters) are intentionally skipped and
hidden on the WNBA trends page.

Inputs (SQLite dfs_nba.db, built by build_wnba_projections.py):
  - wnba_player_value : usage_proxy, fp_avg, fp_sd, min_avg, position
  - wnba_dvp          : team x stat defensive factor
  - wnba_dvp_position : team x position (G/F/C) defensive factor by stat
Outputs:
  - static/images/wnba_value_chart.png
  - static/images/wnba_upside_chart.png
  - static/images/wnba_dvp_heatmap.png
  - static/images/wnba_dvp_position_heatmap.png
"""
import os
import sqlite3
from pathlib import Path

from utils.timezone import get_eastern_date_str

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

DB = "dfs_nba.db"
POS_COLORS = {"G": "#1a1a1a", "F": "#7a7a7a", "C": "#bababa"}
STAT_LABEL = {"pts": "PTS", "reb": "REB", "ast": "AST", "fg3m": "3PM"}


def _style(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=14, fontweight="bold", color="black", pad=12)
    ax.set_xlabel(xlabel, fontsize=11, fontweight="bold", color="black")
    ax.set_ylabel(ylabel, fontsize=11, fontweight="bold", color="black")
    ax.tick_params(colors="black")
    ax.grid(True, linestyle=":", linewidth=0.6, alpha=0.35, color="#888")
    for spine in ax.spines.values():
        spine.set_color("black")
        spine.set_linewidth(2)


def _load_value():
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query(
        "SELECT player_name, team, position, games, min_avg, fp_avg, fp_sd, "
        "pts_avg, reb_avg, ast_avg, usage_proxy FROM wnba_player_value", conn)
    conn.close()
    return df[(df["games"] >= 3) & (df["fp_avg"] > 0)].copy()


def generate_value_chart(df, output_path="static/images/wnba_value_chart.png"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    d = df[(df["usage_proxy"] > 0.05) & df["fp_avg"].notna()].copy()
    if d.empty:
        print("  value chart: no data")
        return None
    d["x"] = d["usage_proxy"] * 100  # show as percent-like axis

    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    for pos in ["G", "F", "C"]:
        p = d[d["position"] == pos]
        if p.empty:
            continue
        sizes = (p["fp_avg"].clip(5, 50) / 50) * 220 + 30
        ax.scatter(p["x"], p["fp_avg"], c=POS_COLORS.get(pos, "#333"),
                   label=pos, alpha=0.75, s=sizes, edgecolors="black", linewidths=0.5)

    for _, pl in d.nlargest(6, "fp_avg").iterrows():
        ax.annotate(pl["player_name"], (pl["x"], pl["fp_avg"]),
                    xytext=(5, 5), textcoords="offset points",
                    fontsize=9, fontweight="bold", color="black")

    _style(ax, "Usage Proxy vs Projected FP (size = FP avg)",
           "Usage Proxy (%)", "Fantasy Points / Game")
    leg = ax.legend(title="Position", frameon=True, edgecolor="black", loc="lower right")
    leg.get_frame().set_linewidth(2)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black")
    plt.close()
    print(f"  value chart -> {output_path}")
    return output_path


def generate_upside_chart(df, output_path="static/images/wnba_upside_chart.png"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    d = df[df["fp_sd"].notna() & (df["fp_avg"] > 5)].copy()
    if d.empty:
        print("  upside chart: no data")
        return None

    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    for pos in ["G", "F", "C"]:
        p = d[d["position"] == pos]
        if p.empty:
            continue
        sizes = (p["min_avg"].clip(5, 36) / 36) * 220 + 30
        ax.scatter(p["fp_avg"], p["fp_sd"], c=POS_COLORS.get(pos, "#333"),
                   s=sizes, alpha=0.75, edgecolors="black", linewidths=0.5, label=pos)

    cash = d[(d["fp_avg"] > d["fp_avg"].quantile(0.7)) & (d["fp_sd"] < d["fp_sd"].quantile(0.3))]
    for _, pl in cash.head(3).iterrows():
        ax.annotate(pl["player_name"], (pl["fp_avg"], pl["fp_sd"]),
                    xytext=(5, -8), textcoords="offset points",
                    fontsize=8, fontweight="bold", color="black")
    breakers = d[(d["fp_avg"] > d["fp_avg"].quantile(0.8)) & (d["fp_sd"] > d["fp_sd"].quantile(0.7))]
    for _, pl in breakers.head(3).iterrows():
        ax.annotate(pl["player_name"], (pl["fp_avg"], pl["fp_sd"]),
                    xytext=(5, 5), textcoords="offset points",
                    fontsize=8, fontweight="bold", color="black")

    mu_med, sd_med = d["fp_avg"].median(), d["fp_sd"].median()
    ax.axvline(x=mu_med, color="black", linestyle=":", linewidth=1, alpha=0.3)
    ax.axhline(y=sd_med, color="black", linestyle=":", linewidth=1, alpha=0.3)
    ax.text(ax.get_xlim()[1] * 0.97, ax.get_ylim()[0] + 0.4, "CASH",
            ha="right", fontsize=7, color="#666", alpha=0.7)
    ax.text(ax.get_xlim()[1] * 0.97, ax.get_ylim()[1] * 0.95, "SLATE BREAKERS",
            ha="right", fontsize=7, color="#666", alpha=0.7)

    _style(ax, "Risk-Reward Frontier (FP avg vs Std Dev)",
           "Projected FP (mu)", "Volatility / Std Dev (sigma)")
    leg = ax.legend(loc="lower right", frameon=True, edgecolor="black",
                    facecolor="white", title="Position")
    leg.get_title().set_fontweight("bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black")
    plt.close()
    print(f"  upside chart -> {output_path}")
    return output_path


def generate_dvp_heatmap(output_path="static/images/wnba_dvp_heatmap.png"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    dvp = pd.read_sql_query("SELECT team, stat, factor FROM wnba_dvp", conn)
    conn.close()
    if dvp.empty:
        print("  dvp heatmap: no data")
        return None
    dvp["stat"] = dvp["stat"].map(lambda s: STAT_LABEL.get(s, str(s).upper()))
    pivot = dvp.pivot_table(index="team", columns="stat", values="factor", aggfunc="first")
    order = [s for s in ["PTS", "REB", "AST", "3PM"] if s in pivot.columns]
    pivot = pivot[order]
    if pivot.empty:
        return None

    fig, ax = plt.subplots(figsize=(8, 12))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    im = ax.imshow(pivot.values, cmap="RdYlGn", aspect="auto", vmin=0.80, vmax=1.20)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=0, fontweight="bold", color="black")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontweight="bold", color="black")
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if pd.notna(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=9,
                        fontweight="bold", color="black" if 0.92 < val < 1.08 else "white")
    for spine in ax.spines.values():
        spine.set_color("black")
        spine.set_linewidth(2)
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Matchup Factor (Higher = Easier Matchup)", fontweight="bold", color="black")
    cbar.outline.set_color("black")
    cbar.outline.set_linewidth(2)
    ax.set_title("WNBA Defense vs Stat - Matchup Heatmap",
                 fontsize=14, fontweight="bold", color="black")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black")
    plt.close()
    print(f"  dvp heatmap -> {output_path}")
    return output_path


def generate_dvp_position_heatmap(output_path="static/images/wnba_dvp_position_heatmap.png"):
    """Defense vs Position heatmap: fantasy points each team allows to Guards /
    Forwards / Centers vs the league average for that position (factor; >1 = team
    gives up more FP to that position = easier matchup). Companion to the Defense
    vs Stat heatmap. Reads wnba_dvp_position (stat='fp')."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    dvp = pd.read_sql_query(
        "SELECT team, position, factor FROM wnba_dvp_position WHERE stat='fp'", conn)
    conn.close()
    if dvp.empty:
        print("  dvp-position heatmap: no data")
        return None
    pivot = dvp.pivot_table(index="team", columns="position", values="factor", aggfunc="first")
    order = [p for p in ["G", "F", "C"] if p in pivot.columns]
    pivot = pivot[order]
    if pivot.empty:
        return None
    label = {"G": "Guards", "F": "Forwards", "C": "Centers"}

    fig, ax = plt.subplots(figsize=(7, 12))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    im = ax.imshow(pivot.values, cmap="RdYlGn", aspect="auto", vmin=0.80, vmax=1.20)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([label.get(c, c) for c in pivot.columns], rotation=0,
                       fontweight="bold", color="black")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontweight="bold", color="black")
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if pd.notna(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=9,
                        fontweight="bold", color="black" if 0.92 < val < 1.08 else "white")
    for spine in ax.spines.values():
        spine.set_color("black")
        spine.set_linewidth(2)
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Matchup Factor (Higher = Easier Matchup)", fontweight="bold", color="black")
    cbar.outline.set_color("black")
    cbar.outline.set_linewidth(2)
    ax.set_title("WNBA Defense vs Position - FP Allowed by Position",
                 fontsize=13, fontweight="bold", color="black")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black")
    plt.close()
    print(f"  dvp-position heatmap -> {output_path}")
    return output_path


def _match_ref(name, lookup):
    """Match an assigned ref name to the ESPN stat lookup.

    Sources differ slightly (e.g. assignment "Timothy Greene" vs ESPN
    "Tim Greene"), so we try exact, then unique last-name + first-initial,
    then unique last-name.
    """
    if not name:
        return None
    key = name.strip().lower()
    if key in lookup:
        return lookup[key]
    parts = key.split()
    if len(parts) < 2:
        return None
    last, first_i = parts[-1], parts[0][0]
    li = [v for k, v in lookup.items()
          if k.split()[-1] == last and k.split()[0][0] == first_i]
    if len(li) == 1:
        return li[0]
    ln = [v for k, v in lookup.items() if k.split()[-1] == last]
    return ln[0] if len(ln) == 1 else None


def generate_ref_foul_chart(output_path="static/images/wnba_ref_foul_chart.png"):
    """WNBA referee crew foul chart: crew fouls/game (X) vs home/away bias (Y).

    Mirrors the NBA referee chart, but the foul tendencies are computed by us
    from ESPN (build_wnba_referee_stats.py) and the crews come from the WNBA
    table on official.nba.com (scrape_wnba_referee_assignments.py).
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    today = get_eastern_date_str()
    conn = sqlite3.connect(DB)
    try:
        assignments = pd.read_sql_query(
            "SELECT home_team, away_team, crew_chief, referee, umpire "
            "FROM wnba_referee_assignments WHERE game_date = ?",
            conn, params=[today])
        ref_stats = pd.read_sql_query(
            "SELECT referee, fouls_pg, foul_diff, foul_pct_home, foul_pct_road, "
            "games_officiated FROM wnba_referee_stats", conn)
    except Exception as e:
        print(f"  ref chart: query failed ({e})")
        conn.close()
        return None
    conn.close()

    if assignments.empty or ref_stats.empty:
        print("  ref chart: no WNBA assignments for today/stats // skipping")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

    assignments = assignments.drop_duplicates(subset=["home_team", "away_team"])
    lookup = {}
    for _, row in ref_stats.iterrows():
        lookup[row["referee"].strip().lower()] = {
            "fouls_pg": row["fouls_pg"], "foul_diff": row["foul_diff"]}

    game_data = []
    for _, game in assignments.iterrows():
        crew = [game["crew_chief"], game["referee"], game["umpire"]]
        crew = [r for r in crew if r and pd.notna(r)]
        fouls, diffs = [], []
        for ref_name in crew:
            st = _match_ref(ref_name, lookup)
            if st:
                fouls.append(st["fouls_pg"])
                diffs.append(st["foul_diff"])
        if len(fouls) >= 2:
            home = game["home_team"] or "?"
            away = game["away_team"] or "?"
            game_data.append({
                "matchup": f"{away} @ {home}",
                "crew_avg_fouls": sum(fouls) / len(fouls),
                "crew_avg_diff": sum(diffs) / len(diffs),
            })

    if not game_data:
        print("  ref chart: could not match any crews to stats // skipping")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

    gdf = pd.DataFrame(game_data)
    league_avg_fouls = ref_stats["fouls_pg"].mean()

    fig, ax = plt.subplots(figsize=(12, 8))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    f_min, f_max = gdf["crew_avg_fouls"].min(), gdf["crew_avg_fouls"].max()
    for _, g in gdf.iterrows():
        shade = 0.15 + 0.7 * ((g["crew_avg_fouls"] - f_min) / max(f_max - f_min, 1))
        color = str(max(0.1, min(0.85, 1.0 - shade)))
        marker = "^" if g["crew_avg_diff"] > 0 else "v"
        ax.scatter(g["crew_avg_fouls"], g["crew_avg_diff"], c=color, s=220,
                   marker=marker, edgecolors="black", linewidths=1.5, zorder=5)
        y_off = 8 if g["crew_avg_diff"] >= 0 else -12
        ax.annotate(g["matchup"], (g["crew_avg_fouls"], g["crew_avg_diff"]),
                    xytext=(6, y_off), textcoords="offset points",
                    fontsize=9, fontweight="bold", color="black", ha="left")

    ax.axhline(y=0, color="black", linewidth=1.5, alpha=0.6)
    ax.axvline(x=league_avg_fouls, color="gray", linestyle="--", linewidth=1, alpha=0.5)

    x_min, x_max = ax.get_xlim()
    y_min, y_max = ax.get_ylim()
    y_max = max(y_max, abs(y_min)) + 0.3
    y_min = -y_max
    ax.set_ylim(y_min, y_max)
    ax.fill_between([x_min, x_max], 0, y_max, color="#e8e8e8", alpha=0.3, zorder=0)
    ax.fill_between([x_min, x_max], y_min, 0, color="#d0d0d0", alpha=0.3, zorder=0)
    ax.text(x_max - (x_max - x_min) * 0.02, y_max * 0.88, "HOME ADVANTAGE",
            ha="right", fontsize=8, color="#555", fontweight="bold",
            alpha=0.7, style="italic")
    ax.text(x_max - (x_max - x_min) * 0.02, y_min * 0.88, "ROAD ADVANTAGE",
            ha="right", fontsize=8, color="#555", fontweight="bold",
            alpha=0.7, style="italic")

    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="^", color="w", markerfacecolor="#555",
               markeredgecolor="black", markersize=10, label="Home-favored crew"),
        Line2D([0], [0], marker="v", color="w", markerfacecolor="#999",
               markeredgecolor="black", markersize=10, label="Road-favored crew"),
        Line2D([0], [0], color="gray", linestyle="--", linewidth=1,
               label=f"League avg ({league_avg_fouls:.1f} fouls/g)")]
    legend = ax.legend(handles=legend_elements, loc="upper center",
                       bbox_to_anchor=(0.5, -0.12), ncol=3, frameon=True,
                       edgecolor="black", facecolor="white")
    legend.get_frame().set_linewidth(2)

    _style(ax, "Tonight's WNBA Referee Crews // Foul Volume vs Home/Away Bias",
           "Crew Avg Fouls Per Game", "Foul Differential (+ = more road fouls)")
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.18)
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black",
                bbox_inches="tight")
    plt.close()
    print(f"  ref foul chart -> {output_path} ({len(gdf)} games)")
    return output_path


def main():
    print("=== WNBA chart generation ===")
    df = _load_value()
    if df.empty:
        print("No WNBA value data // run build_wnba_projections.py first.")
        return
    generate_value_chart(df)
    generate_upside_chart(df)
    generate_dvp_heatmap()
    generate_dvp_position_heatmap()
    generate_ref_foul_chart()
    print("Done.")


if __name__ == "__main__":
    main()
