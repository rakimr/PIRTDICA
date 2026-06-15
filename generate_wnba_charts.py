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
Outputs:
  - static/images/wnba_value_chart.png
  - static/images/wnba_upside_chart.png
  - static/images/wnba_dvp_heatmap.png
"""
import os
import sqlite3
from pathlib import Path

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
    cbar.set_label("DVP Factor (Higher = Easier Matchup)", fontweight="bold", color="black")
    cbar.outline.set_color("black")
    cbar.outline.set_linewidth(2)
    ax.set_title("WNBA Defense vs Position - Matchup Heatmap",
                 fontsize=14, fontweight="bold", color="black")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor="white", edgecolor="black")
    plt.close()
    print(f"  dvp heatmap -> {output_path}")
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
    print("Done.")


if __name__ == "__main__":
    main()
