def _base_template(content: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body {{ margin: 0; padding: 0; background: #f5f5f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}
.container {{ max-width: 560px; margin: 0 auto; background: #ffffff; }}
.header {{ background: #1a1a2e; padding: 32px 40px; text-align: center; }}
.header h1 {{ color: #ffffff; font-family: 'Marvel', sans-serif; font-size: 28px; letter-spacing: 6px; margin: 0; }}
.body {{ padding: 40px; color: #1a1a2e; line-height: 1.6; }}
.body h2 {{ font-size: 22px; margin: 0 0 16px 0; color: #1a1a2e; }}
.body p {{ font-size: 15px; margin: 0 0 16px 0; color: #333; }}
.stat {{ font-size: 32px; font-weight: 700; color: #1a1a2e; margin: 8px 0; }}
.stat-label {{ font-size: 13px; color: #666; text-transform: uppercase; letter-spacing: 1px; }}
.stat-row {{ display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid #eee; }}
.cta {{ display: inline-block; background: #1a1a2e; color: #ffffff; padding: 14px 32px; text-decoration: none; font-weight: 600; letter-spacing: 1px; margin: 24px 0; }}
.footer {{ padding: 24px 40px; text-align: center; font-size: 12px; color: #999; border-top: 1px solid #eee; }}
.win {{ color: #22c55e; }}
.loss {{ color: #ef4444; }}
</style>
</head>
<body>
<div class="container">
<div class="header"><h1>PIRTDICA</h1></div>
<div class="body">
{content}
</div>
<div class="footer">
PIRTDICA Sports Co. &mdash; Competitive Fantasy Analytics<br>
<a href="https://pirtdica.com" style="color: #666;">pirtdica.com</a>
</div>
</div>
</body>
</html>"""


def welcome_email(username: str) -> tuple[str, str]:
    subject = "Welcome to PIRTDICA"
    content = f"""
<h2>Welcome, {username}.</h2>
<p>You've joined a competitive analytics platform, not a casual fantasy app. Every decision here is tracked, ranked, and measured.</p>
<p>You start with <strong>100 Coach Coin</strong>. Here's what matters:</p>
<p><strong>Beat the House</strong> &mdash; Your lineup vs our AI's lineup. Daily.</p>
<p><strong>Coach vs Coach</strong> &mdash; Head-to-head matches with ELO-based ranking.</p>
<p><strong>Climb the ranks</strong> &mdash; Bronze to Champion. Your MMR tells the truth.</p>
<a href="https://pirtdica.com/play" class="cta">ENTER TODAY'S CONTEST</a>
"""
    return subject, _base_template(content)


def contest_result_email(username: str, score: float, house_score: float,
                         beat_house: bool, rank: int = 0) -> tuple[str, str]:
    diff = abs(score - house_score)
    if beat_house:
        subject = f"You beat the house by {diff:.1f} FP"
        result_class = "win"
        result_text = "WIN"
    else:
        if diff < 10:
            subject = f"Close loss — {diff:.1f} FP behind the house"
        else:
            subject = "Contest result posted"
        result_class = "loss"
        result_text = "LOSS"

    content = f"""
<h2>Contest Result</h2>
<div style="text-align: center; margin: 24px 0;">
<div class="stat-label">YOUR SCORE</div>
<div class="stat">{score:.1f} <span style="font-size: 16px;">FP</span></div>
<div class="stat-label">VS HOUSE</div>
<div class="stat">{house_score:.1f} <span style="font-size: 16px;">FP</span></div>
<div class="stat-label">RESULT</div>
<div class="stat {result_class}">{result_text}</div>
</div>
<p>{username}, {'you outperformed the AI.' if beat_house else 'the house had the edge this time.'}</p>
<a href="https://pirtdica.com/history" class="cta">VIEW DETAILS</a>
"""
    return subject, _base_template(content)


def rank_change_email(username: str, old_mmr: int, new_mmr: int,
                      mmr_change: int, division: str,
                      promoted: bool = False) -> tuple[str, str]:
    if promoted:
        subject = f"Promoted to {division}"
        content = f"""
<h2>Division Promotion</h2>
<div style="text-align: center; margin: 24px 0;">
<div class="stat-label">NEW DIVISION</div>
<div class="stat">{division}</div>
<div class="stat-label">MMR</div>
<div class="stat">{new_mmr}</div>
</div>
<p>{username}, you earned your way into {division}. The competition gets tougher from here.</p>
<a href="https://pirtdica.com/leaderboard" class="cta">VIEW LEADERBOARD</a>
"""
    else:
        direction = "climbed" if mmr_change > 0 else "dropped"
        subject = f"MMR update: {old_mmr} → {new_mmr}"
        content = f"""
<h2>MMR Update</h2>
<div style="text-align: center; margin: 24px 0;">
<div class="stat-label">MMR CHANGE</div>
<div class="stat {'win' if mmr_change > 0 else 'loss'}">{mmr_change:+d}</div>
<div class="stat-label">CURRENT MMR</div>
<div class="stat">{new_mmr}</div>
<div class="stat-label">DIVISION</div>
<div class="stat">{division}</div>
</div>
<p>{username}, you {direction} to {new_mmr} MMR. Division: {division}.</p>
<a href="https://pirtdica.com/leaderboard" class="cta">VIEW LEADERBOARD</a>
"""
    return subject, _base_template(content)
