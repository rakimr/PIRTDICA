"""
Manual injury overrides for when RotoGrinders lags behind or misses players.

Run this script to add manual OUT players that aren't being detected by scrapers.
These entries will be merged into the injury_alerts table.

Usage:
    python manual_injuries.py add "Pascal Siakam"
    python manual_injuries.py add "Player Name" --reason "Ankle"
    python manual_injuries.py remove "Pascal Siakam"
    python manual_injuries.py list
"""

import sqlite3
import argparse
from datetime import datetime
from utils.name_normalize import normalize_player_name

def init_table(conn):
    """Create manual injuries table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS manual_injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT UNIQUE,
            status TEXT DEFAULT 'OUT',
            reason TEXT,
            added_at TEXT,
            added_by TEXT DEFAULT 'manual'
        )
    """)
    conn.commit()

def add_injury(player_name, reason="Manual override", status="OUT"):
    """Add a player to manual injuries list."""
    conn = sqlite3.connect('dfs_nba.db')
    init_table(conn)
    
    normalized = normalize_player_name(player_name)
    now = datetime.utcnow().isoformat()
    
    conn.execute("""
        INSERT OR REPLACE INTO manual_injuries (player_name, status, reason, added_at)
        VALUES (?, ?, ?, ?)
    """, (normalized, status, reason, now))
    
    conn.execute("""
        INSERT OR IGNORE INTO injury_alerts (player_name, status, reason, alert_title, scraped_at)
        VALUES (?, ?, ?, ?, ?)
    """, (normalized, status, reason, f"MANUAL: {normalized} {status}", now))
    
    conn.commit()
    conn.close()
    print(f"Added {normalized} as {status} ({reason})")

def remove_injury(player_name):
    """Remove a player from manual injuries list."""
    conn = sqlite3.connect('dfs_nba.db')
    init_table(conn)
    
    normalized = normalize_player_name(player_name)
    
    cursor = conn.execute("DELETE FROM manual_injuries WHERE player_name = ?", (normalized,))
    
    if cursor.rowcount > 0:
        print(f"Removed {normalized} from manual injuries")
    else:
        print(f"Player {normalized} not found in manual injuries")
    
    conn.commit()
    conn.close()

def list_injuries():
    """List all manual injuries."""
    conn = sqlite3.connect('dfs_nba.db')
    init_table(conn)
    
    cursor = conn.execute("SELECT player_name, status, reason, added_at FROM manual_injuries ORDER BY added_at DESC")
    rows = cursor.fetchall()
    
    if rows:
        print("\n=== Manual Injury Overrides ===")
        for row in rows:
            print(f"  {row[0]}: {row[1]} - {row[2]} (added {row[3][:10]})")
    else:
        print("No manual injuries set")
    
    conn.close()

def sync_to_alerts():
    """Sync manual injuries to the injury_alerts table."""
    conn = sqlite3.connect('dfs_nba.db')
    init_table(conn)
    
    now = datetime.utcnow().isoformat()
    
    cursor = conn.execute("SELECT player_name, status, reason FROM manual_injuries")
    for row in cursor.fetchall():
        conn.execute("""
            INSERT OR REPLACE INTO injury_alerts (player_name, status, reason, alert_title, scraped_at)
            VALUES (?, ?, ?, ?, ?)
        """, (row[0], row[1], row[2], f"MANUAL: {row[0]} {row[1]}", now))
    
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Manage manual injury overrides')
    subparsers = parser.add_subparsers(dest='command')
    
    add_parser = subparsers.add_parser('add', help='Add a player to manual injuries')
    add_parser.add_argument('player', help='Player name')
    add_parser.add_argument('--reason', default='Manual override', help='Injury reason')
    add_parser.add_argument('--status', default='OUT', help='Status (OUT, GTD, etc.)')
    
    remove_parser = subparsers.add_parser('remove', help='Remove a player from manual injuries')
    remove_parser.add_argument('player', help='Player name')
    
    subparsers.add_parser('list', help='List all manual injuries')
    subparsers.add_parser('sync', help='Sync manual injuries to alerts table')
    
    args = parser.parse_args()
    
    if args.command == 'add':
        add_injury(args.player, args.reason, args.status)
    elif args.command == 'remove':
        remove_injury(args.player)
    elif args.command == 'list':
        list_injuries()
    elif args.command == 'sync':
        sync_to_alerts()
        print("Manual injuries synced to alerts")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
