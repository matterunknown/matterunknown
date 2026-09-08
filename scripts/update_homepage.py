#!/usr/bin/env python3
"""
Auto-updates the /now page with live data from all models.
Runs nightly after all model rebuilds complete.

(Formerly targeted index.astro; the 2026 redesign moved live stats to
src/pages/now.astro — the homepage is stat-free by design.)
"""

import json, sqlite3, re, subprocess
from datetime import datetime
from pathlib import Path

SITE = Path('/opt/orchid/apps/matterunknown')
NOW  = SITE / 'src/pages/now.astro'

def get_pokemon():
    try:
        conn = sqlite3.connect('/opt/orchid/apps/pokemon-model/db/cards.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM cards'); total = c.fetchone()[0]
        conn.close()
        return total
    except: return None

def get_crypto():
    try:
        d = json.load(open('/opt/orchid/apps/crypto-model/results.json'))
        assets = d.get('results', [])
        buys = sum(1 for a in assets if a.get('signal') == 'BUY')
        return len(assets), buys
    except: return None, None

def get_trading():
    try:
        d = json.load(open('/opt/orchid/apps/vessel-trading/portfolio.json'))
        return len(d.get('positions', []))
    except: return None

def run():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Updating /now...")
    content = original = NOW.read_text()
    today = datetime.now().strftime('%Y-%m-%d')

    total = get_pokemon()
    if total:
        print(f"  Pokemon: {total:,} cards")
        content = re.sub(r'[\d,]+ cards · (\d+) sets', f'{total:,} cards · \\g<1> sets', content, count=1)

    n_assets, n_buys = get_crypto()
    if n_assets:
        print(f"  Crypto: {n_assets} assets | {n_buys} BUY")
        content = re.sub(r'\d+ assets scored daily · \d+ buy signals?',
                         f"{n_assets} assets scored daily · {n_buys} buy signal{'s' if n_buys != 1 else ''}",
                         content)

    n_pos = get_trading()
    if n_pos is not None:
        print(f"  Trading: {n_pos} positions")
        content = re.sub(r'\d+ positions open', f'{n_pos} positions open', content)

    if content != original:
        content = re.sub(r'Model data as of \d{4}-\d{2}-\d{2}', f'Model data as of {today}', content)
        NOW.write_text(content)
        subprocess.run(['git', '-C', str(SITE), 'add', 'src/pages/now.astro'], capture_output=True)
        subprocess.run(['git', '-C', str(SITE), 'commit', '-m',
            f'chore: auto-update /now stats {today}'], capture_output=True)
        r = subprocess.run(['git', '-C', str(SITE), 'push', 'origin', 'main'], capture_output=True, text=True)
        print(f"  Pushed: {r.stdout.strip() or 'ok'}")
    else:
        print("  No changes needed")
    print("Done.")

if __name__ == '__main__':
    run()
