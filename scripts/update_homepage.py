#!/usr/bin/env python3
"""
Auto-updates homepage project card copy with live data from all models.
Runs nightly after all model rebuilds complete.
"""

import json, sqlite3, re, subprocess
from datetime import datetime
from pathlib import Path

SITE  = Path('/opt/orchid/apps/matterunknown')
INDEX = SITE / 'src/pages/index.astro'

def get_pokemon():
    try:
        conn = sqlite3.connect('/opt/orchid/apps/pokemon-model/db/cards.db')
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM cards'); total = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM cards WHERE signal="BUY"'); buys = c.fetchone()[0]
        conn.close()
        return total, buys
    except: return None, None

def get_crypto():
    try:
        d = json.load(open('/opt/orchid/apps/crypto-model/results.json'))
        assets = d.get('results', [])
        buys = sum(1 for a in assets if a.get('signal') == 'BUY')
        fg   = d.get('market_context', {}).get('fear_greed', {}).get('value')
        return len(assets), buys, fg
    except: return None, None, None

def get_trading():
    try:
        d = json.load(open('/opt/orchid/apps/vessel-trading/portfolio.json'))
        return len(d.get('positions', [])), d.get('total_pnl', 0)
    except: return None, None

def fmt_count(n):
    rounded = round(n / 500) * 500
    return f"{rounded:,}+" if n >= 10000 else f"{n:,}"

def fg_str(fg):
    if fg is None: return "Market sentiment tracked daily"
    label = "extreme fear" if fg<=20 else "fear" if fg<=40 else "neutral" if fg<=60 else "greed" if fg<=80 else "extreme greed"
    return f"Fear &amp; Greed at {fg} — {label}"

def run():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Updating homepage...")
    content = original = INDEX.read_text()

    # Pokemon
    total, buys = get_pokemon()
    if total:
        fmt = fmt_count(total)
        print(f"  Pokemon: {total:,} cards | {buys} BUY")
        content = re.sub(
            r'(<p>)[\d,\+]+ cards scored across 172 sets\..*?(</p>)',
            f'\\g<1>{fmt} cards scored across 172 sets. Vintage, modern, and sealed — ranked by rarity, era momentum, price signal, and CM trend. Top 25 updated daily.\\g<2>',
            content)
        content = re.sub(
            r'Live — [\d,\+]+ cards(?=</div><span class="proj-arrow">)',
            f'Live — {fmt} cards', content)

    # Crypto
    n_assets, n_buys, fg = get_crypto()
    if n_assets:
        buy_str = f"{n_buys} buy signal{'s' if n_buys!=1 else ''}"
        print(f"  Crypto: {n_assets} assets | {n_buys} BUY | FG: {fg}")
        content = re.sub(
            r'(<p>)\d+ assets scored daily.*?(</p>)',
            f'\\g<1>{n_assets} assets scored daily across price signals, market structure, and quality. {fg_str(fg)}. AI/data infrastructure tier tracked separately.\\g<2>',
            content)
        content = re.sub(
            r'Live — \d+ buy signals?(?=</div><span class="proj-arrow">)',
            f'Live — {buy_str}', content)

    # Trading
    n_pos, pnl = get_trading()
    if n_pos is not None:
        print(f"  Trading: {n_pos} positions")
        content = re.sub(r'\d+ positions open\.', f'{n_pos} positions open.', content)

    if content != original:
        INDEX.write_text(content)
        subprocess.run(['git','-C',str(SITE),'add','src/pages/index.astro'], capture_output=True)
        subprocess.run(['git','-C',str(SITE),'commit','-m',
            f'chore: auto-update homepage stats {datetime.now().strftime("%Y-%m-%d")}'],
            capture_output=True)
        r = subprocess.run(['git','-C',str(SITE),'push','origin','main'], capture_output=True, text=True)
        print(f"  Pushed: {r.stdout.strip() or 'ok'}")
    else:
        print("  No changes needed")
    print("Done.")

if __name__ == '__main__':
    run()
