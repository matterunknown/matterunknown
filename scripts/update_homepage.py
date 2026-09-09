#!/usr/bin/env python3
"""
Updates the /now page with live data from the models. Runs nightly after
the model rebuilds complete.

Targets `data-stat="..."` attributes in src/pages/now.astro rather than
matching prose, so copy edits on the page can't silently break the job.
Anything it cannot read is left untouched.

(Filename kept for the existing cron entry; this last targeted the
homepage before the 2026 redesign moved live stats to /now.)
"""

import json, sqlite3, re, subprocess
from datetime import datetime
from pathlib import Path

SITE = Path('/opt/orchid/apps/matterunknown')
NOW  = SITE / 'src/pages/now.astro'


def set_stat(content: str, key: str, value: str) -> str:
    """Replace the text inside the element carrying data-stat="key"."""
    pattern = re.compile(r'(data-stat="' + re.escape(key) + r'"[^>]*>)[^<]*')
    updated, n = pattern.subn(lambda m: m.group(1) + value, content)
    if n == 0:
        print(f"  ! no data-stat=\"{key}\" found — skipped")
    return updated


def sqlite_count(db: str, table: str = 'cards') -> int | None:
    try:
        conn = sqlite3.connect(db)
        n = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        conn.close()
        return n
    except Exception as e:
        print(f"  ! {db}: {e}")
        return None


def run():
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Updating /now …")
    if not NOW.exists():
        print(f"  ! {NOW} not found — nothing to do")
        return

    content = original = NOW.read_text()

    # Pokémon
    n = sqlite_count('/opt/orchid/apps/pokemon-model/db/cards.db')
    if n:
        print(f"  Pokemon: {n:,} cards")
        content = set_stat(content, 'pokemon-cards', f'{n:,}')

    # One Piece
    n = sqlite_count('/opt/orchid/apps/onepiece-model/db/cards.db')
    if n:
        print(f"  One Piece: {n:,} cards")
        content = set_stat(content, 'onepiece-cards', f'{n:,}')

    # Garbage Pail Kids
    n = sqlite_count('/opt/orchid/apps/gpk-model/db/cards.db')
    if n:
        print(f"  GPK: {n:,} cards")
        content = set_stat(content, 'gpk-cards', f'{n:,}')

    # Crypto
    try:
        d = json.load(open('/opt/orchid/apps/crypto-model/results.json'))
        assets = d.get('results', [])
        buys = sum(1 for a in assets if a.get('signal') == 'BUY')
        print(f"  Crypto: {len(assets)} assets | {buys} BUY")
        content = set_stat(content, 'crypto-assets', str(len(assets)))
        content = set_stat(content, 'crypto-buys', str(buys))
    except Exception as e:
        print(f"  ! crypto: {e}")

    # Quant
    try:
        d = json.load(open('/opt/orchid/apps/vessel-trading/portfolio.json'))
        pos = len(d.get('positions', []))
        print(f"  Trading: {pos} positions")
        content = set_stat(content, 'quant-positions', str(pos))
    except Exception as e:
        print(f"  ! trading: {e}")

    if content == original:
        print("  No changes needed")
        return

    # Only stamp the as-of date when real numbers actually moved.
    content = set_stat(content, 'asof', f'{datetime.now():%-d %B %Y}')
    NOW.write_text(content)

    git = ['git', '-C', str(SITE)]
    subprocess.run(git + ['add', 'src/pages/now.astro'], capture_output=True)
    subprocess.run(git + ['commit', '-m',
                          f'chore: auto-update /now stats {datetime.now():%Y-%m-%d}'],
                   capture_output=True)
    r = subprocess.run(git + ['push', 'origin', 'main'], capture_output=True, text=True)
    print(f"  Pushed: {r.stdout.strip() or r.stderr.strip() or 'ok'}")
    print("Done.")


if __name__ == '__main__':
    run()
