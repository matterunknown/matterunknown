#!/usr/bin/env python3
"""Generative cover art for matterunknown essay cards.

Dense, high-contrast, with real bloom — designed to read as deliberate
artwork at phone-card size (~380px wide), not as unloaded content.
Palette is brand-locked: amber #c4972a on #090908, teal used sparingly.
"""
from PIL import Image, ImageDraw, ImageFilter, ImageChops
from pathlib import Path
import math, random

OUT = Path('public/images/art')
OUT.mkdir(parents=True, exist_ok=True)

W, H = 2400, 1500          # supersampled; exported at 1200x750
BG        = (9, 9, 8)
AMBER     = (196, 151, 42)
AMBER_HOT = (245, 205, 110)
AMBER_DEEP= (120, 88, 24)
TEAL      = (126, 184, 196)


def mix(c, a, base=BG):
    """Blend a colour toward the background by amount a (0..1)."""
    return tuple(int(base[i] + a * (c[i] - base[i])) for i in range(3))


def wash(spots, w=W, h=H):
    """Soft radial background glows, rendered small then upscaled (smooth + fast)."""
    sw, sh = 300, 188
    img = Image.new('RGB', (sw, sh), BG)
    px = img.load()
    for y in range(sh):
        fy = y / sh
        for x in range(sw):
            fx = x / sw
            r, g, b = float(BG[0]), float(BG[1]), float(BG[2])
            for (cx, cy, rx, ry, col, inten) in spots:
                dx = (fx - cx) / rx
                dy = (fy - cy) / ry
                d = math.sqrt(dx * dx + dy * dy)
                if d < 1.0:
                    a = (1.0 - d) ** 2 * inten
                    r += (col[0] - BG[0]) * a
                    g += (col[1] - BG[1]) * a
                    b += (col[2] - BG[2]) * a
            px[x, y] = (min(255, int(r)), min(255, int(g)), min(255, int(b)))
    return img.resize((w, h), Image.LANCZOS)


def compose(bg, strokes, radius=34, strength=0.85, radius2=90, strength2=0.45):
    """Crisp strokes + two-stage bloom over a washed background."""
    out = bg
    glow_near = strokes.filter(ImageFilter.GaussianBlur(radius))
    glow_far = strokes.filter(ImageFilter.GaussianBlur(radius2))
    out = ImageChops.add(out, glow_far.point(lambda v: int(v * strength2)))
    out = ImageChops.add(out, glow_near.point(lambda v: int(v * strength)))
    out = ImageChops.add(out, strokes)
    return out


def save(img, name):
    img.resize((1200, 750), Image.LANCZOS).save(
        OUT / f'{name}.jpg', quality=84, optimize=True, progressive=True)
    kb = (OUT / f'{name}.jpg').stat().st_size // 1024
    print(f'  {name}.jpg  {kb} KB')


# ── 1. vessel — dense concentric ring field, hot core ───────────────────────
def art_vessel():
    random.seed(17)
    bg = wash([(.52, .5, .78, .95, AMBER, .30), (.5, .5, .3, .42, AMBER, .34),
               (.12, .85, .5, .6, TEAL, .07)])
    s = Image.new('RGB', (W, H), (0, 0, 0))
    d = ImageDraw.Draw(s)
    cx, cy = W * .52, H * .5
    r = 26
    while r < 1180:
        n = random.choice([1, 2, 2, 3, 3, 4])
        start = random.uniform(0, 360)
        near = max(0.0, 1.0 - r / 1180) ** 1.35        # brighter toward the core
        for i in range(n):
            a0 = start + i * (360 / n) + random.uniform(0, 18)
            span = random.uniform(30, max(34, 360 / n - 12))
            hot = random.random() < .16
            col = mix(AMBER_HOT if hot else AMBER,
                      (.95 if hot else .30 + .62 * near) * (.45 + .55 * near),
                      (0, 0, 0))
            d.arc([cx - r, cy - r, cx + r, cy + r], a0, a0 + span,
                  fill=col, width=random.choice([3, 4, 4, 6]))
        r += random.uniform(15, 26)
    d.ellipse([cx - 13, cy - 13, cx + 13, cy + 13], fill=AMBER_HOT)
    save(compose(bg, s, 30, .95, 110, .55), 'vessel')


# ── 2. credential — dense ledger of records, one live band ──────────────────
def art_credential():
    random.seed(4)
    bg = wash([(.30, .48, .85, 1.0, AMBER, .26), (.85, .18, .45, .55, AMBER, .14),
               (.9, .9, .4, .5, TEAL, .06)])
    s = Image.new('RGB', (W, H), (0, 0, 0))
    d = ImageDraw.Draw(s)
    rows, top, gap = 26, 58, 55
    live = 12
    for row in range(rows):
        y = top + row * gap
        dist = abs(row - live)
        near = max(0.0, 1.0 - dist / 13)
        x = random.uniform(-120, 40)
        while x < W + 100:
            seg = random.uniform(70, 330)
            if row == live:
                col = AMBER_HOT
            elif dist <= 2:
                col = mix(AMBER_HOT, .40 + .30 * near, (0, 0, 0))
            else:
                col = mix(AMBER, .10 + .42 * near, (0, 0, 0))
            h = 16 if row == live else (13 if dist <= 2 else 10)
            d.rounded_rectangle([x, y, x + seg, y + h], h // 2, fill=col)
            x += seg + random.uniform(26, 78)
    save(compose(bg, s, 26, .8, 95, .5), 'credential')


# ── 3. workload — dense flow field, one traced session ──────────────────────
def art_workload():
    random.seed(9)
    bg = wash([(.5, .52, .95, 1.0, AMBER, .24), (.18, .2, .5, .6, AMBER, .16),
               (.85, .82, .45, .55, TEAL, .10)])
    s = Image.new('RGB', (W, H), (0, 0, 0))
    d = ImageDraw.Draw(s)
    lines, live = 34, 17
    for k in range(lines):
        y0 = 60 + k * ((H - 120) / (lines - 1))
        dist = abs(k - live)
        near = max(0.0, 1.0 - dist / 15)
        ph = k * .38
        f1, f2 = 1.5 + k * .012, 3.1 - k * .01
        a1, a2 = 26 + 9 * math.sin(k), 13
        pts = []
        for x in range(-40, W + 60, 7):
            y = (y0 + a1 * math.sin(ph + f1 * x / 300)
                 + a2 * math.sin(ph * 1.7 + f2 * x / 190))
            pts.append((x, y))
        if k == live:
            col, wdt = AMBER_HOT, 7
        elif dist <= 2:
            col, wdt = mix(AMBER_HOT, .5, (0, 0, 0)), 5
        elif k % 9 == 3:
            col, wdt = mix(TEAL, .16 + .2 * near, (0, 0, 0)), 3
        else:
            col, wdt = mix(AMBER, .12 + .46 * near, (0, 0, 0)), 3
        d.line(pts, fill=col, width=wdt, joint='curve')
    save(compose(bg, s, 24, .8, 90, .5), 'workload')


# ── 4. cards — filled tile heatmap, edge to edge ───────────────────────────
def art_cards():
    random.seed(23)
    bg = wash([(.5, .5, .95, 1.0, AMBER, .16), (.72, .3, .45, .55, AMBER, .15),
               (.15, .85, .45, .5, TEAL, .06)])
    s = Image.new('RGB', (W, H), (0, 0, 0))
    d = ImageDraw.Draw(s)
    cols, rows = 11, 6
    mw, mh = W / cols, H / rows
    pad = 13
    for r in range(rows):
        for c in range(cols):
            x0, y0 = c * mw + pad, r * mh + pad
            x1, y1 = (c + 1) * mw - pad, (r + 1) * mh - pad
            v = random.random()
            if v > .95:
                fill, out, wd = mix(AMBER_HOT, .42, (0, 0, 0)), AMBER_HOT, 5
            elif v > .86:
                fill, out, wd = mix(AMBER, .20, (0, 0, 0)), mix(AMBER_HOT, .62, (0, 0, 0)), 4
            elif v > .60:
                fill, out, wd = mix(AMBER, .075, (0, 0, 0)), mix(AMBER, .38, (0, 0, 0)), 3
            else:
                fill, out, wd = mix(AMBER_DEEP, .07, (0, 0, 0)), mix(AMBER, .17, (0, 0, 0)), 3
            d.rounded_rectangle([x0, y0, x1, y1], 22, fill=fill, outline=out, width=wd)
    save(compose(bg, s, 20, .45, 76, .32), 'cards')


# ── 5. agents — dense constellation, delegated edges ───────────────────────
def art_agents():
    random.seed(31)
    bg = wash([(.42, .45, .9, 1.0, AMBER, .24), (.8, .75, .5, .6, TEAL, .10),
               (.2, .2, .45, .5, AMBER, .16)])
    s = Image.new('RGB', (W, H), (0, 0, 0))
    d = ImageDraw.Draw(s)
    pts = []
    tries = 0
    while len(pts) < 54 and tries < 4000:
        tries += 1
        p = (random.uniform(90, W - 90), random.uniform(80, H - 80))
        if all(math.dist(p, q) > 150 for q in pts):
            pts.append(p)
    for i, p in enumerate(pts):
        for j, q in enumerate(pts):
            if i < j and math.dist(p, q) < 420:
                a = 1.0 - math.dist(p, q) / 420
                d.line([p, q], fill=mix(AMBER, .10 + .40 * a, (0, 0, 0)),
                       width=2 if a < .5 else 3)
    for i, p in enumerate(pts):
        hot = i % 11 == 0
        teal = i % 17 == 5
        rr = random.uniform(7, 15) * (1.5 if hot else 1.0)
        col = TEAL if teal else (AMBER_HOT if hot else mix(AMBER, random.uniform(.45, .95), (0, 0, 0)))
        d.ellipse([p[0] - rr, p[1] - rr, p[0] + rr, p[1] + rr], fill=col)
    save(compose(bg, s, 28, .85, 95, .5), 'agents')


if __name__ == '__main__':
    print('rendering cover art…')
    art_vessel(); art_credential(); art_workload(); art_cards(); art_agents()
    print('done')
