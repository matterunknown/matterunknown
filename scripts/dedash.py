#!/usr/bin/env python3
"""Replace em dashes in site copy with real punctuation.

Em dashes read as an AI tell, so the site does without them. The nightly
model rebuilds on the server re-emit text containing them, so run this
over src/ after a regeneration:

    python3 scripts/dedash.py $(find src -name "*.astro") public/rss.xml


Only rewrites prose: text between tags, plus description/alt/title
attribute values. Frontmatter, <style>, <script> and tag syntax are left
byte-identical, so imports, CSS and rgba() alpha values cannot be touched.
"""
import re, sys
from pathlib import Path

PROTECT = re.compile(
    r"(\A---\n.*?\n---\n|<style[^>]*>.*?</style>|<script[^>]*>.*?</script>)",
    re.S,
)
TAG = re.compile(r"(<[^>]*>)", re.S)
PROSE_ATTR = re.compile(r'\b(description|alt|title|content)="([^"]*)"')
CONJ = r"(?:and|but|so|or|yet|nor|not|which|who|because|while|though|although|until|unless|then)\b"
DET = r"(?:the|a|an|its|their|his|her|our|one|every|no|not|just|this|that|these|those)\b"


def fix_prose(t: str) -> str:
    if "—" not in t and "–" not in t:
        return t

    # numeric ranges -> hyphen
    t = re.sub(r"(\d)\s*[—–]\s*(\d)", r"\1-\2", t)

    # paired aside: commas normally, parentheses when the aside has commas
    def paired(m):
        inner = m.group(1)
        return f" ({inner}) " if "," in inner else f", {inner}, "

    prev = None
    while prev != t:
        prev = t
        t = re.sub(r"(?<=[a-z0-9\)\]])\s+—\s+([^—\.!?]{1,140}?)\s+—\s+(?=[a-z])", paired, t)

    def single(m):
        after = m.group(1)
        if re.match(rf"^{CONJ}", after, re.I):
            return ", " + after
        tail = re.split(r"(?<=[\.!?])\s", after, 1)[0]
        # noun phrase after a determiner is an appositive: a full stop would
        # leave a fragment, so use a colon (long) or a comma (short)
        if re.match(rf"^{DET}", after, re.I):
            return (": " if len(tail) >= 45 else ", ") + after
        if len(tail) < 45:
            return ", " + after
        return ". " + after[0].upper() + after[1:]

    t = re.sub(r"(?<=[a-zA-Z0-9,\)\]\"'])\s+—\s+([a-zA-Z][^\n]*)", single, t)
    t = t.replace(" — ", ", ").replace("—", ", ").replace("–", "-")

    # safe prose tidy only: never touch '.' or ':' spacing (breaks code/CSS)
    t = re.sub(r",\s*,+", ", ", t)
    t = re.sub(r"\s+,", ",", t)
    t = re.sub(r"::+", ":", t)
    t = re.sub(r",\s*:", ":", t)
    return t


def process(src: str) -> str:
    out = []
    for chunk in PROTECT.split(src):
        if not chunk:
            continue
        if PROTECT.fullmatch(chunk):
            out.append(chunk)                      # untouched
            continue
        for tok in TAG.split(chunk):
            if tok.startswith("<") and tok.endswith(">"):
                tok = PROSE_ATTR.sub(
                    lambda m: f'{m.group(1)}="{fix_prose(m.group(2))}"', tok
                )
                out.append(tok)
            else:
                out.append(fix_prose(tok))
    return "".join(out)


if __name__ == "__main__":
    n = 0
    for f in sys.argv[1:]:
        p = Path(f)
        src = p.read_text()
        new = process(src)
        if new != src:
            p.write_text(new)
            n += 1
    print(f"rewrote {n} files")
