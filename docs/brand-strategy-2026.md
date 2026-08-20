# matterunknown — Brand Strategy Proposal (Aug 2026)

A full-site review (47 pages) and a proposed recharter: retire Vessel, sharpen the
identity thesis, present the fun projects honestly, and remove the Fund page.

## The short version

The site currently describes itself four different ways — "a personal think tank"
(home), "a think tank run by one person and one AI" (about), "an independent identity
security research lab" (start), "closer to a research institution… the Salk Institute
or DeepMind" (mission). Vessel, though retired, is still referenced on **33 pages
beyond its own 12-page section**.

**Proposed positioning, one line, used everywhere:**

> matterunknown is Ryan Silver's independent research lab. Serious work on identity —
> especially the identity of AI agents — and niche projects built for the joy of it.
> Everything documented in public.

Two lanes, one voice:
- **The Research** — identity, with the agentic-identity program (the July essay
  cluster: intent tokens, "The Workload is the Reasoning Session") as the flagship,
  and the Identity Model framework (`/model`, currently orphaned from nav and
  sitemap) as the intellectual spine.
- **The Lab** — card models, quant scanner, Roblox game, client builds — labeled as
  what they are: niche obsessions built openly, not "revenue experiments."

The unifier is not the subject matter; it's the transparency.

## What the review found (ranked by damage)

1. **The "Ask Vessel" chat is simulated** — five hardcoded canned responses —
   while /start says "Talk to Vessel — ask it anything." Direct contradiction of
   the visible-methodology thesis. Comes down first.
2. **The Fund page is anti-marketing.** $0 of $3,500 (0%), an all-"Pending" revenue
   log, crypto-only donations with an unfinished Monero address, and a stale
   hardware wishlist (M4 Max, Llama 3.3) — pushed by the nav's only CTA on every page.
3. **Positioning drift.** Four self-descriptions; "we" (start) vs. "one person"
   (about); an anonymity principle on /about while the homepage quote is signed
   "— Ryan Silver" and /studio names a real client and the author's son.
4. **The Lab undermines the research.** 5 of 11 project pages are collectibles/
   finance; the security flagships (Substrate, Breach Report, Demo Assistant,
   Identity Jobbot) are dead `href="#"` cards claiming "Live"; "Access Granted —
   erotic thrillers" is listed on /projects and as a revenue line on /fund.
5. **Two-speed staleness.** Automated model pages: fresh (nightly). Human pages:
   newest essay Jul 2026, Vessel frozen Apr 2026. Plus contradictions: Broken Heart
   Ohio "Now live" vs. "Going live soon"; Pokémon card counts of 17,000+/17,428/
   17,500+/10,000+ across four pages.
6. **Quiet breakage:** `og.png` referenced sitewide but missing (blank social
   shares); hand-maintained sitemap missing ~20 live pages; fonts loaded twice;
   `global.css` duplicated in `src/` and `public/`.

## Positioning decisions

- **Drop the anonymity framing.** The name is already public (homepage quote,
  Studio). Thought leadership accrues to a person, not an anonymous brand.
  (Alternative: re-establish anonymity — requires scrubbing Studio and the quote.)
- **First person singular.** "I," not "we." One-person honesty reads as capability;
  institution cosplay reads as insecurity.
- **Agentic identity becomes the named flagship research program.**

## Architecture

New global nav — five items, no Fund CTA:

**Research** (essays + /model + agentic-identity program) · **Lab** (all builds,
including Studio's) · **About** (absorbs /mission and /start) · **Now** (living
status page fed by the nightly scripts + a hand-written "currently thinking about"
note) · **Subscribe** (RSS + email)

Lab cards get lane tags — `Research` / `For the joy of it` — replacing the
"Testing: / revenue experiment" framing.

### Page-by-page verdicts

| Page | Verdict | Notes |
|---|---|---|
| /index | Rework | New positioning line; two-lane sections; design unchanged. `scripts/update_homepage.py` regexes must be updated in the same commit. |
| /about | Rework | Absorbs Mission's best material; drops Vessel CTA/note, anonymity section, Salk/DeepMind. |
| /mission, /start | Cut | Merge into /about; redirect. |
| /model | **Promote** | Strongest page on the site. Into nav + sitemap; replace the Vessel outro with the agentic-identity program. |
| /writing | Keep | Remove the "Older writing" drawer (Vessel essays → archive); pin the agentic-identity cluster as a series. |
| /fund | Cut | See below. Redirect to /about. |
| /vessel + 11 subpages | Archive | See Vessel plan. |
| /studio | Merge | Fold into Lab as "for the joy of it"; fix Broken Heart Ohio status contradiction; keep the 10-year-old story. |
| /projects/pokemon-advisor | **Unpublish** | See Pokémon plan. |
| /projects/* models | Keep | Relabel as fun lane; strip "Vessel" from copy; reconcile card counts to one source of truth. |
| Dead cards ×4 | Cut | Remove until each has a real page. Delete the "Access Granted" line outright. |

## Retiring Vessel — in the open

- **Immediately:** delete the simulated chat and every "Talk to Vessel" CTA.
- **Archive, don't delete:** move /vessel content to /archive/vessel with an honest
  retirement header (what it was, when it ran, why it ended); redirect old URLs;
  collapse the four near-identical dream logs to one representative entry.
- **Strip the byline from live tools** (quant trading, Digital Shadow, model pages).
- **Write "Retiring Vessel"** — an essay on ending an AI persona (naming, continuity,
  what an agent's identity was) is squarely inside the agentic-identity thesis and
  doubles as the recharter announcement.
- **Keep the teal.** Rename `--vessel` → `--teal`; use amber = Lab, teal = Research
  as lane coding.

## The Pokémon app

The advisor is currently very public: "Live — try it" on /projects, linked from
/pokemon-model, branded "Vessel Pokémon Advisor," with a tip jar. Plan: delete the
page and both inbound links (it is already absent from the stale sitemap); keep the
code on a branch or an unlisted route for private demos. The **model** page stays
public — it's Lab-lane gold — but stops advertising the app. The app launches later
as its own moment.

## Fund page: kill it, replace the ask with a subscribe

- **Remove** the page, nav CTA, progress bar, revenue log, hardware spec, crypto
  addresses, and Ko-fi overlay. Redirect /fund → /about.
- **Replace** the nav CTA with **Subscribe** (email + RSS). Thought leadership
  monetizes through audience — the list turns essays into invitations, clients,
  and leverage.
- **Demote** Ko-fi to a single quiet footer link, if kept at all.
- **Later:** GitHub Sponsors on the agentic-id repo if it ships open source —
  funding attached to working code, not a wishlist.

Support story going forward: "This is self-funded. If the work is useful,
subscribe, share it, or hire me."

## Roadmap

**Pass 1 — Credibility (~a day):** delete fake chat + CTAs; remove Fund nav/page;
unpublish pokemon-advisor + inbound links; remove dead cards + Access Granted line;
fix contradictions (BHO status, dream-log date, card counts); ship a real og.png
(amber wordmark on #090908); regenerate sitemap; dedupe global.css and font loads.

**Pass 2 — Structure (~a week):** new nav; merge mission/start into about; promote
/model; build /now; propagate the positioning line to all meta descriptions;
first-person voice pass; Vessel → /archive with redirects; `--vessel` → `--teal`;
re-tag the Lab and fold in Studio; update `update_homepage.py` patterns.

**Pass 3 — Content (ongoing):** publish "Retiring Vessel"; name the agentic-identity
program with a series landing page; one research essay a month — cadence over
volume, with /now covering the gaps.

## What doesn't change

The wordmark, amber-on-near-black palette, DM Serif Display + DM Sans + JetBrains
Mono, section-label rules, card grids — the design system stays exactly as is. This
recharter changes what the site *says*, not how it looks. The only visual additions
needed are the missing og.png and a favicon drawn from the wordmark.
