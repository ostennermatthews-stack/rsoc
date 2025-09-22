#!/usr/bin/env python3
"""
EMEA Feed Relay — generate a *filtered, tiered* RSS for Security Ops

Goal
- Pull multiple RSS/Atom sources (tiered by signal strength),
- Filter hard for incidents/unrest/security/environment/transport,
- Exclude sports/lifestyle/culture noise,
- Output a single RSS file you can host (e.g., GitHub Pages or S3),
- Subscribe to that URL in Slack using the official **RSS** app (no webhooks needed).

Usage
    pip install feedparser feedgen
    python emea_feed_relay.py --output emea-filtered.xml --tiers tier1,tier2,tier3 --max-items 250

Common options
    --tiers tier1          # only high-signal tier
    --tiers tier1,tier2    # add contextual geopolitics
    --force                # ignore include filters (still applies exclude) — quick sanity check
    --since-hours 24       # only include items published in the last N hours (best-effort)

Slack
- Host the XML (e.g., https://<your-gh-pages>/emea-filtered.xml)
- In Slack channel: /feed subscribe https://<your-gh-pages>/emea-filtered.xml

GitHub Actions (optional; add to .github/workflows/publish.yml)
    name: Build RSS
    on: { schedule: [ { cron: '*/5 * * * *' } ], workflow_dispatch: {} }
    jobs:
      build:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with: { python-version: '3.11' }
          - run: pip install feedparser feedgen
          - run: python emea_feed_relay.py --output emea-filtered.xml --tiers tier1,tier2,tier3 --max-items 250
          - name: Upload artifact (for Pages or S3)
            uses: actions/upload-artifact@v4
            with:
              name: emea-filtered
              path: emea-filtered.xml

Optional: publish via GitHub Pages (e.g., deploy the XML to /docs and enable Pages), or sync the file to S3 in another step.

"""
from __future__ import annotations
import argparse
import hashlib
import html
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

import feedparser
from feedgen.feed import FeedGenerator

# ------------------------------
# Feeds (Tiered)
# ------------------------------
# Tier 1 — incident/unrest heavy: poll these most frequently in your scheduler
TIER1_FEEDS: List[str] = [
    # OSAC (master). You can also add country-filtered variants.
    "https://www.osac.gov/RSS",
    # UK FCDO Travel Advice — add the countries you care about (examples):
    "https://www.gov.uk/foreign-travel-advice/france.atom",
    "https://www.gov.uk/foreign-travel-advice/germany.atom",
    "https://www.gov.uk/foreign-travel-advice/spain.atom",
    "https://www.gov.uk/foreign-travel-advice/turkiye.atom",
    # Meteoalarm (pan-EU + per-country where signal matters)
    "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe",
    "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-united-kingdom",
    # National Highways (England) — surface transport incidents
    "https://www.nationalhighways.co.uk/feeds/rss/UnplannedEvents.xml",
    # ReliefWeb (Europe region)
    "https://reliefweb.int/updates/rss?region=23",
]

# Tier 2 — contextual geopolitics/security
TIER2_FEEDS: List[str] = [
    "https://www.nato.int/cps/en/natohq/press_releases.htm?format=xml",
    "https://www.osce.org/feeds/newsroom",
    "https://www.consilium.europa.eu/en/rss/pressreleases.ashx",
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml?edition=int",
    "https://www.euronews.com/rss?level=theme&name=my-europe",
    "https://www.europol.europa.eu/rss/news",
]

# Tier 3 — wildcards/sector-specific enrichers (add sparingly)
TIER3_FEEDS: List[str] = [
    "https://www.ifrc.org/rss/disaster-news",
    "https://www.crisisgroup.org/rss.xml?region=74",  # Europe
    # Maritime & Aviation often need scraping/APIs; add your own wrappers here later
]

FEEDS_BY_TIER: Dict[str, List[str]] = {
    "tier1": TIER1_FEEDS,
    "tier2": TIER2_FEEDS,
    "tier3": TIER3_FEEDS,
}

# ------------------------------
# Filters (Tiered)
# ------------------------------
# Global EXCLUDES — remove sports/lifestyle/culture noise universally
GLOBAL_EXCLUDE_PATTERNS: List[str] = [
    r"\b(sport|football|soccer|rugby|tennis|golf|cricket|olympic|f1|motorsport)\b",
    r"\b(entertainment|celebrity|fashion|lifestyle|culture|arts|music|movie|tv|theatre|theater)\b",
    r"\b(award|festival|red carpet|premiere|concert|tour)\b",
]

# Region hints — if present, we’re more likely to include (EMEA-centric)
REGION_HINTS: List[str] = [
    # Macro regions
    r"europe|middle east|mena|north africa|sahel|caucasus|balkans",
    # Selected countries/aliases — extend to your watchlist
    r"uk|united kingdom|england|scotland|wales|northern ireland|ireland",
    r"france|germany|spain|portugal|italy|benelux|belgium|netherlands|luxembourg",
    r"poland|czech|slovak|hungary|romania|bulgaria|greece|turkiye|turkey",
    r"sweden|norway|denmark|finland|iceland|baltic|estonia|latvia|lithuania",
    r"ukraine|moldova|belarus|russia",
    r"israel|palestine|west bank|gaza|lebanon|syria|jordan|iraq|iran|egypt|algeria|morocco|tunisia|libya",
]

# Tier-specific INCLUDES — tuned to incident vs context
TIER_INCLUDE_PATTERNS: Dict[str, List[str]] = {
    # Strong incident/unrest signal
    "tier1": [
        r"security|threat|terror|attack|bomb|blast|explos|shoot|hostage|gunfire|armed",
        r"protest|demonstration|riot|unrest|clashes|strike|walkout|picket|blockade",
        r"evacuate|evacuation|lockdown|shelter in place",
        r"cyber|ransom|malware|ddos|breach|hacker|intrusion|espionage|spy",
        r"earthquake|flood|storm|hurricane|typhoon|heatwave|wildfire|landslide|tsunami",
        r"weather warning|meteoalarm|amber warning|red warning|yellow warning",
        r"closure|disruption|cancel|cancellation|grounded|delay|diverted|shutdown",
        r"airport|airspace|runway|rail|train|metro|tram|motorway|highway|port|harbour|harbor|ferry",
    ],
    # Broader policy/security (less strict)
    "tier2": [
        r"sanction|mobilization|mobilisation|border|security|counterterror|counter-terror|extremism",
        r"attack|explos|clashes|protest|strike|unrest",
    ],
    # Wildcards (keep fairly permissive but still incident-biased)
    "tier3": [
        r"disaster|emergency|outage|disruption|incident|accident|alert|warning",
        r"earthquake|flood|storm|wildfire|heatwave",
    ],
}

# Terms that, if present, flag urgency (for Slack formatting downstream if you choose to parse this feed)
URGENT_TERMS: List[str] = [
    r"explosion|mass casualty|fatalities|red warning|state of emergency|airport closed|airspace closed|terror attack",
]

# ------------------------------
# Helpers
# ------------------------------
@dataclass
class Item:
    title: str
    link: str
    summary: str
    published_ts: float  # epoch seconds
    source: str
    tier: str
    urgent: bool


def compile_res(patterns: Iterable[str]) -> List[re.Pattern]:
    return [re.compile(p, re.I) for p in patterns]

EXCL_RE = compile_res(GLOBAL_EXCLUDE_PATTERNS)
REGION_RE = compile_res(REGION_HINTS)
URGENT_RE = compile_res(URGENT_TERMS)
TIER_INCL_RE: Dict[str, List[re.Pattern]] = {k: compile_res(v) for k, v in TIER_INCLUDE_PATTERNS.items()}


def text_relevant(text: str, tier: str, force: bool = False) -> bool:
    t = text.lower()
    if any(rx.search(t) for rx in EXCL_RE):
        return False
    if force:
        return True
    # require an include match OR a region hint + soft-include
    if any(rx.search(t) for rx in TIER_INCL_RE.get(tier, [])):
        return True
    if any(rx.search(t) for rx in REGION_RE) and any(rx.search(t) for rx in TIER_INCL_RE.get("tier3", [])):
        return True
    return False


def is_urgent(text: str) -> bool:
    t = text.lower()
    return any(rx.search(t) for rx in URGENT_RE)


def safe_pub_ts(entry) -> float:
    # Try multiple fields; fall back to now
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try:
                return time.mktime(v)
            except Exception:
                pass
    return time.time()


def pull_feed(url: str) -> feedparser.FeedParserDict:
    # feedparser handles HTTP/HTTPS/redirects; no requests session needed
    return feedparser.parse(url)


def harvest(tier: str, urls: Iterable[str], since_hours: int, force: bool) -> List[Item]:
    items: List[Item] = []
    cutoff = None
    if since_hours > 0:
        cutoff = time.time() - since_hours * 3600
    for url in urls:
        try:
            feed = pull_feed(url)
        except Exception:
            continue
        source_name = feed.feed.get("title") or url
        for e in feed.entries:
            title = e.get("title", "").strip()
            link = e.get("link", "").strip()
            if not title or not link:
                continue
            summary = html.unescape((e.get("summary") or e.get("description") or "").strip())
            joined = f"{title} {summary}"
            if cutoff is not None and safe_pub_ts(e) < cutoff:
                continue
            if not text_relevant(joined, tier=tier, force=force):
                continue
            items.append(
                Item(
                    title=html.unescape(title),
                    link=link,
                    summary=summary,
                    published_ts=safe_pub_ts(e),
                    source=source_name,
                    tier=tier,
                    urgent=is_urgent(joined),
                )
            )
    return items


def dedupe_and_sort(items: List[Item], max_items: int) -> List[Item]:
    seen: set[str] = set()
    out: List[Item] = []
    for it in sorted(items, key=lambda x: x.published_ts, reverse=True):
        h = hashlib.sha256((it.title + "|" + it.link).encode("utf-8")).hexdigest()
        if h in seen:
            continue
        seen.add(h)
        out.append(it)
        if len(out) >= max_items:
            break
    return out


def build_feed(items: List[Item], title: str, homepage: str) -> str:
    fg = FeedGenerator()
    fg.title(title)
    fg.link(href=homepage, rel='alternate')
    fg.description('Merged & filtered EMEA alerts (security, unrest, weather, transport)')
    fg.language('en')
    fg.updated(datetime.now(timezone.utc))

    for it in items:
        fe = fg.add_entry()
        # Prefix title with tier/urgency marker so Slack readers can scan fast
        prefix = "🚨" if it.urgent else ("🛰️" if it.tier == "tier1" else ("🧭" if it.tier == "tier2" else "🧩"))
        fe.title(f"{prefix} [{it.tier.upper()}] {it.title}")
        fe.link(href=it.link)
        desc = it.summary
        if it.source:
            desc = f"<b>Source:</b> {html.escape(it.source)}<br/>" + desc
        fe.description(desc[:2000])  # keep it reasonable
        fe.pubDate(datetime.fromtimestamp(it.published_ts, tz=timezone.utc))
    return fg.rss_str(pretty=True).decode('utf-8')


def main():
    ap = argparse.ArgumentParser(description="Build a filtered EMEA RSS for Slack RSS app")
    ap.add_argument("--tiers", default="tier1,tier2", help="Comma list: tier1,tier2,tier3")
    ap.add_argument("--since-hours", type=int, default=0, help="Only include items newer than N hours (0=disabled)")
    ap.add_argument("--max-items", type=int, default=250, help="Cap total items in output feed")
    ap.add_argument("--output", default="emea-filtered.xml", help="Output RSS file path")
    ap.add_argument("--title", default="EMEA SOC Filtered Feed", help="Feed title")
    ap.add_argument("--homepage", default="https://example.org/emea-filtered", help="Feed link/homepage")
    ap.add_argument("--force", action="store_true", help="Bypass include filters (still applies global excludes)")
    args = ap.parse_args()

    tiers = [t.strip() for t in args.tiers.split(',') if t.strip()]
    for t in tiers:
        if t not in FEEDS_BY_TIER:
            raise SystemExit(f"Unknown tier '{t}'. Use tier1,tier2,tier3")

    all_items: List[Item] = []
    for t in tiers:
        urls = FEEDS_BY_TIER[t]
        all_items.extend(harvest(t, urls, since_hours=args.since_hours, force=args.force))

    final_items = dedupe_and_sort(all_items, max_items=args.max_items)
    xml = build_feed(final_items, title=args.title, homepage=args.homepage)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Wrote {args.output} with {len(final_items)} items")


if __name__ == "__main__":
    main()
