#!/usr/bin/env python3
# pip install feedparser feedgen
from __future__ import annotations
import re, time, html, hashlib, argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Iterable
import feedparser
from feedgen.feed import FeedGenerator

# ---------- Feeds ----------
TIER1_FEEDS = [
    "https://www.osac.gov/RSS",
    "https://www.gov.uk/foreign-travel-advice/france.atom",
    "https://www.gov.uk/foreign-travel-advice/germany.atom",
    "https://www.gov.uk/foreign-travel-advice/spain.atom",
    "https://www.gov.uk/foreign-travel-advice/turkiye.atom",
    "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-europe",
    "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-united-kingdom",
    "https://www.met.ie/warningsxml/rss.xml",
    "https://www.nationalhighways.co.uk/feeds/rss/UnplannedEvents.xml",
    "https://reliefweb.int/updates/rss?region=23",
]
TIER2_FEEDS = [
    "https://www.nato.int/cps/en/natohq/press_releases.htm?format=xml",
    "https://www.osce.org/feeds/newsroom",
    "https://www.consilium.europa.eu/en/rss/pressreleases.ashx",
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml?edition=int",
    "https://www.euronews.com/rss?level=theme&name=my-europe",
    "https://www.europol.europa.eu/rss/news",
]
TIER3_FEEDS = [
    "https://www.ifrc.org/rss/disaster-news",
    "https://www.crisisgroup.org/rss.xml?region=74",  # Europe
]
FEEDS_BY_TIER = {"tier1": TIER1_FEEDS, "tier2": TIER2_FEEDS, "tier3": TIER3_FEEDS}

# ---------- Filters ----------
GLOBAL_EXCLUDE = [
    r"\b(sport|football|soccer|rugby|tennis|golf|cricket|olympic|f1|motorsport)\b",
    r"\b(entertainment|celebrity|fashion|lifestyle|culture|arts|music|movie|tv|theatre|theater)\b",
    r"\b(award|festival|red carpet|premiere|concert|tour)\b",
]

REGION_HINTS = [
    r"europe|middle east|mena|north africa|sahel|caucasus|balkans",
    r"uk|united kingdom|england|scotland|wales|northern ireland|ireland",
    r"france|germany|spain|portugal|italy|belgium|netherlands|luxembourg",
    r"poland|czech|slovak|hungary|romania|bulgaria|greece|turkiye|turkey",
    r"sweden|norway|denmark|finland|iceland|estonia|latvia|lithuania",
    r"ukraine|moldova|belarus|russia",
    r"israel|palestine|west bank|gaza|lebanon|syria|jordan|iraq|iran|egypt|algeria|morocco|tunisia|libya",
]

TIER_INCLUDE = {
    "tier1": [
        r"security|threat|terror|attack|bomb|blast|explos|shoot|hostage|gunfire|armed",
        r"protest|demonstration|riot|unrest|clashes|strike|walkout|picket|blockade",
        r"evacuate|evacuation|lockdown|shelter in place",
        r"cyber|ransom|malware|ddos|breach|hacker|intrusion|espionage|spy",
        r"earthquake|flood|storm|heatwave|wildfire|landslide|tsunami",
        r"weather warning|meteoalarm|amber warning|red warning|yellow warning",
        r"closure|disruption|cancel|cancellation|grounded|delay|diverted|shutdown",
        r"airport|airspace|runway|rail|train|metro|tram|motorway|highway|port|harbour|harbor|ferry",
    ],
    "tier2": [
        r"sanction|mobilization|mobilisation|border|security|counterterror|counter-terror|extremism",
        r"attack|explos|clashes|protest|strike|unrest",
    ],
    "tier3": [
        r"disaster|emergency|outage|disruption|incident|accident|alert|warning",
        r"earthquake|flood|storm|wildfire|heatwave",
    ],
}

URGENT_TERMS = [
    r"explosion|mass casualty|fatalities|red warning|state of emergency|airport closed|airspace closed|terror attack",
]

# ---------- Helpers ----------
def _compile(patterns: Iterable[str]): return [re.compile(p, re.I) for p in patterns]
EXCL_RE = _compile(GLOBAL_EXCLUDE)
REGION_RE = _compile(REGION_HINTS)
TIER_INCL_RE = {k: _compile(v) for k, v in TIER_INCLUDE.items()}
URGENT_RE = _compile(URGENT_TERMS)

def _relevant(text: str, tier: str, force: bool = False) -> bool:
    t = text.lower()
    if any(rx.search(t) for rx in EXCL_RE): return False
    if force: return True
    if any(rx.search(t) for rx in TIER_INCL_RE.get(tier, [])): return True
    return any(rx.search(t) for rx in REGION_RE) and any(rx.search(t) for rx in TIER_INCL_RE["tier3"])

def _urgent(text: str) -> bool:
    t = text.lower()
    return any(rx.search(t) for rx in URGENT_RE)

def _pub_ts(entry) -> float:
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try: return time.mktime(v)
            except: pass
    return time.time()

@dataclass
class Item:
    title: str
    link: str
    summary: str
    published_ts: float
    source: str
    tier: str
    urgent: bool

def harvest(tier: str, urls: List[str], since_hours: int, force: bool) -> List[Item]:
    out: List[Item] = []
    cutoff = time.time() - since_hours * 3600 if since_hours > 0 else None
    for url in urls:
        try:
            feed = feedparser.parse(url)
        except Exception:
            continue
        source = feed.feed.get("title") or url
        for e in feed.entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link: continue
            summary = html.unescape((e.get("summary") or e.get("description") or "").strip())
            if cutoff and _pub_ts(e) < cutoff: continue
            joined = f"{title} {summary}"
            if not _relevant(joined, tier, force): continue
            out.append(Item(title=html.unescape(title), link=link, summary=summary,
                            published_ts=_pub_ts(e), source=source, tier=tier, urgent=_urgent(joined)))
    return out

def dedupe_sort(items: List[Item], max_items: int) -> List[Item]:
    seen, out = set(), []
    for it in sorted(items, key=lambda x: x.published_ts, reverse=True):
        h = hashlib.sha256((it.title + "|" + it.link).encode("utf-8")).hexdigest()
        if h in seen: continue
        seen.add(h); out.append(it)
        if len(out) >= max_items: break
    return out

def build_feed(items: List[Item], title: str, homepage: str, replay: int = 0, reseed: str = "") -> str:
    fg = FeedGenerator()
    fg.title(title)
    fg.link(href=homepage, rel='alternate')
    fg.description('Merged & filtered EMEA alerts (security, unrest, weather, transport)')
    fg.language('en')
    now = datetime.now(timezone.utc)
    fg.updated(now)

    labels = {"tier1": "Priority 1 - Investigate", "tier2": "Priority 2 - FYSA", "tier3": "Priority 3 - FYSA"}

    for idx, it in enumerate(items):
        fe = fg.add_entry()
        # Emoji prefix with urgent override
        prefix = "🚨" if it.urgent else ("1️⃣" if it.tier == "tier1" else ("2️⃣" if it.tier == "tier2" else "3️⃣"))
        label = labels.get(it.tier, it.tier.upper())
        fe.title(f"{prefix} {label}: {it.title}")
        fe.link(href=it.link)
        desc = it.summary
        if it.source:
            desc = f"<b>Source:</b> {html.escape(it.source)}<br/>" + desc
        fe.description(desc[:2000])

        # Replay mode for backfill into Slack (optional)
        if idx < replay:
            bumped = now + timedelta(seconds=(replay - idx))
            fe.pubDate(bumped)
            seed = reseed or now.strftime("%Y%m%d%H%M%S")
            guid = hashlib.sha256((it.title + "|" + it.link + "|" + seed).encode("utf-8")).hexdigest()
            fe.guid(guid, permalink=False)
        else:
            fe.pubDate(datetime.fromtimestamp(it.published_ts, tz=timezone.utc))
            guid = hashlib.sha256((it.title + "|" + it.link).encode("utf-8")).hexdigest()
            fe.guid(guid, permalink=False)

    return fg.rss_str(pretty=True).decode("utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiers", default="tier1,tier2", help="Comma list: tier1,tier2,tier3")
    ap.add_argument("--since-hours", type=int, default=0)
    ap.add_argument("--max-items", type=int, default=200)
    ap.add_argument("--output", default="public/emea-filtered.xml")
    ap.add_argument("--title", default="EMEA SOC Filtered Feed")
    ap.add_argument("--homepage", default="https://example.org/emea-filtered")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--replay", type=int, default=0, help="Backfill: treat newest N items as fresh")
    ap.add_argument("--reseed", default="", help="GUID reseed token for --replay")
    args = ap.parse_args()

    tiers = [t.strip() for t in args.tiers.split(",") if t.strip()]
    for t in tiers:
        if t not in FEEDS_BY_TIER: raise SystemExit(f"Unknown tier '{t}'")

    items: List[Item] = []
    for t in tiers:
        items.extend(harvest(t, FEEDS_BY_TIER[t], since_hours=args.since_hours, force=args.force))
    final = dedupe_sort(items, max_items=args.max_items)
    xml = build_feed(final, title=args.title, homepage=args.homepage, replay=args.replay, reseed=args.reseed)

    # Ensure output directory exists
    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Wrote {args.output} with {len(final)} items")

if __name__ == "__main__":
    main()
