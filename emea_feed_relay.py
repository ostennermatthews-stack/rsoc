#!/usr/bin/env python3
"""
EMEA Feed Relay — v2 (Scored Priorities)

Why this version?
- Fixes the "tiers by source" problem by switching to a **signal-based scoring model**.
- Priority is now derived from the **content** (unrest/violence/closures > severe weather > generic geopolitics).
- Meteoalarm is **per-country allow-listed** and only high-severity gets elevated.
- Optional **watchlist boosts** for places/assets you care about (e.g., Milan, Heathrow, M25).

Output
- Single RSS file with titles like: "1️⃣ Priority 1 - Investigate: …" (or 2️⃣/3️⃣). Urgent terms still add 🚨 override.

Usage
  pip install feedparser feedgen
  python emea_feed_relay_v2.py --output public/emea-filtered.xml --max-items 200

(Args are compatible with v1; --tiers is accepted but ignored.)
"""
from __future__ import annotations
import argparse
import hashlib
import html
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

import feedparser
from feedgen.feed import FeedGenerator
import json
import urllib.request

# ------------------------------
# Config: Sources
# ------------------------------
# Meteoalarm per-country allow-list (slugs from the Meteoalarm site)
METEOALARM_COUNTRIES = [
    # Allowed Meteoalarm countries (EU/EEA coverage only)
    "united-kingdom",
    "austria",
    "denmark",
    "norway",
    "germany",
    "netherlands",
    "sweden",
    "switzerland",
    "ukraine",
]

MA_FEEDS = [f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-{slug}" for slug in METEOALARM_COUNTRIES]

NEWS_FEEDS = [
    # Incident-forward European desks
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml?edition=int",
    "https://www.france24.com/en/tag/europe/rss",
    "https://www.euronews.com/rss?format=mrss&level=theme&name=news",
]

ALERT_FEEDS = [
    "https://www.osac.gov/RSS",                                      # Security alerts by country
    "https://www.europol.europa.eu/rss/news",                        # Law-enforcement ops
    "https://www.gdacs.org/XML/RSS.xml",                             # Global disaster alerts
    "https://m.highwaysengland.co.uk/feeds/rss/UnplannedEvents.xml", # UK motorways
]

ALL_FEEDS: List[Tuple[str, str]] = []
for url in MA_FEEDS:
    ALL_FEEDS.append(("meteoalarm", url))
for url in ALERT_FEEDS:
    ALL_FEEDS.append(("alerts", url))
for url in NEWS_FEEDS:
    ALL_FEEDS.append(("news", url))

# ------------------------------
# Config: Scoring
# ------------------------------
# Global excludes (always drop)
EXCLUDE_PATTERNS = [
    r"\b(sport|football|soccer|rugby|tennis|golf|cricket|olympic|f1|motorsport)\b",
    r"\b(entertainment|celebrity|fashion|lifestyle|culture|arts|music|movie|tv|theatre|theater)\b",
    r"\b(award|festival|red carpet|premiere|concert|tour)\b",
]

# Core incident signals (higher weight)
VIOLENCE = [r"riot|violent|clashes|looting|molotov|stabbing|shooting|gunfire|shots fired|arson"]
TERROR_ATTACK = [r"terror(?!ism\s*threat)|car bomb|suicide bomb|ied|explosion|blast"]
CASUALTIES = [r"\b(dead|deaths|fatalit|mass casualty|injured|wounded)\b"]
PROTEST_STRIKE = [r"protest|demonstration|march|blockade|strike|walkout|picket"]
CYBER = [r"ransomware|data breach|ddos|phishing|malware|cyber attack|hack(?!ney)"]
TRANSPORT_HARD = [r"airport closed|airspace closed|runway closed|rail suspended|service suspended|motorway closed|port closed"]
TRANSPORT_SOFT = [r"closure|cancel(l|)ed|cancellation|diverted|delay|disruption|grounded|air traffic control"]

# Weather/Meteoalarm — severity mapping via simple heuristics
METEO_RED = [r"\bred\b", r"\bsevere\b", r"\bextreme\b"]
METEO_ORANGE = [r"\borange\b", r"amber"]
METEO_YELLOW = [r"\byellow\b"]
HAZARDS = [r"flood|flash flood|earthquake|aftershock|landslide|wildfire|bushfire|storm|hurricane|typhoon|tornado|heatwave|snow|ice|avalanche|wind|gale"]

# Watchlist — boost if these appear (cities, assets, routes)
WATCHLIST = [
    # Cities / assets / routes to boost (case-insensitive)
    r"london|plymouth|sheffield|abingdon",
    r"kyiv|kiev",
    r"doha|riyadh|dubai",
    r"zurich|yverdon-les-bains",
    r"stockholm|oslo|copenhagen|vienna",
    r"eindhoven|amsterdam",
    r"tel[\s-]*aviv",
    r"hamburg|berlin|paris",
]

RECENT_HOURS_BOOST = 6     # boost items within this age

# Priority thresholds
P1_THRESHOLD = 80
P2_THRESHOLD = 50
P3_THRESHOLD = 30
MIN_SCORE_TO_INCLUDE = 25
# Require Meteoalarm severity of Orange or Red (drop Yellow):
REQUIRE_METEO_ORANGE = True   # drop anything below this

# Urgent override (kept for 🚨)
URGENT_TERMS = [r"explosion|mass casualty|airport closed|airspace closed|terror attack|multiple fatalities"]

# ------------------------------
# Helpers
# ------------------------------

def _compile(patterns: Iterable[str]) -> List[re.Pattern]:
    return [re.compile(p, re.I) for p in patterns]

EXCL_RE = _compile(EXCLUDE_PATTERNS)
VIOLENCE_RE = _compile(VIOLENCE)
TERROR_RE = _compile(TERROR_ATTACK)
CASUALTIES_RE = _compile(CASUALTIES)
PROTEST_RE = _compile(PROTEST_STRIKE)
CYBER_RE = _compile(CYBER)
TRANS_HARD_RE = _compile(TRANSPORT_HARD)
TRANS_SOFT_RE = _compile(TRANSPORT_SOFT)
METEO_RED_RE = _compile(METEO_RED)
METEO_ORANGE_RE = _compile(METEO_ORANGE)
METEO_YELLOW_RE = _compile(METEO_YELLOW)
HAZARDS_RE = _compile(HAZARDS)
WATCHLIST_RE = _compile(WATCHLIST)
# Additional transport hubs to boost
WATCHLIST_EXTRA = [
    # London airports + rail termini
    r"heathrow|lhr|gatwick|lgw|stansted|stn|luton|ltn|london city|lcy",
    r"paddington|king'?s cross|st\s*pancras|waterloo|victoria|liverpool street|london bridge|euston",
    # Paris airports + gares
    r"charles de gaulle|cdg|orly|ory|gare du nord|gare de l'[eé]st|gare de lyon|montparnasse|saint[- ]lazare|austerlitz",
    # Amsterdam / Eindhoven
    r"schiphol|ams|amsterdam centraal|eindhoven centraal|eindhoven airport|ein",
    # Zurich / Vienna
    r"zrh|zurich hb|z[üu]rich hb|wien hbf|vienna hbf|vie",
    # Copenhagen / Stockholm / Oslo
    r"cph|k[øo]benhavns? hovedbaneg[aå]rd|kobenhavn h|stockholm central|arlanda|arn|oslo s|gardermoen|osl",
    # Berlin / Hamburg
    r"berlin hbf|ber airport|ber|hamburg hbf|ham",
    # Tel Aviv / Kyiv / Doha / Riyadh / Dubai
    r"ben gurion|tlv|tel[- ]aviv (ha)?hagana|hashalom|savidor",
    r"boryspil|kbp|zhuliany|iev|kyiv[- ]pasazhyrskyi",
    r"hamad international|doh|msheireb",
    r"king khalid international|ruh|riyadh metro",
    r"dubai international|dxb|al maktoum|dwc|union station|burjuman",
]
WATCHLIST_EXTRA_RE = _compile(WATCHLIST_EXTRA)
URGENT_RE = _compile(URGENT_TERMS)


def is_excluded(text: str) -> bool:
    return any(rx.search(text) for rx in EXCL_RE)


def now_ts() -> float:
    return time.time()


def pub_ts(entry) -> float:
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try:
                return time.mktime(v)
            except Exception:
                pass
    return now_ts()


@dataclass
class Item:
    title: str
    link: str
    summary: str
    published_ts: float
    source: str
    feed_kind: str  # 'meteoalarm'|'alerts'|'news'
    score: int
    priority: int  # 1, 2, 3
    urgent: bool


# ------------------------------
# Scoring model
# ------------------------------

def meteo_severity(text: str) -> int:
    """Return 70 for red, 40 for orange/amber, 0 for yellow when REQUIRE_METEO_ORANGE, else 15."""
    t = text.lower()
    if any(rx.search(t) for rx in METEO_RED_RE):
        return 70
    if any(rx.search(t) for rx in METEO_ORANGE_RE):
        return 40
    if any(rx.search(t) for rx in METEO_YELLOW_RE):
        return 0 if REQUIRE_METEO_ORANGE else 15
    return 0


def watchlist_bonus(text: str) -> int:
    if any(rx.search(text) for rx in (WATCHLIST_RE + WATCHLIST_EXTRA_RE)):
        return 30
    return 0


def incident_score(text: str, feed_kind: str, published_ts: float) -> Tuple[int, bool]:
    """Compute score and urgent flag."""
    t = text.lower()
    score = 0

    # Violence / terror first
    if any(rx.search(t) for rx in TERROR_RE):
        score += 90
    if any(rx.search(t) for rx in VIOLENCE_RE):
        score += 70
    if any(rx.search(t) for rx in CASUALTIES_RE):
        score += 30
    if any(rx.search(t) for rx in PROTEST_RE):
        score += 40

    # Transport
    if any(rx.search(t) for rx in TRANS_HARD_RE):
        score += 60
    if any(rx.search(t) for rx in TRANS_SOFT_RE):
        score += 30

    # Cyber
    if any(rx.search(t) for rx in CYBER_RE):
        score += 50

    # Weather (especially from Meteoalarm feeds)
    met_sev = meteo_severity(t)
    if feed_kind == "meteoalarm" and met_sev < 40 and REQUIRE_METEO_ORANGE:
        # Drop yellow-only Meteoalarm outright
        return 0, False
    if feed_kind == "meteoalarm" or any(rx.search(t) for rx in HAZARDS_RE):
        score += met_sev
        if re.search(r"flood|earthquake|aftershock", t):
            score += 20

    # Watchlist & recency
    score += watchlist_bonus(t)
    score += recency_bonus(published_ts)

    # Urgent override
    urgent = any(rx.search(t) for rx in URGENT_RE) or score >= (P1_THRESHOLD + 10)

    return score, urgent


def to_priority(score: int) -> int:
    if score >= P1_THRESHOLD:
        return 1
    if score >= P2_THRESHOLD:
        return 2
    if score >= P3_THRESHOLD:
        return 3
    return 0  # drop


# ------------------------------
# Harvest & build
# ------------------------------

def harvest() -> List[Item]:
    items: List[Item] = []
    for feed_kind, url in ALL_FEEDS:
        try:
            fp = feedparser.parse(url)
        except Exception:
            continue
        source_name = fp.feed.get("title") or url
        for e in fp.entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            summary = html.unescape((e.get("summary") or e.get("description") or "").strip())
            joined = f"{title} {summary}"
            if is_excluded(joined):
                continue
            ts = pub_ts(e)
            score, urgent = incident_score(joined, feed_kind, ts)
            prio = to_priority(score)
            if prio == 0 or score < MIN_SCORE_TO_INCLUDE:
                continue
            items.append(Item(
                title=html.unescape(title),
                link=link,
                summary=summary,
                published_ts=ts,
                source=source_name,
                feed_kind=feed_kind,
                score=score,
                priority=prio,
                urgent=urgent,
            ))

    # Israel: Home Front Command (rocket sirens) — JSON endpoint
    items.extend(harvest_oref())

    # Dedup by title+link and sort by score/time
    seen = set()
    out: List[Item] = []
    for it in sorted(items, key=lambda x: (x.priority, x.score, x.published_ts), reverse=True):
        h = hashlib.sha256((it.title + '|' + it.link).encode('utf-8')).hexdigest()
        if h in seen:
            continue
        seen.add(h)
        out.append(it)
    return out


def build_feed(items: List[Item], title: str, homepage: str, replay: int = 0, reseed: str = "") -> str:
    fg = FeedGenerator()
    fg.title(title)
    fg.link(href=homepage, rel='alternate')
    fg.description('Merged & filtered EMEA alerts (scored)')
    fg.language('en')
    now = datetime.now(timezone.utc)
    fg.updated(now)

    label_map = {1: "Priority 1 - Investigate", 2: "Priority 2 - FYSA", 3: "Priority 3 - FYSA"}
    emoji_map = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣"}

    for idx, it in enumerate(items):
        if it.priority not in (1, 2, 3):
            continue
        fe = fg.add_entry()
        prefix = "🚨" if it.urgent else emoji_map[it.priority]
        label = label_map[it.priority]
        fe.title(f"{prefix} {label}: {it.title}")
        fe.link(href=it.link)
        desc = it.summary
        if it.source:
            desc = f"<b>Source:</b> {html.escape(it.source)}<br/>Score: {it.score}<br/>" + desc
        fe.description(desc[:2000])
        # Replay logic (for Slack backfill)
        if idx < replay:
            bumped = now + timedelta(seconds=(replay - idx))
            fe.pubDate(bumped)
            seed = reseed or now.strftime("%Y%m%d%H%M%S")
            guid = hashlib.sha256((it.title + '|' + it.link + '|' + seed).encode('utf-8')).hexdigest()
            fe.guid(guid, permalink=False)
        else:
            fe.pubDate(datetime.fromtimestamp(it.published_ts, tz=timezone.utc))
            guid = hashlib.sha256((it.title + '|' + it.link).encode('utf-8')).hexdigest()
            fe.guid(guid, permalink=False)

    return fg.rss_str(pretty=True).decode('utf-8')


def main():
    ap = argparse.ArgumentParser(description="Build scored EMEA RSS for Slack RSS app")
    ap.add_argument("--tiers", default="", help="Ignored (backward compatible)")
    ap.add_argument("--since-hours", type=int, default=0, help="Ignored in v2; keep 0")
    ap.add_argument("--max-items", type=int, default=250, help="Cap total items in output feed")
    ap.add_argument("--output", default="public/emea-filtered.xml", help="Output RSS file path")
    ap.add_argument("--title", default="London RSOC News Monitor", help="Feed title")
    ap.add_argument("--homepage", default="https://example.org/emea-filtered", help="Feed link/homepage")
    ap.add_argument("--force", action="store_true", help="Ignored in v2")
    ap.add_argument("--replay", type=int, default=0, help="Backfill: treat newest N items as fresh")
    ap.add_argument("--reseed", default="", help="GUID reseed token for --replay")
    args = ap.parse_args()

    items = harvest()
    # respect max-items
    items = items[: args.max_items]

    # ensure output dir
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    xml = build_feed(items, title=args.title, homepage=args.homepage, replay=args.replay, reseed=args.reseed)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Wrote {args.output} with {len(items)} items")


# ------------------------------
# Israel Home Front Command (Oref) fetcher
# ------------------------------

def harvest_oref() -> List[Item]:
    url_candidates = [
        # Commonly referenced endpoints in community wrappers
        "https://www.oref.org.il/WarningMessages/alert/Alerts.json",
        "https://www.oref.org.il/warningMessages/alert/alerts.json",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.oref.org.il/",
        "Accept": "application/json, text/plain, */*",
    }
    results: List[Item] = []
    for url in url_candidates:
        try:
            req = urllib.request.Request(url, headers=headers)
            raw = urllib.request.urlopen(req, timeout=10).read()
            try:
                js = json.loads(raw.decode("utf-8", "ignore"))
            except Exception:
                continue
            now = datetime.now(timezone.utc)
            def _mk_item(title: str, link_text: str, when: float, details: str) -> Item:
                text = f"{title} {details}"
                score, urgent = incident_score(text, "alerts", when)
                # Force to Priority 1
                score = max(score, P1_THRESHOLD + 5)
                return Item(
                    title=title,
                    link=link_text,
                    summary=details,
                    published_ts=when,
                    source="Israel Home Front Command (Oref)",
                    feed_kind="alerts",
                    score=score,
                    priority=to_priority(score),
                    urgent=True,
                )
            # Handle common JSON shapes
            if isinstance(js, dict) and js.get("data"):
                data = js.get("data") or []
            elif isinstance(js, list):
                data = js
            else:
                data = []
            for entry in data:
                cities = entry.get("data") or entry.get("cities") or entry.get("areas") or []
                threat = entry.get("title") or entry.get("category") or "Rocket alert"
                ts_str = entry.get("time") or entry.get("alertDate") or entry.get("date")
                try:
                    when = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00")).timestamp() if ts_str else now.timestamp()
                except Exception:
                    when = now.timestamp()
                cities_txt = ", ".join(cities) if isinstance(cities, list) else str(cities)
                details = f"Threat: {threat}; Areas: {cities_txt}"
                results.append(_mk_item(title=f"Rocket siren: {cities_txt or threat}", link_text=url, when=when, details=details))
            if results:
                break
        except Exception:
            continue
    return results

if __name__ == "__main__":
    main()

