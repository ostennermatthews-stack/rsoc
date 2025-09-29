#!/usr/bin/env python3
"""
London RSOC News Monitor — EMEA-focused RSS relay (v2.1)

What this does
- Aggregates curated RSS sources; emphasises EMEA (Europe, Middle East, North Africa)
- Scored, signal-based ranking (violence/unrest, hard transport, cyber, hazards)
- Meteoalarm per-country allow-list with Orange/Red-only policy
- Optional Israel HFC (Oref) rocket siren fetcher
- National Highways (UK) filtering to reduce noise
- **Hidden weighting**: scores/tiers are NOT published in the feed (clean titles); scoring is used internally
  and by your separate workflow step that builds the Top-5 watchlist page/brief.

Usage
  pip install feedparser feedgen
  python emea_feed_relay.py --output public/emea-filtered.xml --max-items 200 [--replay N --reseed TOKEN]

Outputs
  - RSS 2.0 feed at --output (default public/emea-filtered.xml)

Notes
  - Keep NEWS_FEEDS as plain URL strings. The script tags them as "news" internally.
  - Update METEOALARM_COUNTRIES to control which countries you pull from Meteoalarm.
  - To completely drop a noisy source, remove it from NEWS_FEEDS/ALERT_FEEDS.
"""
from __future__ import annotations
import argparse
import hashlib
import html
import json
import os
import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Tuple
from difflib import SequenceMatcher

import feedparser
from feedgen.feed import FeedGenerator
from urllib.parse import urlparse

# ------------------------------
# Config: Sources
# ------------------------------
# Meteoalarm per-country allow-list (slugs from the Meteoalarm site)
METEOALARM_COUNTRIES = [
    "united-kingdom", "austria", "denmark", "norway", "germany",
    "netherlands", "sweden", "switzerland", "ukraine",
]
MA_FEEDS = [f"https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-rss-{slug}" for slug in METEOALARM_COUNTRIES]

# News feeds (strings only)
NEWS_FEEDS = [
    # Pan-Europe desks
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml?edition=int",
    "https://www.france24.com/en/tag/europe/rss",
    "https://www.euronews.com/rss?format=mrss&level=theme&name=news",

    # Israel / region
    "https://www.timesofisrael.com/feed/",
    "https://www.jpost.com/rss",
    "https://www.middleeastmonitor.com/feed/",

    # International broadcasters (EMEA-heavy)
    "https://rss.dw.com/rdf/rss-en-all",
    "https://news.sky.com/feeds/rss/world.xml",
    "https://feeds.npr.org/1004/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",

    # Anadolu Agency (English)
    "https://www.aa.com.tr/en/rss/default?cat=guncel",
    "https://www.aa.com.tr/en/rss/default?cat=live",
]

# Alerts/incident feeds
ALERT_FEEDS = [
    # Law enforcement / alerts / disasters
    \"https://www.europol.europa.eu/rss/news\",
    \"https://www.gdacs.org/XML/RSS.xml\",
    # (Deliberately NOT including ERCC Daily Flash or National Highways)
]

# Aggregate into [(kind, url)] for harvesting
ALL_FEEDS: List[Tuple[str, str]] = []
for url in MA_FEEDS:
    ALL_FEEDS.append(("meteoalarm", url))
for url in ALERT_FEEDS:
    ALL_FEEDS.append(("alerts", url))
for url in NEWS_FEEDS:
    ALL_FEEDS.append(("news", url))

# ------------------------------
# Config: Scoring (kept internal/hidden)
# ------------------------------
EXCLUDE_PATTERNS = [
    r"\b(sport|football|soccer|rugby|tennis|golf|cricket|olympic|f1|motorsport)\b",
    r"\b(entertainment|celebrity|fashion|lifestyle|culture|arts|music|movie|tv|theatre|theater)\b",
    r"\b(award|festival|red carpet|premiere|concert|tour)\b",
]
VIOLENCE = [r"riot|violent|clashes|looting|molotov|stabbing|knife attack|stabbing attack|shooting|gunfire|shots fired|arson"]
TERROR_ATTACK = [r"terror(?!ism\s*threat)|car bomb|suicide bomb|ied|explosion|blast"]
CASUALTIES = [r"\b(dead|deaths|fatalit|injured|wounded|casualt)\b"]
PROTEST_STRIKE = [r"protest|demonstration|march|blockade|strike|walkout|picket"]
CYBER = [r"ransomware|data breach|ddos|phishing|malware|cyber attack|hack(?!ney)"]
TRANSPORT_HARD = [r"airport closed|airspace closed|runway closed|rail suspended|service suspended|motorway closed|port closed|all lanes closed|carriageway closed|road closed|blocked"]
TRANSPORT_SOFT = [r"closure|cancel(l|)ed|cancellation|diverted|delay|disruption|grounded|air traffic control"]

# Weather/Meteoalarm — severity mapping via simple heuristics
METEO_RED = [r"\bred\b", r"\bsevere\b", r"\bextreme\b"]
METEO_ORANGE = [r"\borange\b", r"amber"]
METEO_YELLOW = [r"\byellow\b"]
HAZARDS = [r"flood|flash flood|earthquake|aftershock|landslide|wildfire|bushfire|storm|hurricane|typhoon|tornado|heatwave|snow|ice|avalanche|wind|gale"]

# Watchlist — main cities & major hubs
WATCHLIST = [
    r"london|plymouth|sheffield|abingdon",
    r"kyiv|kiev",
    r"doha|riyadh|dubai",
    r"zurich|yverdon-les-bains",
    r"stockholm|oslo|copenhagen|vienna",
    r"eindhoven|amsterdam",
    r"tel[\s-]*aviv|jerusalem",
    r"hamburg|berlin|paris",
]
WATCHLIST_HUBS = [
    # Airports + rail hubs for those cities
    r"heathrow|lhr|gatwick|lgw|stansted|stn|luton|ltn|london city|lcy",
    r"paddington|king'?s cross|st\s*pancras|waterloo|victoria|liverpool street|london bridge|euston",
    r"charles de gaulle|cdg|orly|ory|gare du nord|gare de l'[eé]st|gare de lyon|montparnasse|saint[- ]lazare|austerlitz",
    r"schiphol|ams|amsterdam centraal|eindhoven centraal|eindhoven airport|ein",
    r"zrh|zurich hb|z[üu]rich hb|wien hbf|vienna hbf|vie",
    r"cph|k[øo]benhavns? hovedbaneg[aå]rd|kobenhavn h|stockholm central|arlanda|arn|oslo s|gardermoen|osl",
    r"berlin hbf|ber airport|ber|hamburg hbf|ham",
    r"ben gurion|tlv|tel[- ]aviv (ha)?hagana|hashalom|savidor",
    r"boryspil|kbp|zhuliany|iev|kyiv[- ]pasazhyrskyi",
    r"hamad international|doh|msheireb",
    r"king khalid international|ruh|riyadh metro",
    r"dubai international|dxb|al maktoum|dwc|union station|burjuman",
]

# EMEA geo filter
EMEA_ALLOW = [
    r"\b(Europe|EU|European Union|Schengen|Eurozone|Middle East|Gulf|Levant|Maghreb|North Africa)\b",
    r"\b(UK|United Kingdom|England|Scotland|Wales|Northern Ireland|Ireland|France|Germany|Austria|Switzerland|Netherlands|Belgium|Denmark|Norway|Sweden|Finland|Iceland|Poland|Czech|Slovakia|Hungary|Romania|Bulgaria|Greece|Italy|Spain|Portugal|Ukraine|Estonia|Latvia|Lithuania|Serbia|Bosnia|Croatia|Slovenia|Albania|Kosovo|Moldova)\b",
    r"\b(Israel|Palestine|Gaza|West Bank|Lebanon|Syria|Jordan|Egypt|Turkey|Türkiye|Cyprus|Qatar|Saudi Arabia|United Arab Emirates|UAE|Bahrain|Kuwait|Oman|Yemen|Iraq|Iran|Libya|Tunisia|Algeria|Morocco)\b",
]
NON_EMEA_BLOCK = [
    r"\b(United States|USA|US|Canada|Mexico|Brazil|Argentina|Chile|Peru)\b",
    r"\b(China|India|Pakistan|Bangladesh|Japan|South Korea|Indonesia|Philippines|Malaysia|Thailand|Vietnam|Singapore)\b",
    r"\b(Australia|New Zealand)\b",
    r"\b(South Africa|Nigeria|Kenya|Ethiopia|Ghana|Uganda|Tanzania|Somalia|DRC|Congo|Angola|Mozambique|Zambia|Zimbabwe|Botswana|Namibia|Senegal|Cameroon)\b",
]

# Priority thresholds (internal)
P1_THRESHOLD = 80
P2_THRESHOLD = 50
P3_THRESHOLD = 30
MIN_SCORE_TO_INCLUDE = 25
REQUIRE_METEO_ORANGE = True  # Drop Yellow-only Meteoalarm

# Urgent override (internal)
URGENT_TERMS = [r"explosion|mass casualty|airport closed|airspace closed|terror attack|multiple fatalities"]

# Hide labels/scores in public RSS output
PUBLIC_LABELS = False

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
WATCHLIST_HUBS_RE = _compile(WATCHLIST_HUBS)
EMEA_ALLOW_RE = _compile(EMEA_ALLOW)
NON_EMEA_RE = _compile(NON_EMEA_BLOCK)
URGENT_RE = _compile(URGENT_TERMS)


def is_excluded(text: str) -> bool:
    return any(rx.search(text) for rx in EXCL_RE)


def is_emea_relevant(text: str) -> bool:
    if any(rx.search(text) for rx in EMEA_ALLOW_RE):
        return True
    if any(rx.search(text) for rx in NON_EMEA_RE):
        return False
    return True


def now_ts() -> float:
    return time.time()


def recency_bonus(published_ts: float) -> int:
    """Small freshness lift for recent items."""
    hours = (now_ts() - published_ts) / 3600.0
    if hours <= 6:
        return 10
    if hours <= 24:
        return 5
    return 0



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
# Scoring model (internal-only)
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
    if any(rx.search(text) for rx in (WATCHLIST_RE + WATCHLIST_HUBS_RE)):
        return 30
    return 0


def incident_score(text: str, feed_kind: str, published_ts: float) -> Tuple[int, bool]:
    t = text.lower()
    score = 0

    # Violence / terror first
    if any(rx.search(t) for rx in TERROR_RE):
        score += 90
    if any(rx.search(t) for rx in VIOLENCE_RE):
        score += 80  # a bit stronger than before
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

    # Weather
    met_sev = meteo_severity(t)
    if feed_kind == "meteoalarm" and met_sev < 40 and REQUIRE_METEO_ORANGE:
        return 0, False  # drop yellow-only meteoalarm
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
    return 0


# ------------------------------
# Harvest & build
# ------------------------------

def pub_ts(entry) -> float:
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try:
                return time.mktime(v)
            except Exception:
                pass
    return now_ts()


def harvest() -> List[Item]:
    items: List[Item] = []
    for feed_kind, url in ALL_FEEDS:
        try:
            fp = feedparser.parse(url)
        except Exception:
            continue
        source_name = fp.feed.get("title") or url
        netloc = urlparse(url).netloc.lower()
        for e in fp.entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            summary = html.unescape((e.get("summary") or e.get("description") or "").strip())
            joined = f"{title} {summary}"

            if is_excluded(joined):
                continue
            if not is_emea_relevant(joined):
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

    # Israel HFC (rocket sirens)
    items.extend(harvest_oref())

    # Deduplicate and sort by (priority, score, time)
    seen_hashes = set()
    seen_norm_titles: List[str] = []
    out: List[Item] = []
    for it in sorted(items, key=lambda x: (x.priority, x.score, x.published_ts), reverse=True):
        # First, basic hash (title|link)
        h = hashlib.sha256((it.title + '|' + it.link).encode('utf-8')).hexdigest()
        if h in seen_hashes:
            continue
        # Then, title-normalised near-dup check
        norm = normalize_title(it.title)
        is_dup = False
        if norm in seen_norm_titles:
            is_dup = True
        else:
            for prev in seen_norm_titles:
                if SequenceMatcher(None, norm, prev).ratio() >= 0.96:
                    is_dup = True
                    break
        if is_dup:
            continue
        seen_hashes.add(h)
        seen_norm_titles.append(norm)
        out.append(it)
    return out


def build_feed(items: List[Item], title: str, homepage: str, replay: int = 0, reseed: str = "") -> str:
    fg = FeedGenerator()
    fg.title(title)
    fg.link(href=homepage, rel='alternate')
    fg.description('Merged & filtered EMEA alerts (internal scoring, clean titles)')
    fg.language('en')
    now = datetime.now(timezone.utc)
    fg.updated(now)

    for idx, it in enumerate(items):
        if it.priority not in (1, 2, 3):
            continue
        fe = fg.add_entry()
        # Public label hidden: just the raw title
        fe.title(it.title)
        fe.link(href=it.link)
        # Keep description minimal: include source if available, no scores/tiers
        desc = it.summary
        if it.source:
            desc = f"<b>Source:</b> {html.escape(it.source)}<br/>" + desc
        fe.description(desc[:2000])
        # Replay/backfill logic (GUID/pubDate bump)
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


# ------------------------------
# Israel Home Front Command (Oref) fetcher
# ------------------------------

def harvest_oref() -> List[Item]:
    url_candidates = [
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
                score = max(score, P1_THRESHOLD + 5)  # force P1
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


# ------------------------------
# CLI
# ------------------------------

def main():
    ap = argparse.ArgumentParser(description="Build scored EMEA RSS for Slack RSS app (public titles only)")
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
    items = items[: args.max_items]

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    xml = build_feed(items, title=args.title, homepage=args.homepage, replay=args.replay, reseed=args.reseed)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Wrote {args.output} with {len(items)} items")


if __name__ == "__main__":
    main()
