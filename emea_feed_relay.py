#!/usr/bin/env python3
"""
London RSOC News Monitor — EMEA-focused RSS relay (v2.3, clean)

- Aggregates curated RSS sources; emphasises EMEA (Europe, Middle East, North Africa)
- Signal-based scoring for violence/unrest, hard transport, cyber, hazards
- Meteoalarm per-country allow-list with Orange/Red-only policy
- Optional Israel HFC (Oref) rocket siren fetcher
- National Highways REMOVED entirely
- Hidden weighting: public RSS shows clean titles (no tiers/scores)

Usage
  pip install feedparser feedgen
  python emea_feed_relay.py --output public/emea-filtered.xml --max-items 200 [--replay N --reseed TOKEN]
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

# Alerts/incident feeds (no National Highways, no ERCC)
ALERT_FEEDS = [
    "https://www.europol.europa.eu/rss/news",
    "https://www.gdacs.org/XML/RSS.xml",
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
# Config: Scoring (internal/hidden)
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
TRANSPORT_HARD = [
    r"airport closed|airspace closed|runway closed|rail suspended|service suspended|motorway closed|port closed|all lanes closed|carriageway closed|road closed|blocked|drone.*(airport|airspace)"
]
TRANSPORT_SOFT = [r"closure|cancel(l|)ed|cancellation|diverted|delay|disruption|grounded|air traffic control"]

# Weather/Meteoalarm — severity mapping via simple heuristics
METEO_RED = [r"\bred\b", r"\bsevere\b", r"\bextreme\b"]
METEO_ORANGE = [r"\borange\b", r"amber"]
METEO_YELLOW = [r"\byellow\b"]
HAZARDS = [r"flood|flash flood|earthquake|aftershock|landslide|wildfire|bushfire|storm|hurricane|typhoon|tornado|heatwave|snow|ice|avalanche|wind|gale"]

# Protest scale / enforcement / government measures / evacuation
PROTEST_SCALE = [
    r"mass (?:protest|demonstration)s?",
    r"nationwide|countrywide",
    r"tens? of thousands|hundreds? of (?:people|protesters)",
    r"general strike|national strike",
    r"roadblocks?|highways? blocked|ports? blocked|airport (?:blocked|closed)"
]
ENFORCEMENT = [r"riot police|tear gas|water cannon|baton|clashes with police|arrests?|detained|detentions?"]
GOV_MEASURES = [r"curfew|state of emergency|martial law|emergency decree|security alert raised"]
EVACUATION = [r"evacuated|evacuation|terminal evacuated|station evacuated|building evacuated"]

# Watchlist — main cities & major hubs
WATCHLIST = [
    r"london|plymouth|sheffield|abingdon",
    r"kyiv|kiev",
    r"doha|riyadh|dubai",
    r"zurich|yverdon-les-bains",
    r"stockholm|oslo|copenhagen|vienna",
    r"eindhoven|amsterdam",
    r"tel\s*-?\s*aviv|jerusalem",
    r"hamburg|berlin|paris",
]
WATCHLIST_HUBS = [
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
    # Americas
    r"\b(United States|USA|US|American|Canada|Canadian|Mexico|Mexican|Brazil|Brazilian|Argentina|Argentinian|Chile|Peru)\b",
    # Asia-Pacific
    r"\b(China|Chinese|India|Indian|Pakistan|Pakistani|Bangladesh|Bangladeshi|Japan|Japanese|South Korea|South Korean|Korea|Korean|Indonesia|Indonesian|Philippines|Philippine|Malaysia|Malaysian|Thailand|Thai|Vietnam|Vietnamese|Singapore|Singaporean)\b",
    r"\b(Australia|Australian|New Zealand|New Zealander|Kiwi)\b",
    # Sub-Saharan Africa (not in North Africa list)
    r"\b(South Africa|South African|Nigeria|Nigerian|Kenya|Kenyan|Ethiopia|Ethiopian|Ghana|Ghanaian|Uganda|Ugandan|Tanzania|Tanzanian|Somalia|Somali|Congo|Congolese|Angola|Mozambique|Zambia|Zimbabwe|Botswana|Namibia|Senegal|Cameroon|Cameroonian)\b",
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
PROTEST_SCALE_RE = _compile(PROTEST_SCALE)
ENFORCEMENT_RE   = _compile(ENFORCEMENT)
GOV_MEASURES_RE  = _compile(GOV_MEASURES)
EVACUATION_RE    = _compile(EVACUATION)
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

# Title normalisation for de-duplication (ignore case/punct/common prefixes)
def normalize_title(s: str) -> str:
    s = s or ""
    s = s.lower()
    s = re.sub(r"^(breaking|live|update|updated|just in|watch|video):\s+", "", s)
    s = re.sub(r"\s*\([^)]+\)$", "", s)  # drop trailing brackets
    s = re.sub(r"[-–—]+", "-", s)  # normalise dashes
    s = re.sub(r"[^a-z0-9]+", " ", s)  # keep alnum
    s = re.sub(r"\b(report|video|live|analysis|opinion)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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


def incident_score(text: str, feed_kind: str, published_ts: float, source: str = "") -> Tuple[int, bool]:
    t = text.lower()
    score = 0

    # Violence / terror
    if any(rx.search(t) for rx in TERROR_RE):
        score += 90
    if any(rx.search(t) for rx in VIOLENCE_RE):
        score += 85
    if any(rx.search(t) for rx in CASUALTIES_RE):
        score += 30

    # Protests & policing / government measures / evacuations
    if any(rx.search(t) for rx in PROTEST_RE):
        score += 40
    if any(rx.search(t) for rx in PROTEST_SCALE_RE):
        score += 25
    if any(rx.search(t) for rx in ENFORCEMENT_RE):
        score += 20
    if any(rx.search(t) for rx in GOV_MEASURES_RE):
        score += 35
    if any(rx.search(t) for rx in EVACUATION_RE):
        score += 40

    # Transport
    if any(rx.search(t) for rx in TRANS_HARD_RE):
        score += 65
    if any(rx.search(t) for rx in TRANS_SOFT_RE):
        score += 25

    # Cyber
    if any(rx.search(t) for rx in CYBER_RE):
        score += 50

    # Weather / hazards
    met_sev = meteo_severity(t)
    if feed_kind == "meteoalarm" and met_sev < 40 and REQUIRE_METEO_ORANGE:
        return 0, False  # drop yellow-only meteoalarm
    if feed_kind == "meteoalarm" or any(rx.search(t) for rx in HAZARDS_RE):
        score += met_sev
        if re.search(r"flood|earthquake|aftershock", t):
            score += 20

    # Quantity-based boosts (arrests/injured/killed)
    qty_patterns = [
        (re.compile(r"(\d{1,3}(?:,\d{3})*)\s+(?:killed|dead|deaths|fatalities)", re.I), 4, 80),
        (re.compile(r"(\d{1,3}(?:,\d{3})*)\s+(?:injured|wounded|casualties)", re.I), 2, 50),
        (re.compile(r"(\d{1,3}(?:,\d{3})*)\s+(?:arrests?|detained|detentions?)", re.I), 1, 40),
    ]
    for rx, mult, cap in qty_patterns:
        for m in rx.finditer(t):
            try:
                n = int(m.group(1).replace(',', ''))
                score += min(cap, max(5, n * mult))
            except Exception:
                pass

    # Watchlist & recency
    score += watchlist_bonus(t)
    score += recency_bonus(published_ts)

    # Trusted publishers small boost (break ties)
    if source and re.search(r"BBC|Sky News|FRANCE 24|France 24|DW|Deutsche Welle|Euronews|TRT|Times of Israel|Jerusalem Post|Al Jazeera|CNN|Reuters|AFP", source, re.I):
        score += 8

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
            score, urgent = incident_score(joined, feed_kind, ts, source_name)
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
        fe.title(it.title)  # public: clean title
        fe.link(href=it.link)
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
                score, urgent = incident_score(text, "alerts", when, "Israel HFC")
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

