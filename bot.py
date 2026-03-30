import html as html_module
import os
import re
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import dotenv_values, load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

from openai import OpenAI

# Load .env next to this file (works regardless of shell cwd). override=True so a
# stale OPENAI_API_KEY in the shell does not mask values from .env.
_BOT_DIR = Path(__file__).resolve().parent


def _strip_env_value(raw: object) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    return s


def load_app_env() -> None:
    """Load .env next to bot.py and sync into os.environ (file wins over shell)."""
    path = _BOT_DIR / ".env"
    load_dotenv(path, override=True)
    if path.is_file():
        for k, v in dotenv_values(path).items():
            if v is None:
                continue
            s = _strip_env_value(v)
            if s:
                os.environ[k] = s


TECH_AI_QUERY = (
    "Apple OR Google OR Nvidia OR Microsoft OR Meta OR Amazon "
    "OR OpenAI OR Anthropic OR DeepMind OR xAI OR Mistral AI OR artificial intelligence"
)

TECH_MAINSTREAM_SOURCES = [
    "nytimes.com",
    "wsj.com",
    "washingtonpost.com",
    "ft.com",
    "reuters.com",
    "apnews.com",
    "bloomberg.com",
    "cnbc.com",
    "bbc.com",
    "theguardian.com",
    "economist.com",
    "techcrunch.com",
    "theverge.com",
    "wired.com",
]

# Google News RSS titles often end with "Headline - Publisher"; match that suffix to domains.
TECH_SOURCE_TITLE_HINTS: Dict[str, List[str]] = {
    "nytimes.com": ["new york times", "nytimes"],
    "wsj.com": ["wall street journal", "wsj"],
    "washingtonpost.com": ["washington post"],
    "ft.com": ["financial times"],
    "reuters.com": ["reuters"],
    "apnews.com": ["associated press", "ap news"],
    "bloomberg.com": ["bloomberg"],
    "cnbc.com": ["cnbc"],
    "bbc.com": ["bbc"],
    "theguardian.com": ["the guardian", "guardian"],
    "economist.com": ["the economist", "economist"],
    "techcrunch.com": ["techcrunch"],
    "theverge.com": ["the verge", "verge"],
    "wired.com": ["wired"],
}

SPORTS_LEAGUES = ["NFL", "MLB", "NBA", "NHL"]

NY_TZ = ZoneInfo("America/New_York")


def email_run_timestamp_et() -> str:
    """NY local time when the summary is built (for email subjects)."""
    return datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M ET")


def is_ncaa_football_season_ny() -> bool:
    """Approx. FBS season window (late summer through bowl season)."""
    m = datetime.now(NY_TZ).month
    return m >= 8 or m == 1


def is_ncaa_basketball_season_ny() -> bool:
    """Approx. NCAA men's basketball season (October–April)."""
    m = datetime.now(NY_TZ).month
    return m in (10, 11, 12, 1, 2, 3, 4)


BIG_TECH_TICKERS: List[tuple[str, str]] = [
    ("AAPL", "Apple"),
    ("MSFT", "Microsoft"),
    ("GOOGL", "Alphabet"),
    ("META", "Meta"),
    ("AMZN", "Amazon"),
    ("NVDA", "Nvidia"),
    ("AMD", "AMD"),
    ("TSLA", "Tesla"),
    ("NFLX", "Netflix"),
    ("CRM", "Salesforce"),
]

TECH_EMAIL_HTML_RULES = (
    "Produce a single HTML email BODY FRAGMENT (no <html>/<body> wrappers). "
    "Use safe, email-client-friendly inline CSS. "
    "Section 1: A cohesive English article of 200–500 words at the top, titled with "
    "<h2 style=\"margin-top:0;color:#1a237e;\">Market & big tech snapshot</h2>. "
    "Weave in the PROVIDED stock snapshot numbers, major tech themes, AND a concise analysis of how "
    "major international / geopolitical / macro headlines (when provided in the feed) could affect "
    "U.S. equities, rates, or risk sentiment—without inventing facts not implied by the headlines. "
    "Section 2: <h2 style=\"color:#1a237e;\">Top stories</h2> with 6–10 story blocks. "
    "Each block: if image_url is provided for that item, include "
    "<img src=\"URL\" alt=\"\" style=\"max-width:100%;max-height:200px;border-radius:8px;margin:8px 0;\" />. "
    "Use <strong> for the headline and for key phrases in the one-sentence summary. "
    "End each story with a source link ONLY as <a href=\"FULL_URL\">Source</a> or the outlet name—"
    "never show raw URLs as visible text. "
    "Wrap everything in <div style=\"font-family:Segoe UI,Helvetica,Arial,sans-serif;max-width:640px;"
    "color:#222;line-height:1.55;\">. "
    "Do not use markdown code fences. Do not repeat the same story twice."
)

SPORTS_EMAIL_HTML_RULES = (
    "Produce a single HTML email BODY FRAGMENT (no <html>/<body> wrappers). "
    "Email-friendly inline CSS, polished layout. "
    "Outer wrapper: <div style=\"font-family:Segoe UI,Helvetica,Arial,sans-serif;max-width:640px;"
    "color:#222;line-height:1.55;\">.\n\n"
    "ORDER WITHIN EACH PRO LEAGUE SECTION (NFL, MLB, NBA, NHL, F1, Premier League): "
    "Use one <h3> per league. UNDER that league, use TWO subsections IN THIS ORDER: "
    "(A) <h4 style=\"color:#37474f;margin:12px 0 6px;\">Prior day — major stories &amp; results</h4> "
    "first: summarize the biggest news, scores, and outcomes from the PRIOR calendar day only—"
    "use <strong> for scores, teams, and player names. "
    "(B) <h4 style=\"color:#37474f;margin:12px 0 6px;\">Focus teams &amp; today's spotlight</h4> "
    "second: Miami Dolphins, Florida Panthers (NHL), San Diego Padres, Arsenal men's first team (ignore women's teams unless "
    "explicitly labeled men), plus today's key games/times and moneyline-style odds from [odds] tags; "
    "Padres pitcher/stats headlines go here. Details beyond odds/times: one line + "
    "<a href=\"URL\">Read more</a>.\n\n"
    "NCAA — SEPARATE TOP-LEVEL SECTIONS (not nested under NFL), ONLY when college headlines are present in the feed: "
    "Include <h2 style=\"color:#1b5e20;border-bottom:2px solid #c8e6c9;padding-bottom:6px;\">"
    "NCAA Football</h2> and/or <h2 style=\"color:#1b5e20;border-bottom:2px solid #c8e6c9;padding-bottom:6px;\">"
    "NCAA Basketball</h2>. If no NCAA items were provided, omit those sections entirely. "
    "Under EACH NCAA heading, repeat the SAME two-part order: "
    "(1) Prior day — major college football/basketball stories and results; "
    "(2) Focus games &amp; today — notable matchups, times (NY), and moneyline odds when tagged.\n\n"
    "Premier League: Arsenal men's first team only. Formula 1: keep F1 in its own league block with the same "
    "two-part order. "
    "For betting, prioritize moneyline / win odds; use compact "
    "<div style=\"background:#f8f9fa;border-left:4px solid #1565c0;padding:10px 14px;margin:10px 0;border-radius:4px;\"> "
    "for odds + game-time rows. Links: <a href=\"URL\">label</a> only, never bare URLs. "
    "No markdown fences."
)

SPORTS_FOCUS_TEAMS = [
    {
        "label": "NFL - Miami Dolphins",
        "query_template": "Miami Dolphins {date}",
    },
    {
        "label": "Premier League - Arsenal men",
        "query_template": "Arsenal FC men's team Premier League {date}",
    },
]


def get_openai_client(api_key: Optional[str] = None) -> OpenAI:
    key = _strip_env_value(api_key if api_key is not None else os.getenv("OPENAI_API_KEY"))
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set in your environment (.env).")
    if key.startswith("sk-your") or key == "sk-your-key-here":
        raise RuntimeError(
            "OPENAI_API_KEY in .env is still the placeholder (sk-your-key-here). "
            "Paste your real key from https://platform.openai.com/api-keys and "
            "save the file (e.g. Cmd+S) before running—Python reads the file on disk, "
            "not unsaved editor tabs."
        )
    return OpenAI(api_key=key)


def _extract_rss_item_image_url(item) -> str:
    """Best-effort thumbnail from Google News RSS (media:content or enclosure)."""
    raw = str(item)
    m = re.search(
        r'<media:content[^>]+url=["\']([^"\']+)["\']',
        raw,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    m = re.search(
        r'url=["\'](https?://[^"\']+\.(?:jpg|jpeg|png|webp|gif)[^"\']*)["\']',
        raw,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    enc = item.find("enclosure")
    if enc and enc.get("url"):
        return str(enc["url"]).strip()
    return ""


def fetch_google_news(query: str, max_items: int = 10) -> List[Dict[str, str]]:
    """
    Fetch news items from Google News RSS for a given query.
    Returns a list of dicts with keys: title, link, published.
    """
    encoded_query = quote_plus(query)
    url = (
        f"https://news.google.com/rss/search?q={encoded_query}"
        "&hl=en-US&gl=US&ceid=US:en"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "xml")
    items = []
    for item in soup.find_all("item")[:max_items]:
        title_tag = item.find("title")
        link_tag = item.find("link")
        pub_tag = item.find("pubDate")
        title = title_tag.text.strip() if title_tag else ""
        link = link_tag.text.strip() if link_tag else ""
        published = pub_tag.text.strip() if pub_tag else ""
        image_url = _extract_rss_item_image_url(item)
        if title and link:
            row: Dict[str, str] = {"title": title, "link": link, "published": published}
            if image_url:
                row["image_url"] = image_url
            items.append(row)
    return items


def extract_source_from_title(title: str) -> str:
    """
    Google News RSS titles are usually "Story title - Publisher" or "Story | Publisher".
    Return the publisher segment, or "" if none found.
    """
    t = title.strip()
    for sep in (" - ", " | ", " – ", " — "):
        if sep in t:
            parts = t.rsplit(sep, 1)
            if len(parts) == 2:
                suffix = parts[1].strip()
                if suffix:
                    return suffix
    return ""


def _title_source_matches_domain(source_from_title: str, domain: str) -> bool:
    low = source_from_title.lower().strip()
    if not low:
        return False
    hints = TECH_SOURCE_TITLE_HINTS.get(domain, [])
    for h in hints:
        if h in low or low in h:
            return True
    # e.g. "Reuters.com" style suffix
    root = domain.replace(".com", "").replace(".co.uk", "").strip(".")
    if root and len(root) >= 4 and root in low:
        return True
    return False


def filter_articles_by_source(
    articles: List[Dict[str, str]], allowed_domains: List[str]
) -> List[Dict[str, str]]:
    """
    Keep articles whose outlet is in allowed_domains, using the publisher suffix
    parsed from the RSS title (Google redirect links rarely expose real domains).
    """
    filtered: List[Dict[str, str]] = []
    for art in articles:
        title = art.get("title", "")
        source_from_title = extract_source_from_title(title)
        if source_from_title:
            art = {**art, "parsed_source": source_from_title}
        matched = False
        if source_from_title:
            matched = any(
                _title_source_matches_domain(source_from_title, d)
                for d in allowed_domains
            )
        if not matched:
            link = art.get("link", "")
            host = urlparse(link).netloc.lower()
            matched = any(host.endswith(domain) for domain in allowed_domains)
        if matched:
            filtered.append(art)
    return filtered


def _normalize_title_for_dedupe(title: str) -> str:
    """Strip trailing publisher suffix so the same story from different feeds dedupes."""
    base = title.strip()
    for sep in (" - ", " | ", " – ", " — "):
        if sep in base:
            base = base.rsplit(sep, 1)[0].strip()
    return " ".join(base.lower().split())


def dedupe_articles(articles: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen_links: set[str] = set()
    seen_norm_titles: set[str] = set()
    out: List[Dict[str, str]] = []
    for art in articles:
        link = (art.get("link") or "").strip()
        title = art.get("title", "")
        norm = _normalize_title_for_dedupe(title)
        if link and link in seen_links:
            continue
        if norm and norm in seen_norm_titles:
            continue
        if link:
            seen_links.add(link)
        if norm:
            seen_norm_titles.add(norm)
        out.append(art)
    return out


def parse_rss_pub_to_ny(published: str) -> Optional[datetime]:
    """Parse Google News RSS pubDate to America/New_York."""
    if not published or not str(published).strip():
        return None
    try:
        dt = parsedate_to_datetime(published.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(NY_TZ)
    except (TypeError, ValueError, OverflowError):
        return None


def get_news_time_window_endpoints(
    now: Optional[datetime] = None,
) -> tuple[datetime, datetime]:
    """
    Keep items whose pubDate falls in [window_start, window_end] (NY).

    Rules (aligned with product spec):
    - End: current time in New York.
    - Start: the later of (two calendar days before NY "today" at 20:00) and (now − 32 hours).
    - If that span still exceeds 32 hours, clip start to (now − 32 hours).
    """
    now = now or datetime.now(NY_TZ)
    if now.tzinfo is None:
        now = now.replace(tzinfo=NY_TZ)
    else:
        now = now.astimezone(NY_TZ)
    today = now.date()
    two_days_ago_8pm = datetime.combine(
        today - timedelta(days=2), time(20, 0), tzinfo=NY_TZ
    )
    thirty_two_h_ago = now - timedelta(hours=32)
    window_start = max(two_days_ago_8pm, thirty_two_h_ago)
    if now - window_start > timedelta(hours=32):
        window_start = now - timedelta(hours=32)
    return window_start, now


def filter_articles_by_time_window(
    articles: List[Dict[str, str]],
    now: Optional[datetime] = None,
) -> List[Dict[str, str]]:
    """Drop RSS items outside the NY publication window or with unparseable dates."""
    window_start, window_end = get_news_time_window_endpoints(now)
    out: List[Dict[str, str]] = []
    for art in articles:
        pub = parse_rss_pub_to_ny(art.get("published", ""))
        if pub is None:
            continue
        if window_start <= pub <= window_end:
            out.append(art)
    return out


def apply_time_window_or_fallback(
    articles: List[Dict[str, str]], label: str
) -> List[Dict[str, str]]:
    """Prefer items inside the NY RSS window; if none remain, keep the deduped list."""
    filtered = filter_articles_by_time_window(articles)
    if not filtered and articles:
        print(
            f"Warning: {label}: no items passed the publication time window; "
            "using unfiltered headlines.",
            file=sys.stderr,
        )
        return articles
    return filtered


def fetch_big_tech_stock_snapshot_text() -> str:
    """Prior U.S. session vs previous close for major tech-related names (yfinance)."""
    try:
        import yfinance as yf
    except ImportError:
        return "Stock data unavailable (install yfinance: pip install yfinance)."
    lines: List[str] = []
    for sym, name in BIG_TECH_TICKERS:
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="5d")
            if hist is None or hist.empty or len(hist) < 2:
                continue
            last = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2])
            pct = (last - prev) / prev * 100.0
            lines.append(
                f"{name} ({sym}): ${last:.2f}, {pct:+.2f}% vs prior U.S. session close"
            )
        except Exception:
            continue
    if not lines:
        return (
            "Stock snapshot: no recent multi-day history returned (holiday/weekend or "
            "data pause). Mention this briefly in the article if needed."
        )
    return (
        "Prior completed U.S. trading sessions — close vs previous close:\n"
        + "\n".join(lines)
    )


def fetch_tech_ai_news() -> List[Dict[str, str]]:
    # One broad query for major tech & AI companies.
    articles = fetch_google_news(TECH_AI_QUERY, max_items=40)
    mainstream_articles = filter_articles_by_source(articles, TECH_MAINSTREAM_SOURCES)
    base = mainstream_articles or articles[:20]
    y_str = (datetime.now(NY_TZ).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    market_extra = fetch_google_news(
        f"Nasdaq S&P technology stocks market earnings {y_str}",
        max_items=8,
    )
    for art in market_extra:
        art["news_segment"] = "market"
    intl_extra = fetch_google_news(
        f"world news geopolitics global economy oil interest rates markets {y_str}",
        max_items=10,
    )
    for art in intl_extra:
        art["news_segment"] = "international"
    return base + market_extra + intl_extra


def fetch_sports_news() -> List[Dict[str, str]]:
    """
    Fetch yesterday's news for NFL/MLB/NBA/NHL using Google News.
    We hint the date in the query string so results focus on the previous day.
    """
    yesterday = datetime.now(NY_TZ).date() - timedelta(days=1)
    y_str = yesterday.strftime("%Y-%m-%d")
    articles: List[Dict[str, str]] = []
    for league in SPORTS_LEAGUES:
        query = f"{league} {y_str}"
        league_articles = fetch_google_news(query, max_items=8)
        # Tag league in each article for the summarizer.
        for art in league_articles:
            art["league"] = league
        articles.extend(league_articles)

    f1_query = f"Formula 1 OR F1 {y_str}"
    f1_articles = fetch_google_news(f1_query, max_items=8)
    for art in f1_articles:
        art["league"] = "F1"
    articles.extend(f1_articles)

    if is_ncaa_football_season_ny():
        ncaa_fb_y = fetch_google_news(
            f"NCAA college football scores results highlights {y_str}",
            max_items=8,
        )
        for art in ncaa_fb_y:
            art["league"] = "NCAA Football"
        articles.extend(ncaa_fb_y)

    if is_ncaa_basketball_season_ny():
        ncaa_bb_y = fetch_google_news(
            f"NCAA college basketball scores results highlights {y_str}",
            max_items=8,
        )
        for art in ncaa_bb_y:
            art["league"] = "NCAA Basketball"
        articles.extend(ncaa_bb_y)

    # Upcoming games / races: betting odds headlines (deduped later with the rest).
    odds_queries: List[tuple[str, str]] = [
        ("NFL", f"NFL odds point spread upcoming {y_str}"),
        ("MLB", f"MLB odds betting lines upcoming {y_str}"),
        ("NBA", f"NBA odds betting upcoming {y_str}"),
        ("NHL", f"NHL odds betting upcoming {y_str}"),
        ("F1", f"F1 Formula 1 odds betting upcoming {y_str}"),
    ]
    if is_ncaa_football_season_ny():
        odds_queries.append(
            ("NCAA Football", f"NCAA college football odds lines upcoming {y_str}")
        )
    if is_ncaa_basketball_season_ny():
        odds_queries.append(
            ("NCAA Basketball", f"NCAA college basketball odds lines upcoming {y_str}")
        )
    for league, odds_q in odds_queries:
        odds_items = fetch_google_news(odds_q, max_items=4)
        for art in odds_items:
            art["league"] = league
            art["odds_context"] = "true"
        articles.extend(odds_items)

    # Extra focus teams: Miami Dolphins (NFL) and Arsenal (Premier League).
    for team in SPORTS_FOCUS_TEAMS:
        query = team["query_template"].format(date=y_str)
        team_articles = fetch_google_news(query, max_items=8)
        for art in team_articles:
            art["league"] = team["label"]
            art["focus_team"] = "true"
        articles.extend(team_articles)

    today = datetime.now(NY_TZ).date()
    t_str = today.strftime("%Y-%m-%d")
    schedule_queries: List[tuple[str, str]] = [
        ("NFL", f"NFL full schedule games today all matchups {t_str}"),
        ("MLB", f"MLB all games today schedule matchups {t_str}"),
        ("NBA", f"NBA all games today slate schedule {t_str}"),
        ("NHL", f"NHL games today full schedule {t_str}"),
        ("F1", f"Formula 1 F1 schedule today {t_str}"),
    ]
    if is_ncaa_football_season_ny():
        schedule_queries.append(
            ("NCAA Football", f"NCAA college football games today schedule all matchups {t_str}")
        )
    if is_ncaa_basketball_season_ny():
        schedule_queries.append(
            ("NCAA Basketball", f"NCAA college basketball games today schedule slate {t_str}")
        )
    for league, sq in schedule_queries:
        sched = fetch_google_news(sq, max_items=15)
        for art in sched:
            art["league"] = league
            art["schedule_today"] = "true"
        articles.extend(sched)

    odds_today_queries: List[tuple[str, str]] = [
        ("NFL", f"NFL moneyline odds today {t_str} win favorite"),
        ("NBA", f"NBA moneyline odds today {t_str} win"),
        ("MLB", f"MLB moneyline odds today {t_str} win"),
        ("NHL", f"NHL moneyline odds today {t_str} win"),
        ("F1", f"F1 odds betting winner today {t_str}"),
    ]
    if is_ncaa_football_season_ny():
        odds_today_queries.append(
            ("NCAA Football", f"NCAA college football moneyline odds today {t_str}")
        )
    if is_ncaa_basketball_season_ny():
        odds_today_queries.append(
            ("NCAA Basketball", f"NCAA college basketball moneyline odds today {t_str}")
        )
    for league, oq in odds_today_queries:
        odds_today = fetch_google_news(oq, max_items=8)
        for art in odds_today:
            art["league"] = league
            art["odds_context"] = "true"
            art["odds_for_today"] = "true"
            art["odds_moneyline"] = "true"
        articles.extend(odds_today)

    # San Diego Padres: news + pitching + stats for prior calendar day and today (NY).
    for ds, day_tag in ((y_str, "prior_day"), (t_str, "game_day")):
        padres_news = fetch_google_news(f"San Diego Padres {ds} MLB", max_items=8)
        for art in padres_news:
            art["league"] = "MLB"
            art["focus_team"] = "true"
            art["focus_club"] = "Padres"
            art["padres_day"] = day_tag
        articles.extend(padres_news)
        pitchers = fetch_google_news(
            f"San Diego Padres starting pitcher probable lineup {ds} MLB",
            max_items=6,
        )
        for art in pitchers:
            art["league"] = "MLB"
            art["focus_team"] = "true"
            art["focus_club"] = "Padres"
            art["padres_pitcher"] = "true"
            art["padres_day"] = day_tag
        articles.extend(pitchers)
        stats = fetch_google_news(
            f"Padres box score batting stats {ds} MLB",
            max_items=5,
        )
        for art in stats:
            art["league"] = "MLB"
            art["focus_team"] = "true"
            art["focus_club"] = "Padres"
            art["padres_stats"] = "true"
            art["padres_day"] = day_tag
        articles.extend(stats)

    # Florida Panthers (NHL focus): prior day + game day.
    for ds, day_tag in ((y_str, "prior_day"), (t_str, "game_day")):
        panthers = fetch_google_news(f"Florida Panthers NHL {ds}", max_items=8)
        for art in panthers:
            art["league"] = "NHL"
            art["focus_team"] = "true"
            art["focus_club"] = "Panthers"
            art["panthers_day"] = day_tag
        articles.extend(panthers)

    return articles


def _strip_optional_code_fences(text: str) -> str:
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9]*\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def html_to_plain_fallback(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = html_module.unescape(text)
    return re.sub(r"\s+", " ", text).strip()[:25000]


def generate_tech_html_email(
    articles: List[Dict[str, str]],
    stock_snapshot: str,
    *,
    openai_api_key: Optional[str] = None,
    openai_model: Optional[str] = None,
) -> str:
    today_str = datetime.now(NY_TZ).strftime("%Y-%m-%d")
    if not articles:
        esc = html_module.escape(stock_snapshot)
        return (
            f'<div style="font-family:Segoe UI,Helvetica,Arial,sans-serif;max-width:640px;">'
            f"<h2>Market &amp; big tech snapshot</h2><pre style=\"white-space:pre-wrap;\">{esc}</pre>"
            f"<p>No headlines matched filters today.</p></div>"
        )

    blocks: List[str] = []
    for idx, art in enumerate(articles, start=1):
        seg = art.get("news_segment", "")
        extra = f"[segment: {seg}] " if seg else ""
        img = art.get("image_url", "").strip()
        img_line = f"thumbnail URL: {img}" if img else "thumbnail URL: (none)"
        blocks.append(
            f"{idx}. {extra}{art['title']}\n   {img_line}\n   article URL: {art['link']}"
        )

    user_content = (
        f"Calendar date (America/New_York): {today_str}.\n"
        f"Headlines below are filtered to a rolling window ending at send time (NY), "
        f"typically within the last 32 hours.\n\n"
        f"=== STOCK DATA (use accurately in the 200–500 word opening section) ===\n"
        f"{stock_snapshot}\n\n"
        f"=== HEADLINES FOR 'TOP STORIES' — [segment: international] feeds inform macro/equity risk; "
        f"[segment: market] informs sector themes (use links only as <a href=...> in output) ===\n"
        + "\n\n".join(blocks)
    )

    client = get_openai_client(openai_api_key)
    model = _strip_env_value(
        openai_model if openai_model is not None else os.getenv("OPENAI_MODEL")
    ) or "gpt-4.1-mini"

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You produce polished, responsive HTML fragments for email clients. "
                    + TECH_EMAIL_HTML_RULES
                ),
            },
            {"role": "user", "content": user_content},
        ],
        temperature=0.35,
    )
    return _strip_optional_code_fences(response.choices[0].message.content or "")


def generate_sports_html_email(
    articles: List[Dict[str, str]],
    *,
    openai_api_key: Optional[str] = None,
    openai_model: Optional[str] = None,
) -> str:
    today_str = datetime.now(NY_TZ).strftime("%Y-%m-%d")
    if not articles:
        return (
            '<div style="font-family:Segoe UI,Helvetica,Arial,sans-serif;">'
            "<p>No sports headlines found for this run.</p></div>"
        )

    lines: List[str] = []
    for idx, art in enumerate(articles, start=1):
        extra = ""
        if "league" in art:
            extra += f"[{art['league']}] "
        if art.get("schedule_today") == "true":
            extra += "[today schedule] "
        if art.get("odds_context") == "true":
            extra += "[odds] "
        if art.get("odds_for_today") == "true":
            extra += "[odds for today games] "
        if art.get("focus_team") == "true":
            extra += "[focus team] "
        if art.get("focus_club"):
            extra += f"[club: {art['focus_club']}] "
        if art.get("padres_day"):
            extra += f"[Padres day: {art['padres_day']}] "
        if art.get("padres_pitcher") == "true":
            extra += "[Padres pitcher] "
        if art.get("padres_stats") == "true":
            extra += "[Padres stats] "
        if art.get("panthers_day"):
            extra += f"[Panthers day: {art['panthers_day']}] "
        if art.get("odds_moneyline") == "true":
            extra += "[moneyline] "
        lines.append(
            f"{idx}. {extra}{art['title']}\n   URL: {art['link']}"
        )

    user_content = (
        f"Today (America/New_York date): {today_str}.\n"
        f"RSS window: headlines are from roughly the last 32 hours (NY) ending at send time.\n"
        f"Use this date for interpreting 'today' vs 'yesterday'.\n\n"
        "STRUCTURE (mandatory): For NFL, MLB, NBA, NHL, F1, Premier League — under each league heading, "
        "write subsection (A) 'Prior day — major stories & results' BEFORE (B) 'Focus teams & today's "
        "spotlight'. NCAA Football and NCAA Basketball each get their own top-level <h2> section (green "
        "style per rules), with the same (A) then (B) order inside.\n\n"
        f"Raw headlines:\n\n" + "\n\n".join(lines)
    )

    client = get_openai_client(openai_api_key)
    model = _strip_env_value(
        openai_model if openai_model is not None else os.getenv("OPENAI_MODEL")
    ) or "gpt-4.1-mini"

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You produce polished HTML email fragments for sports digests. "
                    + SPORTS_EMAIL_HTML_RULES
                ),
            },
            {"role": "user", "content": user_content},
        ],
        temperature=0.35,
    )
    return _strip_optional_code_fences(response.choices[0].message.content or "")


def send_email(
    subject: str,
    recipient: str,
    html_body: str,
    text_body: Optional[str] = None,
) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    _port = os.getenv("SMTP_PORT", "587") or "587"
    smtp_port = int(_port)
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SENDER_EMAIL") or smtp_username

    if not (smtp_host and smtp_username and smtp_password and sender_email):
        raise RuntimeError(
            "SMTP configuration is incomplete. Please set "
            "SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, and SENDER_EMAIL."
        )

    plain = text_body if text_body is not None else html_to_plain_fallback(html_body)
    msg = MIMEMultipart("alternative")
    msg["From"] = sender_email
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.send_message(msg)


def _run_tech_pipeline(
    recipient: str,
    openai_api_key: str,
    openai_model: str,
) -> None:
    print("Fetching Tech & AI news ...")
    tech_ai_articles = apply_time_window_or_fallback(
        dedupe_articles(fetch_tech_ai_news()),
        "tech",
    )
    print("Fetching big-tech stock snapshot (yfinance) ...")
    stock_snapshot = fetch_big_tech_stock_snapshot_text()
    print("Generating Tech HTML email with OpenAI ...")
    tech_html = generate_tech_html_email(
        tech_ai_articles,
        stock_snapshot,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )
    print("Sending Tech & AI News Summary email ...")
    send_email(
        subject=f"Tech & AI News Summary — {email_run_timestamp_et()}",
        recipient=recipient,
        html_body=tech_html,
    )


def _run_sports_pipeline(
    recipient: str,
    openai_api_key: str,
    openai_model: str,
) -> None:
    print("Fetching sports news ...")
    sports_articles = apply_time_window_or_fallback(
        dedupe_articles(fetch_sports_news()),
        "sports",
    )
    print("Generating Sports HTML email with OpenAI ...")
    sports_html = generate_sports_html_email(
        sports_articles,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )
    print("Sending Daily Sports Summary email ...")
    send_email(
        subject=f"Daily Sports (NFL/MLB/NBA/NHL/F1/NCAA) — {email_run_timestamp_et()}",
        recipient=recipient,
        html_body=sports_html,
    )


def run_once() -> None:
    load_app_env()

    recipient = os.getenv("RECIPIENT_EMAIL")
    if not recipient:
        raise RuntimeError("RECIPIENT_EMAIL is not set in your environment (.env).")

    path = _BOT_DIR / ".env"
    if not path.is_file():
        raise RuntimeError(f"Missing .env next to bot.py: {path}")
    cfg = dotenv_values(path)
    openai_api_key = _strip_env_value(cfg.get("OPENAI_API_KEY"))
    openai_model = _strip_env_value(cfg.get("OPENAI_MODEL")) or "gpt-4.1-mini"
    if not openai_api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is empty in .env. Add your key from "
            "https://platform.openai.com/api-keys and save .env to disk."
        )

    # Tech and sports: fetch → summarize → send in parallel (independent pipelines).
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(
                _run_tech_pipeline, recipient, openai_api_key, openai_model
            ): "tech",
            executor.submit(
                _run_sports_pipeline, recipient, openai_api_key, openai_model
            ): "sports",
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception:
                print(f"Pipeline {name!r} failed.", file=sys.stderr)
                raise

    print("Done.")


def schedule_daily_job() -> None:
    """
    Run every day at DAILY_RUN_TIME (HH:MM, 24h) in America/New_York.
    Defaults to 08:00 New York time.
    """
    load_app_env()
    run_time = os.getenv("DAILY_RUN_TIME", "08:00")
    if len(run_time) != 5 or run_time[2] != ":":
        raise RuntimeError("DAILY_RUN_TIME must be HH:MM (24-hour), e.g. 08:00")

    print(
        f"daily-news-bot: America/New_York, every day at {run_time}. "
        "Press Ctrl+C to stop."
    )
    last_run_date: date | None = None
    while True:
        now = datetime.now(NY_TZ)
        if now.strftime("%H:%M") == run_time:
            if last_run_date != now.date():
                run_once()
                last_run_date = now.date()
            time.sleep(60)
        else:
            time.sleep(15)


if __name__ == "__main__":
    # Usage:
    #   python bot.py run-once   -> run immediately one time
    #   python bot.py            -> daily at DAILY_RUN_TIME in America/New_York
    if len(sys.argv) > 1 and sys.argv[1] == "run-once":
        run_once()
    else:
        schedule_daily_job()

