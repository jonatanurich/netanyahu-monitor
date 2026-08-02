"""מערכת ניטור טקסטואלית לנתניהו ולליכוד.

מקורות: טלגרם ציבורי, RSS, ו-X דרך Nitter באופן ניסיוני.
התראות: UltraMsg לקבוצת WhatsApp.
אחסון: SQLite, כדי למנוע דיווחים כפולים גם אחרי restart.
"""

from __future__ import annotations

import hashlib
import html
import logging
import os
import re
import sqlite3
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import feedparser
import pytz
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify

from sources import RSS_FEEDS, TELEGRAM_CHANNELS, X_ACCOUNTS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


ISRAEL_TZ = pytz.timezone("Asia/Jerusalem")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DB_PATH = DATA_DIR / "monitor.db"

ULTRA_ID = os.getenv("ULTRA_ID", "").strip()
ULTRA_TOKEN = os.getenv("ULTRA_TOKEN", "").strip()
GROUP_ID = os.getenv("GROUP_ID", "").strip()
WHATSAPP_DRY_RUN = env_bool("WHATSAPP_DRY_RUN", True)
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", False)
SEND_EXISTING_ON_START = env_bool("SEND_EXISTING_ON_START", False)

SCAN_INTERVAL_SECONDS = env_int("SCAN_INTERVAL_SECONDS", 90, 30, 3600)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 12, 3, 60)
MAX_WORKERS = env_int("MAX_WORKERS", 12, 2, 30)
HOURLY_SUMMARY_ENABLED = env_bool("HOURLY_SUMMARY_ENABLED", True)
DAILY_SUMMARY_ENABLED = env_bool("DAILY_SUMMARY_ENABLED", True)
X_ENABLED = env_bool("X_ENABLED", False)
PORT = env_int("PORT", 10000, 1, 65535)

NITTER_INSTANCES = [
    value.strip().rstrip("/")
    for value in os.getenv(
        "NITTER_INSTANCES",
        "https://nitter.net,https://nitter.privacydev.net",
    ).split(",")
    if value.strip()
]

EXPLICIT_KEYWORDS = (
    "בנימין נתניהו",
    "נתניהו",
    "ביבי",
    "הליכוד",
    "ליכוד",
    "benjamin netanyahu",
    "netanyahu",
    "likud",
)
PRIME_MINISTER_TERMS = ("ראש הממשלה", "רה״מ", 'רה"מ')
ISRAEL_CONTEXT = (
    "ישראל",
    "ישראלי",
    "הליכוד",
    "ליכוד",
    "קבינט",
    "כנסת",
    "ירושלים",
    "ממשלת ישראל",
    "לשכת ראש הממשלה",
    "עזה",
    "חמאס",
    "צה״ל",
    'צה"ל',
)
FOREIGN_CONTEXT = (
    "בריטניה",
    "קנדה",
    "הודו",
    "אוסטרליה",
    "איטליה",
    "ספרד",
    "צרפת",
    "יוון",
    "פולין",
    "יפן",
)
POLL_KEYWORDS = ("סקר", "סקרים", "מנדט", "מנדטים")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("media-monitor")

HTTP = requests.Session()
HTTP.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (compatible; NetanyahuMediaMonitor/8.0; "
            "+https://github.com/)"
        )
    }
)

app = Flask(__name__)
_monitor_thread: threading.Thread | None = None
_monitor_lock = threading.Lock()
_runtime_status: dict[str, Any] = {
    "started_at": None,
    "last_scan_started": None,
    "last_scan_finished": None,
    "last_scan_new_items": 0,
    "last_error": None,
    "iterations": 0,
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def db_connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=20)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def init_db() -> None:
    with db_connect() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS seen_items (
                item_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                link TEXT,
                first_seen_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_key TEXT UNIQUE NOT NULL,
                local_date TEXT NOT NULL,
                local_time TEXT NOT NULL,
                source_key TEXT NOT NULL,
                source_display TEXT NOT NULL,
                text TEXT NOT NULL,
                link TEXT,
                kind TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_mentions_date
            ON mentions(local_date);
            """
        )


def get_state(key: str) -> str | None:
    with db_connect() as db:
        row = db.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else None


def set_state(key: str, value: str) -> None:
    with db_connect() as db:
        db.execute(
            """
            INSERT INTO app_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def is_seen(item_key: str) -> bool:
    with db_connect() as db:
        return db.execute(
            "SELECT 1 FROM seen_items WHERE item_key = ?", (item_key,)
        ).fetchone() is not None


def mark_seen(item_key: str, source: str, link: str) -> None:
    with db_connect() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO seen_items(item_key, source, link, first_seen_utc)
            VALUES(?, ?, ?, ?)
            """,
            (item_key, source, link, datetime.now(timezone.utc).isoformat()),
        )


def add_mention(
    *,
    item_key: str,
    source_key: str,
    source_display: str,
    text: str,
    link: str,
    kind: str,
    now: datetime,
) -> None:
    with db_connect() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO mentions(
                item_key, local_date, local_time, source_key, source_display,
                text, link, kind, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_key,
                now.strftime("%Y-%m-%d"),
                now.strftime("%H:%M:%S"),
                source_key,
                source_display,
                text,
                link,
                kind,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def mentions_for_date(local_date: str) -> list[sqlite3.Row]:
    with db_connect() as db:
        return list(
            db.execute(
                """
                SELECT * FROM mentions
                WHERE local_date = ?
                ORDER BY local_time ASC, id ASC
                """,
                (local_date,),
            ).fetchall()
        )


def cleanup_database() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    with db_connect() as db:
        db.execute("DELETE FROM seen_items WHERE first_seen_utc < ?", (cutoff,))
        db.execute("DELETE FROM mentions WHERE created_at_utc < ?", (cutoff,))


# ---------------------------------------------------------------------------
# Text and relevance
# ---------------------------------------------------------------------------


def israel_now() -> datetime:
    return datetime.now(ISRAEL_TZ)


def clean_text(value: str) -> str:
    soup = BeautifulSoup(html.unescape(value or ""), "html.parser")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def normalized_text(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^\w\u0590-\u05FF]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def make_item_key(source: str, link: str, text: str) -> str:
    # קישור הוא המזהה היציב הטוב ביותר. אם חסר, משתמשים בטקסט מנורמל.
    identity = f"{source}|{link.strip()}" if link.strip() else f"{source}|{normalized_text(text)}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def contains_relevant_mention(text: str, source_key: str = "") -> bool:
    lowered = clean_text(text).lower()
    if any(keyword in lowered for keyword in EXPLICIT_KEYWORDS):
        return True

    has_pm_term = any(term.lower() in lowered for term in PRIME_MINISTER_TERMS)
    if not has_pm_term:
        return False

    # חשבונות רשמיים של נתניהו/רה״מ מקבלים אמון גבוה יותר.
    if source_key.lower() in {"netanyahu", "israelpm"}:
        return True

    has_israel_context = any(word.lower() in lowered for word in ISRAEL_CONTEXT)
    has_foreign_context = any(word.lower() in lowered for word in FOREIGN_CONTEXT)
    return has_israel_context and not has_foreign_context


def contains_poll(text: str) -> bool:
    lowered = clean_text(text).lower()
    has_poll = any(keyword in lowered for keyword in POLL_KEYWORDS)
    has_target = any(keyword in lowered for keyword in EXPLICIT_KEYWORDS)
    return has_poll and has_target


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------


def fetch_telegram(channel: str) -> list[dict[str, str]]:
    url = f"https://t.me/s/{channel}"
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        messages: list[dict[str, str]] = []
        wraps = soup.find_all("div", class_="tgme_widget_message_wrap")[-8:]
        for wrap in wraps:
            text_div = wrap.find("div", class_="tgme_widget_message_text")
            if not text_div:
                continue
            text = clean_text(text_div.get_text(" ", strip=True))
            if len(text) < 15:
                continue
            link_tag = wrap.find("a", class_="tgme_widget_message_date")
            link = str(link_tag.get("href", "")) if link_tag else url
            messages.append({"text": text, "link": link, "source": channel})
        return messages
    except requests.RequestException as exc:
        LOGGER.warning("Telegram source failed: %s | %s", channel, exc)
        return []


def fetch_rss(name: str, url: str) -> list[dict[str, str]]:
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        messages: list[dict[str, str]] = []
        for entry in feed.entries[:10]:
            title = clean_text(str(entry.get("title", "")))
            description = clean_text(
                str(entry.get("summary", entry.get("description", "")))
            )
            text = clean_text(f"{title} {description}")
            link = str(entry.get("link", ""))
            if text:
                messages.append(
                    {
                        "text": text,
                        "link": link,
                        "source": name,
                        "title": title,
                    }
                )
        return messages
    except requests.RequestException as exc:
        LOGGER.warning("RSS source failed: %s | %s", name, exc)
        return []


def fetch_x(username: str) -> list[dict[str, str]]:
    if not X_ENABLED:
        return []

    for instance in NITTER_INSTANCES:
        try:
            response = HTTP.get(
                f"{instance}/{username}/rss", timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            messages: list[dict[str, str]] = []
            for entry in feed.entries[:6]:
                text = clean_text(
                    str(entry.get("title", entry.get("description", "")))
                )
                link = str(entry.get("link", f"https://x.com/{username}"))
                if text:
                    messages.append(
                        {"text": text, "link": link, "source": username}
                    )
            if messages:
                return messages
        except requests.RequestException:
            continue

    LOGGER.warning("All configured Nitter instances failed for @%s", username)
    return []


def fetch_all_sources() -> list[tuple[str, str, str, dict[str, str]]]:
    results: list[tuple[str, str, str, dict[str, str]]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures: dict[Any, tuple[str, str, str]] = {}

        for key, display in TELEGRAM_CHANNELS.items():
            futures[executor.submit(fetch_telegram, key)] = ("TG", key, display)
        for key, display in RSS_FEEDS.items():
            futures[executor.submit(fetch_rss, key, display)] = ("RSS", key, key)
        if X_ENABLED:
            for key, display in X_ACCOUNTS.items():
                futures[executor.submit(fetch_x, key)] = ("X", key, display)

        for future in as_completed(futures):
            source_type, key, display = futures[future]
            try:
                for item in future.result():
                    results.append((source_type, key, display, item))
            except Exception as exc:  # הגנה ברמת source אחד
                LOGGER.exception("Source processing failed: %s | %s", key, exc)
    return results


# ---------------------------------------------------------------------------
# WhatsApp
# ---------------------------------------------------------------------------


def split_message(text: str, limit: int = 3500) -> list[str]:
    if len(text) <= limit:
        return [text]

    parts: list[str] = []
    current = ""
    for paragraph in text.split("\n"):
        candidate = f"{current}\n{paragraph}".strip()
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            parts.append(current)
        while len(paragraph) > limit:
            parts.append(paragraph[:limit])
            paragraph = paragraph[limit:]
        current = paragraph
    if current:
        parts.append(current)
    return parts


def send_whatsapp(text: str) -> bool:
    if WHATSAPP_DRY_RUN:
        LOGGER.info("WHATSAPP_DRY_RUN | %s", text[:300].replace("\n", " | "))
        return True

    if not all((ULTRA_ID, ULTRA_TOKEN, GROUP_ID)):
        LOGGER.error(
            "Missing ULTRA_ID, ULTRA_TOKEN or GROUP_ID. WhatsApp message not sent."
        )
        return False

    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    parts = split_message(text)

    for index, part in enumerate(parts, start=1):
        body = part
        if len(parts) > 1:
            body = f"({index}/{len(parts)})\n{part}"
        payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": body}

        sent = False
        for attempt in range(1, 4):
            try:
                response = HTTP.post(url, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
                response.raise_for_status()
                LOGGER.info("WhatsApp sent: HTTP %s", response.status_code)
                sent = True
                break
            except requests.RequestException as exc:
                LOGGER.warning(
                    "WhatsApp attempt %s/3 failed: %s", attempt, exc
                )
                time.sleep(2 ** (attempt - 1))
        if not sent:
            return False
        time.sleep(0.5)
    return True


# ---------------------------------------------------------------------------
# Alerts and summaries
# ---------------------------------------------------------------------------


def format_alert(display: str, text: str, link: str, is_poll: bool, now: datetime) -> str:
    headline = (
        f"📊 *סקר חדש — ליכוד/נתניהו | {display}*"
        if is_poll
        else f"🚨 *אזכור נתניהו/הליכוד | {display}*"
    )
    link_line = f"\n\n🔗 {link}" if link else ""
    return (
        f"{headline}\n\n"
        f"💬 {text[:900]}"
        f"{link_line}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M:%S')}"
    )


def process_scan() -> int:
    now = israel_now()
    is_bootstrapped = get_state("bootstrap_complete") == "1"
    suppress_alerts = not is_bootstrapped and not SEND_EXISTING_ON_START
    new_mentions = 0

    for _source_type, key, display, item in fetch_all_sources():
        text = clean_text(item.get("text", ""))
        link = item.get("link", "").strip()
        if not text:
            continue

        is_poll = contains_poll(text)
        is_relevant = contains_relevant_mention(text, key)
        if not is_poll and not is_relevant:
            continue

        item_key = make_item_key(key, link, text)
        if is_seen(item_key):
            continue
        mark_seen(item_key, key, link)

        # בהפעלה הראשונה מסמנים את הפריטים הקיימים בלי להציף את הקבוצה.
        if suppress_alerts:
            continue

        kind = "poll" if is_poll else "mention"
        add_mention(
            item_key=item_key,
            source_key=key,
            source_display=display,
            text=text[:1500],
            link=link,
            kind=kind,
            now=now,
        )
        send_whatsapp(format_alert(display, text, link, is_poll, now))
        new_mentions += 1

    if not is_bootstrapped:
        set_state("bootstrap_complete", "1")
        LOGGER.info(
            "Initial source snapshot completed. Existing items were %s.",
            "sent" if SEND_EXISTING_ON_START else "suppressed",
        )

    return new_mentions


def get_hourly_summary() -> str | None:
    headlines_by_site: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as executor:
        futures = {
            executor.submit(fetch_rss, name, url): name
            for name, url in RSS_FEEDS.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            titles: list[str] = []
            for item in future.result()[:3]:
                title = clean_text(item.get("title", item.get("text", "")))[:140]
                if len(title) >= 10:
                    titles.append(title)
            if titles:
                headlines_by_site[name] = titles

    if not headlines_by_site:
        return None

    now = israel_now()
    lines = [
        f"📰 *סיכום כותרות — {now.strftime('%H:00 | %d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for site in sorted(headlines_by_site):
        lines.append(f"\n*{site}:*")
        lines.extend(f"• {title}" for title in headlines_by_site[site])
    lines.append(f"\n⏰ {now.strftime('%H:%M')} | בוט תקשוב")
    return "\n".join(lines)


def send_scheduled_summaries() -> None:
    now = israel_now()
    today = now.strftime("%Y-%m-%d")

    if HOURLY_SUMMARY_ENABLED and 7 <= now.hour <= 22 and now.minute <= 5:
        hourly_key = f"{today}-{now.hour:02d}"
        if get_state("last_hourly_summary") != hourly_key:
            summary = get_hourly_summary()
            if summary and send_whatsapp(summary):
                set_state("last_hourly_summary", hourly_key)

    if DAILY_SUMMARY_ENABLED and now.hour == 18 and now.minute <= 5:
        if get_state("last_daily_summary") != today:
            summary = build_daily_summary(today, now)
            if send_whatsapp(summary):
                set_state("last_daily_summary", today)


def build_daily_summary(local_date: str, now: datetime | None = None) -> str:
    now = now or israel_now()
    mentions = mentions_for_date(local_date)
    if not mentions:
        return (
            f"📊 *סיכום יומי נתניהו והליכוד — {now.strftime('%d/%m/%Y')}*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "לא נמצאו היום אזכורים חדשים שנשלחו על ידי המערכת."
        )

    sources = Counter(row["source_display"] for row in mentions)
    topics: Counter[str] = Counter()
    for row in mentions:
        text = str(row["text"])
        if any(word in text for word in ("שריון", "פריימריז", "ליכוד", "רשימה")):
            topics["ליכוד / פריימריז"] += 1
        if any(word in text for word in ("משפט", "שוחד", "בית משפט", "עדות")):
            topics["משפט נתניהו"] += 1
        if any(word in text for word in ("עזה", "חמאס", "צה״ל", 'צה"ל', "איראן")):
            topics["ביטחוני / מדיני"] += 1
        if any(word in text for word in ("כנסת", "חוק", "ממשלה", "קבינט")):
            topics["פוליטי / כנסת"] += 1
        if any(word in text for word in POLL_KEYWORDS):
            topics["סקרים"] += 1

    lines = [
        f"📊 *סיכום יומי נתניהו והליכוד — {now.strftime('%d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"*סה״כ {len(mentions)} אזכורים חדשים היום*",
    ]
    if topics:
        lines.append("\n🔥 *נושאים מרכזיים:*")
        lines.extend(f"• {topic}: {count}" for topic, count in topics.most_common(5))
    lines.append("\n📈 *פילוח לפי מקור:*")
    lines.extend(f"• {source}: {count}" for source, count in sources.most_common(8))
    lines.append("\n📰 *דיווחים מרכזיים:*")
    for index, row in enumerate(mentions[:25], start=1):
        short = clean_text(str(row["text"]))[:180]
        lines.append(
            f"\n*{index}. [{row['local_time'][:5]}] {row['source_display']}*\n{short}"
        )
    if len(mentions) > 25:
        lines.append(f"\n… ועוד {len(mentions) - 25} דיווחים.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Runtime and web health service
# ---------------------------------------------------------------------------


def monitor_loop() -> None:
    init_db()
    _runtime_status["started_at"] = datetime.now(timezone.utc).isoformat()

    if SEND_STARTUP_MESSAGE:
        send_whatsapp(
            "🚀 *בוט התקשורת עלה*\n\n"
            f"📡 {len(TELEGRAM_CHANNELS)} ערוצי טלגרם + "
            f"{len(RSS_FEEDS)} פידי חדשות"
            f"{' + X ניסיוני' if X_ENABLED else ''}\n"
            "🔍 ניטור: נתניהו, ביבי והליכוד"
        )

    cleanup_counter = 0
    while True:
        cycle_started = time.monotonic()
        _runtime_status["iterations"] += 1
        _runtime_status["last_scan_started"] = datetime.now(timezone.utc).isoformat()
        try:
            new_items = process_scan()
            send_scheduled_summaries()
            _runtime_status["last_scan_new_items"] = new_items
            _runtime_status["last_error"] = None
            LOGGER.info(
                "Scan completed | new=%s | next scan in %ss",
                new_items,
                SCAN_INTERVAL_SECONDS,
            )
        except Exception as exc:
            _runtime_status["last_error"] = str(exc)
            LOGGER.exception("Monitor cycle failed")
        finally:
            _runtime_status["last_scan_finished"] = datetime.now(timezone.utc).isoformat()

        cleanup_counter += 1
        if cleanup_counter >= 720:  # בערך פעם ביום בסריקה של שתי דקות
            try:
                cleanup_database()
            except Exception:
                LOGGER.exception("Database cleanup failed")
            cleanup_counter = 0

        elapsed = time.monotonic() - cycle_started
        time.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))


def start_monitor_thread() -> None:
    global _monitor_thread
    with _monitor_lock:
        if _monitor_thread and _monitor_thread.is_alive():
            return
        _monitor_thread = threading.Thread(
            target=monitor_loop,
            name="media-monitor",
            daemon=True,
        )
        _monitor_thread.start()
        LOGGER.info("Monitor thread started")


@app.before_request
def ensure_monitor_started() -> None:
    start_monitor_thread()


@app.get("/")
def index() -> str:
    return """
    <!doctype html>
    <html lang="he" dir="rtl">
      <meta charset="utf-8">
      <title>מערכת ניטור תקשורת</title>
      <body style="font-family:Arial;max-width:760px;margin:60px auto;line-height:1.6">
        <h1>מערכת ניטור התקשורת פעילה</h1>
        <p>המערכת מנטרת מקורות טקסטואליים עבור אזכורים של נתניהו והליכוד.</p>
        <p><a href="/health">בדיקת מצב</a></p>
      </body>
    </html>
    """


@app.get("/health")
def health():
    init_db()
    return jsonify(
        {
            "ok": True,
            "monitor_alive": bool(_monitor_thread and _monitor_thread.is_alive()),
            "whatsapp_dry_run": WHATSAPP_DRY_RUN,
            "x_enabled": X_ENABLED,
            "database": str(DB_PATH),
            "sources": {
                "telegram": len(TELEGRAM_CHANNELS),
                "rss": len(RSS_FEEDS),
                "x": len(X_ACCOUNTS) if X_ENABLED else 0,
            },
            "runtime": _runtime_status,
        }
    )


if __name__ == "__main__":
    start_monitor_thread()
    app.run(host="0.0.0.0", port=PORT)
