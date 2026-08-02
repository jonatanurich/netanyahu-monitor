"""מערכת ניטור טקסטואלית לנתניהו ולליכוד.

מקורות: ערוצי טלגרם ציבוריים + פידי RSS.
התראות: UltraMsg לקבוצת WhatsApp, דרך תור שליחה עמיד (outbox).
אחסון: SQLite — מונע דיווחים כפולים גם אחרי restart.

עקרונות מרכזיים בגרסה הזאת:
1. דדופ דו-שכבתי: פר-פריט (item_key) ופר-סיפור (clusters), כדי שאותה
   ידיעה משמונה מקורות לא תיהפך לשמונה הודעות בקבוצה.
2. שליחה אסינכרונית מתור מתמיד — סריקה לא נחסמת, ופריט שנכשל בשליחה
   לא הולך לאיבוד.
3. /health מחזיר 503 כשהמנוע תקוע, כדי ש-Render יפעיל restart במקום
   שהמערכת תמות בשקט.
"""

from __future__ import annotations

import hashlib
import html
import json
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
from typing import Any, Iterable

import feedparser
import pytz
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify

from sources import RSS_FEEDS, TELEGRAM_CHANNELS

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


def env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
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
TELEGRAM_MESSAGES_PER_SCAN = env_int("TELEGRAM_MESSAGES_PER_SCAN", 20, 5, 50)

# דדופ בין מקורות
CLUSTER_WINDOW_MINUTES = env_int("CLUSTER_WINDOW_MINUTES", 20, 2, 240)
CLUSTER_SIMILARITY = env_float("CLUSTER_SIMILARITY", 0.5, 0.2, 0.95)
CLUSTER_MIN_SHARED_TOKENS = env_int("CLUSTER_MIN_SHARED_TOKENS", 4, 2, 20)
ESCALATION_THRESHOLD = env_int("ESCALATION_THRESHOLD", 5, 0, 50)

# תור שליחה
SEND_MIN_INTERVAL_SECONDS = env_float("SEND_MIN_INTERVAL_SECONDS", 1.5, 0.2, 30.0)
SEND_MAX_ATTEMPTS = env_int("SEND_MAX_ATTEMPTS", 6, 1, 20)

# סיכומים
DIGEST_HOURS = tuple(
    sorted(
        {
            int(part)
            for part in os.getenv("DIGEST_HOURS", "12,20").split(",")
            if part.strip().isdigit() and 0 <= int(part) <= 23
        }
    )
)
DAILY_SUMMARY_ENABLED = env_bool("DAILY_SUMMARY_ENABLED", True)
DAILY_SUMMARY_HOUR = env_int("DAILY_SUMMARY_HOUR", 18, 0, 23)

RETENTION_DAYS = env_int("RETENTION_DAYS", 30, 3, 365)
PORT = env_int("PORT", 10000, 1, 65535)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("media-monitor")

HTTP = requests.Session()
HTTP.mount(
    "https://",
    requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS + 4, pool_maxsize=MAX_WORKERS + 4
    ),
)
HTTP.headers.update(
    {
        # אתרי חדשות ישראליים חוסמים User-Agent של בוט ב-403.
        "User-Agent": os.getenv(
            "USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        ),
        "Accept": (
            "application/rss+xml, application/xml, application/atom+xml, "
            "text/xml;q=0.9, text/html;q=0.8, */*;q=0.5"
        ),
        "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    }
)

app = Flask(__name__)
_threads_lock = threading.Lock()
_monitor_thread: threading.Thread | None = None
_sender_thread: threading.Thread | None = None

_runtime_status: dict[str, Any] = {
    "started_at": None,
    "last_scan_started": None,
    "last_scan_finished": None,
    "last_scan_new_stories": 0,
    "last_scan_duplicates": 0,
    "last_scan_seconds": None,
    "last_error": None,
    "consecutive_errors": 0,
    "iterations": 0,
    "last_send_ok": None,
    "last_send_error": None,
    "thread_error": None,
}

BOOT_MONOTONIC = time.monotonic()
HEALTH_GRACE_SECONDS = env_int("HEALTH_GRACE_SECONDS", 180, 30, 900)

# ---------------------------------------------------------------------------
# Relevance matching
# ---------------------------------------------------------------------------

HEB = "\u0590-\u05FF"
HEB_PREFIX = "[והבלמשכ]{0,2}"

HEBREW_KEYWORDS = (
    "בנימין נתניהו",
    "נתניהו",
    "ביבי",
    "ליכוד",
    "ליכודניק",
    "ליכודניקים",
)
LATIN_KEYWORDS = ("benjamin netanyahu", "netanyahu", "likud", "bibi")

PRIME_MINISTER_TERMS = ("ראש הממשלה", "רה״מ", 'רה"מ', "רה׳׳מ")

# הקשר זר — מונע התראה על "ראש הממשלה" של מדינה אחרת.
FOREIGN_CONTEXT = (
    "בריטניה",
    "הבריטי",
    "קנדה",
    "הקנדי",
    "הודו",
    "ההודי",
    "אוסטרליה",
    "האוסטרלי",
    "איטליה",
    "האיטלקי",
    "ספרד",
    "הספרדי",
    "צרפת",
    "הצרפתי",
    "יוון",
    "היווני",
    "פולין",
    "הפולני",
    "יפן",
    "היפני",
    "הונגריה",
    "ההונגרי",
    "אלבניה",
    "בלגיה",
    "הולנד",
    "שוודיה",
    "נורבגיה",
    "אירלנד",
    "סלובניה",
    "צ׳כיה",
    'צ"כיה',
    "ניו זילנד",
    "הניו זילנדי",
)

POLL_KEYWORDS = ("סקר", "סקרים", "מנדט", "מנדטים", "מצביעים")


def _hebrew_pattern(word: str) -> str:
    return rf"(?<![{HEB}]){HEB_PREFIX}{re.escape(word)}(?![{HEB}])"


TARGET_RE = re.compile(
    "|".join(
        [_hebrew_pattern(word) for word in HEBREW_KEYWORDS]
        + [rf"\b{re.escape(word)}\b" for word in LATIN_KEYWORDS]
    ),
    re.IGNORECASE,
)
PM_RE = re.compile(
    "|".join(_hebrew_pattern(term) for term in PRIME_MINISTER_TERMS)
)
FOREIGN_RE = re.compile(
    "|".join(_hebrew_pattern(term) for term in FOREIGN_CONTEXT)
)
POLL_RE = re.compile("|".join(_hebrew_pattern(word) for word in POLL_KEYWORDS))

HEBREW_STOPWORDS = {
    "של",
    "על",
    "את",
    "עם",
    "לא",
    "זה",
    "זו",
    "הוא",
    "היא",
    "הם",
    "הן",
    "אחרי",
    "לפני",
    "כי",
    "גם",
    "אבל",
    "יותר",
    "כל",
    "אשר",
    "כדי",
    "מה",
    "מי",
    "אם",
    "או",
    "בין",
    "אל",
    "עד",
    "כך",
    "כמו",
    "היום",
    "אמר",
    "אמרה",
    "אמרו",
    "לאחר",
    "בעקבות",
    "לפי",
    "כפי",
    "נגד",
    "בתוך",
    "עוד",
    "רק",
    "כבר",
    "יש",
    "אין",
    "היה",
    "הייתה",
    "יהיה",
    "תוך",
    "בשל",
    "מול",
    "אצל",
    "הזה",
    "הזו",
    "כאשר",
}


def strip_prefix(token: str) -> str:
    if len(token) > 3 and token[0] in "והבלמשכ":
        return token[1:]
    return token


def significant_tokens(text: str) -> set[str]:
    """טוקנים משמעותיים להשוואת דמיון בין ידיעות ממקורות שונים."""
    cleaned = re.sub(r"https?://\S+", " ", text)
    raw = re.findall(rf"[{HEB}a-zA-Z0-9]+", cleaned)
    tokens: set[str] = set()
    for token in raw:
        token = token.lower()
        if token in HEBREW_STOPWORDS:
            continue
        token = strip_prefix(token)
        if len(token) < 3 or token in HEBREW_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def jaccard(left: set[str], right: set[str]) -> tuple[float, int]:
    if not left or not right:
        return 0.0, 0
    shared = left & right
    union = left | right
    return len(shared) / len(union), len(shared)


def contains_relevant_mention(text: str) -> bool:
    if TARGET_RE.search(text):
        return True
    if not PM_RE.search(text):
        return False
    # "ראש הממשלה" נחשב רלוונטי אלא אם מדובר בבירור בראש ממשלה זר.
    return not FOREIGN_RE.search(text)


def contains_poll(text: str) -> bool:
    return bool(POLL_RE.search(text) and TARGET_RE.search(text))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_local = threading.local()


def db() -> sqlite3.Connection:
    """חיבור SQLite אחד לכל thread, במקום חיבור חדש לכל שאילתה."""
    connection: sqlite3.Connection | None = getattr(_local, "connection", None)
    if connection is not None:
        return connection

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        DB_PATH, timeout=20, isolation_level=None, check_same_thread=False
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA synchronous=NORMAL")
    _local.connection = connection
    return connection


def table_columns(table: str) -> set[str]:
    return {
        str(row["name"]) for row in db().execute(f"PRAGMA table_info({table})").fetchall()
    }


def migrate_db() -> None:
    """הוספת עמודות לטבלאות שכבר קיימות בדיסק מגרסאות קודמות.

    CREATE TABLE IF NOT EXISTS לא נוגע בטבלה קיימת, ולכן בלי המיגרציה
    הזאת כל סריקה תיפול על עמודה חסרה.
    """
    connection = db()
    additions = {
        "mentions": {
            "cluster_id": "INTEGER",
            "is_duplicate": "INTEGER NOT NULL DEFAULT 0",
        },
        "outbox": {"sent_at_utc": "TEXT", "last_error": "TEXT"},
        "clusters": {"escalated": "INTEGER NOT NULL DEFAULT 0"},
    }
    for table, columns in additions.items():
        existing = table_columns(table)
        if not existing:
            continue
        for column, definition in columns.items():
            if column in existing:
                continue
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
            )
            LOGGER.info("Schema migration: added %s.%s", table, column)


def init_db() -> None:
    db().executescript(
        """
        CREATE TABLE IF NOT EXISTS seen_items (
            item_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            link TEXT,
            first_seen_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tokens TEXT NOT NULL,
            sample_text TEXT NOT NULL,
            kind TEXT NOT NULL,
            sources TEXT NOT NULL,
            source_count INTEGER NOT NULL DEFAULT 1,
            escalated INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_key TEXT UNIQUE NOT NULL,
            cluster_id INTEGER,
            local_date TEXT NOT NULL,
            local_time TEXT NOT NULL,
            source_key TEXT NOT NULL,
            source_display TEXT NOT NULL,
            text TEXT NOT NULL,
            link TEXT,
            kind TEXT NOT NULL,
            is_duplicate INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            body TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 5,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_utc TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            sent_at_utc TEXT,
            last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS app_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_mentions_date ON mentions(local_date);
        CREATE INDEX IF NOT EXISTS idx_clusters_created ON clusters(created_at_utc);
        CREATE INDEX IF NOT EXISTS idx_outbox_pending
            ON outbox(status, next_attempt_utc);
        """
    )
    migrate_db()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state(key: str) -> str | None:
    row = db().execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else None


def set_state(key: str, value: str) -> None:
    db().execute(
        """
        INSERT INTO app_state(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def filter_unseen(keys: list[str]) -> set[str]:
    """מחזיר את המפתחות שעדיין לא נראו — בשאילתות batch ולא אחת לפריט."""
    unseen = set(keys)
    connection = db()
    for start in range(0, len(keys), 400):
        chunk = keys[start : start + 400]
        placeholders = ",".join("?" * len(chunk))
        rows = connection.execute(
            f"SELECT item_key FROM seen_items WHERE item_key IN ({placeholders})",
            chunk,
        ).fetchall()
        unseen.difference_update(str(row["item_key"]) for row in rows)
    return unseen


def mark_seen(item_key: str, source: str, link: str) -> None:
    db().execute(
        """
        INSERT OR IGNORE INTO seen_items(item_key, source, link, first_seen_utc)
        VALUES(?, ?, ?, ?)
        """,
        (item_key, source, link, utc_now_iso()),
    )


def mentions_for_date(local_date: str) -> list[sqlite3.Row]:
    return list(
        db().execute(
            """
            SELECT * FROM mentions
            WHERE local_date = ?
            ORDER BY local_time ASC, id ASC
            """,
            (local_date,),
        ).fetchall()
    )


def cleanup_database() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    outbox_cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    connection = db()
    connection.execute("DELETE FROM seen_items WHERE first_seen_utc < ?", (cutoff,))
    connection.execute("DELETE FROM mentions WHERE created_at_utc < ?", (cutoff,))
    connection.execute("DELETE FROM clusters WHERE created_at_utc < ?", (cutoff,))
    connection.execute(
        "DELETE FROM outbox WHERE status IN ('sent','failed') AND created_at_utc < ?",
        (outbox_cutoff,),
    )
    LOGGER.info("Database cleanup completed")


# ---------------------------------------------------------------------------
# Text helpers
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
    text = re.sub(rf"[^\w{HEB}]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def make_item_key(source: str, link: str, text: str) -> str:
    identity = (
        f"{source}|{link.strip()}"
        if link.strip()
        else f"{source}|{normalized_text(text)}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Cross-source clustering
# ---------------------------------------------------------------------------


def find_or_create_cluster(
    text: str, kind: str, source_display: str
) -> tuple[int, bool, int]:
    """מחזיר (cluster_id, is_new, source_count).

    אם הידיעה כבר דווחה במקור אחר בחלון הזמן — מצטרפים לאשכול הקיים
    במקום לשלוח התראה נוספת.
    """
    tokens = significant_tokens(text)
    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(minutes=CLUSTER_WINDOW_MINUTES)).isoformat()
    connection = db()

    if len(tokens) >= CLUSTER_MIN_SHARED_TOKENS:
        rows = connection.execute(
            """
            SELECT id, tokens, sources, source_count FROM clusters
            WHERE updated_at_utc >= ?
            ORDER BY id DESC
            LIMIT 400
            """,
            (window_start,),
        ).fetchall()

        best_id: int | None = None
        best_score = 0.0
        best_sources: list[str] = []
        best_count = 0
        for row in rows:
            other = set(str(row["tokens"]).split())
            score, shared = jaccard(tokens, other)
            if (
                score >= CLUSTER_SIMILARITY
                and shared >= CLUSTER_MIN_SHARED_TOKENS
                and score > best_score
            ):
                best_id = int(row["id"])
                best_score = score
                try:
                    best_sources = list(json.loads(str(row["sources"])))
                except (ValueError, TypeError):
                    best_sources = []
                best_count = int(row["source_count"])

        if best_id is not None:
            if source_display not in best_sources:
                best_sources.append(source_display)
                best_count = len(best_sources)
            connection.execute(
                """
                UPDATE clusters
                SET sources = ?, source_count = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (json.dumps(best_sources, ensure_ascii=False), best_count,
                 now_utc.isoformat(), best_id),
            )
            return best_id, False, best_count

    cursor = connection.execute(
        """
        INSERT INTO clusters(
            tokens, sample_text, kind, sources, source_count,
            escalated, created_at_utc, updated_at_utc
        ) VALUES(?, ?, ?, ?, 1, 0, ?, ?)
        """,
        (
            " ".join(sorted(tokens)),
            text[:400],
            kind,
            json.dumps([source_display], ensure_ascii=False),
            now_utc.isoformat(),
            now_utc.isoformat(),
        ),
    )
    return int(cursor.lastrowid or 0), True, 1


def cluster_sources(cluster_id: int) -> list[str]:
    row = db().execute(
        "SELECT sources FROM clusters WHERE id = ?", (cluster_id,)
    ).fetchone()
    if not row:
        return []
    try:
        return list(json.loads(str(row["sources"])))
    except (ValueError, TypeError):
        return []


def mark_escalated(cluster_id: int) -> bool:
    cursor = db().execute(
        "UPDATE clusters SET escalated = 1 WHERE id = ? AND escalated = 0",
        (cluster_id,),
    )
    return bool(cursor.rowcount)


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------


def fetch_telegram(channel: str) -> list[dict[str, str]]:
    url = f"https://t.me/s/{channel}"
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
        selected = wraps[-TELEGRAM_MESSAGES_PER_SCAN:]
        messages: list[dict[str, str]] = []
        for wrap in selected:
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
        for entry in feed.entries[:15]:
            title = clean_text(str(entry.get("title", "")))
            description = clean_text(
                str(entry.get("summary", entry.get("description", "")))
            )
            text = clean_text(f"{title} {description}")
            if text:
                messages.append(
                    {
                        "text": text,
                        "link": str(entry.get("link", "")),
                        "source": name,
                        "title": title,
                    }
                )
        return messages
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("RSS source failed: %s | %s", name, exc)
        return []


def validate_rss_feeds() -> None:
    """בדיקת תקינות חד-פעמית בעלייה — פיד שבור לא נשאר שקוף."""
    broken: list[str] = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as executor:
        futures = {
            executor.submit(fetch_rss, name, url): name
            for name, url in RSS_FEEDS.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                if not future.result():
                    broken.append(name)
            except Exception:
                broken.append(name)
    if broken:
        LOGGER.warning("RSS feeds returned nothing: %s", ", ".join(sorted(broken)))
    else:
        LOGGER.info("All %s RSS feeds responded", len(RSS_FEEDS))


def fetch_all_sources() -> list[tuple[str, str, str, dict[str, str]]]:
    results: list[tuple[str, str, str, dict[str, str]]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures: dict[Any, tuple[str, str, str]] = {}
        for key, display in TELEGRAM_CHANNELS.items():
            futures[executor.submit(fetch_telegram, key)] = ("TG", key, display)
        for name, url in RSS_FEEDS.items():
            futures[executor.submit(fetch_rss, name, url)] = ("RSS", name, name)

        for future in as_completed(futures):
            source_type, key, display = futures[future]
            try:
                for item in future.result():
                    results.append((source_type, key, display, item))
            except Exception as exc:
                LOGGER.exception("Source processing failed: %s | %s", key, exc)
    return results


# ---------------------------------------------------------------------------
# WhatsApp outbox
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
            current = ""
        while len(paragraph) > limit:
            parts.append(paragraph[:limit])
            paragraph = paragraph[limit:]
        current = paragraph
    if current:
        parts.append(current)
    return parts


def enqueue_message(text: str, priority: int = 5) -> None:
    """כותב לתור. הכתיבה עמידה — restart לא מאבד הודעות."""
    parts = split_message(text)
    now = utc_now_iso()
    connection = db()
    for index, part in enumerate(parts, start=1):
        body = f"({index}/{len(parts)})\n{part}" if len(parts) > 1 else part
        connection.execute(
            """
            INSERT INTO outbox(body, priority, status, attempts,
                               next_attempt_utc, created_at_utc)
            VALUES(?, ?, 'pending', 0, ?, ?)
            """,
            (body, priority, now, now),
        )


def deliver(body: str) -> tuple[bool, str]:
    if WHATSAPP_DRY_RUN:
        LOGGER.info("WHATSAPP_DRY_RUN | %s", body[:300].replace("\n", " | "))
        return True, ""

    if not all((ULTRA_ID, ULTRA_TOKEN, GROUP_ID)):
        return False, "Missing ULTRA_ID / ULTRA_TOKEN / GROUP_ID"

    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": body}
    try:
        response = HTTP.post(url, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return True, ""
    except requests.RequestException as exc:
        return False, str(exc)[:400]


def sender_loop() -> None:
    try:
        _sender_body()
    except Exception as exc:
        _runtime_status["thread_error"] = f"sender: {exc}"[:400]
        LOGGER.exception("Sender thread crashed")


def _sender_body() -> None:
    """שולח הודעה אחת בכל פעם, בקצב קבוע, עם backoff על כישלון."""
    init_db()
    while True:
        try:
            row = db().execute(
                """
                SELECT id, body, attempts FROM outbox
                WHERE status = 'pending' AND next_attempt_utc <= ?
                ORDER BY priority ASC, id ASC
                LIMIT 1
                """,
                (utc_now_iso(),),
            ).fetchone()

            if row is None:
                time.sleep(2)
                continue

            ok, error = deliver(str(row["body"]))
            if ok:
                db().execute(
                    "UPDATE outbox SET status='sent', sent_at_utc=? WHERE id=?",
                    (utc_now_iso(), int(row["id"])),
                )
                _runtime_status["last_send_ok"] = utc_now_iso()
                time.sleep(SEND_MIN_INTERVAL_SECONDS)
                continue

            attempts = int(row["attempts"]) + 1
            _runtime_status["last_send_error"] = error
            if attempts >= SEND_MAX_ATTEMPTS:
                db().execute(
                    "UPDATE outbox SET status='failed', attempts=?, last_error=? "
                    "WHERE id=?",
                    (attempts, error, int(row["id"])),
                )
                LOGGER.error("Message permanently failed after %s attempts: %s",
                             attempts, error)
            else:
                delay = min(300, 5 * (2 ** (attempts - 1)))
                retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=delay)
                ).isoformat()
                db().execute(
                    "UPDATE outbox SET attempts=?, next_attempt_utc=?, last_error=? "
                    "WHERE id=?",
                    (attempts, retry_at, error, int(row["id"])),
                )
                LOGGER.warning(
                    "Send attempt %s failed, retrying in %ss: %s",
                    attempts, delay, error,
                )
                time.sleep(1)
        except Exception:
            LOGGER.exception("Sender loop error")
            time.sleep(5)


def pending_outbox_count() -> int:
    row = db().execute(
        "SELECT COUNT(*) AS c FROM outbox WHERE status = 'pending'"
    ).fetchone()
    return int(row["c"]) if row else 0


# ---------------------------------------------------------------------------
# Alerts and summaries
# ---------------------------------------------------------------------------


def format_alert(display: str, text: str, link: str, is_poll: bool,
                 now: datetime) -> str:
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


def format_escalation(sources: list[str], sample: str, now: datetime) -> str:
    return (
        f"🔥 *הידיעה מתפשטת — {len(sources)} מקורות*\n\n"
        f"💬 {sample[:400]}\n\n"
        f"📡 {', '.join(sources[:12])}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M')}"
    )


def process_scan() -> tuple[int, int]:
    now = israel_now()
    is_bootstrapped = get_state("bootstrap_complete") == "1"
    suppress_alerts = not is_bootstrapped and not SEND_EXISTING_ON_START

    items = fetch_all_sources()

    # שלב 1: סינון רלוונטיות לפני כל גישה ל-DB.
    candidates: list[tuple[str, str, str, str, str, bool]] = []
    for _source_type, key, display, item in items:
        text = clean_text(item.get("text", ""))
        if not text:
            continue
        is_poll = contains_poll(text)
        if not is_poll and not contains_relevant_mention(text):
            continue
        link = item.get("link", "").strip()
        candidates.append((make_item_key(key, link, text), key, display, text,
                           link, is_poll))

    # שלב 2: בדיקת "כבר נראה" בשאילתת batch אחת.
    unseen = filter_unseen([candidate[0] for candidate in candidates])

    new_stories = 0
    duplicates = 0
    new_per_source: Counter[str] = Counter()

    for item_key, key, display, text, link, is_poll in candidates:
        if item_key not in unseen:
            continue
        new_per_source[key] += 1

        if suppress_alerts:
            mark_seen(item_key, key, link)
            continue

        kind = "poll" if is_poll else "mention"
        cluster_id, is_new_story, source_count = find_or_create_cluster(
            text, kind, display
        )

        db().execute(
            """
            INSERT OR IGNORE INTO mentions(
                item_key, cluster_id, local_date, local_time, source_key,
                source_display, text, link, kind, is_duplicate, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_key,
                cluster_id,
                now.strftime("%Y-%m-%d"),
                now.strftime("%H:%M:%S"),
                key,
                display,
                text[:1500],
                link,
                kind,
                0 if is_new_story else 1,
                utc_now_iso(),
            ),
        )

        if is_new_story:
            # התראה נשלחת פעם אחת לסיפור, לא פעם אחת למקור.
            enqueue_message(
                format_alert(display, text, link, is_poll, now),
                priority=1 if is_poll else 3,
            )
            new_stories += 1
        else:
            duplicates += 1
            if (
                ESCALATION_THRESHOLD
                and source_count >= ESCALATION_THRESHOLD
                and mark_escalated(cluster_id)
            ):
                enqueue_message(
                    format_escalation(cluster_sources(cluster_id), text, now),
                    priority=2,
                )

        # מסמנים "נראה" רק אחרי שההודעה כבר בתור העמיד.
        mark_seen(item_key, key, link)

    saturated = [
        source
        for source, count in new_per_source.items()
        if count >= TELEGRAM_MESSAGES_PER_SCAN
    ]
    if saturated:
        LOGGER.warning(
            "Sources filled the whole fetch window — items may have been missed: %s. "
            "Lower SCAN_INTERVAL_SECONDS or raise TELEGRAM_MESSAGES_PER_SCAN.",
            ", ".join(saturated),
        )

    if not is_bootstrapped:
        set_state("bootstrap_complete", "1")
        LOGGER.info(
            "Initial snapshot completed. Existing items were %s.",
            "sent" if SEND_EXISTING_ON_START else "suppressed",
        )

    return new_stories, duplicates


def build_digest(now: datetime) -> str | None:
    """דיג'סט של אזכורים בלבד — לא כותרות כלליות."""
    since = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    rows = db().execute(
        """
        SELECT c.id, c.sample_text, c.sources, c.source_count, c.kind
        FROM clusters c
        WHERE c.created_at_utc >= ?
        ORDER BY c.source_count DESC, c.id DESC
        LIMIT 15
        """,
        (since,),
    ).fetchall()
    if not rows:
        return None

    lines = [
        f"🗞️ *דיג'סט אזכורים — {now.strftime('%H:00 | %d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for index, row in enumerate(rows, start=1):
        try:
            sources: Iterable[str] = json.loads(str(row["sources"]))
        except (ValueError, TypeError):
            sources = []
        source_list = list(sources)
        marker = "📊" if str(row["kind"]) == "poll" else "•"
        lines.append(
            f"\n{marker} *{index}.* {clean_text(str(row['sample_text']))[:200]}"
            f"\n   📡 {row['source_count']} מקורות: {', '.join(source_list[:5])}"
        )
    lines.append(f"\n⏰ {now.strftime('%H:%M')}")
    return "\n".join(lines)


def build_daily_summary(local_date: str, now: datetime | None = None) -> str:
    now = now or israel_now()
    mentions = mentions_for_date(local_date)
    if not mentions:
        return (
            f"📊 *סיכום יומי נתניהו והליכוד — {now.strftime('%d/%m/%Y')}*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "לא נמצאו היום אזכורים חדשים."
        )

    stories = [row for row in mentions if not int(row["is_duplicate"])]
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
        if POLL_RE.search(text):
            topics["סקרים"] += 1

    lines = [
        f"📊 *סיכום יומי נתניהו והליכוד — {now.strftime('%d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"*{len(stories)} סיפורים | {len(mentions)} אזכורים בסך הכל*",
    ]
    if topics:
        lines.append("\n🔥 *נושאים מרכזיים:*")
        lines.extend(f"• {topic}: {count}" for topic, count in topics.most_common(5))
    lines.append("\n📈 *פילוח לפי מקור:*")
    lines.extend(f"• {source}: {count}" for source, count in sources.most_common(8))
    lines.append("\n📰 *דיווחים מרכזיים:*")
    for index, row in enumerate(stories[:25], start=1):
        short = clean_text(str(row["text"]))[:180]
        lines.append(
            f"\n*{index}. [{str(row['local_time'])[:5]}] {row['source_display']}*"
            f"\n{short}"
        )
    if len(stories) > 25:
        lines.append(f"\n… ועוד {len(stories) - 25} סיפורים.")
    return "\n".join(lines)


def send_scheduled_summaries() -> None:
    now = israel_now()
    today = now.strftime("%Y-%m-%d")

    if DIGEST_HOURS and now.hour in DIGEST_HOURS and now.minute <= 5:
        digest_key = f"{today}-{now.hour:02d}"
        if get_state("last_digest") != digest_key:
            digest = build_digest(now)
            if digest:
                enqueue_message(digest, priority=7)
            set_state("last_digest", digest_key)

    if (
        DAILY_SUMMARY_ENABLED
        and now.hour == DAILY_SUMMARY_HOUR
        and now.minute <= 5
        and get_state("last_daily_summary") != today
    ):
        enqueue_message(build_daily_summary(today, now), priority=8)
        set_state("last_daily_summary", today)


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------


def monitor_loop() -> None:
    try:
        _monitor_body()
    except Exception as exc:
        _runtime_status["thread_error"] = f"monitor: {exc}"[:400]
        LOGGER.exception("Monitor thread crashed")


def _monitor_body() -> None:
    init_db()
    _runtime_status["started_at"] = utc_now_iso()
    validate_rss_feeds()

    if SEND_STARTUP_MESSAGE:
        enqueue_message(
            "🚀 *בוט התקשורת עלה*\n\n"
            f"📡 {len(TELEGRAM_CHANNELS)} ערוצי טלגרם + "
            f"{len(RSS_FEEDS)} פידי חדשות\n"
            "🔍 ניטור: נתניהו, ביבי והליכוד\n"
            "🧩 דדופ בין מקורות פעיל",
            priority=9,
        )

    last_cleanup = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        _runtime_status["iterations"] += 1
        _runtime_status["last_scan_started"] = utc_now_iso()
        try:
            new_stories, duplicates = process_scan()
            send_scheduled_summaries()
            _runtime_status["last_scan_new_stories"] = new_stories
            _runtime_status["last_scan_duplicates"] = duplicates
            _runtime_status["last_error"] = None
            _runtime_status["consecutive_errors"] = 0
            LOGGER.info(
                "Scan done | stories=%s | duplicates suppressed=%s | queue=%s",
                new_stories,
                duplicates,
                pending_outbox_count(),
            )
        except Exception as exc:
            _runtime_status["last_error"] = str(exc)[:500]
            _runtime_status["consecutive_errors"] = (
                int(_runtime_status["consecutive_errors"]) + 1
            )
            LOGGER.exception("Monitor cycle failed")
        finally:
            _runtime_status["last_scan_finished"] = utc_now_iso()
            _runtime_status["last_scan_seconds"] = round(
                time.monotonic() - cycle_started, 2
            )

        if time.monotonic() - last_cleanup >= 86400:
            try:
                cleanup_database()
            except Exception:
                LOGGER.exception("Database cleanup failed")
            last_cleanup = time.monotonic()

        elapsed = time.monotonic() - cycle_started
        time.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))


def start_threads() -> None:
    global _monitor_thread, _sender_thread
    with _threads_lock:
        if not (_monitor_thread and _monitor_thread.is_alive()):
            _monitor_thread = threading.Thread(
                target=monitor_loop, name="media-monitor", daemon=True
            )
            _monitor_thread.start()
            LOGGER.info("Monitor thread started")
        if not (_sender_thread and _sender_thread.is_alive()):
            _sender_thread = threading.Thread(
                target=sender_loop, name="whatsapp-sender", daemon=True
            )
            _sender_thread.start()
            LOGGER.info("Sender thread started")


@app.before_request
def ensure_threads() -> None:
    start_threads()


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
    try:
        return health_report()
    except Exception as exc:
        # /health לעולם לא מפיל את הדיפלוי בגלל תקלה בבדיקה עצמה.
        LOGGER.exception("Health check failed")
        return jsonify({"ok": False, "health_error": str(exc)[:300]}), 503


def health_report():
    booting = (time.monotonic() - BOOT_MONOTONIC) < HEALTH_GRACE_SECONDS
    try:
        init_db()
        db_ok, db_error = True, None
    except Exception as exc:
        db_ok, db_error = False, str(exc)[:300]

    monitor_alive = bool(_monitor_thread and _monitor_thread.is_alive())
    sender_alive = bool(_sender_thread and _sender_thread.is_alive())

    stale = False
    age_seconds: float | None = None
    finished = _runtime_status.get("last_scan_finished")
    if finished:
        age_seconds = (
            datetime.now(timezone.utc) - datetime.fromisoformat(str(finished))
        ).total_seconds()
        stale = age_seconds > max(300, SCAN_INTERVAL_SECONDS * 3)
    elif _runtime_status.get("started_at"):
        started_age = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(str(_runtime_status["started_at"]))
        ).total_seconds()
        stale = started_age > max(600, SCAN_INTERVAL_SECONDS * 5)

    failing = int(_runtime_status.get("consecutive_errors") or 0) >= 3
    healthy = (
        booting
        or (monitor_alive and sender_alive and db_ok and not stale and not failing)
    )
    payload = {
        "ok": healthy,
        "monitor_alive": monitor_alive,
        "sender_alive": sender_alive,
        "last_scan_age_seconds": round(age_seconds, 1) if age_seconds else None,
        "stale": stale,
        "failing": failing,
        "booting": booting,
        "db_ok": db_ok,
        "db_error": db_error,
        "whatsapp_dry_run": WHATSAPP_DRY_RUN,
        "credentials_present": bool(ULTRA_ID and ULTRA_TOKEN and GROUP_ID),
        "outbox_pending": pending_outbox_count() if db_ok else None,
        "database": str(DB_PATH),
        "sources": {"telegram": len(TELEGRAM_CHANNELS), "rss": len(RSS_FEEDS)},
        "runtime": _runtime_status,
    }
    return jsonify(payload), (200 if healthy else 503)


# מפעילים כבר ב-import, כדי לא להיות תלויים בבקשת HTTP ראשונה.
# כישלון כאן לא מפיל את השרת — before_request ינסה שוב.
try:
    start_threads()
except Exception:
    LOGGER.exception("Failed to start threads at import")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
