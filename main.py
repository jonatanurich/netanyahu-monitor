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
from flask import Flask, jsonify, request

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
# ברירת מחדל: דיג'סט בכל שעה, 24/7. אפשר לצמצם עם DIGEST_HOURS="8,14,20".
_digest_raw = os.getenv("DIGEST_HOURS", "all").strip().lower()
DIGEST_HOURS = (
    tuple(range(24))
    if _digest_raw in {"all", "24/7", "*", ""}
    else tuple(
        sorted(
            {
                int(part)
                for part in _digest_raw.split(",")
                if part.strip().isdigit() and 0 <= int(part) <= 23
            }
        )
    )
)
DIGEST_LOOKBACK_HOURS = env_int("DIGEST_LOOKBACK_HOURS", 1, 1, 24)
DAILY_SUMMARY_ENABLED = env_bool("DAILY_SUMMARY_ENABLED", True)
DAILY_SUMMARY_HOUR = env_int("DAILY_SUMMARY_HOUR", 18, 0, 23)

# --- סיווג טון דרך ה-API של קלוד ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
TONE_ENABLED = env_bool("TONE_ENABLED", True) and bool(ANTHROPIC_API_KEY)
TONE_MODEL = os.getenv("TONE_MODEL", "claude-sonnet-4-6")
TONE_TIMEOUT = env_int("TONE_TIMEOUT", 20, 5, 60)

# --- מהירות התפשטות ---
VELOCITY_SOURCES = env_int("VELOCITY_SOURCES", 4, 2, 30)
VELOCITY_MINUTES = env_int("VELOCITY_MINUTES", 10, 1, 120)

# --- בריאות מקורות ---
SILENT_SOURCE_HOURS = env_int("SILENT_SOURCE_HOURS", 6, 1, 72)
HEARTBEAT_HOUR = env_int("HEARTBEAT_HOUR", 8, 0, 23)
HEARTBEAT_ENABLED = env_bool("HEARTBEAT_ENABLED", True)

# --- ערוץ גיבוי כשוואטסאפ נופל ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BACKUP_AFTER_FAILURES = env_int("BACKUP_AFTER_FAILURES", 3, 1, 20)

# --- שקט בשבת ובחג ---
# ניטור רציף 24/7. מי שירצה שקט בשבת יגדיר QUIET_SHABBAT=true.
QUIET_SHABBAT = env_bool("QUIET_SHABBAT", False)
CANDLE_MINUTES = env_int("CANDLE_MINUTES", 40, 0, 120)
HAVDALAH_MINUTES = env_int("HAVDALAH_MINUTES", 42, 0, 120)
JERUSALEM_LAT = 31.7683
JERUSALEM_LON = 35.2137

# --- פקודות מתוך קבוצת הוואטסאפ ---
COMMANDS_ENABLED = env_bool("COMMANDS_ENABLED", True)

# --- העברה מקבוצות וואטסאפ אחרות שהמספר חבר בהן ---
# פורמט: RELAY_GROUPS="1203...@g.us=פורום עיתונאים,1204...@g.us=מטה"
RELAY_ENABLED = env_bool("RELAY_ENABLED", True)
RELAY_SHOW_SENDER = env_bool("RELAY_SHOW_SENDER", True)


def _parse_relay_groups() -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in os.getenv("RELAY_GROUPS", "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" in chunk:
            group_id, label = chunk.split("=", 1)
            result[group_id.strip()] = label.strip() or "קבוצת וואטסאפ"
        else:
            result[chunk] = "קבוצת וואטסאפ"
    return result


RELAY_GROUPS = _parse_relay_groups()

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
    "consecutive_send_failures": 0,
    "backup_channel_used": None,
    "quiet_period": False,
    "tone_calls": 0,
    "tone_failures": 0,
}

BOOT_MONOTONIC = time.monotonic()
HEALTH_GRACE_SECONDS = env_int("HEALTH_GRACE_SECONDS", 180, 30, 900)

# ---------------------------------------------------------------------------
# Relevance matching
# ---------------------------------------------------------------------------

HEB = "\u0590-\u05FF"
HEB_PREFIX = "[והבלמשכ]{0,2}"

# --- מטרות הניטור -----------------------------------------------------------
# strong = מספיק לבדו. weak = דורש הקשר פוליטי (למשל "לפיד" שהוא גם אבוקה).

WATCHLIST: dict[str, dict[str, Any]] = {
    "netanyahu": {
        "label": "נתניהו / הליכוד",
        "emoji": "🚨",
        "strong": ("בנימין נתניהו", "נתניהו", "ביבי", "ליכוד", "ליכודניק",
                   "ליכודניקים"),
        "weak": (),
        "latin": ("netanyahu", "bibi", "likud"),
    },
    "eisenkot": {
        "label": "גדי אייזנקוט",
        "emoji": "🎙️",
        "strong": ("אייזנקוט", "איזנקוט", "גדי אייזנקוט"),
        "weak": (),
        "latin": ("eisenkot",),
    },
    "bennett": {
        "label": "נפתלי בנט",
        "emoji": "🎙️",
        "strong": ("נפתלי בנט", "בנט"),
        "weak": (),
        "latin": ("bennett",),
    },
    "lapid": {
        "label": "יאיר לפיד",
        "emoji": "🎙️",
        # "לפיד" לבדו הוא גם אבוקה ומשואה — לכן weak.
        "strong": ("יאיר לפיד",),
        "weak": ("לפיד",),
        "latin": ("lapid",),
    },
    "liberman": {
        "label": "אביגדור ליברמן",
        "emoji": "🎙️",
        "strong": ("ליברמן", "אביגדור ליברמן"),
        "weak": (),
        "latin": ("liberman", "lieberman"),
    },
}

PRIME_MINISTER_TERMS = ("ראש הממשלה", "רה״מ", 'רה"מ', "רה׳׳מ")

FOREIGN_CONTEXT = (
    "בריטניה", "הבריטי", "קנדה", "הקנדי", "הודו", "ההודי", "אוסטרליה",
    "האוסטרלי", "איטליה", "האיטלקי", "ספרד", "הספרדי", "צרפת", "הצרפתי",
    "יוון", "היווני", "פולין", "הפולני", "יפן", "היפני", "הונגריה",
    "ההונגרי", "אלבניה", "בלגיה", "הולנד", "שוודיה", "נורבגיה", "אירלנד",
    "סלובניה", "צ׳כיה", 'צ"כיה', "ניו זילנד", "הניו זילנדי",
)

PARTIES = (
    "ליכוד", "יש עתיד", "המחנה הממלכתי", "ישראל ביתנו", "ש״ס", 'ש"ס', "שס",
    "יהדות התורה", "עוצמה יהודית", "הציונות הדתית", "כחול לבן", "העבודה",
    "מרצ", "הדמוקרטים", "רע״מ", 'רע"מ', "חד״ש", 'חד"ש', "בלד", "נועם",
    "הבית היהודי", "ימינה", "בנט 2026",
)

POLITICAL_CONTEXT = PARTIES + (
    "כנסת", "מפלגה", "מפלגת", "מנדט", "מנדטים", "קואליציה", "אופוזיציה",
    "ממשלה", "בחירות", "ח״כ", 'ח"כ', "חבר הכנסת", "השר", "שרת", "פוליטי",
    "פוליטית", "גוש", "ראשות הממשלה", "אחוז החסימה", "פריימריז",
)

POLL_WORDS = ("סקר", "סקרים", "מדגם", "משאל", "נסקרים", "התפלגות")
POLL_CONTEXT = POLITICAL_CONTEXT + (
    "פאנל", "מכון", "לאזר", "לזר", "מנו גבע", "קמיל פוקס", "דיירקט פולס",
    "מדגם", "מצביעים", "בוחרים",
)

STATEMENT_WORDS = (
    "אמר", "אמרה", "הצהיר", "מסר", "תקף", "הגיב", "כתב", "צייץ", "פרסם",
    "בריאיון", "בראיון", "ריאיון", "ראיון", "בדבריו", "בדבריה", "הודיע",
    "קרא", "טען", "הבהיר", "נאם", "נאום", "התייחס", "זעם", "שיגר",
    "בהודעה", "בהצהרה", "מתראיין", "בכנס", "בישיבת",
)


def _hebrew_pattern(word: str) -> str:
    return rf"(?<![{HEB}]){HEB_PREFIX}{re.escape(word)}(?![{HEB}])"


def _build_re(hebrew: Iterable[str] = (), latin: Iterable[str] = ()) -> re.Pattern[str] | None:
    parts = [_hebrew_pattern(word) for word in hebrew]
    parts += [rf"\b{re.escape(word)}\b" for word in latin]
    if not parts:
        return None
    return re.compile("|".join(parts), re.IGNORECASE)


for _entry in WATCHLIST.values():
    _entry["strong_re"] = _build_re(_entry["strong"], _entry["latin"])
    _entry["weak_re"] = _build_re(_entry["weak"])

PM_RE = _build_re(PRIME_MINISTER_TERMS)
FOREIGN_RE = _build_re(FOREIGN_CONTEXT)
POLITICAL_RE = _build_re(POLITICAL_CONTEXT)
POLL_WORD_RE = _build_re(POLL_WORDS)
POLL_CONTEXT_RE = _build_re(POLL_CONTEXT)
STATEMENT_RE = _build_re(STATEMENT_WORDS)

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


_custom_cache: dict[str, Any] = {"raw": None, "compiled": {}}


def custom_subjects() -> dict[str, dict[str, Any]]:
    raw = get_state("custom_subjects") or "{}"
    if _custom_cache["raw"] != raw:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            data = {}
        compiled: dict[str, dict[str, Any]] = {}
        for key, entry in data.items():
            terms = entry.get("terms") or [entry.get("label", key)]
            compiled[key] = {
                "label": entry.get("label", key),
                "emoji": "🎯",
                "strong_re": _build_re(terms),
                "weak_re": None,
            }
        _custom_cache["raw"] = raw
        _custom_cache["compiled"] = compiled
    return dict(_custom_cache["compiled"])


def all_subjects() -> dict[str, dict[str, Any]]:
    merged = dict(WATCHLIST)
    merged.update(custom_subjects())
    return merged


def subject_entry(key: str) -> dict[str, Any]:
    return all_subjects().get(key, {})


def detect_subjects(text: str) -> list[str]:
    """אילו מהמטרות מוזכרות בטקסט."""
    political = bool(POLITICAL_RE and POLITICAL_RE.search(text))
    subjects: list[str] = []
    for key, entry in all_subjects().items():
        strong_re = entry.get("strong_re")
        weak_re = entry.get("weak_re")
        if strong_re is not None and strong_re.search(text):
            subjects.append(key)
        elif political and weak_re is not None and weak_re.search(text):
            subjects.append(key)

    # "ראש הממשלה" — רלוונטי אלא אם מדובר בבירור בראש ממשלה זר.
    if (
        "netanyahu" not in subjects
        and PM_RE is not None
        and PM_RE.search(text)
        and not (FOREIGN_RE and FOREIGN_RE.search(text))
    ):
        subjects.append("netanyahu")
    return subjects


def is_political_poll(text: str, subjects: list[str]) -> bool:
    """כל סקר פוליטי בישראל — גם כשאף אחת מהמטרות לא מוזכרת בו."""
    if not (POLL_WORD_RE and POLL_WORD_RE.search(text)):
        return False
    if subjects:
        return True
    return bool(POLL_CONTEXT_RE and POLL_CONTEXT_RE.search(text))


def looks_like_statement(text: str) -> bool:
    if STATEMENT_RE and STATEMENT_RE.search(text):
        return True
    return bool(re.search(r'["\u05f4\u201c\u201d].{8,}["\u05f4\u201c\u201d]', text))


def classify(text: str) -> tuple[str | None, list[str], bool]:
    """מחזיר (סוג, מטרות, האם התבטאות)."""
    subjects = detect_subjects(text)
    if is_political_poll(text, subjects):
        return "poll", subjects, False
    if subjects:
        return subjects[0], subjects, looks_like_statement(text)
    return None, [], False


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
        "clusters": {
            "escalated": "INTEGER NOT NULL DEFAULT 0",
            "tone": "TEXT",
            "angle": "TEXT",
            "response_needed": "INTEGER NOT NULL DEFAULT 0",
        },
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
            tone TEXT,
            angle TEXT,
            response_needed INTEGER NOT NULL DEFAULT 0,
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

        CREATE TABLE IF NOT EXISTS source_health (
            source_key TEXT PRIMARY KEY,
            display TEXT NOT NULL,
            last_item_utc TEXT NOT NULL,
            items_seen INTEGER NOT NULL DEFAULT 0
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


def sunset_local(day: datetime) -> datetime | None:
    """חישוב שקיעה בירושלים (NOAA מקורב) — בלי ספריות חיצוניות."""
    import math

    n = day.timetuple().tm_yday
    lat = math.radians(JERUSALEM_LAT)
    gamma = 2 * math.pi / 365 * (n - 1)
    decl = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2 * gamma)
        + 0.000907 * math.sin(2 * gamma)
        - 0.002697 * math.cos(3 * gamma)
        + 0.00148 * math.sin(3 * gamma)
    )
    eqtime = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2 * gamma)
        - 0.040849 * math.sin(2 * gamma)
    )
    cos_ha = (
        math.cos(math.radians(90.833)) / (math.cos(lat) * math.cos(decl))
        - math.tan(lat) * math.tan(decl)
    )
    if not -1 <= cos_ha <= 1:
        return None
    ha = math.degrees(math.acos(cos_ha))
    minutes_utc = 720 + 4 * (ha - JERUSALEM_LON) - eqtime
    utc_midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return (utc_midnight + timedelta(minutes=minutes_utc)).astimezone(ISRAEL_TZ)


def _yom_tov_dates(year: int) -> set[tuple[int, int]]:
    """תאריכי חג שבהם אין מלאכה. משתמש ב-pyluach אם מותקן."""
    try:
        from pyluach import dates as _pd  # type: ignore
    except Exception:
        return set()

    result: set[tuple[int, int]] = set()
    day = datetime(year, 1, 1)
    while day.year == year:
        try:
            heb = _pd.HebrewDate.from_pydate(day.date())
            holiday = heb.holiday(israel=True)
        except Exception:
            holiday = None
        if holiday in {
            "Rosh Hashana", "Yom Kippur", "Succos", "Shemini Atzeres",
            "Simchas Torah", "Pesach", "Shavuos",
        }:
            result.add((day.month, day.day))
        day += timedelta(days=1)
    return result


_yom_tov_cache: dict[int, set[tuple[int, int]]] = {}


def is_no_work_day(day: datetime) -> bool:
    year = day.year
    if year not in _yom_tov_cache:
        _yom_tov_cache[year] = _yom_tov_dates(year)
    return (day.month, day.day) in _yom_tov_cache[year]


def in_quiet_period(now: datetime | None = None) -> bool:
    """שבת או יום טוב — מהדלקת נרות ועד צאת החג."""
    if not QUIET_SHABBAT:
        return False
    now = now or israel_now()

    for offset in (-1, 0):
        day = now + timedelta(days=offset)
        # ערב שבת או ערב חג
        next_day = day + timedelta(days=1)
        starts = day.weekday() == 4 or is_no_work_day(next_day)
        if not starts:
            continue
        sunset = sunset_local(day)
        if sunset is None:
            continue
        start = sunset - timedelta(minutes=CANDLE_MINUTES)

        # סוף התקופה: היום האחרון ברצף שאין בו מלאכה
        end_day = next_day
        for _ in range(3):
            following = end_day + timedelta(days=1)
            if is_no_work_day(following):
                end_day = following
            else:
                break
        end_sunset = sunset_local(end_day)
        if end_sunset is None:
            continue
        end = end_sunset + timedelta(minutes=HAVDALAH_MINUTES)

        if start <= now <= end:
            return True
    return False


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


TONE_PROMPT = """אתה אנליסט תקשורת ישראלי. לפניך ידיעה שפורסמה זה עתה.
קבע:
1. tone — היחס לנושא המרכזי: "עוין", "אוהד" או "נייטרלי".
2. angle — זווית הסיקור בשתיים עד ארבע מילים בעברית.
3. response_needed — האם הידיעה מחייבת תגובה תקשורתית מיידית (true/false).

החזר JSON בלבד, בלי טקסט נוסף ובלי סימוני קוד:
{"tone": "...", "angle": "...", "response_needed": false}

הידיעה:
"""


def analyze_tone(text: str) -> dict[str, Any] | None:
    """קריאה אחת ל-API לכל סיפור חדש. נכשל בשקט — לעולם לא חוסם התראה."""
    if not TONE_ENABLED:
        return None
    try:
        response = HTTP.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": TONE_MODEL,
                "max_tokens": 200,
                "messages": [
                    {"role": "user", "content": TONE_PROMPT + text[:1200]}
                ],
            },
            timeout=TONE_TIMEOUT,
        )
        response.raise_for_status()
        blocks = response.json().get("content", [])
        raw = "".join(
            block.get("text", "") for block in blocks if block.get("type") == "text"
        )
        raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        _runtime_status["tone_calls"] = int(_runtime_status["tone_calls"]) + 1
        tone = str(parsed.get("tone", "")).strip()
        return {
            "tone": tone if tone in {"עוין", "אוהד", "נייטרלי"} else "נייטרלי",
            "angle": str(parsed.get("angle", "")).strip()[:60],
            "response_needed": bool(parsed.get("response_needed")),
        }
    except Exception as exc:
        _runtime_status["tone_failures"] = int(_runtime_status["tone_failures"]) + 1
        LOGGER.warning("Tone analysis failed: %s", str(exc)[:200])
        return None


def save_tone(cluster_id: int, tone: dict[str, Any]) -> None:
    db().execute(
        "UPDATE clusters SET tone=?, angle=?, response_needed=? WHERE id=?",
        (tone["tone"], tone["angle"], 1 if tone["response_needed"] else 0, cluster_id),
    )


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


def muted_until() -> datetime | None:
    raw = get_state("muted_until")
    if not raw:
        return None
    try:
        until = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return until if until > datetime.now(timezone.utc) else None


def should_hold(priority: int) -> bool:
    """שבת/חג או השתקה ידנית — ההודעה נשמרת ולא נשלחת עכשיו."""
    if priority <= 0:
        return False
    return bool(in_quiet_period() or muted_until())


def enqueue_message(text: str, priority: int = 5) -> None:
    """כותב לתור. הכתיבה עמידה — restart לא מאבד הודעות."""
    parts = split_message(text)
    now = utc_now_iso()
    held = should_hold(priority)
    connection = db()
    for index, part in enumerate(parts, start=1):
        body = f"({index}/{len(parts)})\n{part}" if len(parts) > 1 else part
        connection.execute(
            """
            INSERT INTO outbox(body, priority, status, attempts,
                               next_attempt_utc, created_at_utc)
            VALUES(?, ?, ?, 0, ?, ?)
            """,
            (body, priority, "held" if held else "pending", now, now),
        )


def deliver_telegram(body: str) -> bool:
    """ערוץ גיבוי — נכנס לפעולה רק כשוואטסאפ נכשל שוב ושוב."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return False
    try:
        response = HTTP.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": body[:4000]},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        _runtime_status["backup_channel_used"] = utc_now_iso()
        return True
    except requests.RequestException as exc:
        LOGGER.error("Backup channel failed too: %s", str(exc)[:200])
        return False


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
        _runtime_status["consecutive_send_failures"] = 0
        return True, ""
    except requests.RequestException as exc:
        failures = int(_runtime_status["consecutive_send_failures"]) + 1
        _runtime_status["consecutive_send_failures"] = failures
        if failures >= BACKUP_AFTER_FAILURES and deliver_telegram(
            f"⚠️ וואטסאפ לא זמין — נשלח בערוץ גיבוי\n\n{body}"
        ):
            return True, ""
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


def release_held_messages(now: datetime) -> None:
    """יציאה משבת/השתקה — דיג'סט אחד במקום מפולת הודעות."""
    if should_hold(5):
        return
    rows = db().execute(
        "SELECT id, body FROM outbox WHERE status = 'held' ORDER BY priority, id"
    ).fetchall()
    if not rows:
        return

    headlines = []
    for row in rows:
        first = str(row["body"]).split("\n")[0].replace("*", "")
        headlines.append(first[:110])

    db().execute("UPDATE outbox SET status = 'skipped' WHERE status = 'held'")
    summary = "\n".join(f"• {line}" for line in headlines[:60])
    extra = f"\n\n… ועוד {len(headlines) - 60}" if len(headlines) > 60 else ""
    enqueue_message(
        f"🕯️ *סיכום מה שהצטבר ({len(headlines)} התראות)*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n{summary}{extra}\n\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M')}",
        priority=0,
    )
    LOGGER.info("Released %s held messages as one digest", len(headlines))


def pending_outbox_count() -> int:
    row = db().execute(
        "SELECT COUNT(*) AS c FROM outbox WHERE status = 'pending'"
    ).fetchone()
    return int(row["c"]) if row else 0


# ---------------------------------------------------------------------------
# Alerts and summaries
# ---------------------------------------------------------------------------


def subject_labels(subjects: list[str]) -> str:
    return " + ".join(
        str(subject_entry(key).get("label", key)) for key in subjects
    ) or "פוליטי"


TONE_BADGE = {"עוין": "🔴", "אוהד": "🟢", "נייטרלי": "⚪"}


def alert_priority(kind: str, is_statement: bool, tone: dict[str, Any] | None) -> int:
    if tone and tone.get("response_needed"):
        return 0
    if kind == "poll":
        return 1
    if tone and tone.get("tone") == "עוין":
        return 1
    return 2 if is_statement else 4


def format_alert(display: str, text: str, link: str, kind: str,
                 subjects: list[str], is_statement: bool,
                 now: datetime, tone: dict[str, Any] | None = None) -> str:
    if kind == "poll":
        who = subject_labels(subjects)
        headline = f"📊 *סקר פוליטי{f' — {who}' if who else ''} | {display}*"
    else:
        emoji = "💬" if is_statement else str(
            subject_entry(kind).get("emoji", "🔔")
        )
        prefix = "התבטאות" if is_statement else "אזכור"
        headline = f"{emoji} *{prefix}: {subject_labels(subjects)} | {display}*"

    tone_line = ""
    if tone:
        badge = TONE_BADGE.get(str(tone.get("tone")), "⚪")
        angle = str(tone.get("angle") or "").strip()
        tone_line = f"\n{badge} {tone.get('tone')}" + (f" — {angle}" if angle else "")
        if tone.get("response_needed"):
            tone_line += "\n⚠️ *מחייב תגובה*"

    link_line = f"\n\n🔗 {link}" if link else ""
    return (
        f"{headline}{tone_line}\n\n"
        f"💬 {text[:900]}"
        f"{link_line}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M:%S')}"
    )


def format_escalation(cluster_id: int, sample: str, now: datetime,
                      urgent: bool) -> str:
    sources = cluster_sources(cluster_id)
    age = cluster_age_minutes(cluster_id)
    row = db().execute(
        "SELECT created_at_utc, tone, angle FROM clusters WHERE id = ?",
        (cluster_id,),
    ).fetchone()

    first_line = ""
    if sources and row:
        first_time = datetime.fromisoformat(
            str(row["created_at_utc"])
        ).astimezone(ISRAEL_TZ)
        first_line = (
            f"\n🥇 פורסם ראשון: {sources[0]} ב-{first_time.strftime('%H:%M')}"
        )

    header = (
        f"🔥🔥 *מתפוצץ — {len(sources)} מקורות ב-{int(age)} דקות*"
        if urgent
        else f"🔥 *הידיעה מתפשטת — {len(sources)} מקורות*"
    )
    tone_line = ""
    if row and row["tone"]:
        badge = TONE_BADGE.get(str(row["tone"]), "⚪")
        tone_line = f"\n{badge} {row['tone']}"
        if row["angle"]:
            tone_line += f" — {row['angle']}"

    return (
        f"{header}{tone_line}{first_line}\n\n"
        f"💬 {sample[:400]}\n\n"
        f"📡 {', '.join(sources[:12])}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M')}"
    )


def record_source_health(items: list[tuple[str, str, str, dict[str, str]]]) -> None:
    """כל מקור שהחזיר פריט מסמן חיים — כדי לזהות מקור שנשתק."""
    seen: dict[str, str] = {}
    for _source_type, key, display, _item in items:
        seen[key] = display
    now = utc_now_iso()
    connection = db()
    for key, display in seen.items():
        connection.execute(
            """
            INSERT INTO source_health(source_key, display, last_item_utc, items_seen)
            VALUES(?, ?, ?, 1)
            ON CONFLICT(source_key) DO UPDATE SET
                last_item_utc = excluded.last_item_utc,
                display = excluded.display,
                items_seen = source_health.items_seen + 1
            """,
            (key, display, now),
        )


def silent_sources() -> list[tuple[str, float]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SILENT_SOURCE_HOURS)
    rows = db().execute(
        "SELECT display, last_item_utc FROM source_health WHERE last_item_utc < ?",
        (cutoff.isoformat(),),
    ).fetchall()
    result = []
    for row in rows:
        hours = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(str(row["last_item_utc"]))
        ).total_seconds() / 3600
        result.append((str(row["display"]), round(hours, 1)))
    return sorted(result, key=lambda item: -item[1])


def cluster_age_minutes(cluster_id: int) -> float:
    row = db().execute(
        "SELECT created_at_utc FROM clusters WHERE id = ?", (cluster_id,)
    ).fetchone()
    if not row:
        return 999.0
    created = datetime.fromisoformat(str(row["created_at_utc"]))
    return (datetime.now(timezone.utc) - created).total_seconds() / 60


def process_scan() -> tuple[int, int]:
    now = israel_now()
    is_bootstrapped = get_state("bootstrap_complete") == "1"
    suppress_alerts = not is_bootstrapped and not SEND_EXISTING_ON_START

    items = fetch_all_sources()
    record_source_health(items)

    # שלב 1: סינון רלוונטיות לפני כל גישה ל-DB.
    candidates: list[tuple[str, str, str, str, str, str, list[str], bool]] = []
    for _source_type, key, display, item in items:
        text = clean_text(item.get("text", ""))
        if not text:
            continue
        kind, subjects, is_statement = classify(text)
        if kind is None:
            continue
        link = item.get("link", "").strip()
        candidates.append((make_item_key(key, link, text), key, display, text,
                           link, kind, subjects, is_statement))

    # שלב 2: בדיקת "כבר נראה" בשאילתת batch אחת.
    unseen = filter_unseen([candidate[0] for candidate in candidates])

    new_stories: list[dict[str, Any]] = []
    escalations: list[tuple[int, str, bool]] = []
    duplicates = 0
    new_per_source: Counter[str] = Counter()

    for item_key, key, display, text, link, kind, subjects, is_statement in candidates:
        if item_key not in unseen:
            continue
        new_per_source[key] += 1

        if suppress_alerts:
            mark_seen(item_key, key, link)
            continue

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
            new_stories.append(
                {
                    "cluster_id": cluster_id,
                    "display": display,
                    "text": text,
                    "link": link,
                    "kind": kind,
                    "subjects": subjects,
                    "is_statement": is_statement,
                }
            )
        else:
            duplicates += 1
            age = cluster_age_minutes(cluster_id)
            urgent = source_count >= VELOCITY_SOURCES and age <= VELOCITY_MINUTES
            spread = ESCALATION_THRESHOLD and source_count >= ESCALATION_THRESHOLD
            if (urgent or spread) and mark_escalated(cluster_id):
                escalations.append((cluster_id, text, bool(urgent)))

        mark_seen(item_key, key, link)

    # שלב 3: סיווג טון במקביל — רק על סיפורים חדשים, לא על כל פריט.
    if new_stories and TONE_ENABLED:
        with ThreadPoolExecutor(max_workers=min(6, len(new_stories))) as executor:
            futures = {
                executor.submit(analyze_tone, story["text"]): story
                for story in new_stories
            }
            for future in as_completed(futures):
                story = futures[future]
                try:
                    story["tone"] = future.result()
                except Exception:
                    story["tone"] = None

    # שלב 4: שליחה
    for story in new_stories:
        tone = story.get("tone")
        if tone:
            save_tone(int(story["cluster_id"]), tone)
        enqueue_message(
            format_alert(
                str(story["display"]), str(story["text"]), str(story["link"]),
                str(story["kind"]), list(story["subjects"]),
                bool(story["is_statement"]), now, tone,
            ),
            priority=alert_priority(
                str(story["kind"]), bool(story["is_statement"]), tone
            ),
        )

    for cluster_id, text, urgent in escalations:
        enqueue_message(
            format_escalation(cluster_id, text, now, urgent),
            priority=0 if urgent else 3,
        )

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

    return len(new_stories), duplicates


def build_digest(now: datetime) -> str | None:
    """דיג'סט של אזכורים בלבד — לא כותרות כלליות."""
    since = (
        datetime.now(timezone.utc) - timedelta(hours=DIGEST_LOOKBACK_HOURS)
    ).isoformat()
    rows = db().execute(
        """
        SELECT c.id, c.sample_text, c.sources, c.source_count, c.kind, c.tone
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
        f"🗞️ *דיג'סט "
        f"{'השעה האחרונה' if DIGEST_LOOKBACK_HOURS == 1 else f'{DIGEST_LOOKBACK_HOURS} השעות האחרונות'}"
        f" — {now.strftime('%H:00 | %d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for index, row in enumerate(rows, start=1):
        try:
            sources: Iterable[str] = json.loads(str(row["sources"]))
        except (ValueError, TypeError):
            sources = []
        source_list = list(sources)
        kind = str(row["kind"])
        marker = "📊" if kind == "poll" else str(
            subject_entry(kind).get("emoji", "•")
        )
        badge = TONE_BADGE.get(str(row["tone"] or ""), "")
        lines.append(
            f"\n{marker} *{index}.* {badge} "
            f"{clean_text(str(row['sample_text']))[:200]}"
            f"\n   📡 {row['source_count']} מקורות: {', '.join(source_list[:5])}"
        )
    lines.append(f"\n⏰ {now.strftime('%H:%M')}")
    return "\n".join(lines)


def build_daily_summary(local_date: str, now: datetime | None = None) -> str:
    now = now or israel_now()
    mentions = mentions_for_date(local_date)
    if not mentions:
        return (
            f"📊 *סיכום יומי — ניטור פוליטי — {now.strftime('%d/%m/%Y')}*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "לא נמצאו היום אזכורים חדשים."
        )

    stories = [row for row in mentions if not int(row["is_duplicate"])]
    sources = Counter(row["source_display"] for row in mentions)
    by_subject: Counter[str] = Counter()
    for row in mentions:
        kind = str(row["kind"])
        if kind == "poll":
            by_subject["📊 סקרים"] += 1
        elif kind != "poll":
            by_subject[str(subject_entry(kind).get("label", kind))] += 1

    lines = [
        f"📊 *סיכום יומי — ניטור פוליטי — {now.strftime('%d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"*{len(stories)} סיפורים | {len(mentions)} אזכורים בסך הכל*",
    ]
    if by_subject:
        lines.append("\n🎯 *פילוח לפי מטרה:*")
        lines.extend(
            f"• {subject}: {count}" for subject, count in by_subject.most_common(8)
        )
    lines.append("\n📈 *פילוח לפי מקור:*")
    lines.extend(f"• {source}: {count}" for source, count in sources.most_common(8))
    lines.append("\n📰 *דיווחים מרכזיים:*")
    for index, row in enumerate(stories[:25], start=1):
        short = clean_text(str(row["text"]))[:180]
        kind = str(row["kind"])
        tag = (
            "📊 סקר"
            if kind == "poll"
            else str(subject_entry(kind).get("label", ""))
        )
        lines.append(
            f"\n*{index}. [{str(row['local_time'])[:5]}] {tag} | "
            f"{row['source_display']}*\n{short}"
        )
    if len(stories) > 25:
        lines.append(f"\n… ועוד {len(stories) - 25} סיפורים.")
    return "\n".join(lines)


def build_heartbeat(now: datetime) -> str:
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = mentions_for_date(yesterday)
    stories = sum(1 for row in rows if not int(row["is_duplicate"]))
    silent = silent_sources()
    active = db().execute("SELECT COUNT(*) AS c FROM source_health").fetchone()["c"]

    lines = [
        f"✅ *המערכת פעילה — {now.strftime('%d/%m/%Y')}*",
        f"📡 {active} מקורות פעילים מתוך "
        f"{len(TELEGRAM_CHANNELS) + len(RSS_FEEDS)}",
        f"📰 אתמול: {stories} סיפורים, {len(rows)} אזכורים",
    ]
    if silent:
        lines.append(f"\n⚠️ *מקורות שקטים מעל {SILENT_SOURCE_HOURS} שעות:*")
        lines.extend(f"• {display} — {hours} שעות" for display, hours in silent[:10])
    return "\n".join(lines)


def check_silent_sources(now: datetime) -> None:
    silent = silent_sources()
    if not silent:
        return
    today = now.strftime("%Y-%m-%d")
    signature = f"{today}|{len(silent)}"
    if get_state("last_silent_alert") == signature:
        return
    set_state("last_silent_alert", signature)
    lines = "\n".join(f"• {display} — {hours} שעות" for display, hours in silent[:12])
    enqueue_message(
        f"⚠️ *מקורות שנשתקו*\n\nלא התקבל שום פריט מ:\n{lines}\n\n"
        "ייתכן ששם הערוץ השתנה או שהפיד נפל.",
        priority=6,
    )


def send_scheduled_summaries() -> None:
    now = israel_now()
    today = now.strftime("%Y-%m-%d")

    release_held_messages(now)
    _runtime_status["quiet_period"] = in_quiet_period(now)

    if (
        HEARTBEAT_ENABLED
        and now.hour == HEARTBEAT_HOUR
        and now.minute <= 5
        and get_state("last_heartbeat") != today
    ):
        enqueue_message(build_heartbeat(now), priority=7)
        set_state("last_heartbeat", today)

    if now.minute <= 5:
        check_silent_sources(now)

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
            "🎯 מטרות: "
            + ", ".join(str(e["label"]) for e in WATCHLIST.values())
            + "\n📊 כל סקר פוליטי בישראל\n"
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


# ---------------------------------------------------------------------------
# פקודות מתוך קבוצת הוואטסאפ
# ---------------------------------------------------------------------------

HELP_TEXT = """🤖 *פקודות זמינות*

• *סטטוס* — מצב המערכת
• *השתק* / *השתק 3* — שקט לשעה או למספר שעות
• *המשך* — ביטול ההשתקה
• *מטרות* — רשימת המטרות המנוטרות
• *מטרה+ סמוטריץ׳* — הוספת מטרה
• *מטרה- סמוטריץ׳* — הסרת מטרה
• *היום* — סיכום היום עכשיו
• *עזרה* — ההודעה הזו"""


def slugify(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"custom_{digest}"


def handle_command(body: str) -> str | None:
    text = body.strip()
    lowered = text.lower()

    if lowered in {"עזרה", "help", "פקודות"}:
        return HELP_TEXT

    if lowered.startswith("סטטוס"):
        silent = silent_sources()
        muted = muted_until()
        return (
            "📊 *מצב המערכת*\n\n"
            f"• סריקות: {_runtime_status['iterations']}\n"
            f"• סיפורים בסריקה האחרונה: "
            f"{_runtime_status['last_scan_new_stories']}\n"
            f"• ממתין בתור: {pending_outbox_count()}\n"
            f"• מטרות: {len(all_subjects())}\n"
            f"• מקורות שקטים: {len(silent)}\n"
            f"• שבת/חג: {'כן' if in_quiet_period() else 'לא'}\n"
            f"• מושתק עד: {muted.astimezone(ISRAEL_TZ).strftime('%H:%M') if muted else 'לא מושתק'}"
        )

    if lowered.startswith("השתק"):
        parts = text.split()
        hours = 1.0
        if len(parts) > 1:
            try:
                hours = max(0.25, min(48.0, float(parts[1])))
            except ValueError:
                hours = 1.0
        until = datetime.now(timezone.utc) + timedelta(hours=hours)
        set_state("muted_until", until.isoformat())
        local = until.astimezone(ISRAEL_TZ).strftime("%H:%M")
        return f"🔇 מושתק עד {local}. הודעות יצטברו ויגיעו כדיג'סט."

    if lowered in {"המשך", "בטל השתקה", "הפעל"}:
        set_state("muted_until", "")
        release_held_messages(israel_now())
        return "🔔 ההשתקה בוטלה."

    if lowered.startswith("מטרות"):
        lines = [
            f"• {entry.get('label', key)}" for key, entry in all_subjects().items()
        ]
        return "🎯 *מטרות מנוטרות*\n\n" + "\n".join(lines) + "\n• 📊 כל סקר פוליטי"

    if text.startswith("מטרה+"):
        name = text[len("מטרה+"):].strip()
        if len(name) < 2:
            return "צריך שם מטרה, למשל: מטרה+ סמוטריץ׳"
        data = json.loads(get_state("custom_subjects") or "{}")
        data[slugify(name)] = {"label": name, "terms": [name]}
        set_state("custom_subjects", json.dumps(data, ensure_ascii=False))
        return f"🎯 נוספה מטרה: {name}"

    if text.startswith("מטרה-"):
        name = text[len("מטרה-"):].strip()
        data = json.loads(get_state("custom_subjects") or "{}")
        key = slugify(name)
        if key not in data:
            return f"לא נמצאה מטרה בשם {name}. מטרות הליבה לא ניתנות להסרה."
        data.pop(key)
        set_state("custom_subjects", json.dumps(data, ensure_ascii=False))
        return f"הוסרה המטרה: {name}"

    if lowered in {"היום", "סיכום"}:
        return build_daily_summary(israel_now().strftime("%Y-%m-%d"))

    return None


def relay_from_group(data: dict[str, Any], group_id: str) -> bool:
    """הודעה מקבוצת מקור עוברת באותו צינור סינון, דדופ ואשכולות."""
    text = clean_text(str(data.get("body", "")))
    if len(text) < 12:
        return False

    kind, subjects, is_statement = classify(text)
    if kind is None:
        return False

    label = RELAY_GROUPS.get(group_id, "קבוצת וואטסאפ")
    display = f"📲 {label}"
    sender = str(data.get("pushname") or data.get("author") or "").strip()
    if RELAY_SHOW_SENDER and sender:
        display = f"📲 {label} / {sender}"

    message_id = str(data.get("id") or "")
    item_key = make_item_key(f"relay:{group_id}", message_id, text)
    if not filter_unseen([item_key]):
        return False

    cluster_id, is_new_story, source_count = find_or_create_cluster(
        text, kind, display
    )
    now = israel_now()
    db().execute(
        """
        INSERT OR IGNORE INTO mentions(
            item_key, cluster_id, local_date, local_time, source_key,
            source_display, text, link, kind, is_duplicate, created_at_utc
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_key, cluster_id, now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"), f"relay:{group_id}", display,
            text[:1500], "", kind, 0 if is_new_story else 1, utc_now_iso(),
        ),
    )

    if is_new_story:
        tone = analyze_tone(text) if TONE_ENABLED else None
        if tone:
            save_tone(cluster_id, tone)
        enqueue_message(
            format_alert(display, text, "", kind, subjects, is_statement,
                         now, tone),
            priority=alert_priority(kind, is_statement, tone),
        )
    elif (
        ESCALATION_THRESHOLD
        and source_count >= ESCALATION_THRESHOLD
        and mark_escalated(cluster_id)
    ):
        enqueue_message(format_escalation(cluster_id, text, now, False),
                        priority=3)

    mark_seen(item_key, f"relay:{group_id}", "")
    db().execute(
        """
        INSERT INTO source_health(source_key, display, last_item_utc, items_seen)
        VALUES(?, ?, ?, 1)
        ON CONFLICT(source_key) DO UPDATE SET
            last_item_utc = excluded.last_item_utc,
            items_seen = source_health.items_seen + 1
        """,
        (f"relay:{group_id}", display, utc_now_iso()),
    )
    return bool(is_new_story)


@app.get("/groups")
def list_groups():
    """עוזר חד-פעמי: מציג את מזהי כל הקבוצות שהמספר חבר בהן."""
    if not (ULTRA_ID and ULTRA_TOKEN):
        return jsonify({"error": "missing UltraMsg credentials"}), 400
    try:
        response = HTTP.get(
            f"https://api.ultramsg.com/{ULTRA_ID}/groups",
            params={"token": ULTRA_TOKEN},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        groups = response.json()
        return jsonify(
            [
                {"id": g.get("id"), "name": g.get("name")}
                for g in (groups if isinstance(groups, list) else [])
            ]
        )
    except Exception as exc:
        return jsonify({"error": str(exc)[:300]}), 502


@app.post("/webhook/ultramsg")
def ultramsg_webhook():
    if not COMMANDS_ENABLED:
        return jsonify({"ok": False, "reason": "commands disabled"}), 403
    try:
        payload = request.get_json(force=True, silent=True) or {}
        data = payload.get("data", {}) or {}
        if payload.get("event_type") != "message_received" or data.get("fromMe"):
            return jsonify({"ok": True, "ignored": True})
        chat = str(data.get("from", ""))

        # קבוצת מקור — מסננים ומעבירים אלינו
        if RELAY_ENABLED and chat in RELAY_GROUPS:
            forwarded = relay_from_group(data, chat)
            return jsonify({"ok": True, "relayed": forwarded})

        # הקבוצה שלנו — פקודות בלבד
        if GROUP_ID and chat != GROUP_ID:
            return jsonify({"ok": True, "ignored": "other chat"})

        reply = handle_command(str(data.get("body", "")))
        if reply:
            enqueue_message(reply, priority=0)
        return jsonify({"ok": True, "handled": bool(reply)})
    except Exception as exc:
        LOGGER.exception("Webhook error")
        return jsonify({"ok": False, "error": str(exc)[:200]}), 500


@app.get("/search")
def search():
    init_db()
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"error": "missing q"}), 400
    rows = db().execute(
        """
        SELECT local_date, local_time, source_display, kind, text, link
        FROM mentions
        WHERE text LIKE ?
        ORDER BY id DESC LIMIT 100
        """,
        (f"%{query}%",),
    ).fetchall()
    return jsonify(
        {
            "query": query,
            "count": len(rows),
            "results": [dict(row) for row in rows],
        }
    )


@app.get("/report")
def report():
    init_db()
    day = request.args.get("date") or israel_now().strftime("%Y-%m-%d")
    rows = mentions_for_date(day)
    items = "".join(
        f"<tr><td>{row['local_time'][:5]}</td>"
        f"<td>{html.escape(str(row['source_display']))}</td>"
        f"<td>{html.escape(str(subject_entry(str(row['kind'])).get('label', row['kind'])))}</td>"
        f"<td>{html.escape(str(row['text'])[:300])}</td></tr>"
        for row in rows
        if not int(row["is_duplicate"])
    )
    return f"""<!doctype html><html lang="he" dir="rtl"><meta charset="utf-8">
    <title>דוח ניטור {day}</title>
    <body style="font-family:Arial;margin:40px;line-height:1.5">
    <h1>דוח ניטור — {day}</h1>
    <p>{len(rows)} אזכורים</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
    <tr><th>שעה</th><th>מקור</th><th>מטרה</th><th>תוכן</th></tr>{items}</table>
    </body></html>"""


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
        "sources": {
            "telegram": len(TELEGRAM_CHANNELS),
            "rss": len(RSS_FEEDS),
            "relay_groups": len(RELAY_GROUPS),
        },
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
