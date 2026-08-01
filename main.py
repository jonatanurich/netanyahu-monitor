"""
🚨 מערכת תקשוב נתניהו V5 - FINAL
- בלי תמלול טלוויזיה/רדיו בכלל!
- רק ניטור טקסט 100% מדויק: טלגרם + טוויטר + אתרי חדשות
- סיכום כותרות כל שעה עגולה 07:00-22:00

מקורות: 20 ערוצי טלגרם + 12 חשבונות טוויטר + 8 RSS
"""

import os, time, hashlib, requests, feedparser, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

try:
    import pytz
    ISRAEL_TZ = pytz.timezone('Asia/Jerusalem')
except:
    ISRAEL_TZ = None

# ============ CONFIG ============
ULTRA_ID = os.getenv("ULTRA_ID", "instance186865")
ULTRA_TOKEN = os.getenv("ULTRA_TOKEN", "ih8jz0qoxrc166a0")
GROUP_ID = os.getenv("GROUP_ID", "120363429023395450@g.us")

KEYWORDS = ["נתניהו", "ביבי", "בנימין נתניהו", "ראש הממשלה"]

# ============ מקורות - רק אינטרנט ורשתות חברתיות ============
TELEGRAM_CHANNELS = {
    "N12_news_Israel": "📺 חדשות 12",
    "kann_news": "📺 כאן חדשות",
    "newsisrael13": "📺 חדשות 13",
    "Now14Israel": "📺 עכשיו 14",
    "ynet": "📰 Ynet",
    "WallaNews": "📰 וואלה",
    "MaarivOnline": "📰 מעריב",
    "IsraelHayomNews": "📰 ישראל היום",
    "Haaretz": "📰 הארץ",
    "abualiexpress": "⚡ אבו עלי",
    "HamalNews": "⚡ החמ\"ל",
    "NewsFromTheField": "⚡ חדשות מהשטח",
    "RoterNews": "⚡ רוטר",
    "MivzakLive": "⚡ מבזקים",
    "RealTimeSecurity": "⚡ ביטחון שוטף",
    "DoverTzahal": "⚡ דובר צה\"ל",
    "almog_cohen_news": "⚡ אלמוג כהן",
    "GLZRadio": "📻 גל\"צ (טקסט בלבד, בלי תמלול)",
    "PushNews": "⚡ פוש ניוז",
    "BreakingNewsIL": "⚡ מבזקים IL",
}

TWITTER_ACCOUNTS = {
    "N12News": "📺 חדשות 12",
    "kann_news": "📺 כאן",
    "newsisrael13": "📺 חדשות 13",
    "Now14Israel": "📺 עכשיו 14",
    "ynetalerts": "📰 Ynet",
    "WallaNews": "📰 וואלה",
    "MaarivOnline": "📰 מעריב",
    "IsraelHayomHeb": "📰 ישראל היום",
    "haaretznewsvid": "📰 הארץ",
    "Netanyahu": "👤 נתניהו",
    "IsraelPM": "🏛️ רה\"מ",
    "DovrutHaknesset": "🏛️ הכנסת",
}

RSS_FEEDS = {
    "Ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
    "Walla": "https://rss.walla.co.il/feed/22",
    "Maariv": "https://www.maariv.co.il/Rss/RssFeedsMivzakiChadashot",
    "Israel Hayom": "https://www.israelhayom.co.il/rss.xml",
    "Mako N12": "https://rcs.mako.co.il/rss/31750a2610f26110",
    "Kan News": "https://www.kan.org.il/rss/",
    "Haaretz": "https://www.haaretz.co.il/c/1.4841152?l=he-rss",
    "Channel 13": "https://13tv.co.il/feed/",
}

NITTER_INSTANCES = ["https://nitter.net", "https://nitter.privacydev.net"]

seen_hashes = set()
seen_file = "/tmp/seen_v5.json"
last_hourly_sent = None

def load_seen():
    global seen_hashes
    try:
        if os.path.exists(seen_file):
            with open(seen_file, 'r') as f:
                seen_hashes = set(json.load(f))
    except:
        pass

def save_seen():
    try:
        with open(seen_file, 'w') as f:
            json.dump(list(seen_hashes)[-2000:], f)
    except:
        pass

def get_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()[:12]

def get_israel_time():
    if ISRAEL_TZ:
        return datetime.now(ISRAEL_TZ)
    else:
        # fallback UTC+3
        return datetime.utcnow() + __import__('datetime').timedelta(hours=3)

def send_whatsapp(text):
    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": text}
    try:
        r = requests.post(url, data=payload, timeout=10)
        print(f"✅ SENT {r.status_code} | {text[:60]}...")
        return True
    except Exception as e:
        print(f"❌ WA Error: {e}")
        return False

def contains_kw(text):
    if not text:
        return False
    return any(kw in text for kw in KEYWORDS)

# ============ FETCHERS - רק טקסט, בלי תמלול ============
def fetch_telegram(channel):
    try:
        url = f"https://t.me/s/{channel}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')
        msgs = []
        for wrap in soup.find_all('div', class_='tgme_widget_message_wrap')[-4:]:
            text_div = wrap.find('div', class_='tgme_widget_message_text')
            if not text_div:
                continue
            text = text_div.get_text(separator=" ").strip()
            if len(text) < 15:
                continue
            link_tag = wrap.find('a', class_='tgme_widget_message_date')
            link = link_tag['href'] if link_tag else f"https://t.me/{channel}"
            msgs.append({"text": text, "link": link, "source": channel})
        return msgs
    except:
        return []

def fetch_twitter(username):
    for instance in NITTER_INSTANCES:
        try:
            url = f"{instance}/{username}/rss"
            feed = feedparser.parse(url)
            if not feed.entries:
                continue
            msgs = []
            for entry in feed.entries[:3]:
                text = getattr(entry, 'title', '') or entry.get('description', '')
                link = getattr(entry, 'link', f"https://twitter.com/{username}")
                msgs.append({"text": text, "link": link, "source": username})
            if msgs:
                return msgs
        except:
            continue
    return []

def fetch_rss(name, url):
    try:
        feed = feedparser.parse(url)
        msgs = []
        for entry in feed.entries[:5]:
            title = getattr(entry, 'title', '')
            desc = entry.get('description', '')
            text = f"{title} {desc}"
            link = getattr(entry, 'link', '')
            msgs.append({"text": text, "link": link, "source": name, "title": title})
        return msgs
    except:
        return []

# ============ סיכום כותרות כל שעה עגולה ============
def get_hourly_summary():
    """מושך כותרות ראשיות מכל האתרים"""
    print("📰 מכין סיכום כותרות שעתי...")
    headlines_by_site = {}
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_rss, name, url): name for name, url in RSS_FEEDS.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                msgs = future.result()
                # רק 3 כותרות ראשיות לכל אתר
                titles = []
                for m in msgs[:3]:
                    title = m.get('title', m['text'][:100])
                    if title and len(title) > 10:
                        titles.append(title[:120])
                if titles:
                    headlines_by_site[name] = titles
            except:
                pass
    
    if not headlines_by_site:
        return None
    
    now = get_israel_time()
    summary = f"📰 *סיכום כותרות - {now.strftime('%H:00')} - {now.strftime('%d/%m/%Y')}*\n"
    summary += "━━━━━━━━━━━━━━━━━━━━\n"
    
    for site, titles in headlines_by_site.items():
        summary += f"\n*{site}:*\n"
        for t in titles:
            summary += f"• {t}\n"
    
    summary += f"\n⏰ {now.strftime('%H:%M')} | בוט תקשוב נתניהו"
    return summary

def should_send_hourly():
    global last_hourly_sent
    now = get_israel_time()
    hour = now.hour
    minute = now.minute
    
    # בין 07:00 ל-22:00, כל שעה עגולה (דקה 0-2 כדי לא לפספס)
    if hour < 7 or hour > 22:
        return False
    if minute > 2:  # רק ב-3 דקות הראשונות של השעה
        return False
    if last_hourly_sent == hour:
        return False
    
    return True

# ============ MAIN LOOP ============
def main_loop():
    global last_hourly_sent
    load_seen()
    
    print("""
    ╔══════════════════════════════════════╗
    ║  תקשוב נתניהו V5 - בלי תמלול        ║
    ║  רק טלגרם + טוויטר + אתרים         ║
    ║  + סיכום כותרות כל שעה 07-22       ║
    ╚══════════════════════════════════════╝
    """)
    
    now = get_israel_time()
    send_whatsapp(f"""🚀 *בוט תקשוב נתניהו V5 עלה!*

✅ בלי תמלול טלוויזיה/רדיו - רק טקסט מדויק 100%
📡 מנטר:
• {len(TELEGRAM_CHANNELS)} ערוצי טלגרם
• {len(TWITTER_ACCOUNTS)} חשבונות טוויטר
• {len(RSS_FEEDS)} אתרי חדשות

🔍 מסנן: נתניהו / ביבי / ראש הממשלה
📰 סיכום כותרות כל שעה עגולה 07:00-22:00

⏰ {now.strftime('%d/%m %H:%M')}""")
    
    iteration = 0
    while True:
        iteration += 1
        now = get_israel_time()
        print(f"\n=== SCAN {iteration} {now.strftime('%H:%M:%S')} ===")
        
        # 1. בדיקת אזכורי נתניהו
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {}
            for ch in TELEGRAM_CHANNELS:
                futures[executor.submit(fetch_telegram, ch)] = ("TG", ch, TELEGRAM_CHANNELS[ch])
            for acc in TWITTER_ACCOUNTS:
                futures[executor.submit(fetch_twitter, acc)] = ("TW", acc, TWITTER_ACCOUNTS[acc])
            for name, url in RSS_FEEDS.items():
                futures[executor.submit(fetch_rss, name, url)] = ("RSS", name, name)
            
            for future in as_completed(futures):
                try:
                    typ, key, display = futures[future]
                    msgs = future.result()
                    for m in msgs:
                        text = m['text']
                        if not contains_kw(text):
                            continue
                        h = get_hash(text)
                        if h in seen_hashes:
                            continue
                        seen_hashes.add(h)
                        if len(seen_hashes) > 3000:
                            seen_hashes = set(list(seen_hashes)[-1500:])
                        
                        print(f"🔥 [{display}] {text[:80]}")
                        clean = " ".join(text.split())[:500]
                        msg = f"""🚨 *אזכור נתניהו - {display}*

💬 {clean}

🔗 {m.get('link','')}
⏰ {now.strftime('%d/%m %H:%M:%S')}"""
                        send_whatsapp(msg)
                        time.sleep(1)
                except Exception as e:
                    pass
        
        save_seen()
        
        # 2. בדיקת סיכום שעתי
        if should_send_hourly():
            print(f"⏰ {now.hour}:00 - שולח סיכום כותרות!")
            summary = get_hourly_summary()
            if summary:
                send_whatsapp(summary)
                last_hourly_sent = now.hour
                print(f"✅ סיכום {now.hour}:00 נשלח")
        
        # שינה 20 שניות
        print(f"Done, seen={len(seen_hashes)}, sleep 20s")
        time.sleep(20)

if __name__ == "__main__":
    main_loop()
