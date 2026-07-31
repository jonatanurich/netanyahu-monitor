import os, time, re, requests, hashlib
from datetime import datetime
from bs4 import BeautifulSoup
import feedparser

# ========= CONFIG =========
ULTRA_ID = os.getenv("ULTRA_ID", "instance186865")
ULTRA_TOKEN = os.getenv("ULTRA_TOKEN", "ih8jz0qoxrc166a0")
GROUP_ID = os.getenv("GROUP_ID", "120363429023395450@g.us")

KEYWORDS = ["נתניהו", "ביבי", "ראש הממשלה", "בנימין נתניהו", "Netanyahu"]

# ערוצי טלגרם הכי חזקים בישראל - כולם ציבוריים, בלי API
TELEGRAM_CHANNELS = [
    "N12_news_Israel",      # N12
    "kann_news",            # כאן חדשות
    "newsisrael13",         # חדשות 13
    "ynet",                 # ynet
    "WallaNews",            # וואלה
    "GLZRadio",             # גלצ
    "Now14Israel",          # עכשיו 14
    "abualiexpress",        # אבו עלי - הכי מהיר בישראל
    "almog_cohen_news",     # חדשות מהשטח
]

# חשבונות טוויטר / X של ערוצי החדשות
TWITTER_ACCOUNTS = [
    "N12News",
    "kann_news",
    "newsisrael13",
    "ynetalerts",
    "WallaNews",
    "GLZRadio",
    "Now14Israel",
    "DovrutHaknesset",
]

# RSS של אתרי חדשות - הכי אמין
RSS_FEEDS = {
    "Ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
    "Walla": "https://rss.walla.co.il/feed/22",
    "Kan": "https://www.kan.org.il/rss/",
}

NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
]

seen_hashes = set()

def get_hash(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]

def send_whatsapp(text, source, link=""):
    # ניקוי טקסט
    clean = text[:600].replace("\n", " ").strip()
    if not clean:
        return
    
    msg = f"""🚨 *אזכור נתניהו - {source}*

📺 *מקור:* {source}
🕒 {datetime.now().strftime('%d/%m %H:%M:%S')}
💬 {clean}
"""
    if link:
        msg += f"\n🔗 {link}"
    msg += "\n#נתניהו"
    
    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": msg}
    try:
        r = requests.post(url, data=payload, timeout=15)
        print(f"✅ SENT [{source}] {r.status_code} - {clean[:80]}")
    except Exception as e:
        print(f"❌ WA Error {source}: {e}")

def contains_keyword(text):
    if not text:
        return False
    for kw in KEYWORDS:
        if kw.lower() in text.lower() or kw in text:
            return True
    return False

def fetch_telegram_channel(channel):
    """מושך הודעות מערוץ טלגרם ציבורי בלי API - דרך t.me/s/"""
    try:
        url = f"https://t.me/s/{channel}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return []
        
        soup = BeautifulSoup(r.text, 'html.parser')
        messages = []
        
        for wrap in soup.find_all('div', class_='tgme_widget_message_wrap')[-10:]:  # 10 אחרונות
            text_div = wrap.find('div', class_='tgme_widget_message_text')
            if not text_div:
                continue
            text = text_div.get_text(separator=" ").strip()
            if not text:
                continue
            
            # לינק להודעה
            link_tag = wrap.find('a', class_='tgme_widget_message_date')
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else f"https://t.me/{channel}"
            
            messages.append({"text": text, "link": link})
        
        return messages
    except Exception as e:
        print(f"TG Error {channel}: {e}")
        return []

def fetch_twitter_nitter(username):
    """מושך ציוצים דרך Nitter RSS - בלי API"""
    for instance in NITTER_INSTANCES:
        try:
            url = f"{instance}/{username}/rss"
            feed = feedparser.parse(url)
            if not feed.entries:
                continue
            msgs = []
            for entry in feed.entries[:5]:
                text = entry.title if hasattr(entry, 'title') else entry.get('description', '')
                link = entry.link if hasattr(entry, 'link') else f"https://twitter.com/{username}"
                msgs.append({"text": text, "link": link})
            if msgs:
                return msgs
        except Exception as e:
            continue
    return []

def fetch_rss(name, url):
    try:
        feed = feedparser.parse(url)
        msgs = []
        for entry in feed.entries[:8]:
            text = f"{entry.title} {entry.get('description','')}"
            link = entry.link
            msgs.append({"text": text, "link": link})
        return msgs
    except Exception as e:
        print(f"RSS Error {name}: {e}")
        return []

def check_and_send(messages, source_name):
    for m in messages:
        text = m['text']
        link = m.get('link','')
        if not contains_keyword(text):
            continue
        
        h = get_hash(text)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        
        # ניקוי
        if len(seen_hashes) > 5000:
            seen_hashes.clear()
        
        print(f"🔥 FOUND in {source_name}: {text[:100]}")
        send_whatsapp(text, source_name, link)
        time.sleep(1)  # לא להציף

def main_loop():
    print("🚀 מערכת ניטור נתניהו - טלגרם + טוויטר + RSS")
    print(f"מנטר {len(TELEGRAM_CHANNELS)} ערוצי טלגרם, {len(TWITTER_ACCOUNTS)} חשבונות טוויטר, {len(RSS_FEEDS)} RSS")
    print(f"Keywords: {KEYWORDS}")
    
    # הודעת פתיחה
    send_whatsapp(f"המערכת החדשה עלתה! מנטרת {len(TELEGRAM_CHANNELS)} ערוצי טלגרם ו-{len(TWITTER_ACCOUNTS)} חשבונות טוויטר. כל אזכור של נתניהו/ביבי יגיע לכאן מיידית. ללא תמלול - 100% מדויק.", "מערכת V2")
    
    iteration = 0
    while True:
        iteration += 1
        print(f"\n=== סריקה {iteration} - {datetime.now().strftime('%H:%M:%S')} ===")
        
        # 1. טלגרם
        for ch in TELEGRAM_CHANNELS:
            msgs = fetch_telegram_channel(ch)
            if msgs:
                print(f"[{ch}] {len(msgs)} הודעות")
                check_and_send(msgs, f"טלגרם {ch}")
            time.sleep(1)
        
        # 2. טוויטר
        for acc in TWITTER_ACCOUNTS:
            msgs = fetch_twitter_nitter(acc)
            if msgs:
                print(f"[TW {acc}] {len(msgs)} ציוצים")
                check_and_send(msgs, f"טוויטר @{acc}")
            time.sleep(1.5)
        
        # 3. RSS
        for name, url in RSS_FEEDS.items():
            msgs = fetch_rss(name, url)
            if msgs:
                check_and_send(msgs, f"RSS {name}")
            time.sleep(1)
        
        print(f"סריקה {iteration} הסתיימה. מחכה 30 שניות...")
        time.sleep(30)

if __name__ == "__main__":
    main_loop()
