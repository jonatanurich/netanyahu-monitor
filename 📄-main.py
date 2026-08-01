"""
🚨 מערכת תקשוב נתניהו V7 - FINAL+
- בלי תמלול טלוויזיה/רדיו!
- רק טקסט 100% מדויק: טלגרם + טוויטר + אתרים
- סיכום כותרות כל שעה עגולה 07:00-22:00
- סיכום יומי נתניהו כל יום ב-18:00
- התראה מיידית על כל סקר ליכוד/נתניהו
"""

import os, time, hashlib, requests, feedparser, json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from collections import Counter

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
LIKUD_KEYWORDS = ["נתניהו", "ביבי", "בנימין נתניהו", "ליכוד", "הליכוד"]
POLL_KEYWORDS = ["סקר", "סקרים", "מנדט", "מנדטים", "סקר מנדטים", "סקר חדש"]

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
    "GLZRadio": "📻 גל\"צ טקסט",
    "PushNews": "⚡ פוש ניוז",
    "BreakingNewsIL": "⚡ מבזקים IL",
    # ערוצי סקרים
    "PollsIsrael": "📊 סקרים",
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
    # סוקרים
    "MaagarMochot": "📊 מאגר מוחות",
    "MidgamPolls": "📊 מדגם",
    "ManoGeva": "📊 מנו גבע",
    "ShlomoFilber": "📊 שלמה פילבר",
    "PollsIsrael": "📊 סקרים",
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
seen_file = "/tmp/seen_v7.json"
daily_file = "/tmp/daily_netanyahu_v7.json"
daily_mentions = []
last_hourly_sent = None
last_daily_sent_date = None

def load_seen():
    global seen_hashes, daily_mentions
    try:
        if os.path.exists(seen_file):
            with open(seen_file, 'r') as f:
                seen_hashes = set(json.load(f))
    except:
        pass
    try:
        if os.path.exists(daily_file):
            with open(daily_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                today_str = get_israel_time().strftime('%Y-%m-%d')
                daily_mentions = [x for x in data if x.get('date') == today_str]
    except:
        daily_mentions = []

def save_seen():
    try:
        with open(seen_file, 'w') as f:
            json.dump(list(seen_hashes)[-4000:], f)
    except:
        pass
    try:
        with open(daily_file, 'w', encoding='utf-8') as f:
            json.dump(daily_mentions[-600:], f, ensure_ascii=False)
    except:
        pass

def get_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()[:12]

def get_israel_time():
    if ISRAEL_TZ:
        return datetime.now(ISRAEL_TZ)
    else:
        return datetime.utcnow() + timedelta(hours=3)

def send_whatsapp(text):
    if len(text) > 3800:
        parts = [text[i:i+3800] for i in range(0, len(text), 3800)]
        for i, part in enumerate(parts):
            if i > 0:
                part = f"(המשך {i+1}/{len(parts)})\n" + part
            url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
            payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": part}
            try:
                requests.post(url, data=payload, timeout=10)
                time.sleep(1)
            except:
                pass
        return True
    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": text}
    try:
        r = requests.post(url, data=payload, timeout=15)
        print(f"✅ SENT {r.status_code} | {text[:70]}...")
        return True
    except Exception as e:
        print(f"❌ WA Error: {e}")
        return False

def contains_kw(text):
    if not text:
        return False
    return any(kw in text for kw in KEYWORDS)

def contains_poll(text):
    """מזהה סקר על ליכוד/נתניהו"""
    if not text:
        return False
    has_poll = any(kw in text for kw in POLL_KEYWORDS)
    has_likud = any(kw in text for kw in LIKUD_KEYWORDS)
    return has_poll and has_likud

def fetch_telegram(channel):
    try:
        url = f"https://t.me/s/{channel}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')
        msgs = []
        for wrap in soup.find_all('div', class_='tgme_widget_message_wrap')[-5:]:
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
            for entry in feed.entries[:4]:
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
        for entry in feed.entries[:6]:
            title = getattr(entry, 'title', '')
            desc = entry.get('description', '')
            text = f"{title} {desc}"
            link = getattr(entry, 'link', '')
            msgs.append({"text": text, "link": link, "source": name, "title": title})
        return msgs
    except:
        return []

def get_hourly_summary():
    headlines_by_site = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_rss, name, url): name for name, url in RSS_FEEDS.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                msgs = future.result()
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
    summary = f"📰 *סיכום כותרות - {now.strftime('%H:00')} - {now.strftime('%d/%m/%Y')}*\n━━━━━━━━━━━━━━━━━━━━\n"
    for site, titles in headlines_by_site.items():
        summary += f"\n*{site}:*\n"
        for t in titles:
            summary += f"• {t}\n"
    summary += f"\n⏰ {now.strftime('%H:%M')} | בוט תקשוב"
    return summary

def should_send_hourly():
    global last_hourly_sent
    now = get_israel_time()
    hour = now.hour
    minute = now.minute
    if hour < 7 or hour > 22:
        return False
    if minute > 2:
        return False
    if last_hourly_sent == hour:
        return False
    return True

def should_send_daily():
    global last_daily_sent_date
    now = get_israel_time()
    if now.hour != 18:
        return False
    if now.minute > 3:
        return False
    today_str = now.strftime('%Y-%m-%d')
    if last_daily_sent_date == today_str:
        return False
    try:
        if os.path.exists("/tmp/last_daily_v7.txt"):
            with open("/tmp/last_daily_v7.txt") as f:
                if f.read().strip() == today_str:
                    return False
    except:
        pass
    return True

def get_daily_summary():
    now = get_israel_time()
    today_str = now.strftime('%Y-%m-%d')
    if not daily_mentions:
        return f"""📊 *סיכום יומי נתניהו - {now.strftime('%d/%m/%Y')} 18:00*
━━━━━━━━━━━━━━━━━━━━

לא נמצאו אזכורי נתניהו היום.

⏰ {now.strftime('%H:%M')}"""
    
    sources = Counter([m['source_display'] for m in daily_mentions])
    topics = Counter()
    for m in daily_mentions:
        txt = m['text']
        if any(w in txt for w in ["שריון", "פריימריז", "ליכוד", "רשימה"]):
            topics["פריימריז / שריונים בליכוד"] += 1
        if any(w in txt for w in ["משפט", "שוחד", "תיקי האלפים", "בית משפט", "עדות"]):
            topics["משפט נתניהו"] += 1
        if any(w in txt for w in ["לבנון", "חיזבאללה", "עזה", "ביטחוני", "צה\"ל", "חמאס"]):
            topics["ביטחוני / מדיני"] += 1
        if any(w in txt for w in ["כנסת", "הצבעה", "חוק", "ממשלה", "קבינט"]):
            topics["פוליטי / כנסת"] += 1
        if any(w in txt for w in ["טראמפ", "ארה\"ב", "ביידן"]):
            topics["יחסי חוץ / ארה\"ב"] += 1
        if any(w in txt for w in ["סקר", "מנדט"]):
            topics["סקרים"] += 1
    
    summary = f"""📊 *סיכום יומי נתניהו - {now.strftime('%d/%m/%Y')} 18:00*
━━━━━━━━━━━━━━━━━━━━
*סה"כ {len(daily_mentions)} אזכורים היום*

"""
    if topics:
        summary += "🔥 *הנושאים המרכזיים:*\n"
        for topic, count in topics.most_common(5):
            summary += f"• {topic} ({count})\n"
        summary += "\n"
    summary += "📈 *פילוח לפי מקור:*\n"
    for src, cnt in sources.most_common(8):
        summary += f"• {src}: {cnt}\n"
    summary += "\n📰 *כל הכותרות היום:*\n"
    unique = []
    seen_texts = set()
    for m in sorted(daily_mentions, key=lambda x: x['time']):
        short = m['text'][:80]
        if short not in seen_texts:
            seen_texts.add(short)
            unique.append(m)
    for i, m in enumerate(unique[:25], 1):
        time_str = m['time'].split(' ')[1][:5] if ' ' in m['time'] else m['time']
        clean = " ".join(m['text'].split())[:150]
        summary += f"\n*{i}. [{time_str}] {m['source_display']}*\n{clean}\n"
    if len(unique) > 25:
        summary += f"\n... ועוד {len(unique)-25} אזכורים נוספים\n"
    summary += f"\n━━━━━━━━━━━━━━━━━━━━\n⏰ סיכום אוטומטי 18:00 | בוט תקשוב נתניהו"
    return summary

def main_loop():
    global last_hourly_sent, last_daily_sent_date, daily_mentions
    load_seen()
    print("""
    ╔══════════════════════════════════════╗
    ║  תקשוב נתניהו V7                    ║
    ║  - בלי תמלול                       ║
    ║  - סיכום שעתי 07-22                ║
    ║  - סיכום יומי 18:00                ║
    ║  - התראה מיידית סקרים ליכוד/ביבי  ║
    ╚══════════════════════════════════════╝
    """)
    now = get_israel_time()
    try:
        if os.path.exists("/tmp/last_daily_v7.txt"):
            with open("/tmp/last_daily_v7.txt") as f:
                last_daily_sent_date = f.read().strip()
    except:
        pass
    
    send_whatsapp(f"""🚀 *בוט תקשוב V7 עלה!*

✅ בלי תמלול - רק טקסט מדויק
📡 {len(TELEGRAM_CHANNELS)} טלגרם + {len(TWITTER_ACCOUNTS)} טוויטר + {len(RSS_FEEDS)} אתרים
🔍 מסנן: נתניהו / ביבי / ראש הממשלה
📊 סקרים: כל סקר על ליכוד/נתניהו - מיידי!
📰 סיכום כותרות כל שעה 07:00-22:00
📊 סיכום יומי נתניהו כל יום ב-18:00

⏰ {now.strftime('%d/%m %H:%M')}""")
    
    iteration = 0
    while True:
        iteration += 1
        now = get_israel_time()
        print(f"\n=== SCAN {iteration} {now.strftime('%H:%M:%S')} | daily={len(daily_mentions)} ===")
        
        with ThreadPoolExecutor(max_workers=14) as executor:
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
                        is_netanyahu = contains_kw(text)
                        is_poll = contains_poll(text)
                        
                        # אם זה לא נתניהו ולא סקר ליכוד - דלג
                        if not is_netanyahu and not is_poll:
                            continue
                        
                        h = get_hash(text + str(is_poll))
                        if h in seen_hashes:
                            continue
                        seen_hashes.add(h)
                        if len(seen_hashes) > 4000:
                            seen_hashes = set(list(seen_hashes)[-2000:])
                        
                        clean = " ".join(text.split())[:600]
                        
                        if is_poll:
                            print(f"📊 POLL! [{display}] {text[:80]}")
                            msg = f"""📊 *סקר חדש - ליכוד/נתניהו!* - {display}

💬 {clean}

🔗 {m.get('link','')}
⏰ {now.strftime('%d/%m %H:%M:%S')}"""
                            send_whatsapp(msg)
                            # גם סקרים נשמרים לסיכום יומי
                            daily_mentions.append({
                                "date": now.strftime('%Y-%m-%d'),
                                "time": now.strftime('%d/%m %H:%M:%S'),
                                "source": key,
                                "source_display": display + " 📊",
                                "text": clean,
                                "link": m.get('link',''),
                                "hash": h,
                                "type": "poll"
                            })
                            time.sleep(1)
                            continue
                        
                        if is_netanyahu:
                            print(f"🔥 [{display}] {text[:80]}")
                            daily_mentions.append({
                                "date": now.strftime('%Y-%m-%d'),
                                "time": now.strftime('%d/%m %H:%M:%S'),
                                "source": key,
                                "source_display": display,
                                "text": clean,
                                "link": m.get('link',''),
                                "hash": h,
                                "type": "netanyahu"
                            })
                            msg = f"""🚨 *אזכור נתניהו - {display}*

💬 {clean}

🔗 {m.get('link','')}
⏰ {now.strftime('%d/%m %H:%M:%S')}"""
                            send_whatsapp(msg)
                            time.sleep(1)
                except Exception as e:
                    print(f"err {e}")
                    pass
        
        save_seen()
        
        if should_send_hourly():
            print(f"⏰ {now.hour}:00 - סיכום כותרות!")
            summary = get_hourly_summary()
            if summary:
                send_whatsapp(summary)
                last_hourly_sent = now.hour
        
        if should_send_daily():
            print(f"📊 18:00 - סיכום יומי!")
            daily_summary = get_daily_summary()
            if daily_summary:
                send_whatsapp(daily_summary)
                today_str = now.strftime('%Y-%m-%d')
                last_daily_sent_date = today_str
                try:
                    with open("/tmp/last_daily_v7.txt", 'w') as f:
                        f.write(today_str)
                except:
                    pass
        
        print(f"Done, seen={len(seen_hashes)}, daily={len(daily_mentions)}, sleep 20s")
        time.sleep(20)

if __name__ == "__main__":
    main_loop()
