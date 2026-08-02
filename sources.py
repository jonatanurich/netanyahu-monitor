"""מקורות ברירת מחדל למערכת הניטור.

אפשר להוסיף או להסיר מקורות ישירות בקובץ הזה.
יש להשתמש רק במקורות ציבוריים או במקורות שיש לך הרשאה לנטר.
"""

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
    "HamalNews": "⚡ החמ״ל",
    "NewsFromTheField": "⚡ חדשות מהשטח",
    "RoterNews": "⚡ רוטר",
    "MivzakLive": "⚡ מבזקים",
    "RealTimeSecurity": "⚡ ביטחון שוטף",
    "DoverTzahal": "⚡ דובר צה״ל",
    "almog_cohen_news": "⚡ אלמוג כהן",
    "GLZRadio": "📻 גל״צ טקסט",
    "PushNews": "⚡ פוש ניוז",
    "BreakingNewsIL": "⚡ מבזקים IL",
    "PollsIsrael": "📊 סקרים",
}

# X/Nitter אינו מקור יציב ולכן כבוי כברירת מחדל.
# כדי להפעיל, הגדר X_ENABLED=true במשתני הסביבה.
X_ACCOUNTS = {
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
    "IsraelPM": "🏛️ רה״מ",
    "DovrutHaknesset": "🏛️ הכנסת",
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
    "N12": "https://rcs.mako.co.il/rss/31750a2610f26110",
    "Mako N12": "https://rcs.mako.co.il/rss/31750a2610f26110",
    "Kan News": "https://www.kan.org.il/rss/",
    "Haaretz": "https://www.haaretz.co.il/c/1.4841152?l=he-rss",
    "Channel 13": "https://13tv.co.il/feed/",
    "Now 14": "https://www.now14.co.il/feed/",
    "Channel 14": "https://www.now14.co.il/feed/",
}
