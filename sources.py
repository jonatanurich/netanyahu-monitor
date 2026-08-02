"""מקורות ברירת מחדל למערכת הניטור.

הערה: כתובות RSS משתנות מדי פעם. המערכת מריצה בדיקת תקינות בעלייה
(validate_rss_feeds) ומדפיסה ללוג אילו פידים לא ענו, כדי שלא ייווצר מצב
של מקור "מת" בשקט.
"""

# ---------------------------------------------------------------------------
# ערוצי טלגרם ציבוריים (נקראים דרך t.me/s/<channel>)
# ---------------------------------------------------------------------------

TELEGRAM_CHANNELS = {
    # טלוויזיה
    "N12_news_Israel": "📺 חדשות 12",
    "kann_news": "📺 כאן חדשות",
    "newsisrael13": "📺 חדשות 13",
    "Now14Israel": "📺 עכשיו 14",
    # עיתונות
    "ynet": "📰 Ynet",
    "WallaNews": "📰 וואלה",
    "MaarivOnline": "📰 מעריב",
    "IsraelHayomNews": "📰 ישראל היום",
    "Haaretz": "📰 הארץ",
    # מבזקים ובטחון
    "abualiexpress": "⚡ אבו עלי",
    "HamalNews": "⚡ החמ״ל",
    "NewsFromTheField": "⚡ חדשות מהשטח",
    "RoterNews": "⚡ רוטר",
    "MivzakLive": "⚡ מבזקים",
    "RealTimeSecurity": "⚡ ביטחון שוטף",
    "DoverTzahal": "⚡ דובר צה״ל",
    "almog_cohen_news": "⚡ אלמוג כהן",
    "PushNews": "⚡ פוש ניוז",
    "BreakingNewsIL": "⚡ מבזקים IL",
    # רדיו (ערוץ טקסט — לא תמלול שידור)
    "GLZRadio": "📻 גל״צ טקסט",
    # סקרים
    "PollsIsrael": "📊 סקרים",
}

# ---------------------------------------------------------------------------
# פידי RSS
# ---------------------------------------------------------------------------

# פידים שאומתו כעובדים בסביבת הריצה.
RSS_FEEDS = {
    "Ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
    "וואלה": "https://rss.walla.co.il/feed/22",
    "ישראל היום": "https://www.israelhayom.co.il/rss.xml",
    "ערוץ 7": "https://www.inn.co.il/Rss.aspx",
    "גלובס": "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=1725",
    "סרוגים": "https://www.srugim.co.il/feed",
    "JPost": "https://www.jpost.com/rss/rssfeedsisraelnews.aspx",
    # חזרו 403 בגלל חסימת User-Agent — אמורים לעבוד עכשיו עם ה-UA החדש.
    # אם הם עדיין נופלים בלוג, פשוט מוחקים את השורה.
    "מעריב": "https://www.maariv.co.il/Rss/RssFeedsPolitiMedini",
    "Times of Israel": "https://www.timesofisrael.com/feed/",
}

# הוסרו: כיכר השבת וכלכליסט (404 — הכתובות השתנו), הארץ ומאקו (מחזירים
# פיד ריק). ארבעת המקורות האלה מכוסים ממילא בערוצי הטלגרם שלהם.
