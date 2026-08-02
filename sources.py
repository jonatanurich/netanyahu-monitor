"""מקורות ברירת מחדל למערכת הניטור.

הערה: כתובות RSS משתנות מדי פעם. המערכת מריצה בדיקת תקינות בעלייה
(validate_rss_feeds) ומדפיסה ללוג אילו פידים לא ענו, כדי שלא ייווצר מצב
של מקור "מת" בשקט.
"""

# ---------------------------------------------------------------------------
# ערוצי טלגרם ציבוריים (נקראים דרך t.me/s/<channel>)
# ---------------------------------------------------------------------------

# ערוצים ראשוניים — הנושא עצמו מפרסם בהם.
# כל פוסט מהם רלוונטי בהגדרה, גם בלי שם המטרה בטקסט, ונשלח תמיד
# גם אם התקשורת כבר דיווחה על התוכן.
PRIMARY_TELEGRAM = {
    "bnetanyahu": "netanyahu",
}

# ערוצי פרשנים — הניסוח שלהם הוא הידיעה, לכן כל אזכור מהם נשלח
# בנפרד גם אם הסיפור כבר דווח במקום אחר (עוקף דדופ, אבל עדיין
# חייב לעבור את פילטר הרלוונטיות).
ALWAYS_ALERT_TELEGRAM = {"amitsegal", "lieldaphna"}

TELEGRAM_CHANNELS = {
    # ערוץ רשמי של הנושא
    "bnetanyahu": "📢 נתניהו — ערוץ רשמי",

    # פרשנים וכתבים פוליטיים
    "amitsegal": "🎙️ עמית סגל",
    "lieldaphna": "🎙️ דפנה ליאל",
    "MichaelShemesh": "🎙️ מיכאל שמש",
    "yaronyanir1299": "🎙️ ירון אברהם ויניר קוזין",
    "Political_arena": "🎙️ זירה פוליטית",

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
    # רדיו (ערוצי טקסט — לא תמלול שידור)
    "GLZRadio": "📻 גל״צ טקסט",
    "galeyisrael": "📻 גלי ישראל",
    # סקרים
    "PollsIsrael": "📊 סקרים",
}

# ---------------------------------------------------------------------------
# פידי RSS
# ---------------------------------------------------------------------------

# פידים שאומתו כעובדים בסביבת הריצה של Render.
RSS_FEEDS = {
    "Ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
    "וואלה": "https://rss.walla.co.il/feed/22",
    "ערוץ 7": "https://www.inn.co.il/Rss.aspx",
    "גלובס": "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=1725",
    "סרוגים": "https://www.srugim.co.il/feed",
    "JPost": "https://www.jpost.com/rss/rssfeedsisraelnews.aspx",
}

# הוסרו לאחר כישלון עקבי מהשרת ב-Render:
#   ישראל היום, מעריב, Times of Israel — 403, חסימה ברמת הרשת/IP.
#   Google News (סיקור זר) — 503 עקבי מכתובות דאטה-סנטר.
#   כיכר, כלכליסט — 404. הארץ, מאקו — פיד ריק.
# רובם מכוסים בערוצי הטלגרם המקבילים שברשימה למעלה.

# הוסרו: כיכר השבת וכלכליסט (404 — הכתובות השתנו), הארץ ומאקו (מחזירים
# פיד ריק). ארבעת המקורות האלה מכוסים ממילא בערוצי הטלגרם שלהם.


# ---------------------------------------------------------------------------
# משקלי מקורות — לא כל מקור שווה. משמש לדירוג משמעותיות ולזיהוי
# מי באמת פרסם ראשון (אגרגטור שהעתיק מ-Ynet אינו פורץ ידיעה).
# ---------------------------------------------------------------------------

DEFAULT_SOURCE_WEIGHT = 0.6

SOURCE_WEIGHTS = {
    # מקור ראשוני
    "bnetanyahu": 1.0,
    # פרשנים בכירים
    "amitsegal": 1.0,
    "lieldaphna": 0.95,
    "MichaelShemesh": 0.95,
    "yaronyanir1299": 0.9,
    "Political_arena": 0.7,
    # חדשות מובילות
    "N12_news_Israel": 1.0,
    "kann_news": 1.0,
    "newsisrael13": 0.95,
    "Now14Israel": 0.9,
    "ynet": 1.0,
    "Haaretz": 0.95,
    "WallaNews": 0.85,
    "IsraelHayomNews": 0.85,
    "MaarivOnline": 0.8,
    "PollsIsrael": 0.9,
    "DoverTzahal": 0.9,
    "galeyisrael": 0.8,
    "GLZRadio": 0.8,
    "abualiexpress": 0.75,
    # מבזקים ואגרגטורים — מהירים אך לרוב מעתיקים
    "HamalNews": 0.6,
    "NewsFromTheField": 0.55,
    "RealTimeSecurity": 0.55,
    "almog_cohen_news": 0.55,
    "RoterNews": 0.5,
    "MivzakLive": 0.45,
    "BreakingNewsIL": 0.45,
    "PushNews": 0.45,
    # פידי RSS (המפתח הוא שם הפיד)
    "Ynet": 1.0,
    "וואלה": 0.85,
    "ישראל היום": 0.85,
    "מעריב": 0.8,
    "ערוץ 7": 0.7,
    "גלובס": 0.75,
    "סרוגים": 0.65,
    "JPost": 0.8,
    "Times of Israel": 0.8,
    "🌍 חו״ל — Netanyahu": 0.9,
    "🌍 חו״ל — Israel politics": 0.85,
    "🌍 חו״ל — Israeli PM": 0.85,
}
