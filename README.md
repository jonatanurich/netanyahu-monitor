# מערכת ניטור תקשורת — נתניהו והליכוד

מערכת Python פשוטה שרצה אונליין ומנטרת מקורות טקסטואליים:

- ערוצי טלגרם ציבוריים
- פידי RSS של אתרי חדשות
- X דרך Nitter כאפשרות ניסיונית וכבויה כברירת מחדל
- התראות WhatsApp באמצעות UltraMsg
- סיכום כותרות שעתי וסיכום יומי
- שמירה ב-SQLite ומניעת דיווחים כפולים לאחר restart

## חשוב לפני הכול

ה-token שהיה בקוד הישן נחשף. יש לבטל אותו במערכת UltraMsg ולהפיק token חדש. אין להעלות token, סיסמה או מזהה קבוצה לקובץ ב-GitHub.

## הקבצים שמעלים ל-GitHub

העלה את כל תוכן התיקייה הזאת, כולל הקבצים הנסתרים:

- `main.py`
- `sources.py`
- `requirements.txt`
- `.env.example`
- `.gitignore`
- `render.yaml`
- `Dockerfile`
- `tests/`
- `.github/`

אין להעלות קובץ בשם `.env`.

## העלאה פשוטה ל-GitHub דרך הדפדפן

1. צור Repository חדש.
2. בחר **Add file → Upload files**.
3. גרור את כל הקבצים מתוך התיקייה שחילצת.
4. לחץ **Commit changes**.

## העלאה ל-Render אחרי GitHub

הפרויקט כולל `render.yaml`, ולכן ניתן ליצור Blueprint ישירות מה-Repository.

1. היכנס ל-Render.
2. בחר **New → Blueprint**.
3. חבר את ה-Repository ב-GitHub.
4. Render יזהה את `render.yaml`.
5. הזן את המשתנים הסודיים כשהמערכת מבקשת:
   - `ULTRA_ID`
   - `ULTRA_TOKEN`
   - `GROUP_ID`
6. בהתחלה השאר `WHATSAPP_DRY_RUN=true`.
7. אחרי שהכתובת `/health` תקינה, שלח בדיקה ושנה ל-`false`.

כתובת בדיקת המצב תהיה:

```text
https://YOUR-SERVICE.onrender.com/health
```

## משתני סביבה

| משתנה | משמעות |
|---|---|
| `ULTRA_ID` | מזהה instance ב-UltraMsg |
| `ULTRA_TOKEN` | token חדש וסודי |
| `GROUP_ID` | מזהה קבוצת WhatsApp |
| `WHATSAPP_DRY_RUN` | `true` לבדיקה, `false` לשליחה אמיתית |
| `SCAN_INTERVAL_SECONDS` | תדירות סריקה; ברירת מחדל 90 שניות |
| `SEND_EXISTING_ON_START` | האם לשלוח פריטים ישנים בהפעלה הראשונה; מומלץ `false` |
| `HOURLY_SUMMARY_ENABLED` | סיכום כותרות שעתי |
| `DAILY_SUMMARY_ENABLED` | סיכום יומי בשעה 18:00 |
| `X_ENABLED` | הפעלת X דרך Nitter; לא מומלץ כמקור מרכזי |
| `DATA_DIR` | מקום שמירת SQLite; ב-Render הוא `/var/data` |

## עריכת מקורות

המקורות נמצאים בקובץ `sources.py`. אפשר להוסיף או למחוק ערוץ טלגרם, חשבון X או פיד RSS.

## התנהגות בהפעלה הראשונה

ברירת המחדל היא לא לשלוח את ההודעות שכבר נמצאות במקורות בזמן ההקמה. המערכת מסמנת אותן כפריטים שנראו, ומתחילה להתריע רק על פריטים חדשים.

## הפעלה מקומית אופציונלית

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

פתח:

```text
http://localhost:10000/health
```

## בדיקות

```bash
pip install pytest
pytest -q
```

## מגבלות

- scraping של טלגרם הציבורי עלול להשתנות אם טלגרם תשנה את מבנה העמוד.
- Nitter אינו שירות רשמי של X ועלול להיות לא זמין.
- יש לוודא שהשימוש במקורות וב-WhatsApp עומד בתנאי השירות ובהרשאות הרלוונטיות.
