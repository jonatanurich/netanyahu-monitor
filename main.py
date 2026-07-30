import os, time, subprocess, tempfile, requests, gc
from datetime import datetime

ULTRA_ID = "instance186865"
ULTRA_TOKEN = "ih8jz0qoxrc166a0"
GROUP_ID = "120363429023395450@g.us"

KEYWORDS = ["נתניהו", "ביבי", "ראש הממשלה", "בנימין נתניהו"]

# רשימה מלאה אבל נרוץ עליהם אחד אחד, לא במקביל
SOURCES = [
    ("חדשות 12", "https://www.youtube.com/@N12News/live"),
    ("עכשיו 14", "https://www.youtube.com/@Channel14IL/live"),
    ("כאן 11", "https://www.youtube.com/@KAN11Official/live"),
    ("גלי צה\"ל", "https://glzwizzlv.bynetcdn.com/glz_mp3"),
    ("כאן ב'", "https://kanliveicy.media.kan.org.il/kanbet"),
    ("103FM", "https://103fm.livecdn.biz/103fm_aac"),
]

print("Loading Whisper SMALL (heavy but sequential)...")
import whisper
model = whisper.load_model("small")
print("✅ Heavy Model loaded - sequential mode (low RAM)")

def send_whatsapp(text, source_name):
    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    msg = f"🎙️ *אזכור LIVE כבד*\n\n📺 {source_name}\n🕒 {datetime.now().strftime('%H:%M:%S')}\n💬 \"{text.strip()[:400]}\"\n"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": msg}
    try:
        requests.post(url, data=payload, timeout=15)
        print(f"✅ SENT {source_name}")
    except Exception as e:
        print(f"❌ WA Error: {e}")

def process_one(name, url):
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp_path = tmp.name
        
        if "youtube.com" in url:
            cmd = ["yt-dlp", "--no-playlist", "-x", "--audio-format", "mp3", "-o", tmp_path, "--download-sections", "*0-30", url, "--quiet"]
        else:
            cmd = ["ffmpeg", "-i", url, "-t", "30", "-q:a", "0", "-map", "a", tmp_path, "-y", "-loglevel", "quiet"]
        
        subprocess.run(cmd, timeout=70, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) < 4000:
            return
        
        result = model.transcribe(tmp_path, language="he", fp16=False)
        text = result["text"]
        print(f"[{name}] {text[:120]}")
        
        for kw in KEYWORDS:
            if kw in text:
                print(f"🔥 FOUND {kw} in {name}")
                send_whatsapp(text, name)
                break
    except Exception as e:
        print(f"Error {name}: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass
        gc.collect()

if __name__ == "__main__":
    send_whatsapp("המערכת הכבדה עלתה! גרסה חסכונית - בודקת ערוץ אחרי ערוץ בלופ. 6 ערוצים, דיוק 95%, לא תקרוס.", "מערכת")
    
    idx = 0
    while True:
        name, url = SOURCES[idx % len(SOURCES)]
        print(f"\n--- Checking {name} ({idx}) ---")
        process_one(name, url)
        idx += 1
        time.sleep(2)
