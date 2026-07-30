import os, time, subprocess, tempfile, requests
from datetime import datetime

# ========= CONFIG FINAL - READY FOR RAILWAY =========
ULTRA_ID = "instance186865"
ULTRA_TOKEN = "ih8jz0qoxrc166a0"
GROUP_ID = "120363429023395450@g.us"

KEYWORDS = ["נתניהו", "ביבי", "ראש הממשלה", "בנימין נתניהו"]

SOURCES = {
    "חדשות 12": "https://www.youtube.com/@N12News/live",
    "עכשיו 14": "https://www.youtube.com/@Channel14IL/live",
    "כאן 11": "https://www.youtube.com/@KAN11Official/live",
    "גלי צה\"ל": "https://glzwizzlv.bynetcdn.com/glz_mp3",
    "כאן ב'": "https://kanliveicy.media.kan.org.il/kanbet",
    "103FM": "https://103fm.livecdn.biz/103fm_aac"
}

print("Loading Whisper model...")
import whisper
model = whisper.load_model("small")
print("✅ Model loaded! Starting monitors...")

def send_whatsapp(text, source_name):
    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    msg = f"🎙️ *אזכור LIVE - נתניהו*\n\n📺 *מקור:* {source_name}\n🕒 *שעה:* {datetime.now().strftime('%d/%m %H:%M:%S')}\n💬 *ציטוט:* \"{text.strip()[:350]}\"\n\n#נתניהו_LIVE"
    payload = {"token": ULTRA_TOKEN, "to": GROUP_ID, "body": msg}
    try:
        r = requests.post(url, data=payload, timeout=15)
        print(f"✅ SENT to {GROUP_ID}: {source_name} -> {r.status_code}")
    except Exception as e:
        print(f"❌ WhatsApp Error: {e}")

def transcribe_chunk(audio_path):
    try:
        result = model.transcribe(audio_path, language="he", fp16=False)
        return result["text"]
    except Exception as e:
        print(f"Transcribe error: {e}")
        return ""

def monitor_source(name, stream_url):
    print(f"▶️ Monitoring {name}")
    while True:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
                tmp_path = tmp.name
            
            if "youtube.com" in stream_url:
                cmd = ["yt-dlp", "--no-playlist", "-x", "--audio-format", "mp3", "-o", tmp_path, "--download-sections", "*0-30", stream_url]
            else:
                cmd = ["ffmpeg", "-i", stream_url, "-t", "30", "-q:a", "0", "-map", "a", tmp_path, "-y", "-loglevel", "quiet"]
            
            subprocess.run(cmd, timeout=50, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) < 4000:
                time.sleep(5)
                continue

            text = transcribe_chunk(tmp_path)
            if len(text.strip()) > 5:
                print(f"[{name}] {text[:120]}")
            
            for kw in KEYWORDS:
                if kw in text or kw.lower() in text.lower():
                    print(f"🔥 FOUND {kw} in {name}: {text}")
                    send_whatsapp(text, name)
                    break
            
        except Exception as e:
            print(f"Error {name}: {e}")
            time.sleep(10)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except: pass
            time.sleep(2)

if __name__ == "__main__":
    import threading
    for name, url in SOURCES.items():
        t = threading.Thread(target=monitor_source, args=(name, url), daemon=True)
        t.start()
        time.sleep(0.5)
    
    # הודעת בדיקה שהמערכת עלתה
    send_whatsapp("המערכת עלתה בהצלחה ומתחילה לנטר את כל ערוצי הטלוויזיה והרדיו. כל אזכור של נתניהו יגיע לכאן בזמן אמת.", "מערכת ניטור")
    
    while True:
        time.sleep(3600)
