import os
import json
import yt_dlp
import subprocess
import csv
import time
from datetime import datetime
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB, TDRC

INDEX_FILE = "songs_index.csv"
OUTPUT_DIR = "data"
STATE_FILE = "state.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================
# STATE HELPERS
# ========================
def load_state():
    """Load state from JSON file"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_state(state):
    """Save state to JSON file"""
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

# ------------------------
# AUDIO VALIDATION
# ------------------------
def get_audio_duration(file):
    """Get duration of audio file in seconds using ffprobe"""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file
    ]
    return float(subprocess.check_output(cmd).decode().strip())

# ------------------------
# TAGGING
# ------------------------
def tag_audio(file_path, meta, cover_path=None):
    """Apply ID3 tags to audio file
    
    Args:
        file_path: Path to MP3 file
        meta: Metadata dictionary
        cover_path: Path to cover image (optional)
    """
    audio = MP3(file_path, ID3=ID3)
    try:
        audio.add_tags()
    except:
        pass

    audio.tags.add(TIT2(encoding=3, text=meta["title"]))
    audio.tags.add(TPE1(encoding=3, text=meta["artist"][0]))
    audio.tags.add(TALB(encoding=3, text=meta.get("album", "")))
    audio.tags.add(TDRC(encoding=3, text=meta.get("year", "")))

    # Embed cover art if available
    if cover_path and os.path.exists(cover_path):
        with open(cover_path, "rb") as img:
            audio.tags.add(
                APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,
                    desc='Cover',
                    data=img.read()
                )
            )
    audio.save()

# ------------------------
# SEARCH
# ------------------------
def find_best_match(query, expected_duration):
    """Find best YouTube match for given search query and expected duration"""
    ydl = yt_dlp.YoutubeDL({"quiet": True})
    results = ydl.extract_info(f"ytsearch5:{query}", download=False)["entries"]

    best = None
    best_score = -1

    for r in results:
        title = (r.get("title") or "").lower()
        duration = r.get("duration") or 0

        score = 0

        if "official" in title:
            score += 2
        if "audio" in title or "video" in title:
            score += 1
        if "live" not in title and "remix" not in title:
            score += 2

        if duration and abs(duration - expected_duration/1000) < 10:
            score += 3

        if score > best_score:
            best = r
            best_score = score

    return best["webpage_url"] if best else None

# ------------------------
# DOWNLOAD
# ------------------------
def download_track(meta, track_folder):
    """Download and tag audio track
    
    Args:
        meta: Metadata dict loaded from meta.json
        track_folder: Folder path containing meta.json and cover.jpg
    """
    output_file = os.path.join(OUTPUT_DIR, f"{meta['id']}.mp3")
    cover_path = os.path.join(track_folder, "cover.jpg")

    if os.path.exists(output_file):
        return

    url = find_best_match(meta["search_query"], meta["duration_ms"])
    if not url:
        print("No match:", meta["search_query"])
        return

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": output_file,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "0",
        }],
        "quiet": True
    }

    yt_dlp.YoutubeDL(ydl_opts).download([url])

    # Validate downloaded file
    if not os.path.exists(output_file):
        return

    size = os.path.getsize(output_file)
    if size < 1_000_000:
        print("File too small, deleting")
        os.remove(output_file)
        return

    try:
        duration = get_audio_duration(output_file)
    except:
        print("Failed to read audio duration, deleting")
        os.remove(output_file)
        return

    if abs(duration - meta["duration_ms"]/1000) > 10:
        print("Duration mismatch, deleting")
        os.remove(output_file)
        return

    # Apply ID3 tags after validation
    tag_audio(output_file, meta, cover_path=cover_path)

# ------------------------
# INPUT LOADING
# ------------------------
def load_index():
    """Load track index from CSV or JSONL file"""
    if INDEX_FILE.endswith(".csv"):
        with open(INDEX_FILE, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

    elif INDEX_FILE.endswith(".jsonl"):
        with open(INDEX_FILE, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    else:
        raise Exception("Unsupported index format")

# ========================
# MAIN
# ========================
def main():
    """Main entry point for audio downloader"""
    state = load_state()
    
    # Initialize downloader state
    if "downloader" not in state:
        state["downloader"] = {
            "last_downloaded_id": None,
            "downloaded_count": 0,
            "failed_downloads": [],
            "last_error": None,
            "timestamp": None
        }
    
    dl_state = state["downloader"]
    
    print("Loading CSV index...")
    rows = list(load_index())
    print(f"Total rows: {len(rows)}\n")
    
    if not rows:
        print("No rows to download!")
        return
    
    # Resume from last processed track
    start_idx = 0
    if dl_state["last_downloaded_id"]:
        for i, row in enumerate(rows):
            if row["id"] == dl_state["last_downloaded_id"]:
                start_idx = i + 1
                break
    
    # Process each track
    for idx, item in enumerate(rows[start_idx:], start=start_idx):
        track_id = item["id"]
        print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)
        
        try:
            meta_path = item["meta_path"]
            track_folder = os.path.dirname(meta_path)
            
            with open(meta_path, encoding="utf-8") as mf:
                meta = json.load(mf)

            download_track(meta, track_folder)
            
            # Update state on success
            dl_state["last_downloaded_id"] = track_id
            dl_state["downloaded_count"] += 1
            dl_state["timestamp"] = datetime.now().isoformat()
            
            print("OK")
            
        except Exception as e:
            print(f"ERROR: {e}")
            dl_state["failed_downloads"].append(track_id)
            dl_state["last_error"] = str(e)
        
        # Save state after each track
        save_state(state)
        time.sleep(0.1)
    
    print(f"\nDownload complete!")
    print(f"Downloaded: {dl_state['downloaded_count']}")
    if dl_state['failed_downloads']:
        print(f"Failed: {len(dl_state['failed_downloads'])}")
    
    save_state(state)

if __name__ == "__main__":
    main()
