import os
import json
import yt_dlp
import subprocess
import csv
import re
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
MAX_RETRIES = 3

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

def set_csv_status(track_id, new_status):
    """Set the status column in CSV for a track."""
    try:
        if not os.path.exists(INDEX_FILE):
            return
        rows = []
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if row.get("id") == track_id:
                    row["status"] = new_status
                rows.append(row)
        if rows:
            with open(INDEX_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
    except Exception as e:
        print(f"Warning: Failed to update CSV status: {e}")

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

# ========================
# DUPLICATE DETECTION
# ========================
def is_track_already_downloaded(track_id, meta):
    """Check if track is already downloaded in any form
    
    Checks:
    1. File exists by track ID: data/{track_id}.mp3
    2. File exists by renamed name: data/Artist - Title.mp3
    3. CSV index shows download status
    
    Args:
        track_id: Spotify track ID
        meta: Metadata dict with title, artist info
    
    Returns:
        True if track already exists, False otherwise
    """
    # Check 1: File exists by ID
    id_file = os.path.join(OUTPUT_DIR, f"{track_id}.mp3")
    if os.path.exists(id_file):
        return True
    
    # Check 2: File exists by renamed name (Artist - Title.mp3)
    if "artist" in meta and "title" in meta:
        artist = meta["artist"][0] if isinstance(meta["artist"], list) else meta["artist"]
        # Sanitize filename same way tagger.py does
        invalid_chars = r'[<>:"/\\|?*]'
        filename = f"{artist} - {meta['title']}"
        filename = re.sub(invalid_chars, '', filename)
        filename = filename.strip('. ')
        filename = re.sub(r'\s+', ' ', filename)
        renamed_file = os.path.join(OUTPUT_DIR, f"{filename}.mp3")
        
        if os.path.exists(renamed_file):
            return True
    
    # Check 3: Verify CSV index doesn't already have this track as downloaded
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("id") == track_id and row.get("status") in ("downloaded", "tagged"):
                    return True
    
    return False

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

    # Skip if already downloaded in any form
    if is_track_already_downloaded(meta['id'], meta):
        return

    url = find_best_match(meta["search_query"], meta["duration_ms"])
    if not url:
        raise Exception(f"No YouTube match for: {meta['search_query']}")

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

def update_csv_status(track_id, new_status):
    """Update the status column in the CSV for a track"""
    try:
        rows = []
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("id") == track_id:
                    row["status"] = new_status
                rows.append(row)
        
        # Write back
        if rows:
            with open(INDEX_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
    except Exception as e:
        print(f"Warning: Failed to update CSV status: {e}")

# ========================
# MAIN
# ========================
def main():
    """Main entry point for audio downloader"""
    state = load_state()

    if "downloader" not in state:
        state["downloader"] = {
            "last_downloaded_id": None,
            "downloaded_count": 0,
            "failed_downloads": [],
            "permanent_failures": [],
            "retry_counts": {},
            "last_error": None,
            "timestamp": None
        }

    dl_state = state["downloader"]

    if "retry_counts" not in dl_state:
        dl_state["retry_counts"] = {}

    print("Loading CSV index...")
    rows = list(load_index())
    print(f"Total rows: {len(rows)}\n")

    if not rows:
        print("No rows to download!")
        return

    start_idx = 0
    if dl_state["last_downloaded_id"]:
        found = False
        for i, row in enumerate(rows):
            if row["id"] == dl_state["last_downloaded_id"]:
                start_idx = i + 1
                found = True
                break
        if found and start_idx >= len(rows):
            start_idx = 0
            dl_state["last_downloaded_id"] = None
            dl_state["downloaded_count"] = 0
        elif found:
            print(f"Resuming from: {dl_state['last_downloaded_id']}")

    for idx, item in enumerate(rows[start_idx:], start=start_idx):
        track_id = item["id"]
        track_status = item.get("status", "unknown")

        if track_status in ["downloaded", "tagged"]:
            print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)
            print(f"SKIP ({track_status})")
            continue

        if track_status == "download_failed":
            print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)
            print(f"SKIP (download_failed)")
            continue

        if track_id in dl_state.get("permanent_failures", []):
            retry_count = dl_state["retry_counts"].get(track_id, 0)
            if retry_count >= MAX_RETRIES:
                print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)
                print(f"SKIP (exhausted {MAX_RETRIES} retries)")
                continue

        print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)

        try:
            meta_path = item["meta_path"]
            track_folder = os.path.dirname(meta_path)

            with open(meta_path, encoding="utf-8") as mf:
                meta = json.load(mf)

            download_track(meta, track_folder)

            update_csv_status(track_id, "downloaded")

            dl_state["last_downloaded_id"] = track_id
            dl_state["downloaded_count"] += 1
            dl_state["timestamp"] = datetime.now().isoformat()

            if track_id in dl_state.get("permanent_failures", []):
                dl_state["permanent_failures"].remove(track_id)
            dl_state["retry_counts"].pop(track_id, None)

            print("OK")

        except Exception as e:
            print(f"ERROR: {e}")
            if track_id not in dl_state["failed_downloads"]:
                dl_state["failed_downloads"].append(track_id)
            dl_state["last_error"] = str(e)

            retry_count = dl_state["retry_counts"].get(track_id, 0) + 1
            dl_state["retry_counts"][track_id] = retry_count

            if retry_count >= MAX_RETRIES:
                if track_id not in dl_state.get("permanent_failures", []):
                    dl_state.setdefault("permanent_failures", []).append(track_id)
                set_csv_status(track_id, "download_failed")

        save_state(state)
        time.sleep(0.1)

    print(f"\nDownload complete!")
    print(f"Downloaded: {dl_state['downloaded_count']}")
    if dl_state['failed_downloads']:
        print(f"Failed this run: {len(dl_state['failed_downloads'])}")
    if dl_state.get('permanent_failures'):
        print(f"Permanent failures: {len(dl_state['permanent_failures'])}")

    dl_state["failed_downloads"] = []
    save_state(state)

if __name__ == "__main__":
    main()
