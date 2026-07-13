import os
import json
import csv
import re
import time
from datetime import datetime
from pathlib import Path
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB, TDRC

INPUT_DIR = "data"
METADATA_DIR = "metadata"
INDEX_FILE = "songs_index.csv"
STATE_FILE = "state.json"

os.makedirs(INPUT_DIR, exist_ok=True)

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

# ========================
# FILENAME SANITIZATION
# ========================
def sanitize_filename(filename):
    """Remove or replace characters invalid in filenames"""
    # Remove problematic characters but keep common punctuation
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '', filename)
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip('. ')
    # Collapse multiple spaces
    sanitized = re.sub(r'\s+', ' ', sanitized)
    return sanitized

def set_csv_status(track_id, new_status):
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

# ========================
# ID3 TAGGING
# ========================
def apply_id3_tags(file_path, meta, cover_path=None):
    """Apply ID3 v2.4 tags to MP3 file
    
    Args:
        file_path: Path to MP3 file
        meta: Metadata dictionary from meta.json
        cover_path: Path to cover image (optional)
    """
    try:
        audio = MP3(file_path, ID3=ID3)
    except:
        audio = MP3(file_path)
    
    # Create tags if they don't exist
    try:
        audio.add_tags()
    except:
        pass
    
    # Apply metadata tags
    if "title" in meta and meta["title"]:
        audio.tags.add(TIT2(encoding=3, text=meta["title"]))
    
    if "artist" in meta and meta["artist"]:
        artist = meta["artist"][0] if isinstance(meta["artist"], list) else meta["artist"]
        audio.tags.add(TPE1(encoding=3, text=artist))
    
    if "album" in meta and meta["album"]:
        audio.tags.add(TALB(encoding=3, text=meta["album"]))
    
    if "year" in meta and meta["year"]:
        year_str = str(meta["year"])
        audio.tags.add(TDRC(encoding=3, text=year_str))
    
    # Embed cover art if available
    if cover_path and os.path.exists(cover_path):
        try:
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
        except Exception as e:
            print(f"Failed to embed cover: {e}")
    
    audio.save()

# ========================
# FILE RENAMING
# ========================
def rename_and_tag_track(track_id, old_filename, meta, track_folder):
    """Rename file to Artist - Title.mp3 format and apply tags
    
    Args:
        track_id: Spotify track ID
        old_filename: Current filename (e.g., track_id.mp3)
        meta: Metadata dictionary
        track_folder: Path to metadata folder for this track
    
    Returns:
        True if successful, False otherwise
    """
    old_path = os.path.join(INPUT_DIR, old_filename)
    
    # Check if file exists
    if not os.path.exists(old_path):
        print(f"File not found: {old_path}")
        return False
    
    # Build new filename: Artist - Title.mp3
    artist = meta.get("artist", ["Unknown"])[0] if isinstance(meta.get("artist"), list) else meta.get("artist", "Unknown")
    title = meta.get("title", "Unknown")
    
    new_basename = f"{sanitize_filename(artist)} - {sanitize_filename(title)}.mp3"
    new_path = os.path.join(INPUT_DIR, new_basename)
    
    # Handle duplicates by appending number
    counter = 1
    original_new_path = new_path
    while os.path.exists(new_path) and new_path != old_path:
        base = new_basename.replace(".mp3", "")
        new_path = os.path.join(INPUT_DIR, f"{base} ({counter}).mp3")
        counter += 1
    
    try:
        # Rename file
        if new_path != old_path:
            os.rename(old_path, new_path)
        
        # Apply ID3 tags
        cover_path = os.path.join(track_folder, "cover.jpg")
        apply_id3_tags(new_path, meta, cover_path=cover_path)
        
        return True
    
    except Exception as e:
        print(f"Failed to rename/tag: {e}")
        return False

# ========================
# INPUT LOADING
# ========================
def load_index():
    """Load track index from CSV"""
    if not os.path.exists(INDEX_FILE):
        print(f"Index file not found: {INDEX_FILE}")
        return []
    
    with open(INDEX_FILE, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

# ========================
# MAIN
# ========================
def main():
    state = load_state()
    
    # Initialize tagger state
    if "tagger" not in state:
        state["tagger"] = {
            "last_processed_id": None,
            "renamed_count": 0,
            "tagged_count": 0,
            "failed_ids": [],
            "last_error": None,
            "timestamp": None
        }
    
    tagger_state = state["tagger"]
    # Reset failed_ids for this run so we only show current failures
    tagger_state["failed_ids"] = []
    tagger_state["renamed_count"] = 0
    tagger_state["tagged_count"] = 0
    
    print("Loading track index...")
    rows = load_index()
    print(f"Total tracks: {len(rows)}\n")

    if not rows:
        print("No tracks to process!")
        return

    # Pre-scan data/ for all existing MP3 files
    existing_by_id = {}       # track_id -> filename
    existing_by_sanitized = {}  # sanitized basename -> filename
    all_mp3_files = []
    if os.path.isdir(INPUT_DIR):
        for f in os.listdir(INPUT_DIR):
            if not f.endswith(".mp3"):
                continue
            all_mp3_files.append(f)
            base = os.path.splitext(f)[0]
            existing_by_sanitized[base] = f
            # Check if filename starts with a track_id (22 alphanumeric chars)
            tid_candidate = f.split(".")[0]
            if len(tid_candidate) == 22 and tid_candidate.isalnum():
                existing_by_id[tid_candidate] = f
            else:
                # Also check for partial rename (track_id at start before separator)
                for part in f.split(" -"):
                    if len(part) == 22 and part.isalnum():
                        existing_by_id[part] = f
                        break

    print(f"Found {len(all_mp3_files)} MP3 files in data/\n")

    # Reconcile: reset stale statuses for files that no longer exist
    reconciled = 0
    for row in rows:
        if row.get("status") in ("downloaded", "tagged"):
            tid = row["id"]
            if tid in existing_by_id:
                continue
            artist = row.get("artist", "Unknown")
            title = row.get("title", "Unknown")
            base = f"{sanitize_filename(artist)} - {sanitize_filename(title)}"
            if base in existing_by_sanitized:
                continue
            row["status"] = "spotify_metadata_fetched"
            reconciled += 1

    if reconciled:
        fieldnames = rows[0].keys()
        with open(INDEX_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Reset {reconciled} stale statuses to 'spotify_metadata_fetched' (files missing)\n")

    # Resume from last processed track
    start_idx = 0
    if tagger_state["last_processed_id"]:
        for i, row in enumerate(rows):
            if row["id"] == tagger_state["last_processed_id"]:
                start_idx = i + 1
                break

    # Process each track
    for idx, row in enumerate(rows[start_idx:], start=start_idx):
        track_id = row["id"]
        track_status = row.get("status", "unknown")
        print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)

        # Skip already-tagged or not-yet-downloaded tracks
        if track_status == "tagged":
            print(f"SKIP (tagged)")
            tagger_state["last_processed_id"] = track_id
            save_state(state)
            continue
        if track_status == "spotify_metadata_fetched":
            print(f"SKIP (not downloaded)")
            tagger_state["last_processed_id"] = track_id
            save_state(state)
            continue

        try:
            # Load metadata
            meta_path = row["meta_path"]
            meta = None
            if os.path.exists(meta_path):
                with open(meta_path, encoding="utf-8") as mf:
                    meta = json.load(mf)

            # Find the MP3 file using pre-scanned lookup
            mp3_file = existing_by_id.get(track_id)

            if not mp3_file:
                if meta:
                    artist = meta.get("artist", ["Unknown"])[0] if isinstance(meta.get("artist"), list) else meta.get("artist", "Unknown")
                    title = meta.get("title", "Unknown")
                else:
                    artist = row.get("artist", "Unknown")
                    title = row.get("title", "Unknown")
                base = f"{sanitize_filename(artist)} - {sanitize_filename(title)}"
                mp3_file = existing_by_sanitized.get(base)
                # Check for counter-suffixed variant if exact match not found
                if not mp3_file:
                    for b, f in existing_by_sanitized.items():
                        if b == base or b.startswith(base + " ("):
                            mp3_file = f
                            break

            if not mp3_file:
                print("MP3 not found")
                tagger_state["failed_ids"].append(track_id)
                tagger_state["last_processed_id"] = track_id
                save_state(state)
                continue

            if not meta:
                print(f"Metadata not found: {meta_path}")
                tagger_state["failed_ids"].append(track_id)
                tagger_state["last_processed_id"] = track_id
                save_state(state)
                continue

            track_folder = os.path.dirname(meta_path)
            
            # Rename and tag
            success = rename_and_tag_track(track_id, mp3_file, meta, track_folder)
            
            if success:
                tagger_state["renamed_count"] += 1
                tagger_state["tagged_count"] += 1
                set_csv_status(track_id, "tagged")
                print("OK")
            else:
                tagger_state["failed_ids"].append(track_id)
                print("ERROR")
            
            tagger_state["last_processed_id"] = track_id
            tagger_state["timestamp"] = datetime.now().isoformat()
            
        except Exception as e:
            print(f"✗ {e}")
            tagger_state["failed_ids"].append(track_id)
            tagger_state["last_error"] = str(e)
            tagger_state["last_processed_id"] = track_id
        
        # Save state after each track
        save_state(state)
        time.sleep(0.1)
    
    print(f"\nTagging complete!")
    print(f"Renamed: {tagger_state['renamed_count']}")
    print(f"Tagged: {tagger_state['tagged_count']}")
    if tagger_state['failed_ids']:
        print(f"Failed: {len(tagger_state['failed_ids'])}")
    
    save_state(state)

if __name__ == "__main__":
    main()
