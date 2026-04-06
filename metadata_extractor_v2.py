import os
import json
import csv
import requests
import time
from datetime import datetime
from pathlib import Path
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

INPUT_FILE = "song_sources.txt"
INDEX_FILE = "songs_index.csv"
BASE_DIR = "metadata"
STATE_FILE = "state.json"
CONFIG_FILE = "config.json"
BATCH_SIZE = 50

# ========================
# HELPERS
# ========================
def extract_id(url):
    """Extract Spotify track ID, removing ?si= params"""
    try:
        track_id = url.split("track/")[1].split("?")[0]
        return track_id
    except:
        return None

def safe(s):
    """Sanitize folder names"""
    return "".join(c for c in s if c not in r'\/:*?"<>|').strip()

def load_state():
    """Load state from JSON"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_state(state):
    """Save state to JSON file"""
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

def load_config():
    """Load configuration from config.json"""
    defaults = {"metadata": {"sources": ["spotify", "musicbrainz"]}}
    
    if not Path(CONFIG_FILE).exists():
        return defaults
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        if "metadata" not in config:
            config["metadata"] = defaults["metadata"]
        return config
    except Exception as e:
        print(f"Warning: Failed to load config.json: {e}")
        return defaults

def load_existing_ids():
    """Load already-processed track IDs"""
    ids = set()
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ids.add(row["id"])
    return ids

def append_to_csv(row):
    """Append row to CSV index"""
    file_exists = os.path.exists(INDEX_FILE)
    with open(INDEX_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "artist", "album", "meta_path", "source", "status"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ========================
# SPOTIFY API (batch + fallback)
# ========================
def get_tracks_batch(sp, ids, retry_delay=1):
    """Batch request from Spotify with fallback to single"""
    try:
        result = sp.tracks(ids)
        if result.get("tracks"):
            return result["tracks"]
    except SpotifyException as e:
        if e.http_status == 429:
            print(f"    (429 rate limit, waiting {retry_delay}s then retrying individually)")
            time.sleep(retry_delay)
        elif e.http_status == 403:
            print(f"    (403 forbidden - Spotify auth issue, trying individual requests)")
    except Exception as e:
        print(f"    (Batch request failed: {type(e).__name__}, trying individual requests)")
    
    # Fallback: individual requests with retry logic
    results = []
    for tid in ids:
        result = None
        for attempt in range(3):  # Up to 3 attempts per track
            try:
                result = sp.track(tid)
                if result:
                    break
            except SpotifyException as e:
                if e.http_status == 429:
                    backoff = min(2 ** attempt * 2, 10)  # Exponential backoff: 2, 4, 8, 10 seconds
                    print(f"      (429 on {tid}, waiting {backoff}s...)", end="", flush=True)
                    time.sleep(backoff)
                elif e.http_status == 403:
                    print(f"      (403 on {tid}, auth issue)")
                    break
                else:
                    print(f"      ({e.http_status} on {tid})")
                    break
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
        
        results.append(result)
        time.sleep(0.3)  # Small delay between individual requests
    
    return results if any(results) else None

# ========================
# MUSICBRAINZ LOOKUP
# ========================
def get_metadata_musicbrainz(artist, title):
    """
    Query MusicBrainz for enhanced metadata.
    """
    try:
        search_query = f'"{title}" AND artist:"{artist}"'
        url = "https://musicbrainz.org/ws/2/recording"
        params = {
            "query": search_query,
            "fmt": "json",
            "limit": 1
        }
        
        headers = {"User-Agent": "spotify-sync/1.0"}
        response = requests.get(url, params=params, headers=headers, timeout=5)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        if not data.get("recordings"):
            return None
        
        recording = data["recordings"][0]
        
        # Extract cover art from Cover Art Archive
        cover_url = None
        if recording.get("releases") and recording["releases"]:
            release = recording["releases"][0]
            if release.get("id"):
                cover_url = f"https://coverartarchive.org/release/{release['id']}/front"
        
        # Extract album info
        album = ""
        year = ""
        if recording.get("releases"):
            release = recording["releases"][0]
            album = release.get("title", "")
            date = release.get("date", "")
            if date:
                year = date[:4]
        
        return {
            "album": album,
            "year": year,
            "duration_ms": int(recording.get("length", 0)) if recording.get("length") else 0,
            "cover_url": cover_url
        }
    
    except Exception as e:
        pass
    
    return None

# ========================
# DOWNLOAD COVER
# ========================
def download_cover(url, path):
    """Download album cover"""
    if not url:
        return False
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
            return True
    except Exception as e:
        pass
    return False

# ========================
# MAIN EXTRACTOR
# ========================
def main():
    config = load_config()
    metadata_sources = config["metadata"]["sources"]
    
    use_musicbrainz = "musicbrainz" in metadata_sources
    sources_str = " + ".join(s.title() for s in metadata_sources)
    
    print(f"Starting metadata extraction ({sources_str})...\n")
    
    os.makedirs(BASE_DIR, exist_ok=True)
    
    # Initialize Spotify
    try:
        sp = Spotify(auth_manager=SpotifyClientCredentials())
        print("OK Spotify credentials loaded\n")
    except Exception as e:
        print(f"ERROR Spotify auth failed: {e}")
        print("  Set SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables")
        return
    
    # Load state
    state = load_state()
    if "metadata_extractor" not in state:
        state["metadata_extractor"] = {
            "last_processed_id": None,
            "total_processed": 0,
            "successful": 0,
            "pending": 0,
            "failed_ids": [],
            "last_error": None,
            "timestamp": None,
            "last_operation": {
                "id": None,
                "metadata_extraction_complete": False,
                "cover_downloaded": False
            }
        }
    
    extractor_state = state["metadata_extractor"]
    
    # Read all links
    with open(INPUT_FILE) as f:
        links = list(set(l.strip() for l in f if l.strip()))
    
    print(f"Total tracks to process: {len(links)}")
    print(f"Already processed: {len(load_existing_ids())}\n")
    
    existing_ids = load_existing_ids()
    
    # Extract IDs
    id_to_link = {}
    for link in links:
        try:
            tid = extract_id(link)
            if tid and tid not in existing_ids:
                id_to_link[tid] = link
        except:
            continue
    
    all_ids = list(id_to_link.keys())
    print(f"Pending extraction: {len(all_ids)}\n")
    
    if not all_ids:
        print("All tracks already processed!")
        return
    
    # Resume from last_processed_id
    start_idx = 0
    if extractor_state["last_processed_id"]:
        try:
            start_idx = all_ids.index(extractor_state["last_processed_id"]) + 1
            print(f"Resuming from: {extractor_state['last_processed_id']}\n")
        except ValueError:
            pass
    
    # Chunk into batches for Spotify API
    from itertools import islice
    def chunk(lst, size):
        it = iter(lst)
        while True:
            c = list(islice(it, size))
            if not c:
                break
            yield c
    
    processed_count = start_idx
    for batch in chunk(all_ids[start_idx:], BATCH_SIZE):
        print(f"Fetching batch ({len(batch)} tracks from Spotify API)...")
        
        # Get from Spotify (with automatic fallback to individual requests on failure)
        spotify_tracks = get_tracks_batch(sp, batch)
        
        if not spotify_tracks:
            print("  ERROR All tracks in batch failed to fetch\n")
            spotify_tracks = [None] * len(batch)  # Treat as all None for consistent processing
        
        # Process each track from this batch
        for idx, spotify_track in enumerate(spotify_tracks):
            track_id = batch[idx]
            processed_count += 1
            print(f"[{processed_count}/{len(all_ids)}] {track_id}...", end=" ", flush=True)
            
            try:
                if not spotify_track:
                    print("ERROR No data")
                    extractor_state["failed_ids"].append(track_id)
                    extractor_state["last_processed_id"] = track_id
                    extractor_state["total_processed"] += 1
                    save_state(state)
                    continue
                
                # Extract from Spotify
                meta = {
                    "id": track_id,
                    "title": spotify_track.get("name", "Unknown"),
                    "artist": [a["name"] for a in spotify_track.get("artists", [])],
                    "album": spotify_track.get("album", {}).get("name", ""),
                    "year": spotify_track.get("album", {}).get("release_date", "")[:4],
                    "duration_ms": spotify_track.get("duration_ms", 0),
                    "spotify_url": id_to_link[track_id],
                    "cover_url": "",
                    "source": "spotify_api",
                    "search_query": f"{[a['name'] for a in spotify_track.get('artists', [])][0] if spotify_track.get('artists') else 'Unknown'} - {spotify_track.get('name', 'Unknown')}"
                }
                
                # Get cover from Spotify
                if spotify_track.get("album", {}).get("images"):
                    meta["cover_url"] = spotify_track["album"]["images"][0]["url"]
                
                # Enrich with MusicBrainz if configured
                if use_musicbrainz and meta["artist"] and meta["title"]:
                    mb_data = get_metadata_musicbrainz(meta["artist"][0], meta["title"])
                    if mb_data:
                        if mb_data.get("album") and not meta["album"]:
                            meta["album"] = mb_data["album"]
                        if mb_data.get("year") and not meta["year"]:
                            meta["year"] = mb_data["year"]
                        if mb_data.get("cover_url") and not meta["cover_url"]:
                            meta["cover_url"] = mb_data["cover_url"]
                
                # Create folders
                artist_folder = os.path.join(BASE_DIR, safe(meta["artist"][0] if meta["artist"] else "Unknown"))
                track_folder = os.path.join(artist_folder, track_id)
                os.makedirs(track_folder, exist_ok=True)
                
                json_path = os.path.join(track_folder, "meta.json")
                cover_path = os.path.join(track_folder, "cover.jpg")
                
                # Save metadata
                with open(json_path, "w", encoding='utf-8') as f:
                    json.dump(meta, f, indent=2)
                
                # Download cover
                cover_ok = download_cover(meta["cover_url"], cover_path) if meta["cover_url"] else False
                
                # Add to CSV
                append_to_csv({
                    "id": meta["id"],
                    "title": meta["title"],
                    "artist": meta["artist"][0] if meta["artist"] else "Unknown",
                    "album": meta.get("album", ""),
                    "meta_path": json_path,
                    "source": "spotify_api",
                    "status": "success"
                })
                
                # Update state
                extractor_state["successful"] += 1
                print(f"OK {meta['title']}")
                
            except Exception as e:
                print(f"ERROR {e}")
                extractor_state["failed_ids"].append(track_id)
                extractor_state["last_error"] = str(e)
            
            # Always update progress
            extractor_state["last_processed_id"] = track_id
            extractor_state["total_processed"] += 1
            extractor_state["timestamp"] = datetime.now().isoformat()
            save_state(state)
            time.sleep(0.2)
        
        # Small delay between batches
        time.sleep(0.5)
    
    print(f"\nMetadata extraction complete!")
    print(f"Processed: {extractor_state['total_processed']}")
    print(f"Successful: {extractor_state['successful']}")
    print(f"Pending: {extractor_state['pending']}")
    if extractor_state['failed_ids']:
        print(f"Failed: {len(extractor_state['failed_ids'])}")
    
    save_state(state)

if __name__ == "__main__":
    main()
