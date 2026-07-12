import os
import json
import csv
import requests
import time
from datetime import datetime
from pathlib import Path
from itertools import islice

from providers import extract_track_id
from providers.spotify_scraper import SpotifyScraperProvider
from providers.spotify_api import SpotifyAPIProvider

INPUT_FILE = "song_sources.txt"
INDEX_FILE = "songs_index.csv"
BASE_DIR = "metadata"
STATE_FILE = "state.json"
CONFIG_FILE = "config.yaml"
BATCH_SIZE = 50

def safe(s):
    return "".join(c for c in s if c not in r'\/:*?"<>|').strip()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

def load_config():
    defaults = {
        "provider": "spotify_scraper",
        "musicbrainz": True,
        "metadata": {"batch_size": 50}
    }
    if Path(CONFIG_FILE).exists():
        try:
            import yaml
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            for k, v in defaults.items():
                cfg.setdefault(k, v)
            return cfg
        except Exception:
            pass
    legacy = Path("config.json")
    if legacy.exists():
        try:
            with open(legacy, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return defaults

def load_existing_ids():
    ids = set()
    id_status = {}
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ids.add(row["id"])
                id_status[row["id"]] = row.get("status", "unknown")
    return ids, id_status

def is_metadata_already_fetched(track_id, id_status):
    status = id_status.get(track_id, "unknown")
    return status != "pending" and status != "unknown"

def append_to_csv(row):
    file_exists = os.path.exists(INDEX_FILE)
    with open(INDEX_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "artist", "album", "meta_path", "source", "status"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def get_metadata_musicbrainz(artist, title):
    try:
        search_query = f'"{title}" AND artist:"{artist}"'
        url = "https://musicbrainz.org/ws/2/recording"
        params = {"query": search_query, "fmt": "json", "limit": 1}
        headers = {"User-Agent": "spotify-sync/1.0"}
        response = requests.get(url, params=params, headers=headers, timeout=5)
        if response.status_code != 200:
            return None
        data = response.json()
        if not data.get("recordings"):
            return None
        recording = data["recordings"][0]
        cover_url = None
        if recording.get("releases") and recording["releases"]:
            release = recording["releases"][0]
            if release.get("id"):
                cover_url = f"https://coverartarchive.org/release/{release['id']}/front"
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
    except Exception:
        return None

def download_cover(url, path):
    if not url:
        return False
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
            return True
    except Exception:
        pass
    return False

def chunk(lst, size):
    it = iter(lst)
    while True:
        c = list(islice(it, size))
        if not c:
            break
        yield c

def main(provider_override=None):
    global BATCH_SIZE
    config = load_config()

    if "batch_size" in config.get("metadata", {}):
        BATCH_SIZE = config["metadata"]["batch_size"]

    use_musicbrainz = config.get("musicbrainz", False)

    if provider_override:
        provider_name = provider_override
    else:
        provider_name = config.get("provider", "spotify_scraper")

    if provider_name == "spotify_api":
        try:
            provider = SpotifyAPIProvider()
            source_label = "spotify_api"
        except Exception as e:
            print(f"ERROR Spotify API auth failed: {e}")
            print("  Falling back to scraper (unauthenticated)")
            provider = SpotifyScraperProvider()
            source_label = "spotify_scraper"
    else:
        provider = SpotifyScraperProvider()
        source_label = "spotify_scraper"

    print(f"Provider: {provider_name}")
    print(f"MusicBrainz enrichment: {'on' if use_musicbrainz else 'off'}\n")

    os.makedirs(BASE_DIR, exist_ok=True)

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

    with open(INPUT_FILE) as f:
        links = list(set(l.strip() for l in f if l.strip()))

    print(f"Total tracks to process: {len(links)}")

    existing_ids, id_status = load_existing_ids()
    print(f"Already processed: {len(existing_ids)}\n")

    id_to_link = {}
    for link in links:
        try:
            tid = extract_track_id(link)
            if tid and not is_metadata_already_fetched(tid, id_status):
                id_to_link[tid] = link
        except:
            continue

    all_ids = list(id_to_link.keys())
    print(f"Pending extraction: {len(all_ids)}\n")

    if not all_ids:
        print("All tracks already processed!")
        return

    start_idx = 0
    if extractor_state["last_processed_id"]:
        try:
            start_idx = all_ids.index(extractor_state["last_processed_id"]) + 1
            print(f"Resuming from: {extractor_state['last_processed_id']}\n")
        except ValueError:
            pass

    processed_count = start_idx
    for batch in chunk(all_ids[start_idx:], BATCH_SIZE):
        print(f"Fetching batch ({len(batch)} tracks)...")

        tracks = provider.get_tracks_batch(batch)
        if not tracks:
            print("  ERROR All tracks in batch failed to fetch\n")
            tracks = [None] * len(batch)

        for idx, track_data in enumerate(tracks):
            track_id = batch[idx]
            processed_count += 1
            print(f"[{processed_count}/{len(all_ids)}] {track_id}...", end=" ", flush=True)

            try:
                if not track_data:
                    print("ERROR No data")
                    extractor_state["failed_ids"].append(track_id)
                    extractor_state["last_processed_id"] = track_id
                    extractor_state["total_processed"] += 1
                    save_state(state)
                    continue

                meta = dict(track_data)
                meta["spotify_url"] = id_to_link[track_id]

                if use_musicbrainz and meta.get("artist") and meta.get("title"):
                    mb_data = get_metadata_musicbrainz(meta["artist"][0], meta["title"])
                    if mb_data:
                        if mb_data.get("album") and not meta.get("album"):
                            meta["album"] = mb_data["album"]
                        if mb_data.get("year") and not meta.get("year"):
                            meta["year"] = mb_data["year"]
                        if mb_data.get("cover_url") and not meta.get("cover_url"):
                            meta["cover_url"] = mb_data["cover_url"]

                artist_folder = os.path.join(BASE_DIR, safe(meta["artist"][0] if meta["artist"] else "Unknown"))
                track_folder = os.path.join(artist_folder, track_id)
                os.makedirs(track_folder, exist_ok=True)

                json_path = os.path.join(track_folder, "meta.json")
                cover_path = os.path.join(track_folder, "cover.jpg")

                with open(json_path, "w", encoding='utf-8') as f:
                    json.dump(meta, f, indent=2)

                cover_ok = download_cover(meta.get("cover_url", ""), cover_path) if meta.get("cover_url") else False

                append_to_csv({
                    "id": meta["id"],
                    "title": meta["title"],
                    "artist": meta["artist"][0] if meta["artist"] else "Unknown",
                    "album": meta.get("album", ""),
                    "meta_path": json_path,
                    "source": source_label,
                    "status": "spotify_metadata_fetched"
                })

                extractor_state["successful"] += 1
                print(f"OK {meta['title']}")

            except Exception as e:
                print(f"ERROR {e}")
                extractor_state["failed_ids"].append(track_id)
                extractor_state["last_error"] = str(e)

            extractor_state["last_processed_id"] = track_id
            extractor_state["total_processed"] += 1
            extractor_state["timestamp"] = datetime.now().isoformat()
            save_state(state)
            time.sleep(0.2)

        time.sleep(0.5)

    print(f"\nMetadata extraction complete!")
    print(f"Processed: {extractor_state['total_processed']}")
    print(f"Successful: {extractor_state['successful']}")
    print(f"Pending: {extractor_state['pending']}")
    if extractor_state['failed_ids']:
        print(f"Failed: {len(extractor_state['failed_ids'])}")

    save_state(state)

if __name__ == "__main__":
    provider_override = os.environ.get("SPOTIFY_SYNC_PROVIDER")
    main(provider_override=provider_override)
