import os
import json
import csv
import requests
import base64
import time
from datetime import datetime
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

INDEX_FILE = "songs_index.csv"
BASE_DIR = "metadata"
STATE_FILE = "state.json"

# ========================
# CONFIG
# ========================
BATCH_SIZE = 50  # Will be overridden by test results

# ========================
# HELPERS
# ========================
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
    """Save state to JSON"""
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

def load_csv_rows():
    """Load all CSV rows that need Spotify upgrade"""
    rows = []
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("source") != "spotify_api":
                    rows.append(row)
    return rows

def update_csv_row(track_id, new_data):
    """Update a single row in CSV"""
    rows = []
    with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    for row in rows:
        if row["id"] == track_id:
            row.update(new_data)
            break
    
    with open(INDEX_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "artist", "album", "meta_path", "source", "status"])
        writer.writeheader()
        writer.writerows(rows)

def get_json_meta(meta_path):
    """Load metadata JSON"""
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return None

def save_json_meta(meta_path, data):
    """Save metadata JSON"""
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

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
        print(f"    Cover download failed: {e}")
    return False

# ========================
# SPOTIFY API TEST
# ========================
def test_spotify_auth():
    """
    AUTO-TEST MODE:
    1. Try batch of 2
    2. If 429 -> sleep and retry once
    3. If auth works -> note batch_size=2
    4. Try batch of 1 to establish baseline
    5. Determine optimal batch size
    """
    print("\n" + "="*60)
    print("SPOTIFY API AUTO-TEST")
    print("="*60)
    
    state = load_state()
    if "spotify_upgrade" not in state:
        state["spotify_upgrade"] = {}
    
    test_state = state["spotify_upgrade"].setdefault("test_mode_results", {})
    test_results = test_state.setdefault("test_results", {})
    
    try:
        sp = Spotify(auth_manager=SpotifyClientCredentials())
        print("✓ Spotify credentials loaded")
        test_results["auth_valid"] = True
    except Exception as e:
        print(f"✗ Spotify auth failed: {e}")
        print("  -> Check your SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables")
        test_results["auth_valid"] = False
        save_state(state)
        return None
    
    # Get some test IDs from CSV
    rows = load_csv_rows()
    if len(rows) < 2:
        print("✗ Not enough tracks to test (need at least 2)")
        return None
    
    test_ids = [rows[i]["id"] for i in range(min(2, len(rows)))]
    print(f"\nTest IDs: {test_ids}")
    
    # Test 1: Batch of 2
    print("\n[TEST 1] Batch request (2 songs)...", end=" ", flush=True)
    try:
        result = sp.tracks(test_ids)
        if result.get("tracks"):
            print("✓ SUCCESS")
            test_results["batch_2_status"] = "success"
            optimal_batch = 50  # Default larger batch
        else:
            print("✗ EMPTY RESPONSE")
            test_results["batch_2_status"] = "empty"
            optimal_batch = 1
    except SpotifyException as e:
        if e.http_status == 429:
            print(f"↻ RATE LIMITED (429) - Waiting 5s then retrying...")
            test_results["batch_2_status"] = "rate_limited_once"
            time.sleep(5)
            try:
                result = sp.tracks(test_ids)
                print("✓ Retry SUCCESS")
                optimal_batch = 1  # Conservative after rate limit
            except Exception as e2:
                print(f"✗ Retry failed: {e2}")
                test_results["batch_2_status"] = "rate_limited_persistent"
                optimal_batch = 1
        elif e.http_status == 403:
            print(f"✗ FORBIDDEN (403)")
            print("  -> Your Spotify account or app doesn't have API access")
            print("  -> Reason: 'Active premium subscription required for the owner of the app'")
            test_results["batch_2_status"] = "forbidden"
            optimal_batch = None
        else:
            print(f"✗ ERROR: {e.http_status}")
            test_results["batch_2_status"] = f"error_{e.http_status}"
            optimal_batch = None
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        test_results["batch_2_status"] = "error"
        optimal_batch = None
    
    if optimal_batch is None:
        print("\n✗ Spotify API is not accessible. Stopping test.")
        save_state(state)
        return None
    
    # Test 2: Single request (establish baseline)
    if test_ids:
        print(f"\n[TEST 2] Single request (1 song)...", end=" ", flush=True)
        try:
            result = sp.track(test_ids[0])
            if result:
                print("✓ SUCCESS")
                test_results["batch_1_status"] = "success"
        except SpotifyException as e:
            if e.http_status == 429:
                print(f"↻ RATE LIMITED (429)")
                test_results["batch_1_status"] = "rate_limited"
                optimal_batch = 1
            elif e.http_status == 403:
                print(f"✗ FORBIDDEN (403)")
                test_results["batch_1_status"] = "forbidden"
                optimal_batch = None
            else:
                print(f"✗ ERROR: {e.http_status}")
                test_results["batch_1_status"] = f"error_{e.http_status}"
        except Exception as e:
            print(f"✗ Error: {e}")
            test_results["batch_1_status"] = "error"
    
    # Determine rate limit delay
    rate_limit_delay = 250  # Default 250ms between requests
    if test_results.get("batch_2_status") == "rate_limited_once":
        rate_limit_delay = 500
    
    test_state["optimal_batch_size"] = optimal_batch
    test_state["rate_limit_delay_ms"] = rate_limit_delay
    test_state["tested_at"] = datetime.now().isoformat()
    
    print(f"\n{'='*60}")
    print(f"TEST RESULTS:")
    print(f"  Optimal batch size: {optimal_batch}")
    print(f"  Rate limit delay: {rate_limit_delay}ms")
    print(f"  Auth valid: {test_results['auth_valid']}")
    print(f"{'='*60}\n")
    
    save_state(state)

    return optimal_batch

# ========================
# BATCH REQUEST WITH FALLBACK
# ========================
def get_tracks_batch(sp, ids, max_retries=3):
    """
    Batch request with exponential backoff.
    Fallback to single requests if batch fails.
    """
    delays = [5, 10, 20]
    
    for attempt in range(max_retries):
        try:
            return sp.tracks(ids)["tracks"]
        except SpotifyException as e:
            if e.http_status == 429:
                if attempt < len(delays):
                    print(f"    Batch 429 -> sleeping {delays[attempt]}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(delays[attempt])
                else:
                    print(f"    Max retries exceeded for batch")
                    break
            elif e.http_status == 403:
                print(f"    Batch 403 FORBIDDEN -> Fallback to single requests")
                return get_tracks_single(sp, ids)
            else:
                raise
    
    # Final fallback: single requests
    print(f"    Batch failed -> Fallback to single requests")
    return get_tracks_single(sp, ids)

def get_tracks_single(sp, ids):
    """Get tracks one-by-one with delay"""
    results = []
    for tid in ids:
        try:
            results.append(sp.track(tid))
            time.sleep(0.2)
        except SpotifyException as e:
            if e.http_status == 429:
                print(f"      429 on {tid} -> sleep 5s")
                time.sleep(5)
                try:
                    results.append(sp.track(tid))
                except:
                    results.append(None)
            else:
                print(f"      Error {e.http_status} on {tid}")
                results.append(None)
        except:
            results.append(None)
    return results

# ========================
# MAIN UPGRADE
# ========================
def main():
    state = load_state()
    
    # Run auto-test first
    optimal_batch = test_spotify_auth()
    if optimal_batch is None:
        print("Cannot proceed without Spotify API access.")
        return
    
    # Override batch size
    global BATCH_SIZE
    BATCH_SIZE = optimal_batch
    
    print(f"Starting Spotify metadata upgrade (batch size: {BATCH_SIZE})...\n")
    
    # Initialize upgrade state
    if "spotify_upgrade" not in state:
        state["spotify_upgrade"] = {
            "last_processed_id": None,
            "upgraded_count": 0,
            "failed_upgrade_ids": [],
            "last_error": None,
            "timestamp": None,
            "last_operation": {
                "id": None,
                "spotify_upgrade_complete": False
            }
        }
    
    upgrade_state = state["spotify_upgrade"]
    
    # Load rows that need upgrade
    rows = load_csv_rows()
    print(f"Rows to upgrade: {len(rows)}\n")
    
    if not rows:
        print("No rows to upgrade!")
        return
    
    # Resume
    start_idx = 0
    if upgrade_state["last_processed_id"]:
        for i, row in enumerate(rows):
            if row["id"] == upgrade_state["last_processed_id"]:
                start_idx = i + 1
                break
    
    # Connect to Spotify
    try:
        sp = Spotify(auth_manager=SpotifyClientCredentials())
    except Exception as e:
        print(f"Failed to connect: {e}")
        return
    
    # Process rows
    for idx, row in enumerate(rows[start_idx:], start=start_idx):
        track_id = row["id"]
        meta_path = row["meta_path"]
        print(f"[{idx+1}/{len(rows)}] {track_id}...", end=" ", flush=True)
        
        try:
            # Get metadata
            meta = get_json_meta(meta_path)
            if not meta:
                print("✗ No metadata JSON")
                upgrade_state["failed_upgrade_ids"].append(track_id)
                continue
            
            # Get Spotify data
            try:
                spotify_data = sp.track(track_id)
            except:
                spotify_data = None
            
            if not spotify_data:
                print("✗ Spotify returned None")
                upgrade_state["failed_upgrade_ids"].append(track_id)
                continue
            
            # Update metadata
            meta["title"] = spotify_data["name"]
            meta["artist"] = [a["name"] for a in spotify_data["artists"]]
            meta["album"] = spotify_data["album"]["name"]
            meta["year"] = spotify_data["album"]["release_date"][:4]
            meta["duration_ms"] = spotify_data["duration_ms"]
            meta["source"] = "spotify_api"
            meta["search_query"] = f"{meta['artist'][0]} - {meta['title']}"
            
            # Get cover
            cover_url = ""
            if spotify_data["album"]["images"]:
                cover_url = spotify_data["album"]["images"][0]["url"]
                meta["cover_url"] = cover_url
            
            # Update folder structure
            artist_folder = os.path.join(BASE_DIR, safe(meta["artist"][0]))
            track_folder = os.path.join(artist_folder, track_id)
            os.makedirs(track_folder, exist_ok=True)
            
            # Update metadata paths
            new_json_path = os.path.join(track_folder, "meta.json")
            cover_path = os.path.join(track_folder, "cover.jpg")
            
            # Save metadata
            save_json_meta(new_json_path, meta)
            
            # Download cover
            cover_ok = download_cover(cover_url, cover_path) if cover_url else False
            
            # Update CSV
            update_csv_row(track_id, {
                "title": meta["title"],
                "artist": meta["artist"][0],
                "album": meta.get("album", ""),
                "meta_path": new_json_path,
                "source": "spotify_api",
                "status": "success"
            })
            
            # Update state
            upgrade_state["last_processed_id"] = track_id
            upgrade_state["upgraded_count"] += 1
            upgrade_state["timestamp"] = datetime.now().isoformat()
            upgrade_state["last_operation"] = {
                "id": track_id,
                "spotify_upgrade_complete": True
            }
            
            print(f"✓ {meta['title']}")
            
        except SpotifyException as e:
            if e.http_status == 429:
                print(f"✗ 429 Rate limited")
                upgrade_state["failed_upgrade_ids"].append(track_id)
            elif e.http_status == 403:
                print(f"✗ 403 Forbidden")
                upgrade_state["failed_upgrade_ids"].append(track_id)
            else:
                print(f"✗ {e.http_status}")
                upgrade_state["failed_upgrade_ids"].append(track_id)
            upgrade_state["last_error"] = str(e)
        except Exception as e:
            print(f"✗ {e}")
            upgrade_state["failed_upgrade_ids"].append(track_id)
            upgrade_state["last_error"] = str(e)
        
        # Save state
        save_state(state)
        time.sleep(0.3)
    
    print(f"\nSpotify upgrade complete!")
    print(f"Upgraded: {upgrade_state['upgraded_count']}")
    if upgrade_state['failed_upgrade_ids']:
        print(f"Failed: {len(upgrade_state['failed_upgrade_ids'])}")
    
    save_state(state)

if __name__ == "__main__":
    main()
