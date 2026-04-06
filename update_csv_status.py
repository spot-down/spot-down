#!/usr/bin/env python3
"""
Update songs_index.csv with granular status values based on file existence.
Status values:
- spotify_metadata_fetched: metadata extracted, but not downloaded
- downloaded: MP3 downloaded but not yet tagged/renamed
- tagged: file renamed and ID3 tags applied
"""

import csv
import json
import os
from pathlib import Path

STATE_FILE = "state.json"
SONGS_CSV = "songs_index.csv"
DATA_DIR = "data"
METADATA_DIR = "metadata"

def load_state():
    """Load state.json"""
    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def determine_status(track_id, title, artist, state_data):
    """
    Determine status by checking:
    1. Is there a renamed file (Artist - Title.mp3 format)?
    2. Is there a track_id.mp3 file?
    3. Did it fail at download/tagging?
    """
    
    # Check for renamed file (Artist - Title.mp3 format)
    expected_renamed = f"{artist} - {title}.mp3"
    renamed_path = Path(DATA_DIR) / expected_renamed
    if renamed_path.exists():
        return "tagged"
    
    # Check for any downloaded file with track ID
    id_file = Path(DATA_DIR) / f"{track_id}.mp3"
    if id_file.exists():
        return "downloaded"
    
    # Check if it failed during tagging
    if track_id in state_data.get('tagger', {}).get('failed_ids', []):
        return "download_failed_tagging"
    
    # Check if it failed during download
    if track_id in state_data.get('downloader', {}).get('permanent_failures', []):
        return "download_failed"
    
    # Otherwise, it has metadata but hasn't been downloaded
    return "spotify_metadata_fetched"

def update_csv():
    """Update CSV with granular status"""
    state_data = load_state()
    
    rows = []
    with open(SONGS_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            track_id = row['id']
            title = row['title']
            artist = row['artist']
            new_status = determine_status(track_id, title, artist, state_data)
            row['status'] = new_status
            rows.append(row)
    
    # Write updated CSV
    with open(SONGS_CSV, 'w', newline='', encoding='utf-8') as f:
        if rows:
            fieldnames = rows[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    
    # Print summary
    status_counts = {}
    for row in rows:
        status = row['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("CSV updated with granular status values:")
    for status in ["tagged", "downloaded", "download_failed_tagging", "download_failed", "spotify_metadata_fetched"]:
        count = status_counts.get(status, 0)
        if count > 0:
            print(f"  {status}: {count} tracks")
    
    return len(rows), status_counts

if __name__ == "__main__":
    total, counts = update_csv()
    print(f"\nTotal tracks: {total}")
