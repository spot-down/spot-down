#!/usr/bin/env python3
"""
Scan and remove duplicate track IDs across metadata, data, CSV index, and state files.

Usage:
  python deduplicate.py              # Scan only (report duplicates)
  python deduplicate.py --remove     # Scan and remove duplicates
"""

import os
import json
import csv
import sys
import argparse
from pathlib import Path
from collections import defaultdict

METADATA_DIR = "metadata"
DATA_DIR = "data"
INDEX_FILE = "songs_index.csv"
STATE_FILE = "state.json"

def scan_metadata_dir():
    """Scan metadata/ for duplicate track IDs"""
    duplicates = defaultdict(list)
    
    if not os.path.exists(METADATA_DIR):
        return duplicates
    
    for artist_dir in os.listdir(METADATA_DIR):
        artist_path = os.path.join(METADATA_DIR, artist_dir)
        if not os.path.isdir(artist_path):
            continue
        
        for track_id in os.listdir(artist_path):
            track_path = os.path.join(artist_path, track_id)
            if os.path.isdir(track_path):
                duplicates[track_id].append(track_path)
    
    # Filter to actual duplicates
    return {k: v for k, v in duplicates.items() if len(v) > 1}

def scan_data_dir():
    """Scan data/ for duplicate track IDs in filenames"""
    duplicates = defaultdict(list)
    
    if not os.path.exists(DATA_DIR):
        return duplicates
    
    for filename in os.listdir(DATA_DIR):
        if not filename.endswith('.mp3'):
            continue
        
        # Extract track ID (before .mp3 or before (counter))
        if '(' in filename:
            track_id = filename.split('(')[0].strip()
        else:
            track_id = filename.replace('.mp3', '')
        
        file_path = os.path.join(DATA_DIR, filename)
        duplicates[track_id].append(file_path)
    
    # Filter to actual duplicates
    return {k: v for k, v in duplicates.items() if len(v) > 1}

def scan_csv_index():
    """Scan songs_index.csv for duplicate track IDs"""
    duplicates = defaultdict(list)
    
    if not os.path.exists(INDEX_FILE):
        return duplicates
    
    with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # start=2 because row 1 is header
            track_id = row.get('id')
            if track_id:
                duplicates[track_id].append(row_num)
    
    # Filter to actual duplicates
    return {k: v for k, v in duplicates.items() if len(v) > 1}

def scan_state_file():
    """Scan state.json for duplicate IDs in failed_downloads and permanent_failures"""
    duplicates = {}
    
    if not os.path.exists(STATE_FILE):
        return duplicates
    
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)
    
    dl_state = state.get('downloader', {})
    failed = dl_state.get('failed_downloads', [])
    permanent = dl_state.get('permanent_failures', [])
    
    # Check for duplicates within each list
    failed_dupes = [id for id in set(failed) if failed.count(id) > 1]
    permanent_dupes = [id for id in set(permanent) if permanent.count(id) > 1]
    
    if failed_dupes or permanent_dupes:
        duplicates['state.json'] = {
            'failed_downloads_dupes': failed_dupes,
            'permanent_failures_dupes': permanent_dupes
        }
    
    return duplicates

def report_duplicates(metadata_dupes, data_dupes, csv_dupes, state_dupes):
    """Print duplicate scan report"""
    total_issues = 0
    
    print("\n" + "="*70)
    print("DUPLICATE SCAN REPORT")
    print("="*70)
    
    if metadata_dupes:
        print("\nMETADATA DUPLICATES:")
        for track_id, paths in sorted(metadata_dupes.items()):
            print(f"  {track_id}: {len(paths)} copies")
            for path in paths:
                print(f"    - {path}")
            total_issues += len(paths) - 1
    
    if data_dupes:
        print("\nDATA DUPLICATES:")
        for track_id, paths in sorted(data_dupes.items()):
            print(f"  {track_id}: {len(paths)} copies")
            for path in paths:
                print(f"    - {path}")
            total_issues += len(paths) - 1
    
    if csv_dupes:
        print("\nCSV INDEX DUPLICATES:")
        for track_id, row_nums in sorted(csv_dupes.items()):
            print(f"  {track_id}: {len(row_nums)} rows")
            for row_num in row_nums:
                print(f"    - Row {row_num}")
            total_issues += len(row_nums) - 1
    
    if state_dupes:
        print("\nSTATE FILE DUPLICATES:")
        for file, dupes in state_dupes.items():
            if dupes['failed_downloads_dupes']:
                print(f"  failed_downloads: {len(dupes['failed_downloads_dupes'])} duplicate IDs")
                for id in dupes['failed_downloads_dupes']:
                    print(f"    - {id}")
                total_issues += 1
            if dupes['permanent_failures_dupes']:
                print(f"  permanent_failures: {len(dupes['permanent_failures_dupes'])} duplicate IDs")
                for id in dupes['permanent_failures_dupes']:
                    print(f"    - {id}")
                total_issues += 1
    
    print(f"\n{'='*70}")
    if total_issues == 0:
        print("STATUS: No duplicates found - all clean!")
    else:
        print(f"STATUS: Found {total_issues} duplicate items")
    print("="*70)
    
    return total_issues

def remove_metadata_duplicates(metadata_dupes):
    """Remove duplicate metadata folders (keep first, remove rest)"""
    removed = 0
    for track_id, paths in metadata_dupes.items():
        sorted_paths = sorted(paths)  # Deterministic order
        for path in sorted_paths[1:]:  # Skip first, remove rest
            try:
                import shutil
                shutil.rmtree(path)
                print(f"Removed: {path}")
                removed += 1
            except Exception as e:
                print(f"Failed to remove {path}: {e}")
    return removed

def remove_data_duplicates(data_dupes):
    """Remove duplicate data files (keep first, remove rest)"""
    removed = 0
    for track_id, paths in data_dupes.items():
        sorted_paths = sorted(paths)  # Deterministic order
        for path in sorted_paths[1:]:  # Skip first, remove rest
            try:
                os.remove(path)
                print(f"Removed: {path}")
                removed += 1
            except Exception as e:
                print(f"Failed to remove {path}: {e}")
    return removed

def remove_csv_duplicates(csv_dupes):
    """Remove duplicate rows from CSV (keep first occurrence)"""
    if not csv_dupes:
        return 0
    
    rows = []
    seen_ids = set()
    duplicates_removed = 0
    
    with open(INDEX_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            track_id = row.get('id')
            if track_id not in seen_ids:
                rows.append(row)
                seen_ids.add(track_id)
            else:
                duplicates_removed += 1
                print(f"Removed duplicate row: {track_id}")
    
    with open(INDEX_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    return duplicates_removed

def remove_state_duplicates(state_dupes):
    """Remove duplicate IDs from state.json lists"""
    if not state_dupes:
        return 0
    
    removed = 0
    
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)
    
    dl_state = state.get('downloader', {})
    
    # Deduplicate failed_downloads
    if 'failed_downloads' in dl_state:
        original_len = len(dl_state['failed_downloads'])
        dl_state['failed_downloads'] = list(dict.fromkeys(dl_state['failed_downloads']))
        removed += original_len - len(dl_state['failed_downloads'])
        if original_len > len(dl_state['failed_downloads']):
            print(f"Removed {original_len - len(dl_state['failed_downloads'])} duplicates from failed_downloads")
    
    # Deduplicate permanent_failures
    if 'permanent_failures' in dl_state:
        original_len = len(dl_state['permanent_failures'])
        dl_state['permanent_failures'] = list(dict.fromkeys(dl_state['permanent_failures']))
        removed += original_len - len(dl_state['permanent_failures'])
        if original_len > len(dl_state['permanent_failures']):
            print(f"Removed {original_len - len(dl_state['permanent_failures'])} duplicates from permanent_failures")
    
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)
    
    return removed

def main():
    parser = argparse.ArgumentParser(
        description="Scan and remove duplicate track IDs across all pipeline directories and files"
    )
    parser.add_argument(
        "--remove",
        action="store_true",
        help="Remove duplicates (default: scan only)"
    )
    
    args = parser.parse_args()
    
    print("Scanning for duplicates...\n")
    
    metadata_dupes = scan_metadata_dir()
    data_dupes = scan_data_dir()
    csv_dupes = scan_csv_index()
    state_dupes = scan_state_file()
    
    total_issues = report_duplicates(metadata_dupes, data_dupes, csv_dupes, state_dupes)
    
    if args.remove and total_issues > 0:
        print("\n" + "="*70)
        print("REMOVING DUPLICATES")
        print("="*70 + "\n")
        
        total_removed = 0
        total_removed += remove_metadata_duplicates(metadata_dupes)
        total_removed += remove_data_duplicates(data_dupes)
        total_removed += remove_csv_duplicates(csv_dupes)
        total_removed += remove_state_duplicates(state_dupes)
        
        print(f"\n{'='*70}")
        print(f"REMOVED: {total_removed} duplicate items")
        print("="*70)
    elif args.remove:
        print("\nNo duplicates to remove.")
    else:
        print("\nRun with --remove flag to remove duplicates")

if __name__ == "__main__":
    main()
