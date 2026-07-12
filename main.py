#!/usr/bin/env python3
import sys
import os
import json
import argparse
import subprocess
from pathlib import Path

CONFIG_FILES = ["config.yml", "config.yaml", "config.json"]
SONG_SOURCES = "song_sources.txt"
PLAYLISTS_FILE = "playlists.txt"
PLAYLIST_INDEX = "playlist_index.json"


def load_config():
    defaults = {
        "provider": "spotify_scraper",
        "metadata": {"sources": ["musicbrainz"], "batch_size": 50},
        "pipeline": {"default_stages": [1, 3, 4]}
    }
    for path in CONFIG_FILES:
        if not Path(path).exists():
            continue
        try:
            is_json = path.endswith(".json")
            with open(path, 'r', encoding='utf-8') as f:
                cfg = json.load(f) if is_json else __import__("yaml").safe_load(f) or {}
            for k, v in defaults.items():
                cfg.setdefault(k, v)
            return cfg
        except Exception:
            continue
    return defaults


def load_playlist_index():
    if os.path.exists(PLAYLIST_INDEX):
        with open(PLAYLIST_INDEX, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"playlists": {}}


def save_playlist_index(index):
    with open(PLAYLIST_INDEX, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2)


def add_to_song_sources(track_url):
    existing = set()
    if os.path.exists(SONG_SOURCES):
        with open(SONG_SOURCES, 'r', encoding='utf-8') as f:
            existing = set(l.strip() for l in f if l.strip())
    if track_url not in existing:
        with open(SONG_SOURCES, 'a', encoding='utf-8') as f:
            f.write(track_url + "\n")
        return True
    return False


def sync_playlist(playlist_url):
    from providers import extract_playlist_id
    from providers.spotify_scraper import SpotifyScraperProvider

    pid = extract_playlist_id(playlist_url)
    if not pid:
        print(f"  Invalid playlist URL: {playlist_url}")
        return

    provider = SpotifyScraperProvider()
    tracks = provider.get_playlist(pid)
    if not tracks:
        print(f"  Failed to fetch playlist: {playlist_url}")
        return

    index = load_playlist_index()
    pl_index = index["playlists"].get(pid, {"track_ids": []})
    stored_ids = set(pl_index.get("track_ids", []))
    current_ids = {t["id"] for t in tracks}
    current_ids_list = [t["id"] for t in tracks]

    new_ids = current_ids - stored_ids
    removed_ids = stored_ids - current_ids

    if new_ids:
        print(f"  New tracks found: {len(new_ids)}")
        for t in tracks:
            if t["id"] in new_ids:
                song_url = f"https://open.spotify.com/track/{t['id']}"
                added = add_to_song_sources(song_url)
                if added:
                    print(f"    Added: {t['title']} - {t['artist'][0]}")
    elif not removed_ids:
        print(f"  Playlist is up to date ({len(current_ids)} tracks)")
        return

    if removed_ids:
        print(f"  Removed tracks: {len(removed_ids)}")

    index["playlists"][pid] = {
        "url": playlist_url,
        "track_count": len(current_ids),
        "track_ids": current_ids_list,
        "last_synced": __import__("datetime").datetime.now().isoformat()
    }
    save_playlist_index(index)
    print(f"  Playlist index updated ({len(current_ids)} tracks)")


def sync_all_playlists():
    if not os.path.exists(PLAYLISTS_FILE):
        return
    with open(PLAYLISTS_FILE, 'r', encoding='utf-8') as f:
        urls = [l.strip() for l in f if l.strip()]
    if not urls:
        return
    print(f"\nSyncing {len(urls)} playlist(s)...")
    for url in urls:
        print(f"  [{url}]")
        sync_playlist(url)


def run_stage(stage_num, provider_override=None):
    env = os.environ.copy()
    if provider_override:
        env["SPOTIFY_SYNC_PROVIDER"] = provider_override

    if stage_num == 1:
        print("\n" + "=" * 70)
        print("STAGE 1: METADATA EXTRACTION")
        print("=" * 70)
        result = subprocess.run([sys.executable, "metadata_extractor_v2.py"], env=env)
        return result.returncode == 0

    elif stage_num == 2:
        print("\n" + "=" * 70)
        print("STAGE 2: SPOTIFY API UPGRADE")
        print("=" * 70)
        result = subprocess.run([sys.executable, "spotify_upgrade.py"], env=env)
        return result.returncode == 0

    elif stage_num == 3:
        print("\n" + "=" * 70)
        print("STAGE 3: DOWNLOAD MP3s (yt-dlp + tagging)")
        print("=" * 70)
        result = subprocess.run([sys.executable, "downloader.py"], env=env)
        return result.returncode == 0

    elif stage_num == 4:
        print("\n" + "=" * 70)
        print("STAGE 4: RENAME AND TAG (ID3 v2.4 + artwork)")
        print("=" * 70)
        result = subprocess.run([sys.executable, "tagger.py"], env=env)
        return result.returncode == 0

    else:
        print(f"Unknown stage: {stage_num}")
        return False


def print_info(config):
    print("=" * 70)
    print("spotify-sync CONFIGURATION")
    print("=" * 70)
    import yaml
    print(yaml.dump(config, default_flow_style=False).strip())
    print()

    print("AVAILABLE COMMANDS")
    print("=" * 70)
    print("  python main.py                    Run default stages (song_sources.txt + playlists.txt)")
    print("  python main.py --stage N          Run a specific stage (1-4)")
    print("  python main.py -p URL             Add & sync a playlist")
    print("  python main.py -t URL             Add & process a single track")
    print("  python main.py -i                 Show this info")
    print()

    status_counts = {}
    if os.path.exists("songs_index.csv"):
        import csv
        with open("songs_index.csv", newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                s = row.get("status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
        print("PIPELINE STATUS")
        print("=" * 70)
        for s, c in sorted(status_counts.items()):
            print(f"  {s}: {c}")
        if os.path.exists(PLAYLIST_INDEX):
            with open(PLAYLIST_INDEX, 'r', encoding='utf-8') as f:
                pi = json.load(f)
            pls = pi.get("playlists", {})
            if pls:
                print(f"\n  Tracked playlists: {len(pls)}")
                for pid, p in pls.items():
                    print(f"    {pid}: {p.get('track_count', 0)} tracks")
    print("=" * 70)


def main():
    config = load_config()
    default_stages = config["pipeline"]["default_stages"]

    parser = argparse.ArgumentParser(
        description="spotify-sync orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
        epilog="""
Examples:
  python main.py                             Run default stages
  python main.py --stage 1 --stage 3         Extract + download
  python main.py -p https://open.spotify.com/playlist/...
  python main.py -t https://open.spotify.com/track/...
  python main.py -i                          Show config + status
        """
    )

    parser.add_argument("--stage", type=int, choices=[1, 2, 3, 4], action="append", help="Stage(s) to run")
    parser.add_argument("-p", "--playlist", type=str, help="Add and sync a playlist URL", metavar="URL")
    parser.add_argument("-t", "--track", type=str, help="Add and process a single track URL/ID", metavar="URL")
    parser.add_argument("-i", "--info", action="store_true", help="Show configuration and pipeline status")
    parser.add_argument("-h", "--help", action="store_true", help="Show this help message and exit")

    args = parser.parse_args()

    if args.help:
        parser.print_help()
        return

    if args.info:
        print_info(config)
        return

    provider_override = os.environ.get("SPOTIFY_SYNC_PROVIDER")

    if args.playlist:
        url = args.playlist.strip()
        from providers import extract_playlist_id
        pid = extract_playlist_id(url)
        if not pid:
            print(f"Invalid playlist URL: {url}")
            return

        if not os.path.exists(PLAYLISTS_FILE):
            with open(PLAYLISTS_FILE, 'w', encoding='utf-8') as f:
                pass

        with open(PLAYLISTS_FILE, 'r', encoding='utf-8') as f:
            existing = set(l.strip() for l in f if l.strip())

        if url not in existing:
            with open(PLAYLISTS_FILE, 'a', encoding='utf-8') as f:
                f.write(url + "\n")
            print(f"Added playlist to {PLAYLISTS_FILE}")

        print(f"Syncing playlist...")
        sync_playlist(url)

        stages_to_run = args.stage if args.stage else default_stages
    elif args.track:
        url = args.track.strip()
        from providers import extract_track_id
        tid = extract_track_id(url)
        if not tid:
            full_url = f"https://open.spotify.com/track/{url}"
            tid = extract_track_id(full_url)
            if not tid:
                print(f"Invalid track URL/ID: {url}")
                return
            url = full_url

        added = add_to_song_sources(url)
        if added:
            print(f"Added track to {SONG_SOURCES}")
        else:
            print(f"Track already in {SONG_SOURCES}")

        stages_to_run = args.stage if args.stage else [1]
    else:
        sync_all_playlists()
        stages_to_run = args.stage if args.stage else default_stages

    print("\n" + "=" * 70)
    print(f"spotify-sync PIPELINE")
    print(f"Stages to run: {stages_to_run}")
    print(f"Provider: {provider_override or config.get('provider', 'spotify_scraper')}")
    print("=" * 70)

    success_count = 0
    for stage in stages_to_run:
        if run_stage(stage, provider_override=provider_override):
            success_count += 1
        else:
            print(f"\nStage {stage} failed")
            break

    print("\n" + "=" * 70)
    if success_count == len(stages_to_run):
        print(f"All {len(stages_to_run)} stage(s) completed successfully!")
    else:
        print(f"Pipeline failed at stage {stages_to_run[success_count]}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
