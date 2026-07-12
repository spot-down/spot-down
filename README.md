# spotify-sync

Pipeline to extract music metadata from Spotify and download tracks with ID3 tagging.

## Pipeline Overview

4 stages processing tracks from metadata extraction through download to final tagging:

1. **Stage 1: Metadata Extraction** - Get track metadata (scraper or Spotify API) + MusicBrainz enrichment
2. **Stage 2: Spotify Upgrade** (Optional) - API validation and batch optimization (only when provider is spotify_api)
3. **Stage 3: Download** - Download MP3s from YouTube using yt-dlp with basic ID3 tagging
4. **Stage 4: Rename and Tag** - Rename to `Artist - Title.mp3` and apply ID3 v2.4 tags with artwork

Default: Stages 1, 3, 4. Stage 2 is optional.

## Installation

### Requirements

- Python 3.7+
- ffmpeg + ffprobe
- yt-dlp

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**No Spotify credentials needed** by default — the scraper works anonymously.

Only set credentials if using the official API provider:
```bash
export SPOTIPY_CLIENT_ID="your_client_id"
export SPOTIPY_CLIENT_SECRET="your_client_secret"
```

## Configuration

Edit `config.yml` (or `config.yaml` / `config.json`):

```yaml
provider: spotify_scraper

metadata:
  sources:
    - musicbrainz
  batch_size: 50

pipeline:
  default_stages:
    - 1
    - 3
    - 4
```

### Options

**provider** - How to fetch basic track data (name, artist, ID):
- `spotify_scraper` (default) - Unauthenticated, no credentials needed. Gets title, artist, duration. Album/cover enriched by MusicBrainz.
- `spotify_api` - Official Spotify Web API. Requires SPOTIPY_CLIENT_ID + SPOTIPY_CLIENT_SECRET. Full metadata including album and cover art.

**metadata.sources** - Enrichment sources:
- `["musicbrainz"]` (default) - Album name, year, cover art from MusicBrainz
- `[]` - No enrichment

**metadata.batch_size** - Tracks per batch (default: 50)

**pipeline.default_stages** - Default stages (default: [1, 3, 4])

## Usage

```bash
python main.py                          # Normal run: song_sources.txt + playlists.txt sync
python main.py -p <playlist_url>        # Add and sync a playlist
python main.py -t <track_url_or_id>     # Add and process a single track
python main.py -i                       # Show config and pipeline status
python main.py -h                       # Help
python main.py --stage 1                # Metadata extraction only
python main.py --stage 1 --stage 3      # Extract + download
```

### Resume from Interruption

All stages track progress in `state.json` and auto-resume. Status-based skipping avoids redundant work:
- `spotify_metadata_fetched` → downloader skips
- `downloaded` / `tagged` → all stages skip

### Playlist Sync

Playlist URLs in `playlists.txt` get synced on each run. New tracks are automatically added to `song_sources.txt`. Tracked in `playlist_index.json`:
- Detects new or removed tracks by comparing stored track IDs
- Only adds new tracks, never re-downloads existing ones

### Retry Logic

Download failures are retried up to 3 times before being marked permanent. Tracked per-track in `state.json`:
- `retry_counts` - tracks consecutive failures per track
- After 3 failures → moves to `permanent_failures`, sets CSV status to `download_failed`

## Input Data

`song_sources.txt` - Track URLs (one per line):
```
https://open.spotify.com/track/6hFi0gXP8KItwMqfBgf44b
```

## Output

```
metadata/{Artist}/{track_id}/meta.json   # Track metadata
metadata/{Artist}/{track_id}/cover.jpg   # Album artwork
data/Artist - Title.mp3                  # Tagged MP3s
songs_index.csv                          # Master index
state.json                               # Pipeline state
playlist_index.json                      # Playlist sync state
```

## Status Values

- `spotify_metadata_fetched` - Metadata extracted, not downloaded
- `downloaded` - MP3 downloaded, not yet tagged
- `tagged` - Renamed + ID3 tags applied (complete)
- `download_failed` - Failed after 3 retries

## Files

- `main.py` - Orchestrator
- `metadata_extractor_v2.py` - Stage 1
- `spotify_upgrade.py` - Stage 2 (spotify_api only)
- `downloader.py` - Stage 3
- `tagger.py` - Stage 4
- `providers/` - Metadata provider system
  - `spotify_scraper.py` - Unauthenticated scraper (default)
  - `spotify_api.py` - Official Spotify API provider
- `deduplicate.py` - Duplicate scanner/remover
