# spotify-sync

Pipeline to extract music metadata from Spotify and download tracks with ID3 tagging.

## Pipeline Overview

The pipeline consists of 4 stages that process tracks from metadata extraction through download to final tagging:

1. **Stage 1: Metadata Extraction** - Extract track metadata from Spotify API with optional MusicBrainz enrichment
2. **Stage 2: Spotify Upgrade** (Optional) - API validation and batch processing optimization
3. **Stage 3: Download** - Download MP3 files from YouTube using yt-dlp with basic ID3 tagging
4. **Stage 4: Rename and Tag** - Rename files to `Artist - Title.mp3` format and apply ID3 v2.4 tags with album artwork

By default, Stages 1, 3, and 4 run automatically. Stage 2 is optional and can be run independently if needed.

## Installation

### Requirements

- Python 3.7+
- ffmpeg and ffprobe (for audio processing)
- YouTube compatibility (yt-dlp)

### Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set Spotify API credentials
export SPOTIPY_CLIENT_ID="your_client_id"
export SPOTIPY_CLIENT_SECRET="your_client_secret"
```

### Environment Variables

Required for Stage 1 (metadata extraction):
- `SPOTIPY_CLIENT_ID` - Spotify API client ID
- `SPOTIPY_CLIENT_SECRET` - Spotify API client secret

Obtain these from [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)

## Configuration

Edit `config.json` to customize pipeline behavior:

```json
{
  "metadata": {
    "sources": ["spotify", "musicbrainz"]
  },
  "pipeline": {
    "default_stages": [1, 3, 4]
  }
}
```

### Configuration Options

**metadata.sources** - Which data sources to use for metadata extraction:
- `["spotify"]` - Use only Spotify API (faster, requires credentials)
- `["spotify", "musicbrainz"]` - Use Spotify API + MusicBrainz enrichment (default, slower but more complete metadata)

**pipeline.default_stages** - Stages to run when no `--stage` arguments provided:
- `[1, 3, 4]` - Default: extract metadata, download, rename/tag (skips Stage 2)
- `[1, 2, 3, 4]` - Run all stages including Spotify upgrade
- `[1]` - Run only metadata extraction
- Any combination: `[3, 4]`, `[1, 2, 3]`, etc.

## Usage

### Run Default Pipeline

```bash
python main.py
```

Runs Stages 1, 3, and 4 (metadata extraction, download, rename and tag). Stage 2 is optional and skipped by default.

### Run Specific Stages

```bash
python main.py --stage 1          # Metadata extraction only
python main.py --stage 2          # Spotify upgrade only (optional)
python main.py --stage 3          # Download only
python main.py --stage 4          # Rename and tag only
```

### Run Multiple Stages

```bash
python main.py --stage 1 --stage 3 --stage 4    # Default pipeline
python main.py --stage 1 --stage 2 --stage 3 --stage 4  # All stages including Spotify upgrade
python main.py --stage 3 --stage 4  # Download and tag only
```

### Resume from Interruption

All stages track progress in `state.json` and automatically resume from the last processed track on restart.

### Scan and Remove Duplicates

```bash
python deduplicate.py              # Scan for duplicates (report only)
python deduplicate.py --remove     # Remove duplicate track IDs
```

Scans across metadata/, data/, songs_index.csv, and state.json for duplicate track IDs and removes them (keeps first occurrence, removes rest).

## Input Data

### Track List

Provide track URLs in `song_sources.txt` (one per line):
```
https://open.spotify.com/track/6hFi0gXP8KItwMqfBgf44b
https://open.spotify.com/track/5vgVwFMQ7bLQcBq7JVL3op
```

Spotify URLs can use any format; track IDs are extracted automatically.

## Output

### Directory Structure

```
metadata/
  {Artist}/
    {track_id}/
      meta.json        # Complete track metadata
      cover.jpg        # Album artwork

data/
  Artist - Title.mp3   # Final tagged MP3 files

songs_index.csv        # Master index with metadata for all tracks
state.json            # Pipeline state and progress tracking
```

### CSV Index

The `songs_index.csv` file tracks all processed tracks:

```
id,title,artist,album,meta_path,source,status
6hFi0gXP8KItwMqfBgf44b,"Bad Liar – Stripped","Imagine Dragons",...,metadata/Imagine Dragons/.../meta.json,spotify_api,success
```

### Metadata Format (meta.json)

```json
{
  "id": "6hFi0gXP8KItwMqfBgf44b",
  "title": "Bad Liar – Stripped",
  "artist": ["Imagine Dragons"],
  "album": "Bad Liar – Stripped",
  "year": 2018,
  "duration_ms": 257000,
  "cover_url": "https://...",
  "search_query": "Imagine Dragons Bad Liar – Stripped",
  "source": "spotify_api"
}
```

### ID3 Tags Applied (Stage 4)

- Title (TIT2)
- Artist (TPE1)
- Album (TALB)
- Year (TDRC)
- Album Art (APIC) - embedded from cover.jpg

## Performance

Current pipeline status:
- **Total Tracks**: 94
- **Successfully Downloaded**: 69 (73%)
- **Failed Downloads**: 25 (mostly YouTube search/availability issues)
- **Stage 1 Duration**: ~2 minutes for 94 tracks
- **Stage 3 Duration**: ~15 minutes for 69 downloads

### Rate Limiting

Spotify API applies automatic rate limiting at ~429 requests. The pipeline implements exponential backoff (2s, 4s, 8s, 10s) and falls back to individual track requests if batch requests fail.

## Troubleshooting

### "Can't set locale" Warning

This is a system configuration warning and does not affect functionality. To suppress:

```bash
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

### Spotify Authentication Failed

Verify credentials are set:
```bash
echo $SPOTIPY_CLIENT_ID
echo $SPOTIPY_CLIENT_SECRET
```

### YouTube Download Failures

Some tracks may fail to find a matching YouTube video. These are logged in `state.json` under `downloader.failed_downloads`. Common causes:
- Track not available on YouTube
- No exact match found (duration mismatch)
- YouTube region restrictions

### File Already Exists

If a track is processed multiple times, duplicates are handled by appending a counter: `Artist - Title (1).mp3`

## State Management

All stages maintain resumable progress in `state.json`:

```json
{
  "metadata_extractor": {
    "last_processed_id": "...",
    "total_processed": 48,
    "successful": 94,
    "failed_ids": []
  },
  "downloader": {
    "last_downloaded_id": "...",
    "downloaded_count": 69,
    "failed_downloads": [...]
  },
  "tagger": {
    "last_processed_id": "...",
    "renamed_count": 69,
    "tagged_count": 69,
    "failed_ids": []
  }
}
```

Safely delete `state.json` to restart the entire pipeline from scratch.

## Files

### Production Scripts

- `main.py` - Pipeline orchestrator
- `metadata_extractor_v2.py` - Stage 1: Spotify metadata extraction
- `spotify_upgrade.py` - Stage 2: API optimization and validation
- `downloader.py` - Stage 3: YouTube download and basic tagging
- `tagger.py` - Stage 4: File rename and ID3 v2.4 tagging
- `deduplicate.py` - Scan and remove duplicate track IDs across all directories

### Data Files

- `songs_index.csv` - Master track index
- `song_sources.txt` - Input track URLs
- `state.json` - Pipeline progress state
- `metadata/` - Extracted metadata and artwork
- `data/` - Downloaded MP3 files

### Legacy Files

The following files are not needed for normal operation:
- `metadata-extractor.py` - Earlier version
- `metadata-spotify-api.py` - Earlier version
- `test_metadata_extractor.py` - Test file
- `verify_setup.py` - Setup verification
- `cleanup_failed.py` - Utility script

## License

Proprietary - Music synchronization pipeline

## Notes

- Always set environment variables before running the pipeline
- Internet connection required for Spotify API and YouTube downloads
- The pipeline is designed to be run sequentially (Stage 1 → 2 → 3 → 4)
- All stages support resumption and can be re-run safely
