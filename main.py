#!/usr/bin/env python3
"""
Main orchestrator for spotify-sync pipeline.
Run individual stages or the full pipeline.

Usage:
  python main.py                    # Run default stages (extract, download, tag)
  python main.py --stage 1          # Run metadata extraction only
  python main.py --stage 2          # Run Spotify upgrade only
  python main.py --stage 3          # Run download only
  python main.py --stage 4          # Run rename and tag only
  python main.py --help             # Show help
"""

import sys
import argparse
import subprocess
import json
from pathlib import Path

def run_stage(stage_num):
    """Run a specific stage"""
    
    if stage_num == 1:
        print("\n" + "="*70)
        print("STAGE 1: METADATA EXTRACTION (Spotify API + MusicBrainz)")
        print("="*70)
        result = subprocess.run([sys.executable, "metadata_extractor_v2.py"])
        return result.returncode == 0
    
    elif stage_num == 2:
        print("\n" + "="*70)
        print("STAGE 2: SPOTIFY API UPGRADE")
        print("="*70)
        print("This stage will:")
        print("  1. Auto-test Spotify API with 2 songs and 1 song")
        print("  2. Determine optimal batch size")
        print("  3. Upgrade all non-Spotify metadata rows")
        print("  4. Update CSV with new source + cover art")
        print("="*70)
        result = subprocess.run([sys.executable, "spotify_upgrade.py"])
        return result.returncode == 0
    
    elif stage_num == 3:
        print("\n" + "="*70)
        print("STAGE 3: DOWNLOAD MP3s (yt-dlp + tagging)")
        print("="*70)
        result = subprocess.run([sys.executable, "downloader.py"])
        return result.returncode == 0
    
    elif stage_num == 4:
        print("\n" + "="*70)
        print("STAGE 4: RENAME AND TAG (ID3 v2.4 + artwork)")
        print("="*70)
        result = subprocess.run([sys.executable, "tagger.py"])
        return result.returncode == 0
    
    else:
        print(f"Unknown stage: {stage_num}")
        return False

def load_config():
    """Load configuration from config.json"""
    config_file = "config.json"
    defaults = {
        "metadata": {"sources": ["spotify", "musicbrainz"]},
        "pipeline": {"default_stages": [1, 3, 4]}
    }
    
    if not Path(config_file).exists():
        return defaults
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        # Merge with defaults to handle missing keys
        if "metadata" not in config:
            config["metadata"] = defaults["metadata"]
        if "pipeline" not in config:
            config["pipeline"] = defaults["pipeline"]
        return config
    except Exception as e:
        print(f"Warning: Failed to load config.json: {e}")
        return defaults

def main():
    config = load_config()
    default_stages = config["pipeline"]["default_stages"]
    
    parser = argparse.ArgumentParser(
        description="spotify-sync orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Run default stages from config.json
  python main.py --stage 1          # Metadata extraction only
  python main.py --stage 2          # Spotify upgrade only (optional)
  python main.py --stage 3          # Download only
  python main.py --stage 4          # Rename and tag only
  python main.py --stage 1 --stage 2 --stage 3 --stage 4  # Run all stages
  python main.py --stage 3 --stage 4  # Download and tag

Configuration:
  Edit config.json to change:
    - Default stages to run
    - Metadata sources (spotify only, or spotify + musicbrainz)
        """
    )
    
    parser.add_argument(
        "--stage",
        type=int,
        choices=[1, 2, 3, 4],
        action="append",
        help="Stage(s) to run (can be used multiple times)"
    )
    
    args = parser.parse_args()
    
    # If no stages specified, use config default
    stages_to_run = args.stage if args.stage else default_stages
    
    print("\n" + "="*70)
    print(f"spotify-sync PIPELINE")
    print(f"Stages to run: {stages_to_run}")
    print(f"Metadata sources: {config['metadata']['sources']}")
    print("="*70)
    
    success_count = 0
    for stage in stages_to_run:
        if run_stage(stage):
            success_count += 1
        else:
            print(f"\nStage {stage} failed")
            break
    
    print("\n" + "="*70)
    if success_count == len(stages_to_run):
        print(f"All {len(stages_to_run)} stage(s) completed successfully!")
    else:
        print(f"Pipeline failed at stage {stages_to_run[success_count]}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
