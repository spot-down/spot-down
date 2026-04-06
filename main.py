#!/usr/bin/env python3
"""
Main orchestrator for spotify-sync pipeline.
Run individual stages or the full pipeline.

Usage:
  python main.py                    # Run all stages sequentially
  python main.py --stage 1          # Run metadata extraction only
  python main.py --stage 2          # Run Spotify upgrade only
  python main.py --stage 3          # Run download only
  python main.py --stage 4          # Run rename and tag only
  python main.py --help             # Show help
"""

import sys
import argparse
import subprocess
from pathlib import Path

def run_stage(stage_num):
    """Run a specific stage"""
    
    if stage_num == 1:
        print("\n" + "="*70)
        print("STAGE 1: METADATA EXTRACTION (MusicBrainz + Spotify pending)")
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

def main():
    parser = argparse.ArgumentParser(
        description="spotify-sync orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Run all stages (metadata, upgrade, download, tag)
  python main.py --stage 1          # Metadata extraction only
  python main.py --stage 2          # Spotify upgrade only  
  python main.py --stage 3          # Download only
  python main.py --stage 4          # Rename and tag only
  python main.py --stage 2 --stage 3  # Upgrade and download
  python main.py --stage 3 --stage 4  # Download and tag
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
    
    # If no stages specified, run all
    stages_to_run = args.stage if args.stage else [1, 2, 3, 4]
    
    print("\n" + "="*70)
    print(f"spotify-sync PIPELINE")
    print(f"Stages to run: {stages_to_run}")
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
