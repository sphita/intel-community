#!/usr/bin/env python3
"""
pipeline.py — Master Controller
=================================
Single entry point for the entire Intel dataset pipeline.

Commands:
    ~/.venv/bin/python3 pipeline.py sort              # Group raw → discussions
    ~/.venv/bin/python3 pipeline.py sort --force      # Reprocess from scratch
    ~/.venv/bin/python3 pipeline.py upload            # Upload new processed chunks to HF
    ~/.venv/bin/python3 pipeline.py upload --force    # Re-upload everything
    ~/.venv/bin/python3 pipeline.py upload --status   # Show what's uploaded
    ~/.venv/bin/python3 pipeline.py run               # Sort THEN upload (full pipeline)
    ~/.venv/bin/python3 pipeline.py delta             # Fetch new msgs → sort → upload
    ~/.venv/bin/python3 pipeline.py stats             # Show directory summary
"""

import os
import sys
import glob
import json
import subprocess
import argparse

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
RAW_DIR       = os.path.join(BASE_DIR, "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
CRAWLER_DIR   = os.path.join(BASE_DIR, "api_crawler")
VENV_PYTHON   = sys.executable if sys.executable else os.path.expanduser("~/.venv/bin/python3")
CARGO         = "cargo"


def run(cmd: list, cwd=None):
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"❌ Command failed: {' '.join(cmd)}")
        sys.exit(result.returncode)


def cmd_stats():
    raw_files  = sorted(glob.glob(os.path.join(RAW_DIR, "*.parquet")))
    proc_files = sorted(glob.glob(os.path.join(PROCESSED_DIR, "*.parquet")))

    total_raw_size  = sum(os.path.getsize(f) for f in raw_files)
    total_proc_size = sum(os.path.getsize(f) for f in proc_files)

    pending = 0
    modified_file = os.path.join(BASE_DIR, ".modified_chunks.json")
    if os.path.exists(modified_file):
        with open(modified_file) as f:
            pending = len(json.load(f))

    sorter_state_file = os.path.join(BASE_DIR, ".sorter_state.json")
    discussions = 0
    if os.path.exists(sorter_state_file):
        with open(sorter_state_file) as f:
            state = json.load(f)
            discussions = (state.get("next_chunk_index", 1) - 1) * 100  # approx

    print()
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  📊  Intel Dataset Pipeline — Stats")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  Raw parquet chunks    : {len(raw_files):>6}  ({total_raw_size / 1e9:.2f} GB)")
    print(f"  Processed chunks      : {len(proc_files):>6}  ({total_proc_size / 1e6:.1f} MB)")
    print(f"  Discussions (approx)  : {discussions:>6}")
    print(f"  Uploaded to HF        : {len(proc_files) - pending:>6}")
    print(f"  Pending upload        : {pending:>6}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print()


def cmd_sort(force=False):
    print("📦 Step 1: Sorting raw messages into discussions...")
    cmd = [VENV_PYTHON, os.path.join(BASE_DIR, "sorter.py")]
    if force:
        cmd.append("--force")
    run(cmd)


def cmd_upload(force=False, status=False):
    cmd = [VENV_PYTHON, os.path.join(BASE_DIR, "uploader.py")]
    if force:
        cmd.append("--force")
    if status:
        cmd.append("--status")
    run(cmd)


def cmd_delta():
    """Fetch new messages from Intel API → sort → upload."""
    print("🔄 Delta Sync: Fetching new messages from Intel Community...")
    run([CARGO, "run", "--release", "--bin", "delta_sync"], cwd=CRAWLER_DIR)
    print()
    print("📦 Sorting new delta messages into discussions...")
    run([VENV_PYTHON, os.path.join(BASE_DIR, "sorter.py")])
    print()
    print("🚀 Uploading updated chunks to HuggingFace...")
    run([VENV_PYTHON, os.path.join(BASE_DIR, "uploader.py")])


def cmd_run():
    """Full pipeline: sort everything, then upload everything new."""
    cmd_sort()
    print()
    cmd_upload()


def main():
    parser = argparse.ArgumentParser(description="Intel Dataset Pipeline Controller")
    subparsers = parser.add_subparsers(dest="command")

    p_sort = subparsers.add_parser("sort", help="Group raw parquet into discussions")
    p_sort.add_argument("--force", action="store_true")

    p_upload = subparsers.add_parser("upload", help="Upload processed chunks to HuggingFace")
    p_upload.add_argument("--force",  action="store_true")
    p_upload.add_argument("--status", action="store_true")

    subparsers.add_parser("run",   help="Sort + Upload (full pipeline)")
    subparsers.add_parser("delta", help="Fetch deltas → Sort → Upload")
    subparsers.add_parser("stats", help="Show pipeline statistics")

    args = parser.parse_args()

    if args.command == "sort":
        cmd_sort(force=args.force)
    elif args.command == "upload":
        cmd_upload(force=args.force, status=args.status)
    elif args.command == "run":
        cmd_run()
    elif args.command == "delta":
        cmd_delta()
    elif args.command == "stats":
        cmd_stats()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
