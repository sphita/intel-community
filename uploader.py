#!/usr/bin/env python3
"""
uploader.py — Intel Dataset HuggingFace Uploader (On-Demand, Smart)
====================================================================
Uploads processed/ discussion chunks to HuggingFace.
Smart mode: only uploads chunks that were modified by the last sorter run.

Usage:
    ~/.venv/bin/python3 uploader.py                   # Upload modified chunks only
    ~/.venv/bin/python3 uploader.py --all              # Upload ALL processed chunks
    ~/.venv/bin/python3 uploader.py --status           # Show what's on HF
"""

import os
import sys
import json
import glob
import argparse
from datetime import datetime, timezone

try:
    from huggingface_hub import HfApi
except ImportError:
    print("❌ huggingface_hub not installed. Run: ~/.venv/bin/pip install huggingface_hub")
    sys.exit(1)

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR  = os.path.join(BASE_DIR, "processed")
MODIFIED_FILE  = os.path.join(BASE_DIR, ".modified_chunks.json")

# Load local .env if present
env_file = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_file):
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

HF_TOKEN       = os.environ.get("HF_TOKEN")
HF_DATASET_ID  = os.environ.get("HF_DATASET_ID", "sphita/intel-community")
# ───────────────────────────────────────────────────────────────────────────────


def get_api() -> HfApi:
    if not HF_TOKEN:
        print("❌ HF_TOKEN environment variable not set.")
        print("   Export it: export HF_TOKEN=hf_yourtoken")
        sys.exit(1)
    return HfApi(token=HF_TOKEN)


def show_status():
    all_files = sorted(glob.glob(os.path.join(PROCESSED_DIR, "*.parquet")))
    modified = []
    if os.path.exists(MODIFIED_FILE):
        with open(MODIFIED_FILE) as f:
            modified = json.load(f)

    print(f"\n📊 Upload Status — {HF_DATASET_ID}")
    print(f"   Processed chunks on disk : {len(all_files)}")
    print(f"   Pending upload (modified): {len(modified)}")
    if modified:
        for m in modified:
            print(f"     ⏳ {m}")
    print()


def upload_modified(api: HfApi):
    """Upload only chunks that the sorter marked as modified."""
    if not os.path.exists(MODIFIED_FILE):
        print("✅ No modified chunks to upload. Run the sorter first.")
        return

    with open(MODIFIED_FILE) as f:
        modified = json.load(f)

    if not modified:
        print("✅ No modified chunks to upload. Dataset is fully synced.")
        return

    # Ensure repo exists
    try:
        api.dataset_info(HF_DATASET_ID)
    except Exception:
        api.create_repo(repo_id=HF_DATASET_ID, repo_type="dataset", exist_ok=True)

    print(f"\n🚀 Uploading {len(modified)} modified chunk(s) to HuggingFace...")

    for chunk_file in modified:
        file_path = os.path.join(PROCESSED_DIR, chunk_file)
        if not os.path.exists(file_path):
            print(f"  ⚠️  {chunk_file} not found on disk, skipping.")
            continue
        size = os.path.getsize(file_path) // 1024
        print(f"  ↑ Uploading {chunk_file} ({size} KB)...", end=" ", flush=True)
        try:
            api.upload_file(
                path_or_fileobj=file_path,
                path_in_repo=f"data/{chunk_file}",
                repo_id=HF_DATASET_ID,
                repo_type="dataset",
                commit_message=f"Update {chunk_file} with new messages",
            )
            print("✅")
        except Exception as e:
            print(f"❌ {e}")

    # Clear the modified list
    os.remove(MODIFIED_FILE)
    print(f"\n🎉 Upload complete! Dataset: https://huggingface.co/datasets/{HF_DATASET_ID}")


def upload_all(api: HfApi):
    """Bulk upload all processed chunks."""
    try:
        api.dataset_info(HF_DATASET_ID)
    except Exception:
        api.create_repo(repo_id=HF_DATASET_ID, repo_type="dataset", exist_ok=True)

    print(f"\n🚀 Uploading ALL processed chunks to HuggingFace (bulk)...")
    api.upload_folder(
        folder_path=PROCESSED_DIR,
        repo_id=HF_DATASET_ID,
        repo_type="dataset",
        path_in_repo="data",
        commit_message="Bulk upload all discussion chunks",
    )
    # Clear modified list if it exists
    if os.path.exists(MODIFIED_FILE):
        os.remove(MODIFIED_FILE)
    print(f"\n🎉 Bulk upload complete!")


def main():
    parser = argparse.ArgumentParser(description="Intel Dataset HuggingFace Uploader")
    parser.add_argument("--status", action="store_true", help="Show upload status")
    parser.add_argument("--all",    action="store_true", help="Upload ALL chunks (bulk)")
    # Keep --force as alias for --all for backwards compat
    parser.add_argument("--force",  action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    api = get_api()

    if args.all or args.force:
        upload_all(api)
    else:
        upload_modified(api)


if __name__ == "__main__":
    main()
