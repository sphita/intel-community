#!/usr/bin/env python3
import os
import time
import glob
import json
import pandas as pd
from huggingface_hub import HfApi

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(BASE_DIR, "api_crawler")
DEST_DIR = os.path.join(BASE_DIR, "raw")

# Load local .env if present
env_file = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_file):
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

HF_DATASET_ID = os.environ.get("HF_DATASET_ID", "sphita/intel-community")
HF_TOKEN = os.environ.get("HF_TOKEN")

os.makedirs(DEST_DIR, exist_ok=True)

def process_and_upload():
    if not HF_TOKEN:
        print("⚠️ Warning: HF_TOKEN environment variable not set. Will convert to parquet but skip HF upload.")
        api = None
    else:
        api = HfApi(token=HF_TOKEN)
        try:
            api.dataset_info(HF_DATASET_ID)
        except Exception:
            print(f"Dataset {HF_DATASET_ID} not found. Creating it...")
            try:
                api.create_repo(repo_id=HF_DATASET_ID, repo_type="dataset", exist_ok=True)
                print("Dataset created successfully.")
            except Exception as e:
                print(f"Failed to create dataset repo: {e}")
                print("Proceeding anyway, but upload might fail.")
    
    while True:
        # Find all jsonl files (chunks + delta updates)
        files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.jsonl")))
        
        files_to_process = []
        now = time.time()
        
        for i, file_path in enumerate(files):
            # If it's a delta update file, always process it immediately.
            if "delta_updates" in file_path:
                # wait slightly to ensure writes are done
                if now - os.path.getmtime(file_path) > 5:
                    files_to_process.append(file_path)
            # If it's a chunk file...
            else:
                # If it's the absolute last chunk file, wait until it hasn't been modified for 60s
                # meaning the crawler is finished or paused.
                if i == len(files) - 1:
                    mtime = os.path.getmtime(file_path)
                    if (now - mtime) > 60:
                        files_to_process.append(file_path)
                else:
                    # Older chunks can be processed immediately
                    files_to_process.append(file_path)
        
        for file_path in files_to_process:
            filename = os.path.basename(file_path)
            basename = os.path.splitext(filename)[0]
            
            # If it's a delta update, append a timestamp so we don't overwrite previous deltas
            if "delta_updates" in basename:
                timestamp_str = time.strftime("%Y%m%d_%H%M%S")
                basename = f"delta_updates_{timestamp_str}"
                
            parquet_path = os.path.join(DEST_DIR, f"{basename}.parquet")
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Processing {filename} -> {basename}.parquet ...")
            try:
                records = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))
                            
                if not records:
                    print(f"  -> File {filename} is empty. Deleting.")
                    os.remove(file_path)
                    continue

                df = pd.DataFrame(records)
                
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                        
                df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
                print(f"  -> Converted to {parquet_path}")
                
                if api:
                    print(f"  -> Uploading to HuggingFace ({HF_DATASET_ID})...")
                    api.upload_file(
                        path_or_fileobj=parquet_path,
                        path_in_repo=f"data/{basename}.parquet",
                        repo_id=HF_DATASET_ID,
                        repo_type="dataset"
                    )
                    print("  -> Upload successful.")
                
                os.remove(file_path)
                print(f"  -> Deleted original {filename}")
                
            except Exception as e:
                print(f"  -> ❌ Error processing {filename}: {e}")
                
        time.sleep(10)

if __name__ == "__main__":
    print("========================================")
    print(" 🚀 Intel Parquet + HF Upload Daemon (v2)")
    print(f" Monitoring : {SOURCE_DIR}")
    print(f" Output     : {DEST_DIR}")
    print(f" HF Dataset : {HF_DATASET_ID}")
    print(" Smart Sync : Detects completed chunks & delta updates")
    print("========================================")
    process_and_upload()
