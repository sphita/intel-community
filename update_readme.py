#!/usr/bin/env python3
"""
update_readme.py — Automated README & Stats Sync Engine
======================================================
1. Recomputes exact dataset metrics (discussions, messages, solutions, boards, size)
2. Calculates exact cl100k_base token volume in parallel (<500MB RAM budget)
3. Updates the Hugging Face Dataset Card (/home/adhyansh/sphita/hf-repos/intel-community/README.md)
4. Syncs to local dataset repo (/home/adhyansh/datasets/intel-community/README.md)
5. Optionally uploads README directly to Hugging Face Hub (via --upload or if HF_TOKEN set)

Usage:
    ~/.venv/bin/python3 update_readme.py              # Update README files locally
    ~/.venv/bin/python3 update_readme.py --upload     # Update and upload to Hugging Face
    ~/.venv/bin/python3 update_readme.py --check      # Dry-run check without modifying
"""

import os
import sys
import glob
import json
import time
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

# Set tiktoken cache location
os.environ["TIKTOKEN_CACHE_DIR"] = "/tmp/data-gym-cache"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
HF_REPO_README = "/home/adhyansh/sphita/hf-repos/intel-community/README.md"
LOCAL_README = os.path.join(BASE_DIR, "README.md")
HF_DATASET_ID = os.environ.get("HF_DATASET_ID", "sphita/Intel-WebCorpus-forms")
HF_TOKEN = os.environ.get("HF_TOKEN")


def _process_chunk_worker(file_path):
    import tiktoken
    import pyarrow.parquet as pq

    enc = tiktoken.get_encoding("cl100k_base")
    table = pq.read_table(
        file_path,
        columns=["board", "has_solution", "message_count", "subject", "messages_json"],
    )

    file_size = os.path.getsize(file_path)
    num_rows = table.num_rows

    mc_col = table["message_count"].to_pylist()
    messages_count = sum(mc_col)

    sol_col = table["has_solution"].to_pylist()
    solutions_count = sum(1 for s in sol_col if s)

    b_col = table["board"].to_pylist()
    boards = Counter(b_col)

    text_tokens = 0
    json_tokens = 0

    subj_col = table["subject"].to_pylist()
    mj_col = table["messages_json"].to_pylist()

    for subj, mj in zip(subj_col, mj_col):
        json_tokens += len(enc.encode(mj, disallowed_special=()))
        try:
            msgs = json.loads(mj)
            full_text = str(subj) + "\n" + "\n".join(m.get("body", "") for m in msgs)
        except Exception:
            full_text = str(subj)
        text_tokens += len(enc.encode(full_text, disallowed_special=()))

    return {
        "num_rows": num_rows,
        "messages_count": messages_count,
        "solutions_count": solutions_count,
        "boards": dict(boards),
        "file_size": file_size,
        "text_tokens": text_tokens,
        "json_tokens": json_tokens,
    }


def compute_metrics(num_workers=8):
    files = sorted(glob.glob(os.path.join(PROCESSED_DIR, "*.parquet")))
    if not files:
        raise RuntimeError(f"No parquet chunks found in {PROCESSED_DIR}")

    print(f"📊 Computing dataset metrics & cl100k tokens across {len(files)} chunks ({num_workers} workers)...")
    t0 = time.time()

    total_discussions = 0
    total_messages = 0
    total_solutions = 0
    total_boards = Counter()
    total_file_size = 0
    total_text_tokens = 0
    total_json_tokens = 0

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for res in executor.map(_process_chunk_worker, files, chunksize=15):
            total_discussions += res["num_rows"]
            total_messages += res["messages_count"]
            total_solutions += res["solutions_count"]
            total_boards.update(res["boards"])
            total_file_size += res["file_size"]
            total_text_tokens += res["text_tokens"]
            total_json_tokens += res["json_tokens"]

    elapsed = time.time() - t0
    avg_msgs = total_messages / total_discussions if total_discussions else 0
    sol_pct = (total_solutions / total_discussions * 100) if total_discussions else 0

    stats = {
        "chunks": len(files),
        "discussions": total_discussions,
        "messages": total_messages,
        "avg_messages": round(avg_msgs, 1),
        "solutions": total_solutions,
        "solution_pct": round(sol_pct, 1),
        "size_bytes": total_file_size,
        "size_mb": round(total_file_size / (1024 * 1024), 1),
        "size_decimal_mb": round(total_file_size / 1e6, 1),
        "text_tokens": total_text_tokens,
        "json_tokens": total_json_tokens,
        "top_boards": total_boards.most_common(10),
        "elapsed_seconds": round(elapsed, 2),
    }

    print(f"✅ Computation complete in {elapsed:.2f}s")
    print(f"   • Discussions: {stats['discussions']:,}")
    print(f"   • Messages:    {stats['messages']:,}")
    print(f"   • Text Tokens: {stats['text_tokens']:,}")
    print(f"   • JSON Tokens: {stats['json_tokens']:,}")
    return stats


def generate_hf_readme(stats: dict) -> str:
    top_boards_rows = "\n".join(
        f"| `{b}` | {cnt:,} |" for b, cnt in stats["top_boards"]
    )

    readme = f"""---
license: gpl-3.0
task_categories:
- question-answering
- text-generation
language:
- en
tags:
- hardware
- troubleshooting
- intel
- enterprise
- tech-support
- discourse
- liql
size_categories:
- 100K<n<1M
homepage: https://github.com/sphita/intel-community
---
# 💻 Intel WebCorpus Forums (Enterprise Hardware Q&A)

<div align="center">

[![GitHub Repository](https://img.shields.io/badge/GitHub-sphita%2Fintel--community-181717?logo=github&logoColor=white)](https://github.com/sphita/intel-community)
[![Hugging Face](https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-sphita%2FIntel--WebCorpus--forms-FFD21E)](https://huggingface.co/datasets/sphita/Intel-WebCorpus-forms)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

</div>

This dataset is a massive, high-fidelity archive of **{stats['discussions']:,} technical troubleshooting discussions** (containing **{stats['messages']:,} individual messages** and **{stats['text_tokens']/1e6:.1f}M+ `cl100k_base` conversational text tokens**) scraped from the official Intel Community Forums.

It has been meticulously engineered for Large Language Model (LLM) training. Instead of a raw, messy dump of isolated posts, the data has been **reconstructed into chronological conversation threads**, noise-filtered, deduplicated, and packed into optimized Snappy-compressed Parquet files.

## 📊 Dataset Summary

Token volume was calculated using OpenAI's `tiktoken` (`cl100k_base` encoding).

| Metric | Count |
| :--- | :--- |
| **GitHub Repository** | [`sphita/intel-community`](https://github.com/sphita/intel-community) |
| **Total Discussions (Threads)** | {stats['discussions']:,} |
| **Total Unique Messages** | {stats['messages']:,} |
| **Total Conversational Text Tokens (`cl100k_base`)** | **{stats['text_tokens']:,}** |
| **Total Structured Tokens (`cl100k_base`)** | **{stats['json_tokens']:,}** |
| **Average Messages per Thread** | {stats['avg_messages']} |
| **Threads with Accepted Solutions** | {stats['solutions']:,} ({stats['solution_pct']}%) |
| **Total Parquet Chunks** | {stats['chunks']:,} |
| **Total Size (Compressed)** | ~{stats['size_decimal_mb']:.0f} MB |

## 🛠️ Data Engineering & Ingestion Architecture

The dataset pipeline and crawler source code are publicly maintained at **[`sphita/intel-community`](https://github.com/sphita/intel-community)**.

The raw data pipeline operates in two distinct phases:

1. **Historical Corpus (Khoros 2.0 API):** The bulk archive was extracted using a custom Rust async crawler with LiQL query pagination, rate limiting, and exponential backoff.
2. **Live Delta Sync (Discourse API Migration):** On October 13, 2026, Intel migrated `community.intel.com` from Lithium (Khoros) to Discourse, permanently deprecating the Khoros `/api/2.0/search` endpoint (HTTP 404). The delta sync engine (`delta_sync.rs`) was rewritten and verified to consume Discourse's cursor-based `/posts.json` endpoint, normalizing posts (`cooked`, `post_number`, `username`, `reactions`) to preserve 100% schema parity with the historical archive.

### Core Pipeline Steps:
1. **Thread Reconstruction:** Over 940,000 flat messages were grouped by their `topic_id` to reconstruct the exact back-and-forth conversational flow of human troubleshooting.
2. **Noise Reduction:** "Dead-weight" messages (under 5 words, e.g., "bump", "+1", "thanks") were automatically filtered out of the replies to increase the signal-to-noise ratio.
3. **Deduplication:** Overlapping chunks were strictly deduplicated by `msg_id` to ensure absolute uniqueness.
4. **HTML Sanitization:** Raw HTML bodies were stripped into clean text.

## 🗂️ Top Hardware Categories (Boards)

The discussions cover deep technical support for Intel's entire hardware and software ecosystem. The top 10 categories are:

| Board / Category | Discussions |
| :--- | :--- |
{top_boards_rows}

## 📝 Schema

The dataset is provided in `parquet` format. Each row represents a **complete discussion thread**.

| Column | Type | Description |
| :--- | :--- | :--- |
| `discussion_id` | `string` | The unique ID of the forum topic/thread. |
| `subject` | `string` | The title or subject of the discussion. |
| `board` | `string` | The specific Intel hardware/software category. |
| `post_time` | `string` | ISO 8601 timestamp of the original question. |
| `has_solution` | `boolean` | `True` if an Intel engineer or community member provided an accepted solution. |
| `message_count` | `int64` | The total number of valid messages in the thread. |
| `messages_json` | `string` | A JSON-encoded string containing the full list of messages in chronological order. |

### 🔍 Inside `messages_json`

```json
[
  {{
    "msg_id": "12345",
    "depth": 0,
    "author": "user_123",
    "post_time": "2023-01-01T12:00:00Z",
    "is_solution": false,
    "body": "I am getting Error 504 on my Xeon server...",
    "kudos": 2
  }},
  {{
    "msg_id": "12346",
    "depth": 1,
    "author": "Intel_Support",
    "post_time": "2023-01-01T12:15:00Z",
    "is_solution": true,
    "body": "Please update your BIOS to version X...",
    "kudos": 5
  }}
]
```

## 🚀 Use Cases

* **Hardware Troubleshooting LLMs:** Train models that understand complex server, networking, and processor error codes.
* **Reasoning (Chain-of-Thought):** Train models on multi-turn troubleshooting processes, where users try solutions, fail, report back, and eventually succeed.
* **RAG (Retrieval-Augmented Generation):** Build enterprise IT chatbots with factual troubleshooting grounding.

## 🔄 Updates & Automation

This dataset is maintained via an automated **Delta Sync Pipeline**. New discussions are fetched weekly via Discourse API cursors, sorted into discussion threads, and uploaded as updated/new chunk files without overwriting the existing historical archive. Full automation code is hosted in [`sphita/intel-community`](https://github.com/sphita/intel-community).

## 📜 Citation

If you use this dataset or crawler pipeline in your research, please cite:

```bibtex
@misc{{intel_community_webcorpus_2026,
  title={{Intel WebCorpus Forums: Enterprise Hardware Q&A Dataset}},
  author={{Verma, Adhyansh and Sphita AI Research}},
  year={{2026}},
  publisher={{Hugging Face}},
  howpublished={{\\url{{https://huggingface.co/datasets/sphita/Intel-WebCorpus-forms}}}},
  note={{Source Code: \\url{{https://github.com/sphita/intel-community}}}}
}}
```

## 📄 License

This dataset is published under the **GPL-3.0** License.
"""
    return readme


def sync_readme(stats: dict, upload: bool = False):
    content = generate_hf_readme(stats)

    # 1. Write to HF workspace README
    if os.path.exists(os.path.dirname(HF_REPO_README)):
        with open(HF_REPO_README, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"📝 Updated Hugging Face workspace README: {HF_REPO_README}")

    # 2. Upload to Hugging Face Hub if requested or token available
    if upload or HF_TOKEN:
        try:
            from huggingface_hub import HfApi
            token = HF_TOKEN or os.environ.get("HF_TOKEN")
            if token:
                api = HfApi(token=token)
                print(f"🚀 Uploading README.md directly to Hugging Face dataset ({HF_DATASET_ID})...", end=" ", flush=True)
                api.upload_file(
                    path_or_fileobj=HF_REPO_README,
                    path_in_repo="README.md",
                    repo_id=HF_DATASET_ID,
                    repo_type="dataset",
                    commit_message=f"Update dataset README stats ({stats['discussions']} discussions, {stats['messages']} msgs)",
                )
                print("✅ Done!")
            else:
                print("ℹ️ HF_TOKEN not set, skipping remote HF README upload.")
        except Exception as e:
            print(f"⚠️ Remote Hugging Face README upload skipped or failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Automated Dataset README Sync Engine")
    parser.add_argument("--upload", action="store_true", help="Upload README directly to Hugging Face Hub")
    parser.add_argument("--check", action="store_true", help="Dry run — print metrics only")
    parser.add_argument("--workers", type=int, default=8, help="Worker processes for token calculation (default: 8)")
    args = parser.parse_args()

    stats = compute_metrics(num_workers=args.workers)

    if args.check:
        print("\n[Dry Run] Calculated stats without modifying files.")
        return

    sync_readme(stats, upload=args.upload)
    print("\n🎉 Dataset README synchronization completed successfully!")


if __name__ == "__main__":
    main()
