#!/usr/bin/env python3
"""
sorter.py — Intel Community Forum Discussion Reconstructor (v2)
================================================================
Reads all raw flat Parquet chunks from raw/,
groups every message into its parent discussion thread,
filters out pure noise (< 5 words),
and writes output as Parquet files (100 discussions each) to processed/.

FULL INCREMENTAL MERGE SUPPORT:
  - Maintains an index: discussion_id → chunk_file
  - On delta updates, merges new messages INTO existing processed chunks
  - Marks modified chunks for re-upload
  - Tracks which raw files have already been processed

Usage:
    ~/.venv/bin/python3 sorter.py            # Run / resume (incremental)
    ~/.venv/bin/python3 sorter.py --force    # Reprocess everything from scratch
"""

import os
import sys
import json
import re
import glob
import argparse
import pandas as pd
from datetime import datetime
from collections import defaultdict

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
RAW_DIR     = os.path.join(BASE_DIR, "raw")
OUT_DIR     = os.path.join(BASE_DIR, "processed")
STATE_FILE  = os.path.join(BASE_DIR, ".sorter_state.json")
DISCUSSIONS_PER_CHUNK = 100
MIN_WORD_COUNT = 5
# ───────────────────────────────────────────────────────────────────────────────

os.makedirs(OUT_DIR, exist_ok=True)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def clean_html(raw: str) -> str:
    if not isinstance(raw, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_noise(text: str) -> bool:
    return len(text.split()) < MIN_WORD_COUNT


def extract_id(field) -> str | None:
    if not field or isinstance(field, float):
        return None
    if isinstance(field, dict):
        return str(field.get("id", ""))
    if isinstance(field, str):
        try:
            return str(json.loads(field).get("id", ""))
        except Exception:
            return None
    return None


def extract_login(author_field) -> str:
    if not author_field or isinstance(author_field, float):
        return "unknown"
    if isinstance(author_field, dict):
        return author_field.get("login", "unknown")
    if isinstance(author_field, str):
        try:
            return json.loads(author_field).get("login", "unknown")
        except Exception:
            return "unknown"
    return "unknown"


def row_to_message(row: pd.Series) -> dict:
    body_clean = clean_html(row.get("body", ""))
    return {
        "msg_id":      str(row.get("id", "")),
        "depth":       int(row.get("depth", 0)) if pd.notna(row.get("depth")) else 0,
        "author":      extract_login(row.get("author")),
        "post_time":   str(row.get("post_time", "")),
        "is_solution": bool(row.get("is_solution", False)),
        "body":        body_clean,
        "kudos":       int(json.loads(row["kudos"]).get("sum", 0))
                       if isinstance(row.get("kudos"), str) else 0,
    }


def finalize_messages(msgs: list[dict]) -> list[dict]:
    """Deduplicate, sort, and filter noise from a message list."""
    # Sort chronologically
    try:
        msgs = sorted(msgs, key=lambda m: m["post_time"])
    except Exception:
        pass

    # Deduplicate by msg_id
    seen = set()
    deduped = []
    for m in msgs:
        if m["msg_id"] not in seen:
            seen.add(m["msg_id"])
            deduped.append(m)

    # Filter noise (keep depth=0 root even if short)
    cleaned = [m for m in deduped if m["depth"] == 0 or not is_noise(m["body"])]
    return cleaned


def discussion_from_messages(msgs: list[dict], meta: dict) -> dict:
    cleaned = finalize_messages(msgs)
    return {
        "discussion_id": meta["discussion_id"],
        "subject":       meta["subject"],
        "board":         meta["board"],
        "post_time":     meta["post_time"],
        "has_solution":  any(m["is_solution"] for m in cleaned),
        "message_count": len(cleaned),
        "messages":      cleaned,
    }


def write_chunk(discussions: list[dict], chunk_filename: str) -> str:
    """Write a list of discussion dicts to a specific parquet file."""
    rows = []
    for d in discussions:
        rows.append({
            "discussion_id":    d["discussion_id"],
            "subject":          d["subject"],
            "board":            d["board"],
            "post_time":        d["post_time"],
            "has_solution":     d["has_solution"],
            "message_count":    d["message_count"],
            "messages_json":    json.dumps(d["messages"], ensure_ascii=False),
        })
    df = pd.DataFrame(rows)
    out_path = os.path.join(OUT_DIR, chunk_filename)
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
    return out_path


# ─── STATE ────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {
        "processed_files": [],
        "next_chunk_index": 1,
        "discussion_index": {},   # discussion_id -> chunk_filename
    }


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─── INDEX BUILDER ────────────────────────────────────────────────────────────

def build_index_from_processed() -> dict:
    """Scan all processed chunks and build discussion_id → chunk_filename index."""
    index = {}
    for f in sorted(glob.glob(os.path.join(OUT_DIR, "*.parquet"))):
        fname = os.path.basename(f)
        try:
            df = pd.read_parquet(f, columns=["discussion_id"])
            for did in df["discussion_id"]:
                index[str(did)] = fname
        except Exception:
            pass
    return index


def load_chunk(chunk_filename: str) -> list[dict]:
    """Load a processed chunk and return list of discussion dicts."""
    path = os.path.join(OUT_DIR, chunk_filename)
    df = pd.read_parquet(path)
    discussions = []
    for _, row in df.iterrows():
        discussions.append({
            "discussion_id": str(row["discussion_id"]),
            "subject":       str(row["subject"]),
            "board":         str(row["board"]),
            "post_time":     str(row["post_time"]),
            "has_solution":  bool(row["has_solution"]),
            "message_count": int(row["message_count"]),
            "messages":      json.loads(row["messages_json"]),
        })
    return discussions


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Reprocess ALL raw files from scratch")
    args = parser.parse_args()

    state = load_state()

    if args.force:
        print("⚠️  Force mode: reprocessing everything from scratch.")
        state = {"processed_files": [], "next_chunk_index": 1, "discussion_index": {}}
        # Clear existing processed files
        for f in glob.glob(os.path.join(OUT_DIR, "*.parquet")):
            os.remove(f)

    # Determine which raw files are new
    all_raw = sorted(glob.glob(os.path.join(RAW_DIR, "*.parquet")))
    already_done = set(state.get("processed_files", []))
    new_files = [f for f in all_raw if os.path.basename(f) not in already_done] if not args.force else all_raw

    if not new_files:
        print("✅ Nothing new to process. All raw files already sorted.")
        print(f"   Processed chunks in: {OUT_DIR}")
        return

    print(f"📦 Found {len(new_files)} new raw file(s) to process.")

    # Build or load the discussion index
    disc_index = state.get("discussion_index", {})
    if not disc_index and not args.force:
        print("🔍 Building discussion index from existing processed chunks...")
        disc_index = build_index_from_processed()
        print(f"   Indexed {len(disc_index)} existing discussions.")

    # ── Phase 1: Read all new messages, group by topic_id ────────────────────
    # new_messages[topic_id] = { "meta": {...}, "messages": [...] }
    new_messages = defaultdict(lambda: {"meta": None, "messages": []})

    for raw_file in new_files:
        fname = os.path.basename(raw_file)
        print(f"\n🔄 Processing {fname} ...")
        try:
            df = pd.read_parquet(raw_file)
        except Exception as e:
            print(f"  ❌ Failed to read {fname}: {e}")
            continue

        for _, row in df.iterrows():
            topic_id = extract_id(row.get("topic"))
            if not topic_id:
                topic_id = str(row.get("id", "unknown"))

            msg = row_to_message(row)
            if not msg["body"]:
                continue

            if new_messages[topic_id]["meta"] is None:
                board_id = extract_id(row.get("board", {})) or ""
                new_messages[topic_id]["meta"] = {
                    "discussion_id": topic_id,
                    "subject":       str(row.get("subject", "")) if msg["depth"] == 0 else "",
                    "board":         board_id,
                    "post_time":     msg["post_time"],
                }

            if msg["depth"] == 0 and not new_messages[topic_id]["meta"]["subject"]:
                new_messages[topic_id]["meta"]["subject"] = str(row.get("subject", ""))

            new_messages[topic_id]["messages"].append(msg)

        state["processed_files"].append(fname)
        print(f"  → {len(new_messages)} unique discussions found so far.")

    # ── Phase 2: Merge into existing chunks or create new ones ───────────────
    # Track which chunk files need re-writing
    dirty_chunks = {}        # chunk_filename -> { disc_id: discussion_dict, ... }
    brand_new_discussions = []  # discussions with no existing chunk

    merge_count = 0
    new_count = 0

    for topic_id, data in new_messages.items():
        if topic_id in disc_index:
            # MERGE: this discussion already exists in a processed chunk
            chunk_file = disc_index[topic_id]
            if chunk_file not in dirty_chunks:
                # Load chunk into memory
                existing = load_chunk(chunk_file)
                dirty_chunks[chunk_file] = {d["discussion_id"]: d for d in existing}

            # Merge new messages into existing discussion
            existing_disc = dirty_chunks[chunk_file].get(topic_id)
            if existing_disc:
                combined_msgs = existing_disc["messages"] + data["messages"]
                merged = discussion_from_messages(combined_msgs, data["meta"])
                # Preserve original subject/board if existing had better data
                if not merged["subject"] and existing_disc["subject"]:
                    merged["subject"] = existing_disc["subject"]
                if not merged["board"] and existing_disc["board"]:
                    merged["board"] = existing_disc["board"]
                dirty_chunks[chunk_file][topic_id] = merged
                merge_count += 1
            else:
                # Discussion ID was indexed but not found in chunk (shouldn't happen)
                brand_new_discussions.append(
                    discussion_from_messages(data["messages"], data["meta"])
                )
                new_count += 1
        else:
            # NEW: this discussion doesn't exist yet
            disc = discussion_from_messages(data["messages"], data["meta"])
            if disc["message_count"] > 0:
                brand_new_discussions.append(disc)
                new_count += 1

    print(f"\n📊 Delta Summary:")
    print(f"   Merged into existing threads : {merge_count}")
    print(f"   Brand new discussions         : {new_count}")
    print(f"   Chunks to rewrite             : {len(dirty_chunks)}")

    # ── Phase 3: Re-write dirty chunks ───────────────────────────────────────
    modified_chunks = []
    for chunk_file, disc_dict in dirty_chunks.items():
        discussions = list(disc_dict.values())
        write_chunk(discussions, chunk_file)
        modified_chunks.append(chunk_file)
        print(f"  ✏️  Updated: {chunk_file} ({len(discussions)} discussions)")

    # ── Phase 4: Write brand new discussions in fresh chunks ──────────────────
    chunk_index = state.get("next_chunk_index", 1)
    new_chunks = []

    for i in range(0, len(brand_new_discussions), DISCUSSIONS_PER_CHUNK):
        batch = brand_new_discussions[i : i + DISCUSSIONS_PER_CHUNK]
        chunk_file = f"discussions_chunk_{chunk_index:04d}.parquet"
        write_chunk(batch, chunk_file)
        new_chunks.append(chunk_file)
        # Update index
        for d in batch:
            disc_index[d["discussion_id"]] = chunk_file
        print(f"  ✅ Written: {chunk_file} ({len(batch)} discussions)")
        chunk_index += 1

    # ── Save state ────────────────────────────────────────────────────────────
    state["next_chunk_index"] = chunk_index
    state["discussion_index"] = disc_index
    save_state(state)

    total_written = len(modified_chunks) + len(new_chunks)
    print(f"\n🎉 Done! {total_written} chunk(s) written/updated in {OUT_DIR}")
    print(f"   State saved → {STATE_FILE}")

    # Write a list of modified chunks so the uploader knows what to push
    modified_list = modified_chunks + new_chunks
    modified_file = os.path.join(BASE_DIR, ".modified_chunks.json")
    with open(modified_file, "w") as f:
        json.dump(modified_list, f)
    print(f"   Modified chunks list → {modified_file}")


if __name__ == "__main__":
    main()
