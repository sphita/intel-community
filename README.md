# 💻 Intel Community Crawler & Pipeline

End-to-end data pipeline and high-performance crawler for extracting, thread-reconstructing, and syncing troubleshooting discussions from the [Intel Community Forums](https://community.intel.com) to Hugging Face ([sphita/intel-community](https://huggingface.co/datasets/sphita/intel-community)).

---

## 🏗️ Architecture

```
                                  Intel Community Forums (Khoros 2.0 API)
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 1. Rust Async Crawler (`api_crawler/`)                                                      │
│    • High-concurrency worker pool using Tokio & Reqwest                                     │
│    • Lithium Query Language (LiQL) pagination with rate limiting & backoff                  │
│    • TUI live dashboard (`cargo run --release`)                                             │
│    • Smart Delta Sync (`cargo run --release --bin delta_sync`)                              │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               │ Raw JSONL
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 2. Parquet Bridge & Discussion Sorter (`sorter.py`)                                         │
│    • Converts raw JSONL payloads into columnar Parquet (`raw/`)                             │
│    • Reconstructs flat messages into chronological conversational threads by `topic_id`      │
│    • Filters noise (< 5 words) and sanitizes HTML                                           │
│    • Incremental chunk merge (100 discussions per chunk into `processed/`)                  │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               │ Processed Parquet Chunks
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 3. Smart Hugging Face Uploader (`uploader.py`)                                              │
│    • Tracks modified chunks and uploads only delta changes to Hugging Face Hub              │
│    • Pushes Snappy-compressed Parquet datasets directly to `sphita/intel-community`         │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
.
├── intel                   # Interactive CLI Command Center (Bash)
├── pipeline.py             # Python Master Pipeline Controller
├── sorter.py               # Incremental Discussion Thread Reconstructor
├── uploader.py             # Smart Hugging Face Parquet Uploader
├── uploader_daemon.py      # Background continuous sync daemon
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variables template
├── .gitignore              # Data, state, and secret exclusion rules
└── api_crawler/            # Rust Khoros 2.0 API Crawler
    ├── Cargo.toml
    ├── Cargo.lock
    └── src/
        ├── main.rs         # TUI-based full archive crawler
        └── bin/
            └── delta_sync.rs # Incremental delta fetcher
```

---

## 🚀 Quick Start

### 1. Prerequisites

- **Rust & Cargo** (1.70+):
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Python** (3.10+):
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```

### 2. Configure Credentials

Copy `.env.example` to `.env` and fill in your Hugging Face write token:

```bash
cp .env.example .env
```

```env
HF_TOKEN="hf_your_token_here"
HF_DATASET_ID="sphita/intel-community"
```

### 3. Build the Rust Crawler

```bash
cd api_crawler
cargo build --release
cd ..
```

---

## 🎮 Usage

### Interactive CLI (`./intel`)

Launch the unified command center:

```bash
./intel
```

Options:
- `1`   — **Fetch**: Run incremental delta sync from Intel API.
- `12`  — **Fetch + Sort**: Fetch new messages and reconstruct into discussion threads.
- `123` — **Full Pipeline**: Fetch, sort into discussions, and push modified chunks to Hugging Face.
- `s`   — **Stats**: Display current pipeline message counts and chunk metrics.
- `u`   — **Upload Only**: Push pending processed chunks to Hugging Face.

### Scripted Pipeline (`pipeline.py`)

Run individual stages or automated cron workflows directly:

```bash
# View pipeline statistics
python3 pipeline.py stats

# Run incremental delta sync (fetch -> sort -> upload)
python3 pipeline.py delta

# Sort raw Parquet messages into discussion threads
python3 pipeline.py sort

# Upload modified chunks to Hugging Face
python3 pipeline.py upload

# Check upload status
python3 pipeline.py upload --status
```

---

## 📊 Dataset Reference

The output dataset is hosted on Hugging Face:
🔗 **[sphita/intel-community](https://huggingface.co/datasets/sphita/intel-community)**

Each discussion row in the final Parquet dataset contains:
- `discussion_id`: Unique identifier for the thread
- `subject`: Thread title
- `board`: Intel hardware/software category (e.g., `processors`, `graphics`)
- `post_time`: Original question timestamp (ISO 8601)
- `has_solution`: Boolean flag indicating whether an accepted answer was marked
- `message_count`: Total message count in the discussion
- `messages_json`: Serialized JSON array of all chronological replies

---

## 📄 License

Apache License 2.0
