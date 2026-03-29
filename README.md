# 📚 Audiobookshelf Skimmer

> **Fix your audiobook metadata by "skimming" the first few minutes with AI.**

Audiobookshelf Skimmer is a precision tool designed to automatically correct Title, Author, Narrator, and Publisher information in your Audiobookshelf library. It works by "skimming" the first 120 seconds of a book—capturing the intro where these details are usually spoken—and using a combination of local transcription (MLX) and local Large Language Models (LLMs) to identify and update the metadata.

---

## ✨ Features

- **🚀 Optimized Streaming**: Uses the Audiobookshelf `/play` endpoint and `ffmpeg` to capture precisely 120s of audio. No need to download entire 200MB+ files just to read the title.
- **🧠 Local AI Power**: Runs entirely on your machine.
  - **Transcription**: Uses NVIDIA's **Parakeet** model via Apple's **MLX** framework.
  - **Correction**: Uses **mlx-lm** to run models like Llama 3 locally.
  - **Offline Caching**: Models are automatically loaded directly from the local cache on subsequent runs, avoiding redundant network checks.
- **🔄 Memory-Efficient Batching**: Processes books in batches of 10, swapping models in and out of memory to stay within an 8GB RAM target.
- **🛡️ Smart & Safe**:
  - **ASIN Skip**: Automatically skips books that already have an ASIN (assumed to be already identified).
  - **Hallucination Detection**: Filters out LLM responses that invent titles or authors not found in the transcript. Values that match existing metadata are always accepted (the transcript may simply lack an intro).
  - **Dry Run Mode**: Preview all changes before they are committed. Dry-run results are remembered — subsequent runs skip them automatically, so you never re-transcribe the same book twice.
- **🎯 Specialized Refinement**:
  - **Force Tag**: Use `--force-tag` to strictly focus on a set of problematic books. It clears an existing (potentially wrong) ASIN and performs a full re-process.
    - I added this to work with <https://github.com/scruffynerf/audiobookshelf-duration-checker> to fix books that had really wrong durations and incorrect metadata including an ASIN.
  - **Blind Extraction**: Use `--no-metadatahints` to force the AI to rely exclusively on the transcript, ignoring existing metadata.

---

## 🚀 Quick Start

### 1. Requirements

- **macOS with Apple Silicon** (M1/M2/M3/M4) for MLX acceleration.
- **Python 3.10+** (`ffmpeg` is handled automatically by `static-ffmpeg`).
- A running instance of **Audiobookshelf**.

### 2. Installation

We recommend using [uv](https://docs.astral.sh/uv/) for the fastest experience:

```bash
git clone https://github.com/scruffynerf/audiobookshelf-skimmer.git
cd audiobookshelf-skimmer
uv sync
```

### 3. Configuration

Copy the example configuration and fill in your details:

```bash
cp config.json.example config.json
```

Edit `config.json`:

- `abs_url`: Your Audiobookshelf server URL (e.g., `http://192.168.1.50:3789`).
- `abs_api_key`: Your ABS API Key (found in Settings -> Users -> API Keys).
- `llm_model`: The MLX model ID from Hugging Face (e.g., `mlx-community/Llama-3.2-3B-Instruct-4bit`).
- `batch_size`: Number of books to process in one memory-managed batch (default: `10`).
- `slice_duration_sec`: Length of the audio snippet to transcribe (default: `120`).
- `processed_tag`: Tag applied to books after successful processing (default: `ai-skimmed`).
- `ai_retries`: Number of times the LLM will retry if hallucination is detected (default: `1`).
- `dry_run`: If `true`, no changes will be written to Audiobookshelf (default: `true`).
- `llm_system_prompt`: A custom instruction set for the LLM to improve extraction accuracy.

---

## 🖥️ Usage

The project provides a clean `skimmer` command via `uv run`:

### Run a Dry Run (Recommended First Step)

```bash
uv run skimmer --dry-run
```

Dry-run results are saved to the database and **skipped on subsequent runs**. Once you're happy with the preview, apply the changes for real:

```bash
uv run skimmer --redo-dry-run
```

This re-processes only the books that were previously dry-run'd, this time writing to Audiobookshelf.

### Perform a Full Sync (No Preview)

```bash
uv run skimmer
```

### Revert a Change

If you're unhappy with a change, pass the Audiobookshelf Item ID:

```bash
uv run skimmer --revert abs_your_item_id
```

### Options

| Flag | Description |
| --- | --- |
| `--dry-run` | Preview changes without writing anything to Audiobookshelf. |
| `--redo-dry-run` | Re-process items that were previously recorded as dry-run (e.g. to apply them for real). |
| `--retry-failed` | Re-queue items that previously failed (`failed-ai`, `failed-transcription`, or `hallucinated` status). |
| `--reprocess` | Re-process items even if they already have the `ai-skimmed` tag (nuclear option). |
| `--force` | Process all books, even those that already have an ASIN. |
| `--retranscribe` | Force a new transcription even if one exists in the database. |
| `--library <name>` | Only process items from this named library. |
| `--limit <n>` | Stop after processing this many items total. |
| `--force-tag <tag>` | Strictly process only items with this tag. Overrides ASIN skip, clears the ASIN in ABS, and forces a full re-transcription. Tag is removed on success. |
| `--no-metadatahints` | Perform a "blind" extraction by omitting existing metadata from the LLM prompt. |
| `--no-guardrail` | Disable hallucination detection, accepting all AI results regardless of transcription matches. |
| `--throttle <sec>` | Seconds to wait between server requests (default: `1.0`). |
| `--config <path>` | Use a custom configuration file. |
| `--item-id <ID>` | Process only a single specific library item. |
| `--revert <ID>` | Revert a single item to its original metadata. |
| `--report [run_id]` | Show a summary of the latest run or a specific Run ID. |
| `--barebones-report` | Skip the detailed list of changes in the final report, showing only numerical counts. |
| `--list-runs` | Show a history of all past execution runs. |
| `--item-info <ID>` | Show full details for a specific book (metadata, transcript, AI decision). |

---

## 🛠️ How It Works

1. **Discovery (Server-Safe)**: The script fetches library items using **pagination** (100 at a time) and a mandatory throttle delay to avoid overloading your server.
2. **Persistent Queue**: Items are queued with a `run_id` for tracking and reporting.
3. **Stream & Capture**: For each queued item (**one at a time**), `ffmpeg` captures exactly the first 30-120 seconds of audio directly from the stream. (configurable)
4. **Transcribe**: The slice is transcribed locally using Parakeet-MLX and saved to the DB immediately.
5. **Analyze**: Transcripts are processed by a local LLM in a separate phase, ensuring memory is freed between steps.
6. **Apply & Report**: Metadata is updated in ABS, and a detailed report is generated for the run.

---

## 📄 License

GPL-3.0 © 2026

*Made for the messy audiobook library that needs a quick "skim" to get things right.*
