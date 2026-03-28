# 📚 Audiobookshelf Skimmer

> **Fix your audiobook metadata by "skimming" the first few minutes with AI.**

Audiobookshelf Skimmer is a precision tool designed to automatically correct Title, Author, Narrator, and Publisher information in your Audiobookshelf library. It works by "skimming" the first 120 seconds of a book—capturing the intro where these details are usually spoken—and using a combination of local transcription (MLX) and local Large Language Models (LLMs) to identify and update the metadata.

---

## ✨ Features

- **🚀 Optimized Streaming**: Uses the Audiobookshelf `/play` endpoint and `ffmpeg` to capture precisely 120s of audio. No need to download entire 200MB+ files just to read the title.
- **🧠 Local AI Power**: Runs entirely on your machine.
  - **Transcription**: Uses NVIDIA's **Parakeet** model via Apple's **MLX** framework.
  - **Correction**: Uses **mlx-lm** to run models like Llama 3 locally.
- **🔄 Memory-Efficient Batching**: Processes books in batches of 10, swapping models in and out of memory to stay within an 8GB RAM target.
- **🛡️ Smart & Safe**:
  - **ASIN Skip**: Automatically skips books that already have an ASIN (assumed to be already identified).
  - **Hallucination Detection**: Basic logic to ensure the LLM doesn't suggest titles or authors that are completely absent from the transcript.
  - **Dry Run Mode**: Preview all changes before they are committed to your library.

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
- `llm_model`: The model to use (e.g., `llama3`).

---

## 🖥️ Usage

The project provides a clean `skimmer` command via `uv run`:

### Run a Dry Run (Recommended)

```bash
uv run skimmer --dry-run
```

### Perform the Sync

```bash
uv run skimmer
```

### Revert a Change

If you're unhappy with a change, pass the Audiobookshelf Item ID:

```bash
uv run skimmer --revert abs_your_item_id
```

### Options

- `--force`: Process all books, even those that already have an ASIN.
- `--reprocess`: Process books even if they already have the `ai-skimmed` tag.
- `--config <path>`: Use a custom configuration file.
- `--item-id <ID>`: Process only a single specific library item.

---

## 🛠️ How It Works

1. **Scan**: The script fetches your library items, skipping any with ASINs or "exclude" tags.
2. **Stream & Capture**: Uses `ffmpeg` to capture a short slice directly from the ABS stream.
3. **Transcribe**: The slice is transcribed locally on your GPU using Parakeet-MLX.
4. **Analyze**: The transcript is sent to a local LLM via `mlx-lm`.
5. **Apply & Log**: If the LLM finds better metadata, it updates ABS, adds the `ai-skimmed` tag, and logs the change to `history.db`.

---

## 📄 License

GPL-3.0 © 2026

<p align="center">Made for the messy audiobook library that needs a quick "skim" to get things right.</p>
