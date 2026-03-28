import json
import logging
import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import signal
from .abs_client import ABSClient
from .llm_client import LLMClient
from .transcriber import Transcriber
from .history_manager import HistoryManager

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logger.info("Interrupt received. Will exit after current item/phase.")
        self.kill_now = True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync_metadata.log")
    ]
)
logger = logging.getLogger("sync_metadata")

def load_config(config_path: Path) -> Dict:
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)

def handle_revert(args, config: Dict):
    abs_client = ABSClient(config["abs_url"], config["abs_api_key"])
    history_manager = HistoryManager()
    
    item_id = args.revert
    logger.info(f"Reverting changes for item: {item_id}")
    
    original_metadata = history_manager.get_original_metadata(item_id)
    if not original_metadata:
        logger.error(f"No history found for item {item_id} - cannot revert.")
        return

    # Apply original metadata back to ABS
    abs_client.update_metadata(item_id, original_metadata)
    
    # Remove the "already skimmed" tag if it exists
    tag = config.get("processed_tag", "ai-skimmed")
    abs_client.remove_tag(item_id, tag)
    
    logger.info(f"Successfully reverted {item_id} to original state.")

def print_report(summary: Dict, title: str = "Run Report"):
    print(f"\n{'='*40}")
    print(f" {title}")
    print(f"{'='*40}")
    if "run_id" in summary:
        print(f"Run ID: {summary['run_id']}")
    if "start" in summary and summary["start"]:
        print(f"Started: {summary['start']}")
    if "end" in summary and summary["end"]:
        print(f"Ended: {summary['end']}")
    
    print("-" * 20)
    stats = summary.get("stats", summary)
    for status, count in stats.items():
        if status not in ["run_id", "start", "end", "stats"]:
            print(f"{status.capitalize():<20}: {count}")
    print(f"{'='*40}\n")

def handle_report(args, history_manager: HistoryManager):
    run_id = args.report
    if run_id:
        summary = history_manager.get_run_summary(run_id)
        if not summary or not summary.get("stats"):
            logger.error(f"No data found for run: {run_id}")
            return
        print_report(summary, f"Report for Run: {run_id}")
    else:
        # Latest run
        runs = history_manager.list_runs()
        if not runs:
            logger.info("No runs found in history.")
            return
        latest_run_id = runs[0]["run_id"]
        summary = history_manager.get_run_summary(latest_run_id)
        print_report(summary, f"Latest Run Report ({latest_run_id})")
        
        # Also show total history
        total = history_manager.get_total_summary()
        print_report(total, "Total History Summary")

def handle_list_runs(history_manager: HistoryManager):
    runs = history_manager.list_runs()
    if not runs:
        print("No runs found.")
        return
    
    print(f"\n{'Run ID':<20} | {'Start Time':<20} | {'End Time':<20} | {'Items'}")
    print("-" * 75)
    for run in runs:
        print(f"{run['run_id']:<20} | {run['start'][:19]:<20} | {run['end'][:19]:<20} | {run['count']}")
    print("")

def handle_item_info(args, history_manager: HistoryManager):
    item_id = args.item_info
    detail = history_manager.get_item_detail(item_id)
    if not detail:
        print(f"No information found for item ID: {item_id}")
        return
    
    print(f"\n{'='*60}")
    print(f" Item Information: {item_id}")
    print(f"{'='*60}")
    print(f"Status       : {detail['status']}")
    print(f"Last Updated : {detail['last_updated']}")
    print(f"Run ID       : {detail['run_id']}")
    print("-" * 60)
    print("Original Metadata:")
    print(json.dumps(detail['original_metadata'], indent=2))
    print("-" * 60)
    print("Transcript:")
    print(detail['transcript'] or "(No transcript captured)")
    print("-" * 60)
    print("Suggested Metadata:")
    print(json.dumps(detail['suggested_metadata'], indent=2))
    print(f"{'='*60}\n")

def run_sync(args, config: Dict):
    abs_client = ABSClient(config["abs_url"], config["abs_api_key"])
    history_manager = HistoryManager()
    killer = GracefulKiller()
    
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.info(f"Starting sync run: {run_id}")
    
    batch_size = config.get("batch_size", 10)
    slice_duration = config.get("slice_duration_sec", 120)
    dry_run = args.dry_run or config.get("dry_run", True)
    processed_tag = config.get("processed_tag", "ai-skimmed")
    exclude_tag = config.get("exclude_tag", "no-skim")
    ai_retries = config.get("ai_retries", 1)
    throttle_sec = args.throttle

    last_request_time = 0

    def smart_throttle():
        nonlocal last_request_time
        if throttle_sec <= 0:
            return
        elapsed = time.time() - last_request_time
        if elapsed < throttle_sec:
            sleep_time = throttle_sec - elapsed
            logger.debug(f"Smart throttle: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        last_request_time = time.time()

    library_gen = abs_client.iter_items(library_name=args.library)
    library_exhausted = False

    while not killer.kill_now:
        # --- Batch Accumulation ---
        batch = history_manager.get_pending_items(limit=batch_size)
        
        if len(batch) < batch_size and not library_exhausted:
            needed = batch_size - len(batch)
            logger.info(f"Filling batch: need {needed} more items from library discovery...")
            try:
                for item in library_gen:
                    item_id = item.get("id")
                    media = item.get("media", {})
                    metadata = media.get("metadata", {})
                    tags = metadata.get("tags", [])
                    
                    # Eligibility checks
                    if args.item_id and item_id != args.item_id:
                        continue
                    if metadata.get("asin") and not args.force and not args.item_id:
                        continue
                    if exclude_tag in tags and not args.force:
                        continue
                    
                    status = history_manager.get_latest_status(item_id)
                    needs_processing = False
                    if processed_tag not in tags or args.force or args.reprocess:
                        if status is None or status in ["failed-transcription", "hallucinated", "dry-run"]:
                            needs_processing = True
                        elif args.reprocess:
                            needs_processing = True
                    
                    if needs_processing:
                        history_manager.log_start(item_id, metadata, run_id=run_id)
                        batch.append({
                            "item_id": item_id,
                            "metadata": metadata,
                            "transcript": None,
                            "status": "started"
                        })
                        if len(batch) >= batch_size:
                            break
                else:
                    library_exhausted = True
            except StopIteration:
                library_exhausted = True

        if not batch:
            logger.info("No items left to process.")
            break

        # --- Transcription Phase ---
        to_transcribe = [i for i in batch if i["status"] == "started"]
        if to_transcribe:
            logger.info(f"--- Batch Phase: Transcription ({len(to_transcribe)} items) ---")
            transcriber = Transcriber()
            for entry in to_transcribe:
                if killer.kill_now: break
                item_id = entry["item_id"]
                metadata = entry["metadata"]

                smart_throttle()
                logger.info(f"Transcribing: {metadata.get('title', 'Unknown')} ({item_id})")
                
                try:
                    audio_path = abs_client.fetch_audio_slice(item_id, duration_sec=slice_duration)
                    transcript = transcriber.transcribe(audio_path)
                    
                    if not transcript:
                        logger.warning(f"  ⚠️ Empty transcript for {item_id}. Marking as failed.")
                        history_manager.set_status(item_id, "failed-transcription")
                        entry["status"] = "failed-transcription"
                    else:
                        history_manager.save_transcript(item_id, transcript)
                        entry["transcript"] = transcript
                        entry["status"] = "transcribed"
                    
                    if audio_path.exists():
                         audio_path.unlink()
                except Exception as e:
                    logger.error(f"  ❌ Transcription failed: {e}")
                    history_manager.set_status(item_id, "failed-transcription")
                    entry["status"] = "failed-transcription"
            transcriber.unload_model()
        
        # --- AI Analysis Phase ---
        to_analyze = [i for i in batch if i["status"] == "transcribed"]
        if to_analyze:
            if not killer.kill_now:
                logger.info(f"--- Batch Phase: AI Analysis ({len(to_analyze)} items) ---")
                llm_client = LLMClient(
                    model_id=config.get("llm_model", "mlx-community/Llama-3.2-3B-Instruct-4bit"),
                    system_prompt=config.get("llm_system_prompt", "")
                )
                for entry in to_analyze:
                    if killer.kill_now: break
                    item_id = entry["item_id"]
                    metadata = entry["metadata"]
                    transcript = entry["transcript"]

                    logger.info(f"Analyzing: {metadata.get('title', 'Unknown')} ({item_id})")
                    
                    attempts = ai_retries + 1
                    success = False
                    for attempt in range(attempts):
                        try:
                            logger.info(f"Analyzing: {metadata.get('title', 'Unknown')} (Attempt {attempt+1})")
                            suggested = llm_client.query_metadata(transcript, metadata, duration_sec=slice_duration)
                            
                            if not suggested:
                                raise ValueError("Empty LLM response")
                            
                            if llm_client.is_hallucinated(suggested, transcript):
                                if attempt < ai_retries:
                                     continue
                                history_manager.save_result(item_id, suggested, status="hallucinated")
                                break
                            
                            # Apply changes
                            mapping = {"title": "title", "author": "authorName", "narrator": "narratorName", "publisher": "publisher"}
                            update_payload = {}
                            for sug_key, abs_key in mapping.items():
                                val = suggested.get(sug_key)
                                if val and val != metadata.get(abs_key):
                                    update_payload[abs_key] = val
                            
                            if update_payload:
                                smart_throttle()
                                if not dry_run:
                                    abs_client.update_metadata(item_id, update_payload)
                                    abs_client.add_tag(item_id, processed_tag)
                                    history_manager.save_result(item_id, suggested, status="applied")
                                else:
                                    history_manager.save_result(item_id, suggested, status="dry-run")
                            else:
                                history_manager.save_result(item_id, suggested, status="no-change")
                                if not dry_run:
                                     smart_throttle()
                                     abs_client.add_tag(item_id, processed_tag)
                            
                            success = True
                            break
                        except Exception as e:
                            logger.error(f"  ❌ AI step failed for {metadata.get('title', 'Unknown')}: {e}")
                    
                    if not success and attempt == ai_retries:
                         history_manager.set_status(item_id, "failed-ai")
                         
                llm_client.unload_model()

        if args.item_id: # Single item mode
            break

    logger.info("Sync complete.")
    handle_report(argparse.Namespace(report=run_id), history_manager)

def main():
    parser = argparse.ArgumentParser(description="Audiobookshelf Skimmer")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--dry-run", action="store_true", help="Don't update ABS")
    parser.add_argument("--force", action="store_true", help="Process even if ASIN exists")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess even if already tagged")
    parser.add_argument("--revert", help="Item ID to revert to original metadata")
    parser.add_argument("--item-id", help="Process only a single library item ID")
    parser.add_argument("--library", help="Only process items from this library name")
    parser.add_argument("--retranscribe", action="store_true", help="Force re-transcription even if exists")
    parser.add_argument("--report", nargs="?", const="", help="View summary of the latest run or a specific run ID")
    parser.add_argument("--list-runs", action="store_true", help="Display a table of past executions")
    parser.add_argument(
        "--item-info",
        help="Show full details for a specific book ID"
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=1.0,
        help="Seconds to wait between requests to Audiobookshelf (default: 1.0)"
    )
    
    args = parser.parse_args()
    history_manager = HistoryManager()
    
    if args.revert:
        config = load_config(Path(args.config))
        handle_revert(args, config)
    elif args.list_runs:
        handle_list_runs(history_manager)
    elif args.report is not None:
        handle_report(args, history_manager)
    elif args.item_info:
        handle_item_info(args, history_manager)
    else:
        config = load_config(Path(args.config))
        run_sync(args, config)

if __name__ == "__main__":
    main()
