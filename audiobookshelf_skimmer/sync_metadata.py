import json
import logging
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

from .abs_client import ABSClient
from .llm_client import LLMClient
from .transcriber import Transcriber
from .history_manager import HistoryManager

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

def run_sync(args, config: Dict):
    abs_client = ABSClient(config["abs_url"], config["abs_api_key"])
    history_manager = HistoryManager()
    transcriber = Transcriber()
    llm_client = LLMClient(
        model_id=config.get("llm_model", "mlx-community/Llama-3.2-3B-Instruct-4bit"),
        system_prompt=config.get("llm_system_prompt", "")
    )
    
    batch_size = config.get("batch_size", 10)
    slice_duration = config.get("slice_duration_sec", 120)
    dry_run = args.dry_run or config.get("dry_run", True)
    
    processed_tag = config.get("processed_tag", "ai-skimmed")
    exclude_tag = config.get("exclude_tag", "no-skim")
    ai_retries = config.get("ai_retries", 1)

    # Phase 1: Filter and Fetch
    logger.info("Fetching items from Audiobookshelf...")
    items = abs_client.get_all_items()
    
    to_process = []
    for item in items:
        item_id = item.get("id")
        media = item.get("media", {})
        metadata = media.get("metadata", {})
        
        # Filter criteria
        if args.item_id and item_id != args.item_id:
             continue
             
        if metadata.get("asin") and not args.force and not args.item_id:
            continue
            
        tags = metadata.get("tags", [])
        if exclude_tag in tags and not args.force:
            continue
        if processed_tag in tags and not args.force and not args.reprocess:
            continue
            
        to_process.append(item)

    if not to_process:
        logger.info("No items to process.")
        return

    logger.info(f"Identified {len(to_process)} items to process. Using batch size {batch_size}.")

    # Divide into batches
    for i in range(0, len(to_process), batch_size):
        batch = to_process[i:i + batch_size]
        logger.info(f"Starting batch {i // batch_size + 1} ({len(batch)} items)")
        
        transcripts = {} # item_id -> (transcript, original_metadata)
        
        # --- Transcription Phase ---
        logger.info("--- Phase: Transcription ---")
        for item in batch:
            item_id = item.get("id")
            media = item.get("media", {})
            metadata = media.get("metadata", {})
            title = metadata.get("title", "Unknown Title")
            
            try:
                logger.info(f"Processing book: {title} ({item_id})")
                
                # Log start
                history_manager.log_start(item_id, metadata)
                
                # Fetch audio slice
                logger.info(f"  📥 Streaming {slice_duration}s slice from Audiobookshelf...")
                audio_path = abs_client.fetch_audio_slice(item_id, duration_sec=slice_duration)
                
                # Transcribe
                logger.info(f"  📝 Transcribing audio with Parakeet-MLX...")
                transcript = transcriber.transcribe(audio_path)
                
                # Save to manager
                history_manager.save_transcript(item_id, transcript)
                transcripts[item_id] = (transcript, metadata, title)
                
                # Cleanup temp file
                if audio_path.exists():
                     audio_path.unlink()
                     
            except Exception as e:
                logger.error(f"  ❌ Failed transcription for {title}: {e}")
        
        # Free memory!
        transcriber.unload_model()
        
        # --- AI & Application Phase ---
        logger.info("--- Phase: AI Analysis and Application ---")
        for item_id, (transcript, original_meta, title) in transcripts.items():
            attempts = ai_retries + 1
            for attempt in range(attempts):
                try:
                    # LLM Query
                    msg = f"Analyzing: {title} ({item_id})"
                    if attempt > 0:
                        msg += f" (Retry {attempt}/{ai_retries})"
                    logger.info(msg)
                    
                    logger.info(f"  🧠 Querying local LLM (mlx-lm)...")
                    suggested = llm_client.query_metadata(transcript, original_meta, duration_sec=slice_duration)
                    
                    if not suggested:
                        raise ValueError("LLM response was empty or contained no valid JSON")
                    
                    # Hallucination check
                    if llm_client.is_hallucinated(suggested, transcript):
                        if attempt < ai_retries:
                             logger.warning(f"  ⚠️ Hallucination detected. Retrying...")
                             continue
                        else:
                             logger.warning(f"  ⚠️ Hallucination detected. Max retries reached. Skipping update.")
                             history_manager.save_result(item_id, suggested, status="hallucinated")
                             break
                    
                    # Compare and sync
                    mapping = {"title": "title", "author": "authorName", "narrator": "narratorName", "publisher": "publisher"}
                    update_payload = {}
                    for sug_key, abs_key in mapping.items():
                        val = suggested.get(sug_key)
                        if val and val != original_meta.get(abs_key):
                            update_payload[abs_key] = val
                    
                    if update_payload:
                        if not dry_run:
                            logger.info(f"  ✅ Updating metadata in ABS: {update_payload}")
                            abs_client.update_metadata(item_id, update_payload)
                            abs_client.add_tag(item_id, processed_tag)
                            history_manager.save_result(item_id, suggested, status="applied")
                        else:
                            logger.info(f"  🔍 Dry Run: Would update metadata: {update_payload}")
                            history_manager.save_result(item_id, suggested, status="dry-run")
                    else:
                        logger.info(f"  ⏭️ No changes suggested.")
                        history_manager.save_result(item_id, suggested, status="no-change")
                        if not dry_run:
                             abs_client.add_tag(item_id, processed_tag)
                    
                    # If we reached here, succeed and break retry loop
                    break
                             
                except Exception as e:
                    if attempt < ai_retries:
                         logger.error(f"  ❌ Failed AI step for {title}: {e}. Retrying...")
                    else:
                         logger.error(f"  ❌ Failed AI step for {title}: {e}. Max retries reached.")
        
        # Free memory!
        llm_client.unload_model()
        
    logger.info("Processing complete.")

def main():
    parser = argparse.ArgumentParser(description="Audiobookshelf Skimmer")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--dry-run", action="store_true", help="Don't update ABS")
    parser.add_argument("--force", action="store_true", help="Process even if ASIN exists")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess even if already tagged")
    parser.add_argument("--revert", help="Item ID to revert to original metadata")
    parser.add_argument("--item-id", help="Process only a single library item ID")
    
    args = parser.parse_args()
    config = load_config(Path(args.config))
    
    if args.revert:
        handle_revert(args, config)
    else:
        run_sync(args, config)

if __name__ == "__main__":
    main()
