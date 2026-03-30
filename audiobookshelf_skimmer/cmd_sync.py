import json
import time
from datetime import datetime
from typing import Dict

from .abs_client import ABSClient
from .llm_client import LLMClient
from .transcriber import Transcriber
from .history_manager import HistoryManager
from .utils import GracefulKiller, logger

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
    force_tag = args.force_tag
    no_metadatahints = args.no_metadatahints
    no_guardrail = args.no_guardrail

    last_request_time = 0
    total_processed = 0

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
        current_batch_limit = batch_size
        if args.limit:
            remaining = args.limit - total_processed
            if remaining <= 0:
                break
            current_batch_limit = min(batch_size, remaining)

        # --- Batch Accumulation ---
        batch = history_manager.get_pending_items(limit=current_batch_limit)
        
        if len(batch) < current_batch_limit and not library_exhausted:
            needed = current_batch_limit - len(batch)
            logger.info(f"Filling batch: need {needed} more items from library discovery...")
            try:
                for item in library_gen:
                    item_id = item.get("id")
                    media = item.get("media", {})
                    metadata = media.get("metadata", {})
                    tags = media.get("tags", [])
                    
                    # Eligibility checks
                    if args.item_id and item_id != args.item_id:
                        continue
                    
                    is_forced_by_tag = force_tag and force_tag in tags
                    if force_tag and not is_forced_by_tag:
                         continue
                        
                    if metadata.get("asin") and not args.force and not args.item_id and not is_forced_by_tag:
                        continue
                    if exclude_tag in tags and not args.force:
                        continue
                    
                    status = history_manager.get_latest_status(item_id)
                    needs_processing = False
                    
                    if is_forced_by_tag or processed_tag not in tags or args.force or args.reprocess:
                        auto_retry_statuses = set()
                        if args.retry_failed:
                            auto_retry_statuses = {"failed-ai", "failed-transcription", "hallucinated"}
                        if is_forced_by_tag or status is None or status in auto_retry_statuses or (args.redo_dry_run and status == "dry-run"):
                            needs_processing = True
                        elif args.reprocess:
                            needs_processing = True
                    
                    if needs_processing:
                        if status is not None:
                            # If forced by tag, we want a fresh start (re-transcribe and clear ASIN)
                            if is_forced_by_tag:
                                history_manager.reset_for_reprocess(item_id, metadata, run_id=run_id)
                                existing_transcript = None
                                # Remove ASIN locally so it doesn't show in hints
                                metadata.pop("asin", None)
                            else:
                                existing_transcript = history_manager.reset_for_reprocess(
                                    item_id, metadata, run_id=run_id
                                )
                        else:
                            # Brand new item — insert a fresh started row
                            history_manager.log_start(item_id, metadata, run_id=run_id)
                            existing_transcript = None
                            if is_forced_by_tag:
                                metadata.pop("asin", None) # Ensure ASIN is gone from payload regardless
                        
                        batch.append({
                            "item_id": item_id,
                            "metadata": metadata,
                            "transcript": existing_transcript,
                            "status": "transcribed" if existing_transcript else "started",
                            "is_forced_by_tag": is_forced_by_tag
                        })
                        if len(batch) >= current_batch_limit:
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
        if not args.retranscribe:
            # Also skip items that already have a non-empty transcript even if status==started
            to_transcribe = [i for i in to_transcribe if not i.get("transcript")]
        if to_transcribe:
            logger.info(f"--- Batch Phase: Transcription ({len(to_transcribe)} items) ---")
            transcriber = Transcriber(history_manager=history_manager)
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
            logger.info(f"--- Batch Phase: Analysis ({len(to_analyze)} items) ---")
            llm_client = LLMClient(
                model_id=config.get("llm_model", LLMClient.DEFAULT_MODEL),
                system_prompt=config.get("llm_system_prompt", ""),
                history_manager=history_manager
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
                        suggested = llm_client.query_metadata(
                            transcript, 
                            metadata, 
                            duration_sec=slice_duration,
                            no_metadatahints=no_metadatahints
                        )
                        
                        if not suggested:
                            raise ValueError("Empty LLM response")
                            
                        logger.info(f"LLM suggested: {json.dumps(suggested)}")
                        
                        if not no_guardrail and llm_client.is_hallucinated(suggested, transcript, current_metadata=metadata):
                            if attempt < ai_retries:
                                continue
                            history_manager.save_result(item_id, suggested, status="hallucinated")
                            break
                        
                        # Apply changes mapping to ABS-compliant fields
                        field_map = {
                            "title": {"abs_key": "title", "type": "string", "summary_key": "title"},
                            "author": {"abs_key": "authors", "type": "author_list", "summary_key": "authorName"},
                            "narrator": {"abs_key": "narrators", "type": "string_list", "summary_key": "narratorName"},
                            "publisher": {"abs_key": "publisher", "type": "string", "summary_key": "publisher"}
                        }
                        
                        update_payload = {}
                        for sug_key, info in field_map.items():
                            val = suggested.get(sug_key)
                            if not val:
                                continue
                            
                            abs_key = info["abs_key"]
                            field_type = info["type"]
                            summary_key = info["summary_key"]
                            
                            is_match = False
                            existing_summary = metadata.get(summary_key)
                            if str(existing_summary or "").strip().lower() == val.strip().lower():
                                is_match = True
                            
                            if not is_match:
                                existing_struct = metadata.get(abs_key)
                                if isinstance(existing_struct, list):
                                    for item in existing_struct:
                                        text = item.get('name', '') if isinstance(item, dict) else str(item)
                                        if text.strip().lower() == val.strip().lower():
                                            is_match = True
                                            break
                            
                            if not is_match:
                                if field_type == "author_list":
                                    update_payload[abs_key] = [{"name": val}]
                                elif field_type == "string_list":
                                    update_payload[abs_key] = [val]
                                else:
                                    update_payload[abs_key] = val
                        
                        if is_forced_by_tag:
                             # Ensure ASIN is cleared in ABS
                             update_payload["asin"] = None
                        
                        if update_payload:
                            smart_throttle()
                            if not dry_run:
                                abs_client.update_metadata(item_id, update_payload)
                                abs_client.add_tag(item_id, processed_tag)
                                history_manager.save_result(item_id, suggested, status="applied")
                                if entry.get("is_forced_by_tag"):
                                    abs_client.remove_tag(item_id, force_tag)
                            else:
                                history_manager.save_result(item_id, suggested, status="dry-run")
                        else:
                            # If forced, we still might want to clear the ASIN and tag even if no other change
                            if is_forced_by_tag and not dry_run:
                                 abs_client.update_metadata(item_id, {"asin": None})
                                 abs_client.remove_tag(item_id, force_tag)
                            
                            history_manager.save_result(item_id, suggested, status="no-change")
                            if not dry_run:
                                abs_client.add_tag(item_id, processed_tag)
                        
                        success = True
                        break
                    except Exception as e:
                        logger.error(f"Error during AI analysis for {item_id}: {e}")
                        if attempt >= ai_retries:
                            history_manager.set_status(item_id, "failed-ai")

            llm_client.unload_model()

        total_processed += len(batch)
        if args.limit and total_processed >= args.limit:
            logger.info(f"Total limit reached ({total_processed}/{args.limit}). Stopping.")
            break

        if args.item_id: # Single item mode
            break

    logger.info(f"Sync run completed. Total items: {total_processed}")
    
    # Detailed Report
    summary = history_manager.get_run_summary(run_id)
    print("\n" + "="*40)
    print(f" Report for Run: {run_id}")
    print("="*40)
    
    if not args.barebones_report:
        details = history_manager.get_run_items(run_id)
        
        # Applied / Dry-Run
        updates = [i for i in details if i["status"] in ["applied", "dry-run"]]
        if updates:
            print(f"\n✅ UPDATED ({len(updates)} items):")
            if dry_run: print(" (DRY RUN - No changes applied to ABS)")
            for item in updates:
                orig = item["original_metadata"]
                sug = item["suggested_metadata"]
                print(f"  - {orig.get('authorName', 'Unknown')} - {orig.get('title', 'Unknown')}")
                print(f"    -> {sug.get('author', 'Unknown')} - {sug.get('title', 'Unknown')}")
        
        # No-Change (Confirmed)
        no_changes = [i for i in details if i["status"] == "no-change"]
        if no_changes:
            print(f"\n🤝 CONFIRMED (No change needed - {len(no_changes)} items):")
            for item in no_changes:
                orig = item["original_metadata"]
                print(f"  - {orig.get('authorName', 'Unknown')} - {orig.get('title', 'Unknown')}")

        # Failed
        failed = [i for i in details if i["status"] in ["failed-ai", "failed-transcription", "hallucinated"]]
        if failed:
            print(f"\n❌ FAILED ({len(failed)} items):")
            for item in failed:
                orig = item["original_metadata"]
                print(f"  - {orig.get('authorName', 'Unknown')} - {orig.get('title', 'Unknown')} [{item['status']}]")
    else:
        # Barebones report
        print(f"Run ID: {run_id}")
        print(f"Started: {summary['start']}")
        print(f"Ended: {summary['end']}")
        print("-" * 20)
        for status, count in summary["stats"].items():
            print(f"{status.capitalize():<20}: {count}")
            
    print("="*40 + "\n")
