import json
import logging
from typing import Dict, Optional

from .abs_client import ABSClient
from .history_manager import HistoryManager

logger = logging.getLogger("sync_metadata")

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

def handle_item_info(args, history_manager: HistoryManager, abs_client: Optional[ABSClient] = None):
    item_id = args.item_info
    detail = history_manager.get_item_detail(item_id)
    
    # Try to get live path if client is available
    abs_path = None
    if abs_client:
        try:
            abs_path = abs_client.get_item_path(item_id)
        except Exception as e:
            logger.debug(f"Could not fetch live path from ABS: {e}")

    if not detail and not abs_path:
        print(f"No information found for item ID: {item_id}")
        return
    
    print(f"\n{'='*60}")
    print(f" Item Information: {item_id}")
    print(f"{'='*60}")
    
    if abs_path:
        print(f"ABS Folder   : {abs_path}")
    
    if detail:
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
    else:
        print("(Item not found in local history)")
    
    print(f"{'='*60}\n")
