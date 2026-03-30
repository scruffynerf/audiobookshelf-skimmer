import argparse
import sys
from pathlib import Path

from .abs_client import ABSClient
from .history_manager import HistoryManager
from .utils import load_config, logger
from .cmd_info import handle_revert, handle_report, handle_list_runs, handle_item_info
from .cmd_foldercheck import run_foldercheck
from .cmd_sync import run_sync

def main():
    parser = argparse.ArgumentParser(description="Audiobookshelf Skimmer")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--dry-run", action="store_true", help="Don't update ABS")
    parser.add_argument("--force", action="store_true", help="Process even if ASIN exists")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess even if already tagged")
    parser.add_argument("--revert", help="Item ID to revert to original metadata")
    parser.add_argument("--item-id", help="Process only a single library item ID")
    parser.add_argument("--library", help="Only process items from this library name")
    parser.add_argument("--limit", type=int, help="Total items to process before stopping (will finish the last batch)")
    parser.add_argument("--retranscribe", action="store_true", help="Force re-transcription even if exists")
    parser.add_argument("--redo-dry-run", action="store_true", help="Re-process items previously recorded as dry-run (e.g. to apply changes for real)")
    parser.add_argument("--retry-failed", action="store_true", help="Re-queue items that previously failed (failed-ai, failed-transcription, hallucinated status)")
    parser.add_argument("--force-tag", help="Reprocess all items with this tag and remove it when done (forces re-transcription)")
    parser.add_argument("--no-metadatahints", action="store_true", help="Do not provide existing metadata to the LLM (test blind extraction)")
    parser.add_argument("--no-guardrail", action="store_true", help="Disable hallucination detection (always accept AI results)")
    parser.add_argument("--report", nargs="?", const="", help="View summary of the latest run or a specific run ID")
    parser.add_argument("--list-runs", action="store_true", help="Display a table of past executions")
    parser.add_argument("--barebones-report", action="store_true", help="Skip detailed change list in final report")
    parser.add_argument("--foldercheck", action="store_true", help="Audit folder structure vs metadata")
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
    
    try:
        if args.revert:
            config = load_config(Path(args.config))
            handle_revert(args, config)
        elif args.list_runs:
            handle_list_runs(history_manager)
        elif args.report is not None:
            handle_report(args, history_manager)
        elif args.item_info:
            config = load_config(Path(args.config))
            abs_client = ABSClient(config["abs_url"], config["abs_api_key"])
            handle_item_info(args, history_manager, abs_client)
        elif args.foldercheck:
            config = load_config(Path(args.config))
            run_foldercheck(args, config)
        else:
            config = load_config(Path(args.config))
            run_sync(args, config)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
