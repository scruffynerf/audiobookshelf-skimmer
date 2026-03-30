import time
from pathlib import Path
from typing import Dict

from .abs_client import ABSClient
from .author_utils import normalize_author
from .title_utils import normalize_title
from .utils import GracefulKiller, logger

def run_foldercheck(args, config: Dict):
    abs_client = ABSClient(config["abs_url"], config["abs_api_key"])
    dry_run = args.dry_run or config.get("dry_run", True)
    killer = GracefulKiller()
    throttle_sec = args.throttle
    
    mismatch_tag = config.get("library_mismatch_tag", "library_mismatch")
    wrong_title_tag = config.get("wrong_title_tag", "wrong_title")
    wrong_author_tag = config.get("wrong_author_tag", "wrong_author")
    
    logger.info(f"Starting folder check (Dry Run: {dry_run})")
    
    stats = {
        "processed": 0,
        "matches": 0,
        "mismatches": 0,
        "wrong_author": 0,
        "wrong_title": 0,
        "both_wrong": 0,
        "fixed": 0,
        "errors": 0
    }
    
    mismatch_list = []
    fixed_list = []
    last_request_time = 0

    def smart_throttle():
        nonlocal last_request_time
        if throttle_sec <= 0:
            return
        elapsed = time.time() - last_request_time
        if elapsed < throttle_sec:
            sleep_time = throttle_sec - elapsed
            time.sleep(sleep_time)
        last_request_time = time.time()

    items_iterator = abs_client.iter_items(library_name=args.library)
    for item in items_iterator:
        if killer.kill_now:
            break
            
        if args.limit and stats["processed"] >= args.limit:
            break
            
        item_id = item.get("id")
        media = item.get("media", {})
        metadata = media.get("metadata", {})
        current_tags = media.get("tags", [])
        
        # Metadata values
        meta_title = metadata.get("title", "Unknown")
        meta_subtitle = metadata.get("subtitle", "")
        meta_author = metadata.get("authorName", "Unknown")
        
        # Provide feedback
        stats["processed"] += 1
        display_title = f"{meta_title}: {meta_subtitle}" if meta_subtitle else meta_title
        logger.info(f"[{stats['processed']}] Checking: {meta_author} - {display_title} ({item_id})")
        
        # Optimization: Item already contains path
        try:
            # First try item['path'] then media['path'] then fetch if really missing
            full_path = item.get("path") or media.get("path")
            if not full_path:
                smart_throttle()
                full_path = abs_client.get_item_path(item_id)
                
            if not full_path:
                logger.warning(f"  ⚠️ Could not find path for {item_id}")
                stats["errors"] += 1
                continue
                
            path_obj = Path(full_path)
            if len(path_obj.parts) < 2:
                logger.warning(f"  ⚠️ Path too short for {item_id}: {full_path}")
                stats["errors"] += 1
                continue
                
            # Handle Author/Series/Title or Author/Title
            path_title = path_obj.name
            path_parent_name = path_obj.parent.name
            
            # Series aware check: If parent folder is the series name, author is the grandparent
            meta_series = metadata.get("seriesName", "")
            norm_series = normalize_author(meta_series) if meta_series else set()
            norm_parent = normalize_author(path_parent_name)
            
            # If parent is a very close match to the series name (subset/superset), we assume it's a series folder
            is_series_folder = False
            if norm_series and norm_parent:
                # If either is a subset of the other (handles "Series Name" vs "Series Name #1")
                if norm_parent.issubset(norm_series) or norm_series.issubset(norm_parent):
                    is_series_folder = True
            
            if is_series_folder and len(path_obj.parts) >= 3:
                # Structure is Author / Series / Title
                path_author = path_obj.parent.parent.name
                logger.debug(f"  📂 Series folder detected: '{path_parent_name}', using grandparent '{path_author}' as author")
            else:
                path_author = path_parent_name
            
            # Normalize for comparison
            # Use meta_author as context for title normalization (to strip it from the title folder name)
            # Author match via set comparison (order independent)
            # Relaxed rule: if one author set is a full subset of the other, count it as a match (handles co-authors / ghostwriters)
            norm_path_author = normalize_author(path_author)
            norm_meta_author = normalize_author(meta_author)
            
            author_match = False
            if norm_path_author and norm_meta_author:
                author_match = norm_path_author.issubset(norm_meta_author) or norm_meta_author.issubset(norm_path_author)
            elif not norm_path_author and not norm_meta_author:
                author_match = True # Both empty/unknown
            
            # Title match logic (strips punctuation, common terms, and handles series)
            norm_path_title = normalize_title(path_title, author_name=meta_author)
            norm_meta_title = normalize_title(meta_title, author_name=meta_author)
            
            primary_match = (norm_path_title == norm_meta_title)
            combined_match = False
            if meta_subtitle:
                norm_subtitle = normalize_title(meta_subtitle, author_name=meta_author)
                combined_match = (norm_path_title == (norm_meta_title + norm_subtitle))
            
            title_match = primary_match or combined_match
            
            # Advanced matching: handle suffix match (series), prefix match (The), and stripping series name
            if not title_match:
                # Case 1: Title exists but metadata includes Series/Book details
                if (len(norm_path_title) > 5 and norm_meta_title.endswith(norm_path_title)) or \
                   (len(norm_meta_title) > 5 and norm_path_title.endswith(norm_meta_title)):
                    title_match = True
                
                # Case 2: Handing leading "The"
                if not title_match:
                    if (norm_meta_title.startswith('the') and norm_meta_title[3:] == norm_path_title) or \
                       (norm_path_title.startswith('the') and norm_path_title[3:] == norm_meta_title):
                        title_match = True
                
                # Case 3: Series stripping (ignoring common Series tags in the Title field)
                if not title_match and meta_series:
                    norm_series = normalize_title(meta_series)
                    if norm_meta_title.startswith(norm_series):
                        stripped_meta = norm_meta_title[len(norm_series):]
                        if norm_path_title == stripped_meta:
                            title_match = True

            all_mismatch_tags = {mismatch_tag, wrong_title_tag, wrong_author_tag}
            active_mismatch_tags = [t for t in current_tags if t in all_mismatch_tags]
            
            if author_match and title_match:
                stats["matches"] += 1
                if active_mismatch_tags:
                    stats["fixed"] += 1
                    fixed_list.append({
                        "id": item_id,
                        "title": meta_title,
                        "author": meta_author,
                        "tags": active_mismatch_tags
                    })
                    logger.info(f"  ✨ FIXED: Now matches. Removing: {active_mismatch_tags}")
                    if not dry_run:
                        for tag in active_mismatch_tags:
                            smart_throttle()
                            abs_client.remove_tag(item_id, tag)
            else:
                stats["mismatches"] += 1
                correct_tag = None
                mismatch_desc = ""
                
                if not author_match and not title_match:
                    stats["both_wrong"] += 1
                    correct_tag = mismatch_tag
                    mismatch_desc = "Both Author and Title mismatch"
                elif not author_match:
                    stats["wrong_author"] += 1
                    correct_tag = wrong_author_tag
                    mismatch_desc = "Author mismatch"
                elif not title_match:
                    stats["wrong_title"] += 1
                    correct_tag = wrong_title_tag
                    mismatch_desc = "Title mismatch"
                
                logger.warning(f"  ❌ {mismatch_desc}: {full_path}")
                
                mismatch_list.append({
                    "id": item_id,
                    "title": meta_title,
                    "subtitle": meta_subtitle,
                    "author": meta_author,
                    "path_title": path_title,
                    "path_author": path_author,
                    "norm_path_title": norm_path_title,
                    "norm_meta_title": norm_meta_title,
                    "issue": mismatch_desc,
                    "current_tags": active_mismatch_tags
                })
                
                if not dry_run:
                    # Ensure correct tag exists
                    if correct_tag not in current_tags:
                        smart_throttle()
                        abs_client.add_tag(item_id, correct_tag)
                    
                    # Remove incorrect ones
                    wrong_ones = [t for t in active_mismatch_tags if t != correct_tag]
                    for tag in wrong_ones:
                        smart_throttle()
                        abs_client.remove_tag(item_id, tag)
                        
        except Exception as e:
            logger.error(f"  ❌ Error checking {item_id}: {e}")
            stats["errors"] += 1
            
    # Report results
    print(f"\n{'='*60}")
    print(f" Folder Check Report")
    print(f" (Dry Run: {dry_run})")
    print(f"{'='*60}")
    print(f"Total Items Scanned : {stats['processed']}")
    print(f"Perfect Matches     : {stats['matches']}")
    print(f"Mismatches Found    : {stats['mismatches']}")
    print(f"  - Wrong Author    : {stats['wrong_author']}")
    print(f"  - Wrong Title     : {stats['wrong_title']}")
    print(f"  - Both Wrong      : {stats['both_wrong']}")
    print(f"Items Fixed (Repaired): {stats['fixed']}")
    print(f"Errors Encountered  : {stats['errors']}")
    print("-" * 60)
    
    if fixed_list:
        print("\nRecently Fixed (Tags Removed):")
        for f in fixed_list:
            print(f"  ✨ {f['author']} - {f['title']} ({f['id']})")
            print(f"    Tags cleared: {f['tags']}")
            print("")

    if mismatch_list:
        print("\nMismatch Details [Diagnostics]:")
        for m in mismatch_list:
            # Truncate long display strings for readability
            m_sub = m.get('subtitle') or ""
            meta_title_disp = (m['title'][:50] + '...') if len(m['title']) > 50 else m['title']
            meta_sub_disp = (m_sub[:50] + '...') if len(m_sub) > 50 else m_sub
            
            print(f"  ❌ {m['author']} - {meta_title_disp} ({m['id']})")
            if m.get('subtitle'):
                print(f"    Meta Subtitle: {meta_sub_disp}")
            print(f"    Path Author  : {m['path_author']}")
            print(f"    Path Title   : {m['path_title']}")
            
            # Show Detailed Diagnostics
            print(f"    Norm Path    : {m['norm_path_title']}")
            print(f"    Norm Meta T  : {m['norm_meta_title']}")
            if m.get('subtitle'):
                norm_meta_subtitle = normalize_title(m['subtitle'], m['author'])
                print(f"    Norm Meta C  : {m['norm_meta_title'] + norm_meta_subtitle}")
            
            # Show Author Diagnostics
            path_author_set = normalize_author(m['path_author'])
            meta_author_set = normalize_author(m['author'])
            print(f"    Norm Author P: {sorted(list(path_author_set))}")
            print(f"    Norm Author M: {sorted(list(meta_author_set))}")
            
            print(f"    Issue        : {m['issue']}")
            if m['current_tags']:
                print(f"    Existing tags: {m['current_tags']}")
            print("")
    
    print(f"{'='*60}\n")
