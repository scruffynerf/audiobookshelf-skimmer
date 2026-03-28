import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class HistoryManager:
    def __init__(self, db_path: Path = Path("history.db")):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,
                    run_id TEXT,
                    timestamp TEXT NOT NULL,
                    last_updated TEXT,
                    original_metadata TEXT,
                    transcript TEXT,
                    suggested_metadata TEXT,
                    status TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS app_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            # Simple migration for existing DBs
            try:
                conn.execute("ALTER TABLE history ADD COLUMN run_id TEXT")
            except sqlite3.OperationalError: pass
            try:
                conn.execute("ALTER TABLE history ADD COLUMN last_updated TEXT")
            except sqlite3.OperationalError: pass
            
            conn.commit()

    def get_app_metadata(self, key: str) -> Optional[str]:
        """Retrieves a value from the app_metadata table."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT value FROM app_metadata WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else None

    def set_app_metadata(self, key: str, value: str):
        """Sets a value in the app_metadata table."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_metadata (key, value) VALUES (?, ?)",
                (key, value)
            )
            conn.commit()

    def log_start(self, item_id: str, original_metadata: Dict, run_id: Optional[str] = None):
        """Logs the start of processing for an item with its original metadata."""
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO history (item_id, run_id, timestamp, last_updated, original_metadata, status) VALUES (?, ?, ?, ?, ?, ?)",
                (item_id, run_id, now, now, json.dumps(original_metadata), "started")
            )
            conn.commit()

    def save_transcript(self, item_id: str, transcript: str):
        """Updates the history record with the transcript."""
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE history SET transcript = ?, status = ?, last_updated = ? WHERE item_id = ? AND status = ?",
                (transcript, "transcribed", now, item_id, "started")
            )
            conn.commit()

    def save_result(self, item_id: str, suggested_metadata: Dict, status: str = "applied"):
        """Updates the history record with the suggested metadata and final status."""
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE history SET suggested_metadata = ?, status = ?, last_updated = ? WHERE item_id = ? AND status = ?",
                (json.dumps(suggested_metadata), status, now, item_id, "transcribed")
            )
            conn.commit()

    def set_status(self, item_id: str, status: str):
        """Manually sets the status of an item."""
        now = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE history SET status = ?, last_updated = ? WHERE item_id = ?",
                (status, now, item_id)
            )
            conn.commit()

    def get_original_metadata(self, item_id: str) -> Optional[Dict]:
        """Retrieves the most recent original metadata for an item ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT original_metadata FROM history WHERE item_id = ? ORDER BY timestamp DESC LIMIT 1",
                (item_id,)
            )
            row = cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0])
        return None

    def get_latest_status(self, item_id: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT status FROM history WHERE item_id = ? ORDER BY timestamp DESC LIMIT 1",
                (item_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def get_latest_transcript(self, item_id: str) -> Optional[str]:
        """Returns the most recent non-empty transcript for an item, if any."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT transcript FROM history WHERE item_id = ? AND transcript IS NOT NULL AND transcript != '' ORDER BY timestamp DESC LIMIT 1",
                (item_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def reset_for_reprocess(self, item_id: str, original_metadata: Dict, run_id: Optional[str] = None):
        """
        Re-queues an item for reprocessing by updating the most recent row in-place.
        If a transcript already exists it is preserved and the status is set to
        'transcribed' (skip re-transcription). Otherwise status is set to 'started'.
        """
        existing_transcript = self.get_latest_transcript(item_id)
        now = datetime.now().isoformat()
        new_status = "transcribed" if existing_transcript else "started"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE history SET run_id = ?, last_updated = ?, original_metadata = ?,
                    transcript = COALESCE(transcript, NULL), status = ?
                WHERE item_id = ? AND id = (
                    SELECT id FROM history WHERE item_id = ? ORDER BY timestamp DESC LIMIT 1
                )
                """,
                (run_id, now, json.dumps(original_metadata), new_status, item_id, item_id)
            )
            conn.commit()
        return existing_transcript

    def get_items_by_status(self, status: str, limit: Optional[int] = None) -> List[Dict]:
        """Returns all items currently in a specific status."""
        query = "SELECT item_id, original_metadata, transcript FROM history WHERE status = ?"
        params = [status]
        if limit:
            query += " LIMIT ?"
            params.append(limit)
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            items = []
            for row in cursor.fetchall():
                items.append({
                    "item_id": row[0],
                    "metadata": json.loads(row[1]) if row[1] else {},
                    "transcript": row[2]
                })
            return items

    def get_pending_items(self, limit: int = 10) -> List[Dict]:
        """Returns items that are either 'started' or 'transcribed', up to limit."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT item_id, original_metadata, transcript, status FROM history WHERE status IN ('started', 'transcribed') LIMIT ?",
                (limit,)
            )
            items = []
            for row in cursor.fetchall():
                items.append({
                    "item_id": row[0],
                    "metadata": json.loads(row[1]) if row[1] else {},
                    "transcript": row[2],
                    "status": row[3]
                })
            return items

    def get_run_items(self, run_id: str) -> List[Dict]:
        """Returns all items associated with a specific run."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT item_id, original_metadata, suggested_metadata, status FROM history WHERE run_id = ?",
                (run_id,)
            )
            items = []
            for row in cursor.fetchall():
                items.append({
                    "item_id": row[0],
                    "original_metadata": json.loads(row[1]) if row[1] else {},
                    "suggested_metadata": json.loads(row[2]) if row[2] else {},
                    "status": row[3]
                })
            return items

    def list_runs(self) -> List[Dict]:
        """Returns a list of unique runs and their summary."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT run_id, MIN(timestamp), MAX(last_updated), COUNT(*)
                FROM history 
                GROUP BY run_id 
                ORDER BY MIN(timestamp) DESC
            """)
            runs = []
            for row in cursor.fetchall():
                runs.append({
                    "run_id": row[0] or "unknown",
                    "start": row[1],
                    "end": row[2],
                    "count": row[3]
                })
            return runs

    def get_run_summary(self, run_id: str) -> Dict:
        """Returns stats for a specific run."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) 
                FROM history 
                WHERE run_id = ? 
                GROUP BY status
            """, (run_id,))
            stats = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get timing
            cursor = conn.execute("SELECT MIN(timestamp), MAX(last_updated) FROM history WHERE run_id = ?", (run_id,))
            timing = cursor.fetchone()
            
            return {
                "run_id": run_id,
                "stats": stats,
                "start": timing[0] if timing else None,
                "end": timing[1] if timing else None
            }

    def get_total_summary(self) -> Dict:
        """Returns global stats."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT status, COUNT(*) FROM history GROUP BY status")
            stats = {row[0]: row[1] for row in cursor.fetchall()}
            return stats

    def get_item_detail(self, item_id: str) -> Optional[Dict]:
        """Returns full history for a single item ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT * FROM history WHERE item_id = ? ORDER BY timestamp DESC LIMIT 1",
                (item_id,)
            )
            row = cursor.fetchone()
            if row:
                # Column names from history table: id, item_id, run_id, timestamp, last_updated, original_metadata, transcript, suggested_metadata, status
                return {
                    "item_id": row[1],
                    "run_id": row[2],
                    "timestamp": row[3],
                    "last_updated": row[4],
                    "original_metadata": json.loads(row[5]) if row[5] else {},
                    "transcript": row[6],
                    "suggested_metadata": json.loads(row[7]) if row[7] else {},
                    "status": row[8]
                }
        return None
