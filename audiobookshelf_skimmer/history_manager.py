import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

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
                    timestamp TEXT NOT NULL,
                    original_metadata TEXT,
                    transcript TEXT,
                    suggested_metadata TEXT,
                    status TEXT
                )
            """)
            conn.commit()

    def log_start(self, item_id: str, original_metadata: Dict):
        """Logs the start of processing for an item with its original metadata."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO history (item_id, timestamp, original_metadata, status) VALUES (?, ?, ?, ?)",
                (item_id, datetime.now().isoformat(), json.dumps(original_metadata), "started")
            )
            conn.commit()

    def save_transcript(self, item_id: str, transcript: str):
        """Updates the history record with the transcript."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE history SET transcript = ?, status = ? WHERE item_id = ? AND status = ?",
                (transcript, "transcribed", item_id, "started")
            )
            conn.commit()

    def save_result(self, item_id: str, suggested_metadata: Dict, status: str = "applied"):
        """Updates the history record with the suggested metadata and final status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE history SET suggested_metadata = ?, status = ? WHERE item_id = ? AND status = ?",
                (json.dumps(suggested_metadata), status, item_id, "transcribed")
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
