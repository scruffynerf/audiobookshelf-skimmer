import requests
import logging
from pathlib import Path
from typing import List, Dict, Optional
from .audio_utils import slice_audio

logger = logging.getLogger(__name__)

class ABSClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {"X-Token": self.api_key}

    def get_all_items(self) -> List[Dict]:
        """Fetches all library items from Audiobookshelf."""
        url = f"{self.base_url}/api/items"
        logger.info(f"Fetching items from {url}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()
        return data.get("results", data) if isinstance(data, dict) else data

    def get_item_details(self, item_id: str) -> Dict:
        """Fetches full details for a specific library item."""
        url = f"{self.base_url}/api/items/{item_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_stream_info(self, item_id: str) -> Dict:
        """Starts a playback session via POST /api/items/{itemId}/play."""
        url = f"{self.base_url}/api/items/{item_id}/play"
        response = requests.post(url, headers=self.headers, json={})
        response.raise_for_status()
        return response.json()

    def fetch_audio_slice(self, item_id: str, duration_sec: int = 120) -> Path:
        """Fetches a slice of audio directly from the stream."""
        # Note: reuse the logic from Turn 13
        stream_info = self.get_stream_info(item_id)
        stream_obj = stream_info.get("stream", {})
        stream_url = stream_obj.get("url") or stream_info.get("url")
        
        if not stream_url:
            raise ValueError(f"Could not find stream URL for {item_id}")
        if stream_url.startswith("/"):
            stream_url = f"{self.base_url}{stream_url}"

        headers = {"X-Token": self.api_key}
        return slice_audio(stream_url, duration_sec=duration_sec, headers=headers)

    def update_metadata(self, item_id: str, metadata: Dict):
        """Updates library item metadata."""
        url = f"{self.base_url}/api/items/{item_id}"
        payload = {"media": {"metadata": metadata}}
        requests.patch(url, headers=self.headers, json=payload).raise_for_status()

    def add_tag(self, item_id: str, tag: str):
        """Adds a tag to a library item."""
        item = self.get_item_details(item_id)
        current_tags = item.get("media", {}).get("metadata", {}).get("tags", [])
        if tag not in current_tags:
            current_tags.append(tag)
            self.update_metadata(item_id, {"tags": current_tags})

    def remove_tag(self, item_id: str, tag: str):
        """Removes a tag from a library item."""
        item = self.get_item_details(item_id)
        current_tags = item.get("media", {}).get("metadata", {}).get("tags", [])
        if tag in current_tags:
            current_tags.remove(tag)
            self.update_metadata(item_id, {"tags": current_tags})
            
    def get_tags(self, item_id: str) -> List[str]:
        item = self.get_item_details(item_id)
        return item.get("media", {}).get("metadata", {}).get("tags", [])
