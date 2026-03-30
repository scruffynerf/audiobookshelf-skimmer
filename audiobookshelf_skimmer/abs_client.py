import requests
import logging
import time
from pathlib import Path
from typing import List, Dict, Optional
from .audio_utils import slice_audio

logger = logging.getLogger(__name__)

class ABSClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def iter_items(self, page_size: int = 50, library_name: Optional[str] = None):
        """Yields library items from Audiobookshelf by iterating through libraries with pagination."""
        url = f"{self.base_url}/api/libraries"
        logger.info(f"Fetching libraries from {url}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        libraries = response.json().get("libraries", [])
        
        book_libraries = [lib for lib in libraries if lib.get("mediaType") == "book"]
        lib_names = [lib.get("name") for lib in book_libraries]
        
        if library_name and library_name not in lib_names:
            raise ValueError(f"Library '{library_name}' not found. Available book libraries: {', '.join(lib_names)}")
        
        for lib in book_libraries:
            lib_id = lib.get("id")
            lib_name = lib.get("name")
            
            if library_name and lib_name != library_name:
                continue
                
            logger.info(f"Scanning library '{lib_name}' ({lib_id})")
            page = 0
            while True:
                items_url = f"{self.base_url}/api/libraries/{lib_id}/items"
                params = {"limit": page_size, "page": page}
                item_resp = requests.get(items_url, headers=self.headers, params=params)
                
                if item_resp.status_code != 200:
                    logger.warning(f"Failed to fetch page {page} for library '{lib_name}': {item_resp.status_code}")
                    break
                    
                data = item_resp.json()
                items = data.get("results", [])
                if not items:
                    break
                    
                for item in items:
                    yield item
                
                if len(items) < page_size:
                    break
                    
                page += 1
                time.sleep(0.1) # Minimum throttle between pages

    def get_item_details(self, item_id: str) -> Dict:
        """Fetches full details for a specific library item."""
        url = f"{self.base_url}/api/items/{item_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_item_path(self, item_id: str) -> Optional[str]:
        """Returns the filesystem path for a library item."""
        item = self.get_item_details(item_id)
        # In ABS, the path is usually in item['path'] or item['media']['path']
        # Based on API docs, LibraryItem has a 'path' field.
        return item.get("path") or item.get("media", {}).get("path")

    def get_stream_info(self, item_id: str) -> Dict:
        """Starts a playback session via POST /api/items/{itemId}/play."""
        url = f"{self.base_url}/api/items/{item_id}/play"
        
        payload = {
            "deviceInfo": {
                "clientVersion": "0.0.1",
                "clientName": "Audiobookshelf Skimmer"
            },
            "forceDirectPlay": True,
            "supportedMimeTypes": [
                "audio/flac",
                "audio/mpeg", 
                "audio/mp4",
                "audio/m4a",
                "audio/m4b",
                "audio/aac",
                "audio/ogg",
                "audio/x-m4b"
            ]
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def fetch_audio_slice(self, item_id: str, duration_sec: int = 120) -> Path:
        """Fetches a slice of audio directly from the stream."""
        stream_info = self.get_stream_info(item_id)
        
        # Try different possible locations for the stream URL
        stream_url = None
        
        # 1. Check newer 'audioTracks' structure (v2.29.0+)
        tracks = stream_info.get("audioTracks", [])
        if tracks and isinstance(tracks, list):
             stream_url = tracks[0].get("contentUrl")
        
        # 2. Check traditional 'stream' object
        if not stream_url:
             stream_obj = stream_info.get("stream", {})
             stream_url = stream_obj.get("url")
             
        # 3. Check direct 'url' mapping
        if not stream_url:
             stream_url = stream_info.get("url")
        
        if not stream_url:
            raise ValueError(f"Could not find stream URL for {item_id}")
            
        if stream_url.startswith("/"):
            stream_url = f"{self.base_url}{stream_url}"

        headers = {"Authorization": f"Bearer {self.api_key}"}
        return slice_audio(stream_url, duration_sec=duration_sec, headers=headers)

    def update_metadata(self, item_id: str, metadata: Dict):
        """Updates library item metadata via a safe merge-before-patch approach."""
        current_item = self.get_item_details(item_id)
        current_media = current_item.get("media", {})
        current_metadata = current_media.get("metadata", {})
        
        # Deep merge our updates into the existing metadata
        updated_metadata = current_metadata.copy()
        updated_metadata.update(metadata)
        
        url = f"{self.base_url}/api/items/{item_id}/media"
        payload = {
            "metadata": updated_metadata,
            "tags": current_media.get("tags", [])
        }
        requests.patch(url, headers=self.headers, json=payload).raise_for_status()

    def add_tag(self, item_id: str, tag: str):
        """Adds a tag to a library item via a safe merge-before-patch approach."""
        current_item = self.get_item_details(item_id)
        current_media = current_item.get("media", {})
        current_tags = current_media.get("tags", [])
        
        if tag not in current_tags:
            current_tags.append(tag)
            url = f"{self.base_url}/api/items/{item_id}/media"
            payload = {
                "metadata": current_media.get("metadata", {}),
                "tags": current_tags
            }
            requests.patch(url, headers=self.headers, json=payload).raise_for_status()

    def remove_tag(self, item_id: str, tag: str):
        """Removes a tag from a library item via a safe merge-before-patch approach."""
        current_item = self.get_item_details(item_id)
        current_media = current_item.get("media", {})
        current_tags = current_media.get("tags", [])
        
        if tag in current_tags:
            current_tags.remove(tag)
            url = f"{self.base_url}/api/items/{item_id}/media"
            payload = {
                "metadata": current_media.get("metadata", {}),
                "tags": current_tags
            }
            requests.patch(url, headers=self.headers, json=payload).raise_for_status()
            
    def get_tags(self, item_id: str) -> List[str]:
        item = self.get_item_details(item_id)
        return item.get("media", {}).get("tags", [])
