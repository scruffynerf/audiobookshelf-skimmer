import time
import logging
import gc
from pathlib import Path
import mlx.core as mx
from parakeet_mlx import from_pretrained

logger = logging.getLogger(__name__)

class Transcriber:
    DEFAULT_MODEL = "mlx-community/parakeet-tdt-0.6b-v2"
    
    def __init__(self, model_id: str = DEFAULT_MODEL, history_manager=None):
        self.model_id = model_id
        self.model = None
        self.history_manager = history_manager

    def load_model(self):
        if not self.model:
            import os
            verification_key = f"model_verified_{self.model_id}"
            is_verified = False
            if self.history_manager:
                is_verified = self.history_manager.get_app_metadata(verification_key) == "True"

            if is_verified:
                logger.info(f"Model {self.model_id} previously verified. Loading in OFFLINE mode.")
                os.environ["HF_HUB_OFFLINE"] = "1"
            
            try:
                logger.info(f"Loading transcription model: {self.model_id}")
                t0 = time.time()
                load_path = self.model_id
                if is_verified:
                    try:
                        from huggingface_hub import snapshot_download
                        load_path = snapshot_download(self.model_id, local_files_only=True)
                        logger.info(f"Resolved local path: {load_path}")
                    except Exception as e:
                        logger.warning(f"Could not resolve local path for {self.model_id}: {e}")
                self.model = from_pretrained(load_path)
                elapsed = time.time() - t0
                logger.info(f"Model loaded in {elapsed:.1f}s")
                
                # If we successfully loaded and weren't verified, mark as verified now
                if self.history_manager and not is_verified:
                    logger.info(f"Model {self.model_id} loaded successfully. Marking as verified for future offline runs.")
                    self.history_manager.set_app_metadata(verification_key, "True")
                    
            except Exception as e:
                if is_verified:
                    logger.warning(f"Offline load failed for {self.model_id}, retrying in ONLINE mode: {e}")
                    os.environ["HF_HUB_OFFLINE"] = "0"
                    t0 = time.time()
                    self.model = from_pretrained(self.model_id)
                    elapsed = time.time() - t0
                    logger.info(f"Model loaded in {elapsed:.1f}s (Online fallback)")
                else:
                    raise e

    def unload_model(self):
        """
        Explicitly unloads the model and clears the Metal cache
        to free up memory for the LLM.
        """
        if self.model:
            logger.info(f"Unloading transcription model: {self.model_id}")
            self.model = None
            gc.collect()
            mx.clear_cache()
            logger.info("Transcription model unloaded and cache cleared.")

    def transcribe(self, audio_file: Path) -> str:
        """
        Transcribes the full audio file provided (assumed to be a short slice).
        Returns a single concatenated string of the transcript.
        """
        self.load_model()
        
        logger.info(f"Transcribing {audio_file.name}...")
        t0 = time.time()
        
        # Parakeet returns a result with a list of sentence objects.
        result = self.model.transcribe(
            str(audio_file),
            chunk_duration=120.0,
            overlap_duration=15.0
        )
        
        full_text = " ".join([s.text.strip() for s in result.sentences])
        
        elapsed = time.time() - t0
        logger.info(f"Transcription complete in {elapsed:.1f}s ({len(full_text)} chars)")
        
        return full_text
