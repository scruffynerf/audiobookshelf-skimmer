import time
import logging
import gc
from pathlib import Path
import mlx.core as mx
from parakeet_mlx import from_pretrained

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "mlx-community/parakeet-tdt-0.6b-v2"

class Transcriber:
    def __init__(self, model_id: str = DEFAULT_MODEL):
        self.model_id = model_id
        self.model = None

    def load_model(self):
        if not self.model:
            logger.info(f"Loading transcription model: {self.model_id}")
            t0 = time.time()
            self.model = from_pretrained(self.model_id)
            elapsed = time.time() - t0
            logger.info(f"Model loaded in {elapsed:.1f}s")

    def unload_model(self):
        """
        Explicitly unloads the model and clears the Metal cache
        to free up memory for the LLM.
        """
        if self.model:
            logger.info(f"Unloading transcription model: {self.model_id}")
            self.model = None
            gc.collect()
            mx.metal.clear_cache()
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
