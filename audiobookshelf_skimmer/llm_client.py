import json
import logging
import gc
import mlx.core as mx
from mlx_lm import load, generate
from typing import Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "mlx-community/Llama-3.2-3B-Instruct-4bit"

class LLMClient:
    def __init__(self, model_id: str = DEFAULT_MODEL, system_prompt: str = ""):
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.model = None
        self.tokenizer = None

    def load_model(self):
        if not self.model:
            logger.info(f"Loading LLM model: {self.model_id}")
            self.model, self.tokenizer = load(self.model_id)
            logger.info("LLM model loaded.")

    def unload_model(self):
        """
        Explicitly unloads the model and clears the Metal cache
        to free up memory for the transcriber.
        """
        if self.model:
            logger.info(f"Unloading LLM model: {self.model_id}")
            self.model = None
            self.tokenizer = None
            gc.collect()
            mx.metal.clear_cache()
            logger.info("LLM model unloaded and cache cleared.")

    def query_metadata(self, transcript: str, current_metadata: Dict, duration_sec: int = 120) -> Dict:
        """
        Uses local mlx-lm to analyze transcript and metadata.
        """
        self.load_model()
        
        prompt_text = f"System: {self.system_prompt}\n\n"
        prompt_text += (
            f"TRANSCRIPT (first {duration_sec}s):\n{transcript}\n\n"
            f"CURRENT METADATA:\n{json.dumps(current_metadata, indent=2)}\n\n"
            "Please provide the corrected metadata for Title, Author, Narrator, and Publisher for this book based on the transcript. "
            "Return only a JSON object with keys: 'title', 'author', 'narrator', 'publisher'. "
            "If you are unsure of a field, use the current metadata value."
        )

        logger.info(f"Generating LLM response for {self.model_id}...")
        
        # Apply prompt template if instruction model
        if hasattr(self.tokenizer, "apply_chat_template") and self.tokenizer.chat_template:
             messages = [
                 {"role": "system", "content": self.system_prompt},
                 {"role": "user", "content": f"TRANSCRIPT (first {duration_sec}s):\n{transcript}\n\nCURRENT METADATA:\n{json.dumps(current_metadata)}"}
             ]
             prompt_text = self.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

        response = generate(self.model, self.tokenizer, prompt=prompt_text, verbose=False)
        
        # Attempt to extract JSON from response
        try:
            # Find JSON block
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                suggested = json.loads(json_match.group(0))
                return {k.lower(): v for k, v in suggested.items()}
            logger.error(f"No JSON found in LLM response: {response}")
            return {}
        except json.JSONDecodeError:
            logger.error(f"LLM did not return valid JSON: {response}")
            return {}

    def is_hallucinated(self, suggested: Dict, transcript: str) -> bool:
        """Simple check for hallucinations."""
        transcript_lower = transcript.lower()
        import re
        
        fields_to_check = ["title", "author", "narrator"]
        for field in fields_to_check:
            val = suggested.get(field, "")
            if not val or not isinstance(val, str):
                continue
                
            words = re.findall(r'\b\w{3,}\b', val.lower())
            if not words: continue
            
            missing_words = [w for w in words if w not in transcript_lower]
            if field in ["title", "author"] and len(missing_words) > (len(words) / 2):
                logger.warning(f"Hallucination detection: '{field}' suggested '{val}' but major words missing from transcript.")
                return True
                
        return False
