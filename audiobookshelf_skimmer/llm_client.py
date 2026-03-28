import json
import logging
import gc
import mlx.core as mx
from mlx_lm import load, generate
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class LLMClient:
    DEFAULT_MODEL = "mlx-community/Llama-3.2-3B-Instruct-4bit"
    
    def __init__(self, model_id: str = DEFAULT_MODEL, system_prompt: str = "", history_manager=None):
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.model = None
        self.tokenizer = None
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
                logger.info(f"Loading LLM model: {self.model_id}")
                load_path = self.model_id
                if is_verified:
                    try:
                        from huggingface_hub import snapshot_download
                        load_path = snapshot_download(self.model_id, local_files_only=True)
                        logger.info(f"Resolved local path: {load_path}")
                    except Exception as e:
                        logger.warning(f"Could not resolve local path for {self.model_id}: {e}")
                self.model, self.tokenizer = load(load_path)
                logger.info("LLM model loaded.")
                
                # If we successfully loaded and weren't verified, mark as verified now
                if self.history_manager and not is_verified:
                    logger.info(f"Model {self.model_id} loaded successfully. Marking as verified for future offline runs.")
                    self.history_manager.set_app_metadata(verification_key, "True")
                    
            except Exception as e:
                if is_verified:
                    logger.warning(f"Offline load failed for {self.model_id}, retrying in ONLINE mode: {e}")
                    os.environ["HF_HUB_OFFLINE"] = "0"
                    self.model, self.tokenizer = load(self.model_id)
                    logger.info("LLM model loaded (Online fallback).")
                else:
                    raise e

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
            mx.clear_cache()
            logger.info("LLM model unloaded and cache cleared.")

    def query_metadata(self, transcript: str, current_metadata: Dict, duration_sec: int = 120, no_metadatahints: bool = False) -> Dict:
        """
        Uses local mlx-lm to analyze transcript and metadata.
        """
        self.load_model()
        
        prompt_text = f"System: {self.system_prompt}\n\n"
        prompt_text += f"TRANSCRIPT (first {duration_sec}s):\n{transcript}\n\n"
        
        if not no_metadatahints:
            prompt_text += f"CURRENT METADATA:\n{json.dumps(current_metadata, indent=2)}\n\n"
            instruction = (
                "Please provide the corrected metadata for Title, Author, Narrator, and Publisher for this book based on the transcript. "
                "Return only a JSON object with keys: 'title', 'author', 'narrator', 'publisher'. "
                "If you are unsure of a field, use the current metadata value."
            )
        else:
            instruction = (
                "Please extract the Title, Author, Narrator, and Publisher for this book based ONLY on the provided transcript. "
                "Do not use any external knowledge. If a field is not mentioned in the transcript, use 'Unknown' or your best guess from context. "
                "Return only a JSON object with keys: 'title', 'author', 'narrator', 'publisher'."
            )
            
        prompt_text += instruction

        logger.info(f"Generating LLM response for {self.model_id}...")
        
        # Apply prompt template if instruction model
        if hasattr(self.tokenizer, "apply_chat_template") and self.tokenizer.chat_template:
             user_content = f"TRANSCRIPT (first {duration_sec}s):\n{transcript}"
             if not no_metadatahints:
                 user_content += f"\n\nCURRENT METADATA:\n{json.dumps(current_metadata)}"
             
             messages = [
                 {"role": "system", "content": self.system_prompt},
                 {"role": "user", "content": user_content}
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

    def is_hallucinated(self, suggested: Dict, transcript: str, current_metadata: Dict = None) -> bool:
        """
        Check if the LLM invented values not supported by the transcript.
        
        If the suggested value matches the existing metadata, the LLM was just
        preserving what it was given (transcript lacks the info) — not hallucinating.
        """
        transcript_lower = transcript.lower()
        import re
        
        # Map suggested keys to the ABS metadata keys for comparison.
        # We check both flat summary fields and structured list fields.
        metadata_key_map = {
            "title": ["title"],
            "author": ["authorName", "authors"],
            "narrator": ["narratorName", "narrators"],
            "publisher": ["publisher"]
        }
        
        fields_to_check = ["title", "author", "narrator"]
        for field in fields_to_check:
            val = suggested.get(field, "")
            if not val or not isinstance(val, str):
                continue
            
            # If this matches what was already in the metadata, the LLM is
            # just falling back to the given value — skip hallucination check.
            if current_metadata:
                abs_keys = metadata_key_map.get(field, [field])
                is_match = False
                
                for abs_key in abs_keys:
                    existing = current_metadata.get(abs_key)
                    if not existing:
                        continue
                        
                    if isinstance(existing, list):
                        for item in existing:
                            text = item.get('name', '') if isinstance(item, dict) else str(item)
                            if text.strip().lower() == val.strip().lower():
                                is_match = True
                                break
                    elif str(existing).strip().lower() == val.strip().lower():
                        is_match = True
                    
                    if is_match:
                        break
                
                if is_match:
                    logger.debug(f"'{field}' matches existing metadata ('{val}') — not a hallucination.")
                    continue
                
            words = re.findall(r'\b\w{3,}\b', val.lower())
            if not words:
                continue
            
            missing_words = [w for w in words if w not in transcript_lower]
            if field in ["title", "author"] and len(missing_words) > (len(words) / 2):
                logger.warning(f"Hallucination detection: '{field}' suggested '{val}' but major words missing from transcript (and doesn't match existing metadata).")
                return True
                
        return False
