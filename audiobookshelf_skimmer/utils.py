import json
import logging
import signal
import sys
from pathlib import Path
from typing import Dict

# Configure global logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync_metadata.log")
    ]
)
logger = logging.getLogger("sync_metadata")

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logger.info("Interrupt received. Will exit after current item/phase.")
        self.kill_now = True

def load_config(config_path: Path) -> Dict:
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)
