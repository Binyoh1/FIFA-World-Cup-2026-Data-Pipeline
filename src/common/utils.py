import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
from functools import reduce

import polars as pl

# get today's utc timestamp
UTC_NOW = datetime.now(timezone.utc)

# request delay
DELAY = 5

# configure logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_json(path: Path, json_content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(json_content, f, indent=4)


def load_json(path: Path) -> dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)