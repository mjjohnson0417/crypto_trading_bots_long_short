"""Load config.json into a dict. Called exactly once, from main.py."""
import json
from pathlib import Path


def load_config(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)
