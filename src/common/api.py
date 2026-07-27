import requests
from pathlib import Path
from typing import Any

# api base variables
BASE_URL = "https://www.fotmob.com"

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:151.0) Gecko/20100101 Firefox/151.0',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': f'{BASE_URL}/leagues/77/overview/world-cup',
}

def fetch_json(endpoint: str, headers: dict, timeout: int=10) -> dict[str, Any]:
    response = requests.get(endpoint, headers=headers, timeout=timeout)
    response.raise_for_status()
    json_content = response.json()
    return json_content
