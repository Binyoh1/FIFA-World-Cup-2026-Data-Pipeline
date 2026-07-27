import requests
import time
import logging
from pathlib import Path
from random import randint

from prefect import task

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path, teams_dir
from common.utils import DELAY, save_json, setup_logging
from extract_comp import load_comp_data, extract_unique_teams

setup_logging()

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=DELAY)
def fetch_and_save_match_data(match_id: int, headers: dict, dir_path: Path) -> None:
    logging.info(f"Fetching and saving match-{match_id} data...")    
    try:
        endpoint = f"{BASE_URL}/api/data/matchDetails?matchId={match_id}"
        match_json = fetch_json(endpoint, headers)
        
        match_path = dir_path / f"match_data_{match_id}.json"
        save_json(match_path, match_json)
        logging.info(f"Fetched and saved match-{match_id} data successfully\n")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error match-{match_id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")


if __name__ == "__main__":
    raw_comp_df = load_comp_data(comp_path)
    unique_teams = extract_unique_teams(raw_comp_df)
    
    for team_id in unique_teams['team_id']:
        fetch_and_save_match_data(team_id, DEFAULT_HEADERS, teams_dir)