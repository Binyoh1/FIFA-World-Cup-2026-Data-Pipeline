import logging
import time
from pathlib import Path
from random import randint

import requests
from prefect import task

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path, matches_dir
from common.utils import DELAY, save_json, setup_logging
from extract_comp import load_comp_data, get_played_fixtures

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


@task
def get_saved_match_ids(dir_path: Path) -> set[int]:
    return {int(json_file.stem.split("_")[-1]) for json_file in list(dir_path.glob("*.json"))}

@task
def extract_matches(played_match_ids: set[int], dir_path: Path, headers: dict=DEFAULT_HEADERS) -> None:
    matches_to_fetch = played_match_ids - get_saved_match_ids(dir_path)
    logger.info(f"Found {len(matches_to_fetch)} matches to fetch (of {len(played_match_ids)} played)")
    for match_id in matches_to_fetch:
        fetch_and_save_match_data(match_id, headers, dir_path)



if __name__ == "__main__":
    # get played matches
    raw_comp_df = load_comp_data(comp_path)
    played_fixtures = get_played_fixtures(raw_comp_df)
    played_match_ids = set(played_fixtures['match_id'].to_list())
    
    # fetch and save match data
    extract_matches(played_match_ids, matches_dir)