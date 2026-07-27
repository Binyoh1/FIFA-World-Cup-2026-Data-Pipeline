import logging
from pathlib import Path

import requests
from prefect import task
import polars as pl

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path, matches_dir
from common.utils import DELAY, save_json, setup_logging

from extract.extract_comp import load_comp_data

setup_logging()

logger = logging.getLogger(__name__)

@task(retries=3, retry_delay_seconds=DELAY)
def fetch_and_save_match_data(match_id: int, headers: dict, dir_path: Path) -> None:
    logger.info(f"Fetching match-{match_id} data...")   
    try:
        endpoint = f"{BASE_URL}/api/data/matchDetails?matchId={match_id}"        
        match_json = fetch_json(endpoint, headers)
        match_path = dir_path / f"match_data_{match_id}.json"
        save_json(match_path, match_json)
        logger.info(f"Successfully saved match-{match_id} data\n")        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error match-{match_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise


@task
def get_played_match_ids(df: pl.LazyFrame) -> set[int]:
    played_fixtures = (
        df
        .select(pl.col('fixtures').struct.field('allMatches'),)
        .explode('allMatches', empty_as_null=True)
        .select(
            # basic match info
            pl.col('allMatches').struct.field('id').cast(pl.Int64).alias('match_id'),
            pl.col('allMatches').struct.field('status').struct.field('finished')
                .cast(pl.Boolean).alias('is_finished'),
        )
        .filter(pl.col('is_finished'))
        .select(pl.col('match_id'))
        .collect()
    )
    return set(played_fixtures['match_id'].to_list())


@task
def get_saved_match_ids(dir_path: Path) -> set[int]:
    return {int(json_file.stem.split("_")[-1]) for json_file in list(dir_path.glob("*.json"))}

@task
def extract_matches(dir_path: Path, headers: dict=DEFAULT_HEADERS) -> None:
    raw_comp_df = load_comp_data(comp_path)
    played_match_ids = get_played_match_ids(raw_comp_df)
    matches_to_fetch = played_match_ids - get_saved_match_ids(dir_path)
    logger.info(f"Found {len(matches_to_fetch)} matches to fetch (of {len(played_match_ids)} played)")
    for match_id in matches_to_fetch:
        fetch_and_save_match_data(match_id, headers, dir_path)



if __name__ == "__main__":
    extract_matches(matches_dir)