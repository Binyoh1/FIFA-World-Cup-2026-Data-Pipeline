import logging
from pathlib import Path

import requests
import polars as pl
from prefect import task

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path
from common.utils import DELAY, setup_logging, save_json, load_json

setup_logging()

logger = logging.getLogger(__name__)

# fetch and save competition data
@task(retries=3, retry_delay_seconds=DELAY)
def fetch_and_save_comp_data(endpoint: str, headers: dict, ouput_path: Path) -> None:
    logging.info("Fetching competition data...")    
    try:        
        comp_json = fetch_json(endpoint, headers)
        save_json(ouput_path, comp_json)        
        logging.info("Successfully saved competition data\n")        
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise


@task
def load_comp_data(file_path: Path) -> pl.LazyFrame:
    return pl.DataFrame([load_json(file_path)], strict=False).lazy()


@task
def get_played_fixtures(df: pl.LazyFrame) -> pl.DataFrame:
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
    )
    return played_fixtures.collect()


@task
def extract_unique_teams(df: pl.LazyFrame) -> pl.DataFrame:
    comp_teams = (
            df
            .select(pl.col('overview').struct.field('table'))
            .explode('table', empty_as_null=True)
            .select(pl.col('table').struct.field('data').struct.field('tables'))
            .explode('tables', empty_as_null=True)
            .unnest('tables')
            .select(pl.col('table').struct.field('all'),)
            .explode('all', empty_as_null=True)
            .select(pl.col('all').struct.field('id').alias('team_id'),)
            .unique(subset=['team_id'])
        )
    return comp_teams.collect()


if __name__ == "__main__":
    comp_endpoint = f"{BASE_URL}/api/data/leagues?id=77"
    fetch_and_save_comp_data(comp_endpoint, DEFAULT_HEADERS, comp_path)