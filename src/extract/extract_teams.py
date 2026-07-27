import logging
from pathlib import Path

import requests
from prefect import task
import polars as pl

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path, teams_dir
from common.utils import DELAY, save_json, setup_logging

from extract.extract_comp import load_comp_data

setup_logging()

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=DELAY)
def fetch_and_save_team_data(team_id: int, headers: dict, dir_path: Path) -> None:
    logging.info(f"Fetching team-{team_id} data...")
    try:
        endpoint = f"{BASE_URL}/api/data/teams?id={team_id}"
        team_json = fetch_json(endpoint, headers)
        team_path = dir_path / f"team_data_{team_id}.json"
        save_json(team_path, team_json)
        logging.info(f"Successfully saved team-{team_id} data\n")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error team-{team_id}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise


@task
def get_team_ids(df: pl.LazyFrame) -> set[int]:
    comp_teams = (
            df
            .select(pl.col('overview').struct.field('table'))
            .explode('table', empty_as_null=True)
            .select(pl.col('table').struct.field('data').struct.field('tables'))
            .explode('tables', empty_as_null=True)
            .unnest('tables')
            .select(pl.col('table').struct.field('all'),)
            .explode('all', empty_as_null=True)
            .select(pl.col('all').struct.field('id').cast(pl.Int64).alias('team_id'),)
            .unique(subset=['team_id'])
        ).collect()
    return set(comp_teams['team_id'].to_list())


@task
def get_saved_team_ids(dir_path: Path) -> set[int]:
    return {int(json_file.stem.split("_")[-1]) for json_file in list(dir_path.glob("*.json"))}


@task
def extract_teams(dir_path: Path, headers: dict=DEFAULT_HEADERS) -> None:
    raw_comp_df = load_comp_data(comp_path)
    team_ids = get_team_ids(raw_comp_df)
    teams_to_fetch = team_ids - get_saved_team_ids(dir_path)
    logger.info(f"Found {len(teams_to_fetch)} teams to fetch (of {len(team_ids)} total)")
    for team_id in teams_to_fetch:
        fetch_and_save_team_data(team_id, headers, dir_path)


if __name__ == "__main__":
    extract_teams(teams_dir)