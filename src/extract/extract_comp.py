import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
import polars as pl
from prefect import task

from common.api import COMP_ENDPOINT, DEFAULT_HEADERS, fetch_json
from common.paths import comp_path
from common.utils import DELAY, setup_logging, save_json, load_json

setup_logging()

logger = logging.getLogger(__name__)

# fetch and save competition data
@task(retries=3, retry_delay_seconds=DELAY)
def extract_comp(endpoint: str, headers: dict, output_path: Path) -> None:
    logger.info("Fetching competition data...")
    
    def fetch_and_save_comp_data() -> None:
        try:
            comp_json = fetch_json(endpoint, headers)
            save_json(output_path, comp_json)
            logger.info("Successfully saved competition data\n")
        except requests.exceptions.RequestException as inner_e1:
            logger.error(f"Network error while fetching competition_data: {inner_e1}")
            raise
        except Exception as inner_e2:
            logger.error(f"Error fetching competition data: {inner_e2}")
            raise
        
    if not output_path.exists():
        logger.info("No saved competition data found. Fetching new data...")
        fetch_and_save_comp_data()
        return
        
    last_saved_timestamp = datetime.fromtimestamp(
        output_path.stat().st_mtime, 
        tz=timezone.utc
    )
    
    saved_comp_json = load_json(output_path)
    comp_lf = pl.DataFrame([saved_comp_json], strict=False).lazy()
    last_played_fixture_timestamp = (
        comp_lf
        .select(pl.col('fixtures').struct.field('allMatches'))
        .explode('allMatches', empty_as_null=True)
        .select(
            pl.col('allMatches')
                .struct.field('status')
                .struct.field('utcTime')
                .str.to_datetime(time_zone='UTC'),
            pl.col('allMatches')
                .struct.field('status')
                .struct.field('finished')
                .cast(pl.Boolean)
                .alias('is_finished'),
        )
        .filter(pl.col('is_finished'))
        .select(pl.col('utcTime').max())
        .collect()
        .item()
    )
    
    if last_played_fixture_timestamp is not None:
        last_finished_fixture_end_time = last_played_fixture_timestamp + timedelta(hours=3)
    
    if last_finished_fixture_end_time > last_saved_timestamp:
        fetch_and_save_comp_data()
    else:
        logger.debug("Competition data is up-to-date. No new data fetched.")


@task
def load_comp_data(file_path: Path) -> pl.LazyFrame:
    return pl.DataFrame([load_json(file_path)], strict=False).lazy()


if __name__ == "__main__":
    extract_comp(COMP_ENDPOINT, DEFAULT_HEADERS, comp_path)