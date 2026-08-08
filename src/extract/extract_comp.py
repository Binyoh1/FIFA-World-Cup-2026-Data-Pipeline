import logging
from pathlib import Path

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
def extract_comp(endpoint: str, headers: dict, ouput_path: Path) -> None:
    logger.info("Fetching competition data...")
    try:        
        comp_json = fetch_json(endpoint, headers)
        save_json(ouput_path, comp_json)        
        logger.info("Successfully saved competition data\n")        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise


@task
def load_comp_data(file_path: Path) -> pl.LazyFrame:
    return pl.DataFrame([load_json(file_path)], strict=False).lazy()


if __name__ == "__main__":
    extract_comp(COMP_ENDPOINT, DEFAULT_HEADERS, comp_path)