import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import teams_dir
from common.utils import setup_logging, load_json

setup_logging()

logger = logging.getLogger(__name__)


@task
def extract_teams_and_groups(df: pl.LazyFrame) -> pl.LazyFrame:
    pass