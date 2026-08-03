import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import comp_path
from common.utils import setup_logging, load_json

setup_logging()

logger = logging.getLogger(__name__)


@task
def load_comp_data() -> pl.LazyFrame:
    try:
        return pl.DataFrame([load_json(comp_path)], strict=False).lazy()
    except FileNotFoundError:
        logger.error(f"File not found: {comp_path}")
        raise