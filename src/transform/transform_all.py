import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from transform.transform_comp import load_comp_data

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_transform", log_prints=True)
def transform_all() -> None:
    logger.info(f"Data transformation started at {datetime.now()}...\n")
    load_comp_data()
    logger.info(f"Data transformation completed at {datetime.now()}\n")