import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from extract.extract_all import extract_all

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_pipeline", log_prints=True)
def pipeline() -> None:
    logger.info(f"Starting pipeline at {datetime.now()}...\n")
    extract_all()
    logger.info(f"Pipeline run completed at {datetime.now()}")


if __name__ == "__main__":
    pipeline()