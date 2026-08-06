import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from transform.transform_comp import load_comp_data, create_group_ref
from transform.transform_matches import load_matches, create_match_reference

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_transform", log_prints=True)
def transform_all() -> None:
    logger.info(f"Data transformation started at {datetime.now()}...\n")
    raw_comp_lf = load_comp_data()
    group_ref_lf = create_group_ref(raw_comp_lf)
    match_ref_lf = create_match_reference(raw_comp_lf)
    logger.info(f"Data transformation completed at {datetime.now()}\n")