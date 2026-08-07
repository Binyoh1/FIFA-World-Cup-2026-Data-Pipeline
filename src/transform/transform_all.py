import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from transform.transform_comp import load_comp_data, extract_groups
from transform.transform_matches import load_matches, extract_match_info, extract_match_venues

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_transform", log_prints=True)
def transform_all() -> None:
    logger.info(f"Data transformation started at {datetime.now()}...\n")
    raw_comp_lf = load_comp_data()
    raw_matches_lf = load_matches()
    group_ref_lf = extract_groups(raw_comp_lf)
    match_info_ref_lf = extract_match_info(raw_comp_lf)
    match_venue_ref_lf = extract_match_venues(raw_matches_lf)
    logger.info(f"Data transformation completed at {datetime.now()}\n")