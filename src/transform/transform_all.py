import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from transform.transform_comp import load_comp_data, get_groups
from transform.transform_matches import load_matches, get_match_info, get_match_venues
from transform.transform_teams import load_teams, get_teams, get_team_info

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_transform", log_prints=True)
def transform_all() -> None:
    logger.info(f"Data transformation started at {datetime.now()}...\n")
    raw_comp_lf = load_comp_data()
    raw_matches_lf = load_matches()
    raw_teams_lf = load_teams()
    
    # teams and groups
    group_ref_lf = get_groups(raw_comp_lf)
    team_ref_lf = get_teams(raw_comp_lf)
    team_info_ref_lf = get_team_info(raw_teams_lf)
    
    # matches
    match_info_ref_lf = get_match_info(raw_comp_lf)
    match_venue_ref_lf = get_match_venues(raw_matches_lf)
    logger.info(f"Data transformation completed at {datetime.now()}\n")