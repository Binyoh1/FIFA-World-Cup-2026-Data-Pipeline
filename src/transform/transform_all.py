import logging
from datetime import datetime

from prefect import flow

from common.utils import setup_logging

from transform.transform_comp import load_comp_data, select_groups
from transform.transform_matches import (
    load_matches,
    select_match_info,
    select_match_venues
)
from transform.transform_teams import (
    load_teams,
    select_teams,
    select_team_info
)
from transform.transform_players import load_players, select_player_info

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_transform", log_prints=True)
def transform_all() -> None:
    logger.info(f"Data transformation started at {datetime.now()}...\n")
    raw_comp_lf = load_comp_data()
    raw_matches_lf = load_matches()
    raw_teams_lf = load_teams()
    raw_players_lf = load_players()
    
    # teams and groups
    group_ref_lf = select_groups(raw_comp_lf)
    team_ref_lf = select_teams(raw_comp_lf)
    team_info_ref_lf = select_team_info(raw_teams_lf)
    
    # matches
    match_info_ref_lf = select_match_info(raw_comp_lf)
    match_venue_ref_lf = select_match_venues(raw_matches_lf)
    
    # players
    player_info_ref_lf = select_player_info(raw_players_lf)
    logger.info(f"Data transformation completed at {datetime.now()}\n")