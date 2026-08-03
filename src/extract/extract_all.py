import logging
from datetime import datetime

from prefect import flow

from common.api import COMP_ENDPOINT, DEFAULT_HEADERS
from common.paths import comp_path, matches_dir, teams_dir, players_dir
from common.utils import setup_logging

from extract.extract_comp import extract_comp
from extract.extract_matches import extract_matches
from extract.extract_players import extract_players
from extract.extract_teams import extract_teams

setup_logging()

logger = logging.getLogger(__name__)

@flow(name="fotmob_fwc2026_extract", log_prints=True)
def extract_all() -> None:
    logger.info(f"Data extraction started at {datetime.now()}...\n")
    extract_comp(COMP_ENDPOINT, DEFAULT_HEADERS, comp_path)
    extract_matches(matches_dir)
    extract_teams(teams_dir)
    extract_players(teams_dir, players_dir)
    logger.info(f"Data extraction completed at {datetime.now()}\n")

if __name__ == "__main__":
    extract_all()