import polars as pl
from prefect import task

from common.paths import players_dir

from transform.utils.data_io import load_data


# load player data
@task
def load_players() -> pl.LazyFrame:
    return load_data(players_dir, "player")