import json
import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import matches_dir
from common.utils import setup_logging, load_json

from transform.transform_comp import load_comp_data

setup_logging()

logger = logging.getLogger(__name__)


@task
def load_matches(paths: list[Path], schema) -> pl.LazyFrame:
    # pass 2: load json
    match_dfs = []
    for json_file in paths:
        with open(json_file, "r", encoding="utf-8") as file:
            match_json = json.load(file)
        match_df = pl.DataFrame([match_json], strict=False, schema=schema).lazy()
        match_dfs.append(match_df)
    
    return pl.concat(match_dfs, how="diagonal_relaxed")


# @task
# def extract_fixtrues(df: pl.LazyFrame) -> pl.LazyFrame:
#     pass