import json
import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import matches_dir, schemas_dir, lookup_dir
from common.utils import setup_logging, load_json

from transform.utils.schema_build import create_master_schema
from transform.utils.schema_io import export_schema, load_schema

setup_logging()

logger = logging.getLogger(__name__)


# load match data
@task
def load_matches() -> pl.LazyFrame:
    match_json_list = list(matches_dir.glob("*.json"))
    # throw error if no match files found
    if not match_json_list:
        logger.error("No match data files found")
        raise
    
    schema_path = schemas_dir / "match_schema.yaml"        
    try:
        schema = load_schema(schema_path)
    except FileNotFoundError:
        # infer schema if schema file not found
        logger.warning(f"Schema file not found: {schema_path}. Inferring match schema from data...")
        schema = create_master_schema(match_json_list)
        export_schema(schema, schema_path)
    
    match_dfs = []
    for json_file in match_json_list:
        with open(json_file, "r", encoding="utf-8") as file:
            match_json = json.load(file)
        try:
            match_df = pl.DataFrame([match_json], strict=False, schema=schema).lazy()
            match_dfs.append(match_df)
        except Exception as e:
            # throw error if json parsing fails even after schema inference attempt
            logger.error(f"Schema mismatch or error parsing {json_file.name}: {e}")
            raise
    
    return pl.concat(match_dfs, how="diagonal_relaxed")


# extract matches reference
@task
def select_match_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    round_map = {
        "1/16": 4,
        "1/8": 5,
        "1/4": 6,
        "1/2": 7,
        "bronze": 8,
        "final": 8,
    }
    
    matches_ref_lf = (
        lf
        .select(pl.col('fixtures').struct.field('allMatches'),)
        .explode('allMatches', empty_as_null=True)
        .select(
            # basic match info
            pl.col('allMatches').struct.field('id').cast(pl.Int64).alias('match_id'),
            pl.col('allMatches').struct.field('round').replace(round_map).cast(pl.Int64).alias('round_num'),
            pl.when(pl.col('allMatches').struct.field('round').is_in(['1', '2', '3']))
                .then(pl.lit('group phase'))
                .otherwise(pl.col('allMatches').struct.field('roundName').str.to_lowercase())
                .alias('phase'),
            
            # teams
            pl.col('allMatches').struct.field('home').struct.field('id').cast(pl.Int64).alias('hometeam_id'),   
            pl.col('allMatches').struct.field('home').struct.field('name').alias('hometeam'),   
            pl.col('allMatches').struct.field('away').struct.field('id').cast(pl.Int64).alias('awayteam_id'),   
            pl.col('allMatches').struct.field('away').struct.field('name').alias('awayteam'),
            
            # time
            pl.col('allMatches').struct.field('status').struct.field('utcTime').str.to_datetime(time_zone='UTC')
                .dt.date().alias('match_date_utc'),
            pl.col('allMatches').struct.field('status').struct.field('utcTime').str.to_datetime(time_zone='UTC')
                .dt.time().alias('start_time_utc'),
            
            # match status
            pl.col('allMatches').struct.field('status').struct.field('started').alias('is_started'),
            pl.col('allMatches').struct.field('status').struct.field('finished').alias('is_finished'),
            pl.col('allMatches').struct.field('status').struct.field('cancelled').alias('is_cancelled'),
            pl.col('allMatches').struct.field('status').struct.field('awarded').alias('is_awarded'),
            pl.col('allMatches').struct.field('status').struct.field('reason').struct.field('long')
                .str.to_lowercase().alias('status'),
        )
    )
    
    return matches_ref_lf


@task
def select_match_venues(lf: pl.LazyFrame) -> pl.LazyFrame:
    match_venue_lf = (
        lf
        .select(
            pl.col('general').struct.field('matchId').cast(pl.Int64).alias('match_id'),
            pl.col('seo').struct.field('eventJSONLD').struct.field('location')
                .struct.field('name').alias('venue'),
            pl.col('seo').struct.field('eventJSONLD').struct.field('location')
                .struct.field('address').struct.field('addressCountry').alias('host_nation'),
            pl.col('seo').struct.field('eventJSONLD').struct.field('location')
                .struct.field('address').struct.field('latitude'),
            pl.col('seo').struct.field('eventJSONLD').struct.field('location')
                .struct.field('address').struct.field('longitude'),
        )
    )
    
    return match_venue_lf


if __name__ == "__main__":
    load_matches()