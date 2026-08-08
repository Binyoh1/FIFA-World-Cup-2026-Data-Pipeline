import json
import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import teams_dir, schemas_dir
from common.utils import setup_logging, load_json

from transform.utils.schema_build import create_master_schema
from transform.utils.schema_io import load_schema, export_schema

setup_logging()

logger = logging.getLogger(__name__)


# load team data
@task
def load_teams() -> pl.LazyFrame:
    team_json_list = list(teams_dir.glob("*.json"))
    
    if not team_json_list:
        logger.error("No team data files found")
        raise
    
    schema_path = schemas_dir / "team_schema.yaml"
    try:
        schema = load_schema(schema_path)
    except FileNotFoundError:
        logger.warning(f"Schema file not found: {schema_path}. Inferring team schema from data...")
        schema = create_master_schema(team_json_list)
        export_schema(schema, schema_path)
    
    team_dfs = []
    for json_file in team_json_list:
        team_json = load_json(json_file)
        try:
            team_df = pl.DataFrame([team_json], strict=False, schema=schema).lazy()
            team_dfs.append(team_df)
        except Exception as e:
            # throw error if json parsing fails even after schema inference attempt
            logger.error(f"Schema mismatch or error parsing {json_file.name}: {e}")
            raise
    
    return pl.concat(team_dfs, how="diagonal_relaxed")


@task
def select_teams(lf: pl.LazyFrame) -> pl.LazyFrame:
    teams_ref_lf = (
        lf
        .select(pl.col('overview').struct.field('table'))
        .explode('table', empty_as_null=True)
        .select(pl.col('table').struct.field('data').struct.field('tables'))
        .explode('tables', empty_as_null=True)
        .unnest('tables')
        .select(
            pl.col('leagueId').cast(pl.Int32).alias('group_id'),
            pl.col('leagueName').str.replace("Grp.", "Group").alias('group'),
            pl.col('table').struct.field('all')
        )
        .filter(pl.col('group').str.starts_with('Group'))
        .explode('all', empty_as_null=True)
        .select(
            pl.col('all').struct.field('id').cast(pl.Int64).alias('team_id'),
            pl.col('all').struct.field('name').alias('team'),
            pl.col('group_id'),
        )
    )
    
    return teams_ref_lf


@task
def select_team_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    team_info_lf = (
        lf
        .select(
            pl.col('details').struct.field('id').alias('team_id'),
            pl.col('details').struct.field('fifaRanking').struct.field('rank').alias('fifa_rank'),        
            pl.col('details').struct.field('fifaRanking').struct.field('points').alias('rank_points'),     
            pl.col('details').struct.field('fifaRanking').struct.field('updated')
                .str.to_date("%d.%m.%Y").alias('rank_last_updated'),
            pl.col('squad').struct.field('squad').list.get(0).struct.field('members'),
        )
        .explode('members', empty_as_null=True)
        .with_columns(
            pl.col('members').struct.field('id').alias('head_coach_id'),
            pl.col('members').struct.field('name').alias('head_coach'),
        )
    )
    
    return team_info_lf