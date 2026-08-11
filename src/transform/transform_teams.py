import polars as pl
from prefect import task

from common.paths import teams_dir

from transform.utils.data_io import load_data


# load team data
@task
def load_teams() -> pl.LazyFrame:
    return load_data(teams_dir, "team")


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
            pl.col('leagueId')
                .cast(pl.Int32)
                .alias('group_id'),
            pl.col('leagueName').
                str.replace("Grp.", "Group")
                .alias('group'),
            pl.col('table')
                .struct.field('all')
        )
        .filter(pl.col('group').str.starts_with('Group'))
        .explode('all', empty_as_null=True)
        .select(
            pl.col('all')
                .struct.field('id')
                .cast(pl.Int64)
                .alias('team_id'),
            pl.col('all')
                .struct.field('name')
                .alias('team'),
            pl.col('group_id'),
        )
    )
    
    return teams_ref_lf


@task
def select_team_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    team_info_lf = (
        lf
        .select(
            pl.col('details')
                .struct.field('id')
                .cast(pl.UInt32)
                .alias('team_id'),
            pl.col('details')
                .struct.field('fifaRanking')
                .struct.field('rank')
                .cast(pl.UInt8)
                .alias('fifa_rank'),        
            pl.col('details')
                .struct.field('fifaRanking')
                .struct.field('points')
                .cast(pl.UInt16)
                .alias('rank_points'),     
            pl.col('details')
                .struct.field('fifaRanking')
                .struct.field('updated')
                .str.to_date("%d.%m.%Y")
                .alias('rank_last_updated'),
            pl.col('squad')
                .struct.field('squad')
                .list.get(0)
                .struct.field('members')
                .alias('head_coach_info'),
            pl.col('overview')
                .struct.field('venue')
                .struct.field('widget')
                .alias('home_info'),
            pl.col('overview')
                .struct.field('venue')
                .struct.field('statPairs')
                .alias('stadium_info'),
        )
        .explode('head_coach_info', empty_as_null=True)
        .with_columns(
            pl.col('head_coach_info')
                .struct.field('id')
                .alias('head_coach_id'),
            pl.col('head_coach_info')
                .struct.field('name')
                .alias('head_coach'),
            pl.col('home_info')
                .struct.field('city')
                .alias('home_city'),
            pl.col('home_info')
                .struct.field('name')
                .alias('home_stadium'),
            pl.col('home_info')
                .struct.field('location')
                .list.get(0)
                .cast(pl.Float64)
                .alias('latitude'),
            pl.col('home_info')
                .struct.field('location')
                .list.get(1)
                .cast(pl.Float64)
                .alias('longitude'),
            pl.col('stadium_info')
                .list.get(0)
                .list.get(-1)
                .str.replace("turf", "")
                .str.strip_chars()
                .alias('surface'),
            pl.col('stadium_info')
                .list.get(1)
                .list.get(-1)
                .cast(pl.UInt32)
                .alias('capacity'),
            pl.col('stadium_info')
                .list.get(2)
                .list.get(-1)
                .cast(pl.UInt16)
                .alias('year_opened'),
        )
    )
    
    return team_info_lf