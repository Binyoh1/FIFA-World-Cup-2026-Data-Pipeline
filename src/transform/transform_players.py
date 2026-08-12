import polars as pl
from prefect import task

from common.paths import players_dir

from transform.utils.data_helpers import PLAYER_FIELD_MAPPINGS, load_data


# load player data
@task
def load_players() -> pl.LazyFrame:
    return load_data(players_dir, "player")

@task
def select_player_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    players_lf = (
        lf
        .select(
            # basic player info
            pl.col('id').cast(pl.UInt32).alias('player_id'),
            pl.col('name').alias('player_name'),
            
            # country id
            pl.col('careerHistory')
                .struct.field('careerItems')
                .struct.field('national team')
                .struct.field('teamEntries')
                .list.get(0, null_on_oob=True)
                .struct.field('teamId')
                .cast(pl.UInt32)
                .alias('team_id'),
            
            # date of birth
            pl.col('birthDate')
                .struct.field('utcTime')
                .str.to_datetime(time_zone="UTC")
                .dt.date()
                .alias('birth_date'),
            
            # primary position
            pl.col('positionDescription')
                .struct.field('primaryPosition')
                .struct.field('label')
                .str.to_lowercase()
                .alias('primary_position'),
            
            # additional player information (list of dict structs)
            pl.col('playerInformation'),
            
            # club info
            pl.col('primaryTeam'),            
        )
        .explode('playerInformation', empty_as_null=True)
        .unnest('playerInformation')
        .filter(pl.col('title').is_in(["Height", "Preferred foot", "Market value"]))
        .with_columns(
            # rename additional player info columns
            pl.col('title').str.replace_many(
                ["Height", "Preferred foot", "Market value"],
                ["height_info", "foot_info", "value_info"]
            ),
        )
        .drop('translationKey', 'icon', 'countryCode')        
    )
    
    # pivot to fold player data back into single row per player
    players_pivoted_lf = (
        players_lf
        .pivot(
            on='title',
            on_columns=[
                'height',
                'preferred_foot',
                'transfer_value',
            ],
            values='value',
            index = [
                'player_id',
                'player_name',
                'team_id',
                'birth_date',
                'primary_position',
                'primaryTeam',
            ],
            aggregate_function='first'
        )
    )
    
    # safely parse player info fields, cast to correct dtype, and fallback to null if missing in raw JSON
    expressions = [
        pl.col(src).struct.fiel(cfg["field"]).cast(cfg["dtype"]).alias(cfg["alias"])
        if src in players_pivoted_lf.columns
        else pl.lit(None).cast(cfg["dtype"]).alias(cfg["alias"])
        for src, cfg in PLAYER_FIELD_MAPPINGS.items()
    ]
    expressions.extend([
        pl.col('primaryTeam')
            .struct.field('teamId')
            .alias('club_id'),
        pl.col('primaryTeam')
            .struct.field('teamName')
            .alias('club'),
    ])
    
    # drop unnecessary columns
    drop_cols = [src for src in PLAYER_FIELD_MAPPINGS if src in players_pivoted_lf.columns]
    drop_cols.append('primaryTeam')
    
    # consoilidate parsing expressions and fields
    players_clean_lf = (
        players_pivoted_lf
        .with_columns(expressions)
        .drop(drop_cols)
    )
    
    
    return players_clean_lf