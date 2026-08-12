import polars as pl
from prefect import task

from common.paths import players_dir

from transform.utils.data_io import load_data


# load player data
@task
def load_players() -> pl.LazyFrame:
    return load_data(players_dir, "player")

@task
def select_player_info(lf: pl.LazyFrame) -> pl.LazyFrame:
    player_info_lf = (
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
            pl.col('title').str.replace_many(
                ["Height", "Preferred foot", "Market value"],
                ["height", "preferred_foot", "transfer_value"]
            ),
        )
        .drop('translationKey', 'icon', 'countryCode')
        
    )
    
    return player_info_lf