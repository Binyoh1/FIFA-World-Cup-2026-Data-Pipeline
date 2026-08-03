import json
import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import matches_dir, schemas_dir
from common.utils import setup_logging, load_json

from transform.utils.schema_build import create_master_schema
from transform.utils.schema_io import export_schema, load_schema
from transform.transform_comp import load_comp_data

setup_logging()

logger = logging.getLogger(__name__)


# load match data
@task
def load_matches() -> pl.LazyFrame:
    match_json_list = list(matches_dir.glob("*.json"))
    # throw error if no match files found
    if not match_json_list:
        logger.error("No match files found")
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
            # throw error if json parsing fails even after schema inference
            logger.error(f"Schema mismatch or error parsing {json_file.name}: {e}")
            raise
    
    return pl.concat(match_dfs, how="diagonal_relaxed")


if __name__ == "__main__":
    load_matches()