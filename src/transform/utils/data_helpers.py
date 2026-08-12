import logging
from pathlib import Path

import polars as pl

from common.paths import schemas_dir
from common.utils import setup_logging, load_json

from transform.utils.schema_build import create_master_schema
from transform.utils.schema_io import load_schema, export_schema

setup_logging()

logger = logging.getLogger(__name__)


# field mapping for player info processing
PLAYER_FIELD_MAPPINGS = {
    'height_info': {
        "alias": "height_cm",
        "field": "numberValue",
        "dtype": pl.UInt8,
    },
    'value_info': {
        "alias": "transfer_value_eur",
        "field": "numberValue",
        "dtype": pl.UInt32,
    },
    'foot_info': {
        "alias": "preferred_foot",
        "field": "label",
        "dtype": pl.String,
    }
}


# load data from multi-file folder
def load_data(file_dir: Path, schema_name: str) -> pl.LazyFrame:
    json_list = list(file_dir.rglob("*.json"))
    
    # throw error if no json files found
    if not json_list:
        logger.error("No {schema_name} data files found")
        raise
    
    # look for schema file
    schema_path = schemas_dir / f"{schema_name}_schema.yaml"
    try:
        schema = load_schema(schema_path)
    except FileNotFoundError:
        # infer and save schema if schema file not found
        logger.warning(f"Schema file not found: {schema_path.name}. Inferring {schema_name} schema from data...")
        schema = create_master_schema(json_list)
        export_schema(schema, schema_path)
    
    lfs = []
    for json_file in json_list:
        parsed_json = load_json(json_file)
        try:
            lf = pl.DataFrame([parsed_json], strict=False, schema=schema).lazy()
            lfs.append(lf)
        except Exception as e:
            # throw error if json parsing fails even after schema inference attempt
            logger.error(f"Schema mismatch or error parsing {json_file.name}: {e}")
            raise
    
    return pl.concat(lfs, how="diagonal_relaxed")