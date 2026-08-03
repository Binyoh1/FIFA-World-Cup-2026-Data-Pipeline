import logging
from pathlib import Path

import polars as pl
from prefect import task

from common.paths import comp_path, schemas_dir
from common.utils import setup_logging, load_json

from transform.utils.schema_io import load_schema, export_schema, infer_and_export_schema

setup_logging()

logger = logging.getLogger(__name__)


# load competition data
@task
def load_comp_data() -> pl.LazyFrame:
    schema_path = schemas_dir / "comp_schema.yaml"
    
    # throw error if json file not found
    try:
        data = load_json(comp_path)
    except FileNotFoundError:
        logger.error(f"JSON file not found: {comp_path}")
        raise
    
    # infer schema if schema file not found
    try:
        schema = load_schema(schema_path)
    except FileNotFoundError:
        logger.warning(f"Schema file not found: {schema_path}. Inferring comp schema from data...")
        df = pl.DataFrame([data], strict=False)
        infer_and_export_schema(df, schema_path)
        return df.lazy()
    
    try:
        return pl.DataFrame([data], strict=False, schema=schema).lazy()
    # polars has no dedicated schema mismatch exception
    # catch any stored schema application error and regenerate schema
    except Exception as e:
        logger.warning(f"Failed to apply schema: {e}. Regenerating comp schema from data...")
        df = pl.DataFrame([data], strict=False)
        infer_and_export_schema(df, schema_path)
        return df.lazy()


# export comp schema
if __name__ == "__main__":
    load_comp_data()
    