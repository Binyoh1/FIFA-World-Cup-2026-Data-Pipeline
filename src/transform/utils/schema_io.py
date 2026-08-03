import yaml
import logging
from pathlib import Path
from typing import Any

import polars as pl

from common.utils import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

TYPE_MAP = {
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "String": pl.String,
    "Null": pl.Null
}


# recursive map dtype to dict string for schema export
def dtype_to_dict(dtype: pl.DataType | Any) -> dict[str, Any]:
    # primitive types
    if dtype == pl.String:
        return {"type": "String"}
    if dtype == pl.Int32:
        return {"type": "Int32"}
    if dtype == pl.Int64:
        return {"type": "Int64"}
    if dtype == pl.Float32:
        return {"type": "Float32"}
    if dtype == pl.Float64:
        return {"type": "Float64"}
    if dtype == pl.Boolean:
        return {"type": "Boolean"}
    
    # datetime
    if dtype == pl.Date:
        return {"type": "Date"}
    if isinstance(dtype, pl.Datetime):
        return {
            "type": "Datetime",
            "time_unit": dtype.time_unit,
            "time_zone": dtype.time_zone,
        }
    
    # list
    if isinstance(dtype, pl.List):
        return {
            "type": "List",
            "inner": dtype_to_dict(dtype.inner),
        }
    
    # struct
    if isinstance(dtype, pl.Struct):
        return {
            "type": "Struct",
            "fields": {
                field.name: dtype_to_dict(field.dtype) for field in dtype.fields
            },
        }
    
    # null
    if dtype == pl.Null:
        return {"type": "Null"}
    
    return {"type": str(dtype)}


# export schema
def export_schema(schema: pl.Schema, output_path: Path) -> None:
    schema_dict = {col: dtype_to_dict(dtype) for col, dtype in schema.items()}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as file:
        yaml.safe_dump(
            schema_dict, 
            file,
            sort_keys=False,
            default_flow_style=False
        )


# infer and export schema from JSON data
def infer_and_export_schema(df: pl.DataFrame, output_path: Path) -> None:
    export_schema(df.schema, output_path)


# recursive map dict string to dtype for schema import
def dict_to_dtype(obj: dict[str, Any]) -> pl.DataType:
    dtype = obj["type"]
    
    if dtype in TYPE_MAP:
        return TYPE_MAP[dtype]
    
    # datetime
    if dtype == "Datetime":
        return pl.Datetime(
            time_unit=obj.get("time_unit", "us"),
            time_zone=obj.get("time_zone", "UTC"),
        )
    
    # list
    if dtype == "List":
        return pl.List(dict_to_dtype(obj["inner"]))
    
    # struct
    if dtype == "Struct":
        fields = []
        for name, val in obj['fields'].items():
            fields.append(pl.Field(name, dict_to_dtype(val)))
        return pl.Struct(fields)
    
    raise ValueError(f"Unsupported datatype: {dtype}")


# load schema
def load_schema(input_path: Path) -> dict[str, pl.DataType]:
    try:
        with open(input_path, "r") as file:
            schema_dict = yaml.safe_load(file)
        return {col: dict_to_dtype(dtype) for col, dtype in schema_dict.items}
    except FileNotFoundError:
        logger.error(f"File not found: {input_path}")
        raise