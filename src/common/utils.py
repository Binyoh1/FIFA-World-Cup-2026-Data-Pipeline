import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
from functools import reduce

import polars as pl

# get today's utc timestamp
UTC_NOW = datetime.now(timezone.utc)

# request delay
DELAY = 5

# configure logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_json(path: Path, json_content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(json_content, f, indent=4)


def load_json(path: Path) -> dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


# recursive schema dtype merge
def merge_dtypes(d1, d2):
    # same type
    if d1 == d2:
        return d1
    
    # structs
    if isinstance(d1, pl.Struct) and isinstance(d2, pl.Struct):
        fields = {}
        f1 = {f.name: f.dtype for f in d1.fields}
        f2 = {f.name: f.dtype for f in d2.fields}
        for name in f1.keys() | f2.keys():
            if name in f1 and name in f2:
                fields[name] = merge_dtypes(f1[name], f2[name])
            elif name in f1:
                fields[name] = f1[name]
            elif name in f2:
                fields[name] = f2[name]
        return pl.Struct([pl.Field(name, dtype) for name, dtype in fields.items()])
    
    # lists
    if isinstance(d1, pl.List) and isinstance(d2, pl.List):
        return pl.List(merge_dtypes(d1.inner, d2.inner))
    
    # structs over scalars
    if isinstance(d1, pl.Struct):
        return d1
    if isinstance(d2, pl.Struct):
        return d2
    
    # lists of scalars
    if isinstance(d1, pl.List):
            return d1
    if isinstance(d2, pl.List):
        return d2
    
    # numeric
    if d1.is_numeric() and d2.is_numeric():
        return pl.Float64
    elif d1 == pl.Null and d2.is_numeric():
        return d2
    elif d1.is_numeric() and d2 == pl.Null:
        return d1
    
    # fallback
    return pl.String


# schema column and dtype merge
def merge_schemas(s1, s2):
    merged = {}
    for col in s1.keys() | s2.keys():
        if col in s1 and col in s2:
            merged[col] = merge_dtypes(s1[col], s2[col])
        elif col in s1:
            merged[col] = s1[col]
        elif col in s2:
            merged[col] = s2[col]
    return merged


# create initial schema
def create_init_schema(paths: list[Path]) -> dict:
    """Infer schema from first file"""
    try:
        with open(paths[0], "r", encoding="utf-8") as file:
            match_json = json.load(file)
        match_df = pl.DataFrame([match_json], strict=False)
    except Exception as e:
        print(f"Couldn't find any file: {e}")
    return match_df.schema


# create master schema
def create_master_schema(paths: list[Path]) -> dict:
    """Create master schema from all files"""
    # use initial schema
    schema = create_init_schema(paths)
    iterations = 0
    max_iterations = 5
    # set max iterations to 5
    while iterations < max_iterations:
        iterations+=1
        failed_files = set()
        valid_files = set()
        failed_schemas = []
        print(f"\nIteration {iterations}:")
        
        # loop through files
        for json_file in paths:
            # skip missing files
            if not json_file.exists():
                print(f"Couldn't find {json_file.name}")
                failed_files.add(json_file)
                continue
            try:
                # load json
                with open(json_file, "r", encoding="utf-8") as file:
                    match_json = json.load(file)
                match_df = pl.DataFrame([match_json], strict=False, schema=schema)
                valid_files.add(json_file)
            except Exception as e:
                print(f"Schema mismatch or error parsing {json_file.name}: {e}")
                failed_files.add(json_file)
                try:
                    inferred_df = pl.DataFrame([match_json], strict=False)
                    failed_schemas.append(inferred_df.schema)
                except Exception as inner_e:
                    print(f"Error inferring schema: {inner_e}")
                    continue
        
        # update schema
        print(f"{len(valid_files)} valid files, {len(failed_files)} failed files")
                
        # exit if no failed files
        if not failed_files:
            print(f"Master schema created after {iterations} iterations. Exiting...")
            break
        
        # merge schemas
        
        if failed_schemas:
            schema = reduce(merge_schemas, [schema] + failed_schemas)
        else:
            # if files failed without infering new schema, break to prevent an infinite loop.
            print("Couldn't infer any new schemas. Exiting...")
            break
        
        # exit if max iterations reached
        if iterations == max_iterations:
            print("Max iterations reached without success. Exiting...")
            break
        
    return schema