from typing import Any
from pyspark.sql.functions import (
    substring, make_date, create_map, 
    lit, col, regexp_replace
)
import json


def format_date(df: Any) -> Any:
    df = df.withColumn(
        'Trading Date Format',
        make_date(
            substring(df['Trading Date'], 1, 4),
            substring(df['Trading Date'], 5, 2),
            substring(df['Trading Date'], 6, 2)
        )
    )
    return df

def format_bdi(df: Any) -> Any:
    with open('bdi_map.json', 'r') as f:
        map_bdi = json.load(f)
    mapping_bdi = create_map(
        [lit(x) for kv in map_bdi.items() for x in kv]
    )
    df = df.withColumn(
    'BDI Code',
    regexp_replace(col('BDI Code'), r'\.0$', '')
    )
    df = df.withColumn(
        'BDI Code Name',
        mapping_bdi[col('BDI Code').cast('int')]
    )
    return df

def format_spec(df: Any) -> Any:
    with open('spec_map.json', 'r') as f:
        map_spec = json.load(f)
    mapping_spec = create_map(
        [lit(x) for kv in map_spec.items() for x in kv]
    )
    df = df.withColumn(
        'Specification Name',
        mapping_spec[col('Specification').cast('string')]
    )
    return df
