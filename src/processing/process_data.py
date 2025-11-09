from typing import Any
from pyspark.sql.functions import (
    substring, make_date, create_map, 
    lit, col, regexp_replace
)
import json
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def format_date(df: Any) -> Any:
    df = df.withColumn(
        'Trading Date Format',
        make_date(
            substring(df['Trading Date'], 1, 4),
            substring(df['Trading Date'], 5, 2),
            substring(df['Trading Date'], 7, 2)
        )
    )
    return df

def format_bdi(df: Any) -> Any:
    with open(os.path.join(BASE_DIR, 'bdi_map.json'), 'r') as f:
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
    with open(os.path.join(BASE_DIR, 'spec_map.json'), 'r') as f:
        map_spec = json.load(f)
    mapping_spec = create_map(
        [lit(x) for kv in map_spec.items() for x in kv]
    )
    df = df.withColumn(
        'Specification Name',
        mapping_spec[col('Specification').cast('string')]
    )
    return df

def to_numeric(df: Any, columns: list) -> Any:
    for column in columns:
        df = df.withColumn(column, col(f"`{column}`").cast('double'))
    return df

def remove_outliers(df: Any, columns: list) -> Any:
    for column in columns:
        q1, q3 = df.approxQuantile(f"`{column}`", [0.25, 0.75], 0.0)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df.filter((col(f"`{column}`") >= lower) & (col(f"`{column}`") <= upper))
    return df

def remove_duplicates(df: Any) -> Any:
    df = df.dropDuplicates()
    return df

def remove_nan(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    return df

def norm_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    scaler = StandardScaler()

    for column in columns:
        new_column = f'{column}-norm'
        df[new_column] = scaler.fit_transform(df[[column]])
    return df

def norm_cat(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    encoder = OneHotEncoder(
        sparse_output=False, 
        drop='first', 
        handle_unknown='ignore'
    )
    valid_columns = [col for col in columns if col in df.columns]
    encoded_array = encoder.fit_transform(df[valid_columns])
    encoded_cols = encoder.get_feature_names_out(valid_columns)
    df_encoded = pd.concat(
        [
            df.drop(columns=valid_columns).reset_index(drop=True),
            pd.DataFrame(encoded_array, columns=encoded_cols)
        ], 
        axis=1
    )
    return df_encoded

def split_data(
    df: pd.DataFrame, 
    target_column: str,
    test_size: float = 0.2, 
    random_state: int = 42
) -> tuple:
    X = df.drop(columns=[target_column])
    y = df[target_column]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size = test_size, 
        random_state = random_state, 
        stratify = y if y.nunique() < 10 else None
    )
    return X_train, X_test, y_train, y_test
