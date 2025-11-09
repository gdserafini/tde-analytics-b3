from typing import Any
from pyspark.sql.functions import (
    substring, make_date, create_map, 
    lit, col, regexp_replace
)
import json
import os
import pandas as pd
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml import Pipeline


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

def prep_ml(df: Any) -> Any:
    df = df.toDF(*[c.replace(".", "").replace(" ", "_") for c in df.columns])
    numeric_columns = [
        'Opening_Price', 'Max_Price', 'Min_Price',
        'Mean_Price', 'Last_Trade_Price', 'Best_Purshase_Order_Price',
        'Best_Purshase_Sale_Price', 'Numbor_Of_Trades', 'Number_Of_Traded_Stocks',
        'Volume_Of_Traded_Stocks'
    ]
    cat_cols = [
        "Currency", "Market_Type", "Trade_Name",
        "Specification_Name", "BDI_Code_Name"
    ]
    for c in numeric_columns:
        df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast("double"))
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") 
        for c in cat_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") 
        for c in cat_cols
    ]
    feature_cols = [
        *numeric_columns,
        *[f"{c}_vec" for c in cat_cols]
    ]
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
    model_prep = pipeline.fit(df)
    df_prep = model_prep.transform(df)
    return df_prep
