from pyspark.sql import SparkSession 
from pyspark.sql.functions import substring
from pathlib import Path


PATH = '/app/datalake'


def _data_loaded() -> bool:
    path = Path(f'{PATH}/Bovespa/')
    return path.exists()


if _data_loaded():
    print('[INFO] - Data already loaded in the Data Lake')
else:
    print('[INFO] - Starting data loading')
    spark = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('load')\
        .getOrCreate()
    df = spark.read.csv(
        f'{PATH}/tmp/BMFBovespa_Cons_Dataset_1986-2019.csv',
        header=True,
        sep=','
    )
    df = df.withColumn(
        'Year', substring(df['Trading Date'], 1, 4)
    )
    df = df.withColumn(
        'Month', substring(df['Trading Date'], 5, 2)
    )
    df\
        .write\
        .mode('overwrite')\
        .partitionBy('Year', 'Month')\
        .parquet(f'{PATH}/Bovespa')
    print('[INFO] - Data successfully loaded in the Data Lake')
