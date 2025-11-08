import os
os.environ['KAGGLE_CONFIG_DIR'] = os.path.abspath('../..')

from kaggle.api.kaggle_api_extended import KaggleApi


PATH = '../../datalake/tmp'


def _data_exists() -> bool:
    return os.path.exists(
        f'{PATH}/BMFBovespa_Cons_Dataset_1986-2019.csv'
    )


if _data_exists():
    print(f'[INFO] - Data already exists at {PATH}/')
else:
    print('[INFO] - Starting data ingestion')
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(
        'brunotly/bmfbovespas-time-series-19862019',
        path=PATH,
        unzip=True
    )
    print('[INFO] - Ingestion successfully completed')
