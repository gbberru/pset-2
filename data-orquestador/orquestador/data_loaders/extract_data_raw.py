from mage_ai.settings.repo import get_repo_path
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import os
import pandas as pd


@data_loader
def load_data(*args, **kwargs):
    """
    Descarga uno o varios meses según las variables del trigger.
    Ejemplo:
    {
      "year": 2025,
      "months": [1, 2, 3, 4],
      "dataset": "yellow"
    }
    """

    year = int(kwargs.get('year', 2025))
    months = kwargs.get('months', [1, 2, 3, 4])
    dataset = kwargs.get('dataset', 'yellow')
    force_download = bool(kwargs.get('force_download', False))

    if not isinstance(months, list) or len(months) == 0:
        raise ValueError('months debe ser una lista con uno o más meses.')

    for month in months:
        month = int(month)
        if month < 1 or month > 12:
            raise ValueError(f'Mes inválido: {month}')

    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    local_dir = path.join(get_repo_path(), 'data', 'raw_landing')
    os.makedirs(local_dir, exist_ok=True)

    files_info = []

    for month in months:
        month = int(month)
        month_str = f'{month:02d}'
        file_name = f'{dataset}_tripdata_{year}-{month_str}.parquet'
        url = f'{base_url}/{file_name}'
        local_path = path.join(local_dir, f'{dataset}_tripdata_{year}_{month_str}.parquet')

        if force_download or not path.exists(local_path):
            print(f'Leyendo con pandas desde: {url}')
            df = pd.read_parquet(url)

            print(f'Guardando archivo local: {local_path}')
            df.to_parquet(local_path, index=False)

            del df
        else:
            print(f'Archivo ya existe, se reutiliza: {local_path}')

        files_info.append({
            'year': year,
            'month': month,
            'month_str': month_str,
            'dataset': dataset,
            'schema': 'raw',
            'table_name': f'{dataset}_tripdata_{year}_{month_str}',
            'url': url,
            'local_path': local_path,
        })

    return files_info