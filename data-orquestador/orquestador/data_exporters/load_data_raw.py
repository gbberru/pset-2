from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import math

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import re
import pandas as pd
import pyarrow.parquet as pq


def normalize_column_name(col_name: str) -> str:
    col_name = col_name.strip()
    col_name = re.sub(r'[^0-9a-zA-Z_]+', '_', col_name)
    col_name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    return col_name.lower().strip('_')


def run_queries(loader, queries):
    if hasattr(loader, 'execute_queries'):
        loader.execute_queries(queries)
    else:
        for query in queries:
            loader.execute(query)


@data_exporter
def export_data_to_postgres(files_info, **kwargs) -> None:
    """
    Exporta los archivos parquet descargados localmente al schema raw
    usando solo io_config.yaml.
    """

    if not files_info:
        raise ValueError('No se recibió información de archivos desde el bloque anterior.')

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    batch_size = int(kwargs.get('batch_size', 50000))

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        run_queries(loader, [
            'CREATE SCHEMA IF NOT EXISTS raw;'
        ])

        for file_info in files_info:
            schema_name = file_info['schema']
            table_name = file_info['table_name']
            local_path = file_info['local_path']
            url = file_info['url']
            year = file_info['year']
            month = file_info['month']

            print(f'Procesando archivo: {local_path}')
            print(f'Destino: {schema_name}.{table_name}')

            parquet_file = pq.ParquetFile(local_path)
            total_rows = 0
            first_batch = True

            for batch in parquet_file.iter_batches(batch_size=batch_size):
                df = batch.to_pandas()

                if df.empty:
                    continue

                # cambios técnicos mínimos permitidos en raw
                df.columns = [normalize_column_name(c) for c in df.columns]
                df['source_year'] = year
                df['source_month'] = month
                df['source_file_url'] = url
                df['ingested_at'] = pd.Timestamp.utcnow().tz_localize(None)

                loader.export(
                    df,
                    schema_name,
                    table_name,
                    index=False,
                    if_exists='replace' if first_batch else 'append',
                )

                rows_batch = len(df)
                total_rows += rows_batch
                first_batch = False

                print(f'Lote cargado: {rows_batch} filas | acumulado: {total_rows}')

            print(f'Carga finalizada: {schema_name}.{table_name} con {total_rows} filas')