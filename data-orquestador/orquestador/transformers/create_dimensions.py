from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


def run_queries(loader, queries):
    if hasattr(loader, 'execute_queries'):
        loader.execute_queries(queries, commit=True)
    else:
        for query in queries:
            loader.execute(query, commit=True)


@transformer
def create_dimensions(params, *args, **kwargs):
    """
    Crea las dimensiones del modelo clean.
    """

    year = int(params['year'])
    clean_schema = params['clean_schema']

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    queries = [
        f"""
        DROP TABLE IF EXISTS {clean_schema}.dim_vendor CASCADE;
        """,
        f"""
        CREATE TABLE {clean_schema}.dim_vendor (
            vendor_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            vendor_id INT NOT NULL UNIQUE,
            vendor_name TEXT NOT NULL
        );
        """,
        f"""
        INSERT INTO {clean_schema}.dim_vendor (vendor_id, vendor_name)
        SELECT DISTINCT
            vendor_id,
            CASE
                WHEN vendor_id = 1 THEN 'vendor_1'
                WHEN vendor_id = 2 THEN 'vendor_2'
                ELSE 'unknown_vendor'
            END AS vendor_name
        FROM {clean_schema}.stg_trip_{year}
        WHERE vendor_id IS NOT NULL
        ORDER BY vendor_id;
        """,

        f"""
        DROP TABLE IF EXISTS {clean_schema}.dim_payment_type CASCADE;
        """,
        f"""
        CREATE TABLE {clean_schema}.dim_payment_type (
            payment_type_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            payment_type_id INT NOT NULL UNIQUE,
            payment_type_name TEXT NOT NULL
        );
        """,
        f"""
        INSERT INTO {clean_schema}.dim_payment_type (payment_type_id, payment_type_name)
        SELECT DISTINCT
            payment_type_id,
            CASE
                WHEN payment_type_id = 1 THEN 'credit_card'
                WHEN payment_type_id = 2 THEN 'cash'
                WHEN payment_type_id = 3 THEN 'no_charge'
                WHEN payment_type_id = 4 THEN 'dispute'
                WHEN payment_type_id = 5 THEN 'unknown'
                WHEN payment_type_id = 6 THEN 'voided_trip'
                ELSE 'other_payment_type'
            END AS payment_type_name
        FROM {clean_schema}.stg_trip_{year}
        WHERE payment_type_id IS NOT NULL
        ORDER BY payment_type_id;
        """,

        f"""
        DROP TABLE IF EXISTS {clean_schema}.dim_pickup_location CASCADE;
        """,
        f"""
        CREATE TABLE {clean_schema}.dim_pickup_location (
            pickup_location_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            pickup_location_id INT NOT NULL UNIQUE,
            pickup_location_name TEXT NOT NULL
        );
        """,
        f"""
        INSERT INTO {clean_schema}.dim_pickup_location (pickup_location_id, pickup_location_name)
        SELECT DISTINCT
            pickup_location_id,
            'pickup_location_' || pickup_location_id::text
        FROM {clean_schema}.stg_trip_{year}
        WHERE pickup_location_id IS NOT NULL
        ORDER BY pickup_location_id;
        """,

        f"""
        DROP TABLE IF EXISTS {clean_schema}.dim_dropoff_location CASCADE;
        """,
        f"""
        CREATE TABLE {clean_schema}.dim_dropoff_location (
            dropoff_location_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            dropoff_location_id INT NOT NULL UNIQUE,
            dropoff_location_name TEXT NOT NULL
        );
        """,
        f"""
        INSERT INTO {clean_schema}.dim_dropoff_location (dropoff_location_id, dropoff_location_name)
        SELECT DISTINCT
            dropoff_location_id,
            'dropoff_location_' || dropoff_location_id::text
        FROM {clean_schema}.stg_trip_{year}
        WHERE dropoff_location_id IS NOT NULL
        ORDER BY dropoff_location_id;
        """,

        f"""
        CREATE INDEX IF NOT EXISTS ix_dim_vendor_vendor_id
        ON {clean_schema}.dim_vendor(vendor_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_dim_payment_type_payment_type_id
        ON {clean_schema}.dim_payment_type(payment_type_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_dim_pickup_location_pickup_location_id
        ON {clean_schema}.dim_pickup_location(pickup_location_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_dim_dropoff_location_dropoff_location_id
        ON {clean_schema}.dim_dropoff_location(dropoff_location_id);
        """,
    ]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        run_queries(loader, queries)

    return params