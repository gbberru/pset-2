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
def create_fact_trip(params, *args, **kwargs):
    """
    Crea la tabla de hechos principal del modelo dimensional.
    Granularidad: 1 fila = 1 viaje limpio de 2025.
    """

    year = int(params['year'])
    clean_schema = params['clean_schema']

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    queries = [
        f"""
        DROP TABLE IF EXISTS {clean_schema}.fact_trip CASCADE;
        """,
        f"""
        CREATE TABLE {clean_schema}.fact_trip (
            trip_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            trip_hash TEXT NOT NULL UNIQUE,

            vendor_key INT REFERENCES {clean_schema}.dim_vendor(vendor_key),
            payment_type_key INT REFERENCES {clean_schema}.dim_payment_type(payment_type_key),
            pickup_location_key INT NOT NULL REFERENCES {clean_schema}.dim_pickup_location(pickup_location_key),
            dropoff_location_key INT NOT NULL REFERENCES {clean_schema}.dim_dropoff_location(dropoff_location_key),

            trip_date DATE NOT NULL,
            pickup_datetime TIMESTAMP NOT NULL,
            dropoff_datetime TIMESTAMP NOT NULL,

            passenger_count INT,
            trip_distance NUMERIC(12, 2),
            fare_amount NUMERIC(12, 2),
            extra NUMERIC(12, 2),
            mta_tax NUMERIC(12, 2),
            tip_amount NUMERIC(12, 2),
            tolls_amount NUMERIC(12, 2),
            improvement_surcharge NUMERIC(12, 2),
            congestion_surcharge NUMERIC(12, 2),
            airport_fee NUMERIC(12, 2),
            cbd_congestion_fee NUMERIC(12, 2),
            total_amount NUMERIC(12, 2),
            trip_duration_minutes NUMERIC(12, 2),

            rate_code_id INT,
            store_and_fwd_flag TEXT,

            trip_count INT NOT NULL DEFAULT 1
        );
        """,
        f"""
        INSERT INTO {clean_schema}.fact_trip (
            trip_hash,
            vendor_key,
            payment_type_key,
            pickup_location_key,
            dropoff_location_key,
            trip_date,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee,
            total_amount,
            trip_duration_minutes,
            rate_code_id,
            store_and_fwd_flag,
            trip_count
        )
        SELECT
            s.trip_hash,
            dv.vendor_key,
            dpt.payment_type_key,
            dpl.pickup_location_key,
            ddl.dropoff_location_key,
            s.trip_date,
            s.pickup_datetime,
            s.dropoff_datetime,
            s.passenger_count,
            s.trip_distance,
            s.fare_amount,
            s.extra,
            s.mta_tax,
            s.tip_amount,
            s.tolls_amount,
            s.improvement_surcharge,
            s.congestion_surcharge,
            s.airport_fee,
            s.cbd_congestion_fee,
            s.total_amount,
            s.trip_duration_minutes,
            s.rate_code_id,
            s.store_and_fwd_flag,
            1 AS trip_count
        FROM {clean_schema}.stg_trip_{year} s
        JOIN {clean_schema}.dim_vendor dv
          ON s.vendor_id = dv.vendor_id
        LEFT JOIN {clean_schema}.dim_payment_type dpt
          ON s.payment_type_id = dpt.payment_type_id
        JOIN {clean_schema}.dim_pickup_location dpl
          ON s.pickup_location_id = dpl.pickup_location_id
        JOIN {clean_schema}.dim_dropoff_location ddl
          ON s.dropoff_location_id = ddl.dropoff_location_id;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_fact_trip_trip_date
        ON {clean_schema}.fact_trip(trip_date);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_fact_trip_vendor_key
        ON {clean_schema}.fact_trip(vendor_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_fact_trip_payment_type_key
        ON {clean_schema}.fact_trip(payment_type_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_fact_trip_pickup_location_key
        ON {clean_schema}.fact_trip(pickup_location_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS ix_fact_trip_dropoff_location_key
        ON {clean_schema}.fact_trip(dropoff_location_key);
        """,
    ]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        run_queries(loader, queries)

    return {
        **params,
        'fact_table': f'{clean_schema}.fact_trip',
    }