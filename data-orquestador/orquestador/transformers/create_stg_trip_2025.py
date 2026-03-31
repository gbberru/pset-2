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
def create_stg_trip_2025(params, *args, **kwargs):
    """
    Construye la tabla staging limpia para 2025.
    """

    year = int(params['year'])
    dataset = params['dataset']
    raw_schema = params['raw_schema']
    clean_schema = params['clean_schema']

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    union_sql = '\nUNION ALL\n'.join([
        f"SELECT * FROM {raw_schema}.{dataset}_tripdata_{year}_{month:02d}"
        for month in range(1, 13)
    ])

    create_stage_sql = f"""
    CREATE TABLE {clean_schema}.stg_trip_{year} AS
    WITH raw_union AS (
        {union_sql}
    ),
    normalized AS (
        SELECT
            md5(
                concat_ws(
                    '|',
                    COALESCE(vendor_id::text, ''),
                    COALESCE(tpep_pickup_datetime::text, ''),
                    COALESCE(tpep_dropoff_datetime::text, ''),
                    COALESCE(passenger_count::text, ''),
                    COALESCE(trip_distance::text, ''),
                    COALESCE(ratecode_id::text, ''),
                    COALESCE(store_and_fwd_flag::text, ''),
                    COALESCE(pu_location_id::text, ''),
                    COALESCE(do_location_id::text, ''),
                    COALESCE(payment_type::text, ''),
                    COALESCE(fare_amount::text, ''),
                    COALESCE(extra::text, ''),
                    COALESCE(mta_tax::text, ''),
                    COALESCE(tip_amount::text, ''),
                    COALESCE(tolls_amount::text, ''),
                    COALESCE(improvement_surcharge::text, ''),
                    COALESCE(total_amount::text, ''),
                    COALESCE(congestion_surcharge::text, ''),
                    COALESCE(airport_fee::text, ''),
                    COALESCE(cbd_congestion_fee::text, '')
                )
            ) AS trip_hash,

            vendor_id::int AS vendor_id,
            tpep_pickup_datetime::timestamp AS pickup_datetime,
            tpep_dropoff_datetime::timestamp AS dropoff_datetime,

            CASE
                WHEN passenger_count BETWEEN 1 AND 8 THEN passenger_count::int
                ELSE NULL
            END AS passenger_count,

            CASE
                WHEN trip_distance >= 0 THEN ROUND(trip_distance::numeric, 2)
                ELSE NULL
            END AS trip_distance,

            CASE
                WHEN ratecode_id > 0 THEN ratecode_id::int
                ELSE NULL
            END AS rate_code_id,

            NULLIF(TRIM(store_and_fwd_flag), '') AS store_and_fwd_flag,

            CASE
                WHEN pu_location_id > 0 THEN pu_location_id::int
                ELSE NULL
            END AS pickup_location_id,

            CASE
                WHEN do_location_id > 0 THEN do_location_id::int
                ELSE NULL
            END AS dropoff_location_id,

            CASE
                WHEN payment_type > 0 THEN payment_type::int
                ELSE NULL
            END AS payment_type_id,

            ROUND(COALESCE(fare_amount, 0)::numeric, 2) AS fare_amount,
            ROUND(COALESCE(extra, 0)::numeric, 2) AS extra,
            ROUND(COALESCE(mta_tax, 0)::numeric, 2) AS mta_tax,
            ROUND(COALESCE(tip_amount, 0)::numeric, 2) AS tip_amount,
            ROUND(COALESCE(tolls_amount, 0)::numeric, 2) AS tolls_amount,
            ROUND(COALESCE(improvement_surcharge, 0)::numeric, 2) AS improvement_surcharge,
            ROUND(COALESCE(congestion_surcharge, 0)::numeric, 2) AS congestion_surcharge,
            ROUND(COALESCE(airport_fee, 0)::numeric, 2) AS airport_fee,
            ROUND(COALESCE(cbd_congestion_fee, 0)::numeric, 2) AS cbd_congestion_fee,
            ROUND(COALESCE(total_amount, 0)::numeric, 2) AS total_amount
        FROM raw_union
    ),
    dedup AS (
        SELECT DISTINCT ON (trip_hash) *
        FROM normalized
        ORDER BY trip_hash, pickup_datetime
    )
    SELECT
        trip_hash,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
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
        CAST(pickup_datetime AS date) AS trip_date,
        ROUND((EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0)::numeric, 2) AS trip_duration_minutes
    FROM dedup
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime > pickup_datetime
      AND pickup_location_id IS NOT NULL
      AND dropoff_location_id IS NOT NULL
      AND trip_distance IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0;
    """

    queries = [
        f'DROP SCHEMA IF EXISTS {clean_schema} CASCADE;',
        f'CREATE SCHEMA {clean_schema};',
        create_stage_sql,
        f'CREATE UNIQUE INDEX IF NOT EXISTS ux_stg_trip_{year}_trip_hash ON {clean_schema}.stg_trip_{year}(trip_hash);',
        f'CREATE INDEX IF NOT EXISTS ix_stg_trip_{year}_trip_date ON {clean_schema}.stg_trip_{year}(trip_date);',
    ]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        run_queries(loader, queries)

    return params