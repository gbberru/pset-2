from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from datetime import date

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


def run_queries(loader, queries):
    loader.execute_queries(queries, commit=True)


@transformer
def create_fact_trip(params, *args, **kwargs):
    """
    Crea una fact table liviana para cumplir el taller.
    Granularidad: 1 fila = 1 viaje limpio de 2025.
    """

    year = int(params['year'])
    clean_schema = params['clean_schema']

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Diagnóstico: confirma base y schema actuales
        info = loader.load("""
        SELECT current_database() AS db_name,
               current_schema() AS schema_name;
        """)
        print(info)

        # Recrear fact table
        run_queries(loader, [
            f"DROP TABLE IF EXISTS {clean_schema}.fact_trip CASCADE;",
            f"""
            CREATE TABLE {clean_schema}.fact_trip (
                trip_key BIGSERIAL PRIMARY KEY,
                trip_date DATE NOT NULL,
                pickup_datetime TIMESTAMP NOT NULL,
                dropoff_datetime TIMESTAMP NOT NULL,

                vendor_key INT,
                payment_type_key INT,
                pickup_location_key INT,
                dropoff_location_key INT,

                trip_count INT NOT NULL DEFAULT 1,
                trip_distance NUMERIC(12, 2),
                fare_amount NUMERIC(12, 2),
                tip_amount NUMERIC(12, 2),
                tolls_amount NUMERIC(12, 2),
                total_amount NUMERIC(12, 2),
                trip_duration_minutes NUMERIC(12, 2)
            );
            """
        ])

        # Carga por mes
        for month in range(1, 13):
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1)
            else:
                end_date = date(year, month + 1, 1)

            print(f'Cargando fact_trip mes {month:02d}...')

            run_queries(loader, [
                f"""
                INSERT INTO {clean_schema}.fact_trip (
                    trip_date,
                    pickup_datetime,
                    dropoff_datetime,
                    vendor_key,
                    payment_type_key,
                    pickup_location_key,
                    dropoff_location_key,
                    trip_count,
                    trip_distance,
                    fare_amount,
                    tip_amount,
                    tolls_amount,
                    total_amount,
                    trip_duration_minutes
                )
                SELECT
                    s.trip_date,
                    s.pickup_datetime,
                    s.dropoff_datetime,
                    dv.vendor_key,
                    dpt.payment_type_key,
                    dpl.pickup_location_key,
                    ddl.dropoff_location_key,
                    1 AS trip_count,
                    s.trip_distance,
                    s.fare_amount,
                    s.tip_amount,
                    s.tolls_amount,
                    s.total_amount,
                    s.trip_duration_minutes
                FROM {clean_schema}.stg_trip_{year} s
                JOIN {clean_schema}.dim_vendor dv
                  ON s.vendor_id = dv.vendor_id
                LEFT JOIN {clean_schema}.dim_payment_type dpt
                  ON s.payment_type_id = dpt.payment_type_id
                JOIN {clean_schema}.dim_pickup_location dpl
                  ON s.pickup_location_id = dpl.pickup_location_id
                JOIN {clean_schema}.dim_dropoff_location ddl
                  ON s.dropoff_location_id = ddl.dropoff_location_id
                WHERE s.trip_date >= DATE '{start_date}'
                  AND s.trip_date < DATE '{end_date}';
                """
            ])

        # Índice mínimo y estadísticas
        run_queries(loader, [
            f"""
            CREATE INDEX IF NOT EXISTS ix_fact_trip_trip_date
            ON {clean_schema}.fact_trip(trip_date);
            """,
            f"ANALYZE {clean_schema}.fact_trip;"
        ])

        # Verificación final
        validation = loader.load(f"""
        SELECT
            current_database() AS db_name,
            COUNT(*) AS fact_trip_rows
        FROM {clean_schema}.fact_trip;
        """)
        print(validation)

    return {
        **params,
        'fact_table': f'{clean_schema}.fact_trip',
    }