if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from mage_ai.orchestration.triggers.api import trigger_pipeline


def parse_months(months):
    if isinstance(months, list):
        return [int(x) for x in months]
    if isinstance(months, str):
        return [int(x.strip()) for x in months.strip('[]').split(',') if x.strip()]
    return []


@transformer
def trigger_pipe_clean_after_last_raw(data, *args, **kwargs):
    """
    Solo dispara pipe_clean cuando el trigger de raw corresponde
    a los meses 09-12.
    """

    year = int(kwargs.get('year', 2025))
    dataset = kwargs.get('dataset', 'yellow')
    months = parse_months(kwargs.get('months', []))

    print(f'Runtime variables recibidas -> year={year}, dataset={dataset}, months={months}')

    if months == [9, 10, 11, 12]:
        print('Último tramo raw detectado. Disparando pipe_clean...')

        trigger_pipeline(
            'pipe_clean',
            variables={
                'year': year,
                'dataset': dataset,
            },
            check_status=True,
            error_on_failure=True,
            poll_interval=60,
            poll_timeout=None,
            schedule_name='clean_after_raw_2025',
            verbose=True,
        )
    else:
        print('No es el último trigger raw. No se dispara pipe_clean.')

    return data