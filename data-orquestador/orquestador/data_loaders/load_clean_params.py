from mage_ai.settings.repo import get_repo_path
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_clean_params(*args, **kwargs):
    """
    Parámetros del pipeline clean.
    """

    year = int(kwargs.get('year', 2025))
    dataset = kwargs.get('dataset', 'yellow')

    return {
        'year': year,
        'dataset': dataset,
        'raw_schema': 'raw',
        'clean_schema': 'clean',
    }