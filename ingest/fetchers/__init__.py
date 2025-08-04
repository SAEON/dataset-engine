from .dataset_fetcher_interface import DatasetFetcherInterface
from ocean_dataset.ocean_dataset_fetcher import OceanDatasetFetcher


REGISTERED_FETCHERS: list[DatasetFetcherInterface] = [
    OceanDatasetFetcher(),
]



