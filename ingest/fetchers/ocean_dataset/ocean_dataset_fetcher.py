import logging

from ingest.fetchers.dataset_fetcher_interface import DatasetFetcherInterface
from ingest.fetchers.models import FetchedDataset
from .client import cli

logger = logging.getLogger(__name__)


class OceanDatasetFetcher(DatasetFetcherInterface):

    def fetch_datasets(self) -> list[FetchedDataset]:
        try:
            ocean_products = cli.get('/product/all_products')
            return self.__get_datasets(ocean_products)
        except Exception as e:
            logger.exception(f'Failed to fetch datasets: {e}')
            return []

    def __get_datasets(self, ocean_products) -> list[FetchedDataset]:
        fetched_datasets = []

        for product in ocean_products:
            for dataset in product['datasets']:
                if not dataset['visualize']:
                    continue

                fetched_dataset = FetchedDataset()
                fetched_dataset.dataset_type = dataset['type']
                fetched_dataset.dataset_id = dataset['identifier']
                fetched_dataset.dataset_path = dataset['folder_path']
                fetched_datasets.append(fetched_dataset)

        return fetched_datasets
