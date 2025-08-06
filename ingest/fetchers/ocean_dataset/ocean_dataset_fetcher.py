import logging

import json

from db import Session
from db.models import Dataset
from ingest.fetchers.dataset_fetcher_interface import DatasetFetcherInterface
from ingest.fetchers.models import FetchedDataset
from .client import cli

logger = logging.getLogger(__name__)


class OceanDatasetFetcher(DatasetFetcherInterface):

    def fetch_datasets(self) -> list[FetchedDataset]:
        # ocean_products_json = """
        #     [
        #       {
        #         "north_bound": 45.123456,
        #         "south_bound": 30.789012,
        #         "east_bound": -70.987654,
        #         "west_bound": -85.432109,
        #         "datasets": [
        #           {
        #             "identifier": "test_dataset",
        #             "type": "OCEAN",
        #             "folder_path": "/home/dylan/srv/netcdf-extractor/data/croco_avg_t2.nc"
        #           }
        #         ]
        #       }
        #     ]
        #     """
        #
        # ocean_products = json.loads(ocean_products_json)
        # return self.__get_datasets(ocean_products)

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
                dataset_id = dataset['identifier']

                if Session.get(Dataset, dataset_id) is None:
                    Dataset(
                        id=dataset_id,
                        dataset_type=dataset['type'],
                        north_bound=product['north_bound'],
                        south_bound=product['south_bound'],
                        east_bound=product['east_bound'],
                        west_bound=product['west_bound'],
                    ).save()

                fetched_dataset = FetchedDataset()
                fetched_dataset.dataset_type = dataset['type']
                fetched_dataset.dataset_id = dataset_id
                fetched_dataset.dataset_path = dataset['folder_path']
                fetched_datasets.append(fetched_dataset)

        return fetched_datasets
