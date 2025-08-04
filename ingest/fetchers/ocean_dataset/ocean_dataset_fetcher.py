from ingest.fetchers.dataset_fetcher_interface import DatasetFetcherInterface
from ingest.fetchers.models import FetchedDataset
from etc.config import config
from db import Session
from db.models import Dataset
import json
import requests

ocean_dataset_api_url = config['OCEAN_DATASET']['API_URL']


class OceanDatasetFetcher(DatasetFetcherInterface):

    def fetch_datasets(self) -> list[FetchedDataset]:
        response = requests.get(ocean_dataset_api_url)

        if response.status_code == 200:
            ocean_products_json = response.json()
            ocean_products = json.loads(ocean_products_json)
            return self.__get_datasets(ocean_products)
        else:
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
                        north_bound=dataset['north_bound'],
                        south_bound=dataset['south_bound'],
                        east_bound=dataset['east_bound'],
                        west_bound=dataset['west_bound'],
                    ).save()

                fetched_dataset = FetchedDataset()
                fetched_dataset.dataset_type = dataset['type']
                fetched_dataset.dataset_id = dataset_id
                fetched_dataset.dataset_path = dataset['folder_path']
                fetched_datasets.append(fetched_dataset)

        return fetched_datasets
