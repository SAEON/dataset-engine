import logging

from .models import Product, Dataset
from .client import cli

logger = logging.getLogger(__name__)


class ProductFetcher:

    def fetch_products(self) -> list[Product]:
        try:
            fetched_products = cli.get('/product/all_products')
            return self.__extract_products(fetched_products)
        except Exception as e:
            logger.exception(f'Failed to fetch datasets: {e}')
            return []

    def __extract_products(self, fetched_products) -> list[Product]:
        products = []

        for p in fetched_products:
            visualized_datasets = []
            for d in p['datasets']:
                if d['visualize']:
                    dataset = Dataset()
                    dataset.id = d['identifier']
                    dataset.title = d['title']
                    dataset.dataset_type = d['type']
                    dataset.file_path = d['folder_path']
                    visualized_datasets.append(dataset)

            if visualized_datasets:
                product = Product()
                product.id = p['id']
                product.title = p['title']
                product.bounds = [
                    p['north_bound'],
                    p['south_bound'],
                    p['east_bound'],
                    p['west_bound']
                ]
                product.datasets = visualized_datasets
                products.append(product)

        return products
