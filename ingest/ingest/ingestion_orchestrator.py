import json
import os
import logging
from datetime import datetime
from pathlib import Path
from ingest.fetch.models import Product, Dataset
from etc.config import config
from .ingestion_queuer import IngestionQueuer
from ingest.synchronise.product_synchroniser import ProductSynchroniser

logger = logging.getLogger(__name__)

class IngestionOrchestrator:
    def __init__(self):
        self.products_json_path = config['FILES']['PRODUCTS_JSON_PATH']
        self.queuer = IngestionQueuer()
        self.synchroniser = ProductSynchroniser()

    def run(self):
        products = self._get_products()
        products_to_ingest = []

        for product in products:
            datasets_to_update = self._check_for_dataset_updates(product.id, product.datasets)
            if datasets_to_update:
                product.datasets = datasets_to_update
                products_to_ingest.append(product)

        if products_to_ingest:
            self.queuer.queue_ingestion(products_to_ingest)

    def _get_products(self) -> list[Product]:
        products = []
        if os.path.exists(self.products_json_path) and os.path.getsize(self.products_json_path) > 0:
            with open(self.products_json_path, 'r') as f:
                data = json.load(f)
                for p_dict in data:
                    product = Product()
                    product.id = p_dict['id']
                    product.title = p_dict['title']
                    product.bounds = p_dict['bounds']
                    product.datasets = []
                    for d_dict in p_dict['datasets']:
                        dataset = Dataset()
                        dataset.id = d_dict['id']
                        dataset.title = d_dict['title']
                        dataset.dataset_type = d_dict['dataset_type']
                        dataset.file_path = d_dict['file_path']
                        dataset.file_last_updated = d_dict['file_last_updated']
                        product.datasets.append(dataset)
                    products.append(product)
        return products

    def _check_for_dataset_updates(self, product_id, datasets: list[Dataset]) -> list[Dataset]:
        updated_datasets = []
        for dataset in datasets:
            if not os.path.exists(dataset.file_path):
                logger.warning(f"File not found for dataset {dataset.id}: {dataset.file_path}")
                continue

            try:
                # Parse stored datetime string: 01/01/1900 00:00:00
                stored_time = datetime.strptime(dataset.file_last_updated, '%d/%m/%Y %H:%M:%S')
                stored_timestamp = stored_time.timestamp()

                # Get actual file modification time
                actual_timestamp = os.path.getmtime(dataset.file_path)

                if actual_timestamp > stored_timestamp:
                    updated_datasets.append(dataset)
                    now_str = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                    self.synchroniser.sync_dataset_file_last_updated(product_id, dataset.id, now_str)
            except Exception as e:
                logger.error(f"Error checking updates for dataset {dataset.id}: {e}")

        return updated_datasets
