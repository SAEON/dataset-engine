import json
import os
from ingest.fetch.models import Product, Dataset
from etc.config import config

class ProductSynchroniser:
    def __init__(self):
        self.products_json_path = config['FILES']['PRODUCTS_JSON_PATH']

    def synchronise_products(self, products: list[Product]):
        current_data = []
        if os.path.exists(self.products_json_path) and os.path.getsize(self.products_json_path) > 0:
            with open(self.products_json_path, 'r') as f:
                current_data = json.load(f)

        # Create a lookup for current products by id
        current_products_lookup = {p['id']: p for p in current_data}

        for new_p in products:
            if new_p.id in current_products_lookup:
                # Update existing product
                existing_p = current_products_lookup[new_p.id]
                existing_p['title'] = new_p.title
                existing_p['bounds'] = new_p.bounds
                
                # Update datasets
                current_datasets_lookup = {d['id']: d for d in existing_p['datasets']}
                new_datasets = []
                for new_d in new_p.datasets:
                    if new_d.id in current_datasets_lookup:
                        # Update existing dataset, keep file_last_updated
                        existing_d = current_datasets_lookup[new_d.id]
                        existing_d['title'] = new_d.title
                        existing_d['dataset_type'] = new_d.dataset_type
                        existing_d['file_path'] = new_d.file_path
                        new_datasets.append(existing_d)
                    else:
                        # Add new dataset with default date
                        dataset_dict = {
                            'id': new_d.id,
                            'title': new_d.title,
                            'dataset_type': new_d.dataset_type,
                            'file_path': new_d.file_path,
                            'file_last_updated': '01/01/1900 00:00:00'
                        }
                        new_datasets.append(dataset_dict)
                existing_p['datasets'] = new_datasets
            else:
                # Add new product
                product_dict = {
                    'id': new_p.id,
                    'title': new_p.title,
                    'bounds': new_p.bounds,
                    'datasets': [
                        {
                            'id': d.id,
                            'title': d.title,
                            'dataset_type': d.dataset_type,
                            'file_path': d.file_path,
                            'file_last_updated': '01/01/1900 00:00:00'
                        }
                        for d in new_p.datasets
                    ]
                }
                current_data.append(product_dict)

        with open(self.products_json_path, 'w') as f:
            json.dump(current_data, f, indent=2)

    def sync_dataset_file_last_updated(self, product_id, dataset_id, new_time: str):
        """
        Updates the file_last_updated field of a specific dataset within a product.
        """
        current_data = []
        if os.path.exists(self.products_json_path) and os.path.getsize(self.products_json_path) > 0:
            with open(self.products_json_path, 'r') as f:
                current_data = json.load(f)

        updated = False
        for product in current_data:
            if str(product.get('id')) == str(product_id):
                for dataset in product.get('datasets', []):
                    if str(dataset.get('id')) == str(dataset_id):
                        dataset['file_last_updated'] = new_time
                        updated = True
                        break
                if updated:
                    break

        if updated:
            with open(self.products_json_path, 'w') as f:
                json.dump(current_data, f, indent=2)
        else:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not find product {product_id} and dataset {dataset_id} to update file_last_updated.")
