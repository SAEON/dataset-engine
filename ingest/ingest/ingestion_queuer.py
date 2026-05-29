import logging
import queue
import threading
from pathlib import Path
from ingest.fetch.models import Product
from etc.config import config
from .dataset_ingester import DatasetIngester

logger = logging.getLogger(__name__)

class IngestionQueuer:
    def __init__(self):
        self.q = queue.Queue()
        self.data_dir = Path(config['FILES']['DATA_DIR_PATH'])
        self.ingester = DatasetIngester()

    def queue_ingestion(self, products_to_ingest: list[Product]):
        """
        Starts the worker thread and populates the queue with datasets from the products.
        """
        # Start worker thread
        threading.Thread(target=self._worker, daemon=True).start()

        for product in products_to_ingest:
            product_folder_path = self.data_dir / product.title
            product_folder_path.mkdir(parents=True, exist_ok=True)
            
            for dataset in product.datasets:
                self.q.put((dataset, product_folder_path))

        # Add sentinel to stop worker
        self.q.put(None)
        self.q.join()
        logger.info("Ingestion queue processed.")

    def _worker(self):
        while True:
            item = self.q.get()
            if item is None:
                self.q.task_done()
                break
            
            dataset, product_folder_path = item
            try:
                logger.info(f"Ingesting dataset {dataset.id}")
                self.ingester.ingest_dataset(dataset.id, dataset.file_path, product_folder_path)
            except Exception as e:
                logger.error(f"Error ingesting dataset {dataset.id}: {e}")
            finally:
                self.q.task_done()
