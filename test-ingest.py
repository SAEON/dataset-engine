import logging
import queue
import threading

from ingest.fetchers.models import FetchedDataset
from ingest.fetchers import REGISTERED_FETCHERS
from ingest.ingest import data_processor_factory
from etc.const import DatasetType

logging.basicConfig(level=logging.INFO)

item = FetchedDataset()
item.dataset_type = DatasetType.OCEAN.value
item.dataset_id = 'sa_west_mercator'
item.dataset_path = 'croco_avg_t2.nc'


data_processor = data_processor_factory(item.dataset_type)
data_processor.ingest_dataset(item.dataset_id, item.dataset_path)

logging.info("Ingestion complete")
