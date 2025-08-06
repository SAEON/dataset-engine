import logging
import queue
import threading

from ingest.fetchers.models import FetchedDataset
from .fetchers import REGISTERED_FETCHERS
from .ingesters import data_processor_factory

logger = logging.getLogger(__name__)

q = queue.Queue()

SENTINEL = None


def queue_worker():
    while True:
        item: FetchedDataset = q.get()

        if item is SENTINEL:
            q.task_done()
            logger.info('Ingestion queue emptied')
            break

        logger.info(f'Picked up item: {item.dataset_id} from the queue and sending for ingestion')
        data_processor = data_processor_factory(item.dataset_type)
        data_processor.process_dataset(item.dataset_id, item.dataset_path)
        q.task_done()


def run():
    """
    We first wake up the queue and get it 'listening'
    Then we fire off the fetchers which will add items to the queue. As soon as an item is added it will be picked up.
    """
    threading.Thread(target=queue_worker).start()

    for fetcher in REGISTERED_FETCHERS:
        fetched_datasets = fetcher.fetch_datasets()
        for dataset in fetched_datasets:
            q.put(dataset)

    # Add an object to the end, so we know the queue is empty
    q.put(SENTINEL)
    q.join()

