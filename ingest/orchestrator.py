import logging

from ingest.fetch import ProductFetcher
from ingest.synchronise.product_synchroniser import ProductSynchroniser
from ingest.ingest.ingestion_orchestrator import IngestionOrchestrator

logger = logging.getLogger(__name__)


def synchronise():
    """
    Fetches the latest products, synchronises them to the local product metadata registry.
    """
    logger.info("Fetching products...")
    fetcher = ProductFetcher()
    products = fetcher.fetch_products()

    logger.info(f"Synchronising {len(products)} products...")
    synchroniser = ProductSynchroniser()
    synchroniser.synchronise_products(products)


def ingest():
    """
    Directly runs the ingestion orchestrator without fetching new product updates first.
    """
    logger.info("Running ingestion orchestrator...")
    orchestrator = IngestionOrchestrator()
    orchestrator.run()
