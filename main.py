import logging

from ingest.dataset_orchestrator import fetch_and_ingest

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    fetch_and_ingest()
