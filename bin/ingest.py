#!/usr/bin/env python

import logging

from ingest.dataset_orchestrator import fetch_and_ingest

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)
    logger.info("Starting data ingestion process.")

    fetch_and_ingest()

