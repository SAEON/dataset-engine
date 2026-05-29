#!/usr/bin/env python

import argparse
import logging
import sys

from ingest.orchestrator import synchronise, ingest

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Somisana Dataset Ingestion Engine")
    parser.add_argument(
        "--sync", 
        action="store_true", 
        help="Fetch products from the API and synchronise the local metadata registry"
    )
    parser.add_argument(
        "--ingest", 
        action="store_true", 
        help="Check local dataset files and run the ingestion orchestrator"
    )

    args = parser.parse_args()

    if not args.sync and not args.ingest:
        logger.error("Please specify at least one action: --sync or --ingest.")
        parser.print_help()
        sys.exit(1)

    if args.sync:
        logger.info("Executing product synchronisation...")
        synchronise()

    if args.ingest:
        logger.info("Executing dataset ingestion...")
        ingest()
