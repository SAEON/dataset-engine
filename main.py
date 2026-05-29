import logging

from ingest.orchestrator import synchronise, ingest

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    synchronise()
    ingest()
