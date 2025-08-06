import logging

from ingest.ingesters.ocean_dataset.ocean_dataset_processor import OceanDatasetProcessor
from ingest.dataset_pipeline_orchestrator import run

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    # ocean_data_processor = OceanDatasetProcessor()
    # ocean_data_processor.process_dataset('test_dataset', '/home/dylan/srv/netcdf-extractor/data/croco_avg_t2.nc')

    run()