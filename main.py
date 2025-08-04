import logging

from ingest.ingesters.ocean_data.ocean_data_processor import OceanDataProcessor

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    ocean_data_processor = OceanDataProcessor()
    ocean_data_processor.process_data('test_dataset', '/home/dylan/srv/netcdf-extractor/data/croco_avg_t2.nc')
