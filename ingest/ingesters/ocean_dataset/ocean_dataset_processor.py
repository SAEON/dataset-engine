import logging

import xarray as xr

from db.models import ocean_dataset_data
from db.ocean_dataset_data_table_orchestrator import OceanDatasetDataTableOrchestrator
from db.utils import BulkInserter
from ingest.ingesters.dataset_processor_interface import DatasetProcessorInterface
from .models import NetcdfFileData
from .ocean_dataset_ingester import OceanDatasetIngester
from .utils import parse_ocean_dataset_path

logger = logging.getLogger(__name__)

INGEST_BATCH_SIZE = 50000


class OceanDatasetProcessor(DatasetProcessorInterface):

    def process_dataset(self, dataset_id: str, data_path: str) -> bool:
        try:
            logger.info(f"Ingesting data from {dataset_id}")

            # Decipher path
            parsed_path = parse_ocean_dataset_path(data_path)

            # Load data into data object
            netcdf_file_data = get_netcdf_file_data(parsed_path)

            ocean_dataset_data_table_orchestrator = OceanDatasetDataTableOrchestrator(dataset_id)
            ocean_dataset_data_table_orchestrator.create_ingest_into_table()

            # Initialise the bulk inserter with the correct sql
            bulk_inserter = BulkInserter(
                insert_sql=ocean_dataset_data_table_orchestrator.get_bulk_insert_sql(),
                batch_size=INGEST_BATCH_SIZE
            )

            # Iterate through data and save records
            netcdf_dataset_ingester = OceanDatasetIngester(dataset_id, netcdf_file_data)
            netcdf_dataset_ingester.set_bulk_inserter(bulk_inserter)
            netcdf_dataset_ingester.ingest_data()

            # Switch Temp table with original table
            ocean_dataset_data_table_orchestrator.switch_tables()

            return True
        except Exception as e:
            logger.exception(f"Failed to ingest dataset: {dataset_id}, with error: {str(e)}")
            return False


def get_netcdf_file_data(nc_file_path) -> NetcdfFileData:
    logger.info(f"Opening NetCDF file: {nc_file_path}")
    ds = xr.open_dataset(nc_file_path)

    logger.info("Dataset Information:")
    logger.info(ds)

    netcdf_file_data = NetcdfFileData()

    netcdf_file_data.set_coordinates(ds['lon_rho'].values, ds['lat_rho'].values)
    netcdf_file_data.set_dimensions(ds['time'].values, ds['depth'].values, ds['eta_rho'], ds['xi_rho'])

    netcdf_file_data.mask = ds['mask'].values
    netcdf_file_data.temps = ds['temp'].values
    netcdf_file_data.salts = ds['salt'].values
    netcdf_file_data.us = ds['u'].values
    netcdf_file_data.vs = ds['v'].values

    logger.info(
        f"Dataset dimensions: Time={netcdf_file_data.num_times}, Depth={netcdf_file_data.num_depths}, Eta_rho={netcdf_file_data.num_eta}, Xi_rho={netcdf_file_data.num_xi}")
    logger.info(
        f"Psi-grid dimensions: Eta_psi={netcdf_file_data.lon_psi.shape[0]}, Xi_psi={netcdf_file_data.lon_psi.shape[1]}")

    return netcdf_file_data
