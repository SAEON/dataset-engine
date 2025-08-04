import xarray as xr

from ingest.ingesters.dataset_processor_interface import DatasetProcessorInterface
from .models import NetcdfFileData
from .ocean_dataset_ingester import OceanDatasetIngester
from .utils import parse_ocean_dataset_path
import logging

logger = logging.getLogger(__name__)


class OceanDatasetProcessor(DatasetProcessorInterface):

    def process_dataset(self, dataset_id: str, data_path: str) -> bool:
        try:
            logger.info(f"Ingesting data from {dataset_id}")

            # Deal with existing data

            # Decipher path
            parsed_path = parse_ocean_dataset_path(data_path)

            # Load data into data object
            netcdf_file_data = get_netcdf_file_data(parsed_path)

            # Iterate through data and save records
            netcdf_dataset_processor = OceanDatasetIngester(dataset_id, netcdf_file_data)
            netcdf_dataset_processor.ingest_data()

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
