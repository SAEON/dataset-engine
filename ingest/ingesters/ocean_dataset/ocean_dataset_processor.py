import logging

from ingest.ingesters.dataset_processor_interface import DatasetProcessorInterface
from pathlib import Path
from .utils import parse_ocean_dataset_path
from .ocean_dataset_ingester import convert_netcdf_to_zarr, extract_and_save_metadata

logger = logging.getLogger(__name__)

INGEST_BATCH_SIZE = 50000

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent.parent

DATA_DIR = project_root / "data"
METADATA_OUTPUT_FILE = DATA_DIR / "datasets_metadata.json"
ZARR_OUTPUT_DIR = DATA_DIR


class OceanDatasetProcessor(DatasetProcessorInterface):

    def process_dataset(self, dataset_id: str, data_path: str) -> bool:
        try:
            logger.info(f"Ingesting data from {dataset_id}")

            # Decipher path
            parsed_path = parse_ocean_dataset_path(data_path)

            zarr_file_path = Path(f'{ZARR_OUTPUT_DIR}/{dataset_id}.zarr')

            # Convert netcdf dataset to Zarr Datastore
            convert_netcdf_to_zarr(Path(parsed_path), zarr_file_path)

            # Write Zarr metadata to metadata json
            extract_and_save_metadata(dataset_id, zarr_file_path, Path(METADATA_OUTPUT_FILE))

            return True
        except Exception as e:
            logger.exception(f"Failed to ingest dataset: {dataset_id}, with error: {str(e)}")
            return False
