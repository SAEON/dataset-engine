from etc.const import DatasetType
from ocean_dataset.ocean_dataset_processor import OceanDatasetProcessor
from dataset_processor_interface import DatasetProcessorInterface


def data_processor_factory(dataset_type: DatasetType.value) -> DatasetProcessorInterface:
    match dataset_type:
        case DatasetType.OCEAN:
            return OceanDatasetProcessor()
        case _:
            raise ValueError(f'Unknown dataset type: {dataset_type}')
