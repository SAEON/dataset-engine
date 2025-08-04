class DatasetProcessorInterface:
    def process_dataset(self, dataset_id: str, data_path: str) -> bool:
        raise NotImplementedError
