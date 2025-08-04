from models import FetchedDataset


class DatasetFetcherInterface:
    def fetch_datasets(self) -> list[FetchedDataset]:
        raise NotImplementedError
