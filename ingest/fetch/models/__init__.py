class Dataset:
    id: str
    title: str
    dataset_type: str
    file_path: str
    file_last_updated: str

class Product:
    id: int
    datasets: list[Dataset]
    title: str
    bounds: list[float]

