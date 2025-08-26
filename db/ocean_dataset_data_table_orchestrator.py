from db.utils import switch_tables, BulkInserter, create_table, table_exists
from db.models import ocean_dataset_data


class OceanDatasetDataTableOrchestrator:
    dataset_id: str
    ocean_dataset_data_table_exists: bool
    ingest_into_table_name: str
    ingest_into_table_attributes: {}

    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.ocean_dataset_data_table_exists = table_exists(ocean_dataset_data.get_table_name(dataset_id))

    def create_ingest_into_table(self) -> str:
        """
        Create a table to ingest into.
        If an ocean_dataset_data table exists, create a temp table.
        If one doesn't exist, create one
        """
        if self.ocean_dataset_data_table_exists:
            self.ingest_into_table_name = ocean_dataset_data.get_temp_table_name(self.dataset_id)
        else:
            self.ingest_into_table_name = ocean_dataset_data.get_table_name(self.dataset_id)

        self.ingest_into_table_attributes = ocean_dataset_data.get_attributes(self.ingest_into_table_name)
        create_table(self.ingest_into_table_attributes, self.ingest_into_table_name)

        return self.ingest_into_table_name

    def switch_tables(self):
        """
        Only switch out the tables if a temp table was needed.
        """
        if not self.ocean_dataset_data_table_exists:
            return

        switch_tables(ocean_dataset_data.get_table_name(self.dataset_id), self.ingest_into_table_name)

    def get_bulk_insert_sql(self) -> str:
        return f"""INSERT INTO {self.ingest_into_table_name} (
            dataset_id, date_time, depth, cell_points, temperature, salinity, u_velocity, v_velocity
        ) VALUES %s"""
