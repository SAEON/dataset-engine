import logging

import numpy as np

from db.bulk_query import BulkInserter
from db.models.ocean_dataset_data import INSERT_SQL
from .models import NetcdfFileData, VariableThreshold
from .utils import insert_variables_and_thresholds, set_dataset_dates

logger = logging.getLogger(__name__)

BATCH_SIZE = 50000
LAND_MASK = 0

TEMPERATURE_VARIABLE_NAME = 'temperature'
SALINITY_VARIABLE_NAME = 'salinity'


class OceanDatasetIngester:
    dataset_id = 0
    total_skipped_land_points = 0
    total_skipped_nan_points = 0
    total_inserted_cells_count = 0
    total_failed_cells_count = 0
    records_to_insert = []
    netcdf_file_data: NetcdfFileData
    bulk_inserter: BulkInserter
    temperature_thresholds: dict[float, VariableThreshold] = dict()
    salinity_thresholds: dict[float, VariableThreshold] = dict()

    def __init__(self, dataset_id: str, netcdf_file_data: NetcdfFileData):
        self.dataset_id = dataset_id
        self.netcdf_file_data = netcdf_file_data

        self.bulk_inserter = BulkInserter(insert_sql=INSERT_SQL, batch_size=BATCH_SIZE)

    def ingest_data(self):
        for time_index in range(self.netcdf_file_data.num_times):
            current_time = np.datetime_as_string(self.netcdf_file_data.times[time_index], unit='s')
            logger.info(f"Processing time step {time_index + 1}/{self.netcdf_file_data.num_times} ({current_time})")

            for depth_index in range(self.netcdf_file_data.num_depths):
                current_depth = self.netcdf_file_data.depths[depth_index]

                self.temperature_thresholds[current_depth] = VariableThreshold(TEMPERATURE_VARIABLE_NAME)
                self.salinity_thresholds[current_depth] = VariableThreshold(SALINITY_VARIABLE_NAME)

                self.__iterate_over_points_and_insert_cells(current_time, current_depth, time_index, depth_index)

        # Insert any remaining records
        self.bulk_inserter.flush()

        # insert_variables_and_thresholds(self.dataset_id, self.temperature_thresholds, self.salinity_thresholds)

        # Set time

    def __iterate_over_points_and_insert_cells(self, current_time, current_depth, time_index, depth_index):
        current_temp_slice = self.netcdf_file_data.temps[time_index, depth_index, :, :]
        current_salt_slice = self.netcdf_file_data.salts[time_index, depth_index, :, :]
        current_u_slice = self.netcdf_file_data.us[time_index, depth_index, :, :]
        current_v_slice = self.netcdf_file_data.vs[time_index, depth_index, :, :]

        # Iterate over the original rho-grid dimensions, but stop 2 short
        # to ensure (i_idx+1, j_idx+1) for psi-points are within bounds.
        for i_idx in range(self.netcdf_file_data.num_eta - 2):  # row index
            for j_idx in range(self.netcdf_file_data.num_xi - 2):  # column index

                try:

                    # Check land-sea mask for the *rho-point* associated with this cell.
                    if self.netcdf_file_data.mask[i_idx, j_idx] == LAND_MASK:
                        self.total_skipped_land_points += 1
                        continue

                    grid_cell = self.netcdf_file_data.get_grid_cell(i_idx, j_idx)

                    # Get data values for the primary point (rho-point) of this cell
                    # These are still indexed by the original rho-grid indices
                    grid_cell.temp_val = current_temp_slice[i_idx, j_idx]
                    grid_cell.salt_val = current_salt_slice[i_idx, j_idx]
                    grid_cell.u_val = current_u_slice[i_idx, j_idx]
                    grid_cell.v_val = current_v_slice[i_idx, j_idx]

                    # Check for NaN values in any of the critical data points or coordinates
                    if not grid_cell.is_fully_populated():
                        self.total_skipped_nan_points += 1
                        continue

                    self.temperature_thresholds[current_depth].check_set_thresholds(grid_cell.temp_val)
                    self.salinity_thresholds[current_depth].check_set_thresholds(grid_cell.salt_val)

                    record = (
                        self.dataset_id,
                        current_time,
                        float(current_depth),
                        # grid_cell.get_cell_vertices_json(),
                        grid_cell.get_cell_vertices_geometry(),
                        float(grid_cell.temp_val),
                        float(grid_cell.salt_val),
                        float(grid_cell.u_val),
                        float(grid_cell.v_val),
                    )

                    self.bulk_inserter.add_record(record)
                    self.bulk_inserter.insert_batch_records()

                except Exception as e:
                    logger.exception(f"Failed to ingest data cell with error: {str(e)}")
                    self.total_failed_cells_count += 1
