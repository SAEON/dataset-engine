import logging
import time

import numpy as np

from db.utils import BulkInserter
from .models import NetcdfFileData, VariableThreshold
from .utils import insert_variables_and_thresholds, set_dataset_dates

logger = logging.getLogger(__name__)

LAND_MASK = 0
TIME_STEP_MINUTES = 60

TEMPERATURE_VARIABLE_NAME = 'temperature'
SALINITY_VARIABLE_NAME = 'salinity'
ZETA_VARIABLE_NAME = 'zeta'
SURFACE_DEPTH = 0.0


class OceanDatasetIngester:
    dataset_id = 0
    temp_dataset_id: str
    total_skipped_land_points = 0
    total_skipped_nan_points = 0
    total_inserted_cells_count = 0
    total_failed_cells_count = 0
    records_to_insert = []
    netcdf_file_data: NetcdfFileData
    bulk_inserter: BulkInserter
    temperature_thresholds: dict[float, VariableThreshold] = dict()
    salinity_thresholds: dict[float, VariableThreshold] = dict()
    zeta_thresholds: dict[float, VariableThreshold] = dict()

    def __init__(self, dataset_id: str, netcdf_file_data: NetcdfFileData):
        self.dataset_id = dataset_id
        self.temp_dataset_id = f'temp_{self.dataset_id}'
        self.netcdf_file_data = netcdf_file_data

    def set_bulk_inserter(self, bulk_inserter: BulkInserter):
        self.bulk_inserter = bulk_inserter

    def ingest_data(self):
        start_time = time.time()
        start_date = end_date = np.datetime_as_string(self.netcdf_file_data.times[0])

        self.zeta_thresholds[SURFACE_DEPTH] = VariableThreshold(ZETA_VARIABLE_NAME)

        for time_index in range(5):
            # for time_index in range(self.netcdf_file_data.num_times):
            current_time = np.datetime_as_string(self.netcdf_file_data.times[time_index], unit='s')
            end_date = np.datetime_as_string(self.netcdf_file_data.times[time_index])

            logger.info(f"Processing time step {time_index + 1}/{self.netcdf_file_data.num_times} ({current_time})")

            for depth_index in range(self.netcdf_file_data.num_depths):
                current_depth = float(self.netcdf_file_data.depths[depth_index])

                self.temperature_thresholds[current_depth] = VariableThreshold(TEMPERATURE_VARIABLE_NAME)
                self.salinity_thresholds[current_depth] = VariableThreshold(SALINITY_VARIABLE_NAME)

                self.__iterate_over_points_and_insert_cells(current_time, current_depth, time_index, depth_index)

        # Insert any remaining records
        self.bulk_inserter.flush()

        insert_variables_and_thresholds(self.dataset_id, self.temperature_thresholds, self.salinity_thresholds,
                                        self.zeta_thresholds)

        set_dataset_dates(
            self.dataset_id,
            start_date,
            end_date,
            TIME_STEP_MINUTES
        )

        end_time = time.time()
        elapsed_time = end_time - start_time
        with open("ingest_data_runtime.txt", "w") as f:
            f.write(f"The ingest_data function took {elapsed_time:.2f} seconds to run.")

    def __iterate_over_points_and_insert_cells(self, current_time, current_depth, time_index, depth_index):
        current_temp_slice = self.netcdf_file_data.temps[time_index, depth_index, :, :]
        current_salt_slice = self.netcdf_file_data.salts[time_index, depth_index, :, :]
        current_u_slice = self.netcdf_file_data.us[time_index, depth_index, :, :]
        current_v_slice = self.netcdf_file_data.vs[time_index, depth_index, :, :]
        current_zeta_slice = self.netcdf_file_data.zetas[time_index, :, :] if current_depth == SURFACE_DEPTH else None

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

                    if current_depth == SURFACE_DEPTH:
                        grid_cell.zeta_val = current_zeta_slice[i_idx, j_idx]

                    # Check for NaN values in any of the critical data points or coordinates
                    if not grid_cell.is_fully_populated():
                        self.total_skipped_nan_points += 1
                        continue

                    self.temperature_thresholds[current_depth].check_set_thresholds(float(grid_cell.temp_val))
                    self.salinity_thresholds[current_depth].check_set_thresholds(float(grid_cell.salt_val))

                    zeta_value = None
                    if not np.isnan(grid_cell.zeta_val):
                        zeta_value = float(grid_cell.zeta_val)
                        self.zeta_thresholds[SURFACE_DEPTH].check_set_thresholds(zeta_value)

                    record = (
                        self.temp_dataset_id,
                        current_time,
                        current_depth,
                        grid_cell.get_cell_vertices_geometry(),
                        float(grid_cell.temp_val),
                        float(grid_cell.salt_val),
                        float(grid_cell.u_val),
                        float(grid_cell.v_val),
                        zeta_value
                    )

                    self.bulk_inserter.add_record(record)
                    self.bulk_inserter.insert_batch_records()

                except Exception as e:
                    logger.exception(f"Failed to ingest data cell with error: {str(e)}")
                    self.total_failed_cells_count += 1
