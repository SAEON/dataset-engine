import logging
import json
import os
from pathlib import Path

import xarray as xr
import pandas as pd
import numpy as np

from etc.config import config
from .models import DatasetMetadata, VariableInfo, DepthStat

logger = logging.getLogger(__name__)

VARIABLES = {
    "temp": {"name": "Temperature", "units": "Celsius"},
    "u": {"name": "U-component of velocity", "units": "m/s"},
    "v": {"name": "V-component of velocity", "units": "m/s"},
    "salt": {"name": "Salinity", "units": "PSU"},
    "zeta": {"name": "Sea surface height", "units": "m"},
}

DATA_DIR = Path(config['FILES']['DATA_DIR_PATH'])


class DatasetIngester:

    def ingest_dataset(self, dataset_id: str, data_path: str, product_folder_path: Path) -> bool:
        """
        Converts a NetCDF file to a set of JSON files (grid.json and depth_X.json).
        Downsamples time to every 4 hours.
        """
        netcdf_dataset_path = Path(data_path)
        output_dir = product_folder_path / dataset_id

        if not netcdf_dataset_path.exists():
            logger.error(f"Input file not found at '{netcdf_dataset_path}'")
            return False

        try:
            logger.info(f"Ingesting data from {dataset_id}")
            logger.info(f"Converting Dataset netcdf to JSON in: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)

            ds = xr.open_dataset(netcdf_dataset_path)

            # Downsample to every 4 hours
            ds = ds.sel(time=ds.time.dt.hour % 4 == 0)
            # Ensure we only pick the first 30min of that hour if multiple exist
            ds = ds.resample(time="4H").nearest(tolerance="1H")

            # 1. Save grid.json
            grid_data = {
                "lons": self.__replace_nans(ds.lon_rho.values.flatten()),
                "lats": self.__replace_nans(ds.lat_rho.values.flatten())
            }
            with open(output_dir / "grid.json", "w") as f:
                json.dump(grid_data, f)

            # 2. Save depth_X.json
            depth_levels = ds.depth.values.tolist()
            time_steps = ds.time.values

            for i, depth in enumerate(depth_levels):
                logger.debug(f"Saving depth index: {i}")
                with open(output_dir / f"depth_{i}.json", "w") as f:
                    for t_idx, t in enumerate(time_steps):
                        step_data = {
                            "time": str(t),
                            "temp": self.__replace_nans(ds.temp.isel(depth=i, time=t_idx).values.flatten()),
                            "salt": self.__replace_nans(ds.salt.isel(depth=i, time=t_idx).values.flatten()),
                            "u": self.__replace_nans(ds.u.isel(depth=i, time=t_idx).values.flatten()),
                            "v": self.__replace_nans(ds.v.isel(depth=i, time=t_idx).values.flatten()),
                        }
                        if i == 0 and "zeta" in ds:
                            step_data["zeta"] = self.__replace_nans(ds.zeta.isel(time=t_idx).values.flatten())

                        f.write(json.dumps(step_data) + "\n")

            # 3. Save metadata
            self.__extract_and_save_metadata(dataset_id, ds, output_dir)

            logger.info(f"JSON conversion and metadata save successful for {dataset_id}!")
            return True

        except Exception as e:
            logger.exception(f"Failed to ingest dataset: {dataset_id}, with error: {str(e)}")
            return False

    def __extract_and_save_metadata(self, dataset_id: str, ds: xr.Dataset, output_dir: Path):
        """
        Calculates metadata from an xarray Dataset, populates a structured class instance,
        and saves it to a JSON file in the output directory.
        """
        logger.info(f"Calculating metadata for dataset '{dataset_id}'")

        metadata = DatasetMetadata()

        lon_min, lon_max = ds.lon_rho.min().compute().item(), ds.lon_rho.max().compute().item()
        lat_min, lat_max = ds.lat_rho.min().compute().item(), ds.lat_rho.max().compute().item()
        metadata.bounds = [lon_min, lat_min, lon_max, lat_max]

        metadata.grid_height, metadata.grid_width = ds.lon_rho.shape

        metadata.u_min_global = float(ds['u'].min(skipna=True).compute().item())
        metadata.u_max_global = float(ds['u'].max(skipna=True).compute().item())
        metadata.v_min_global = float(ds['v'].min(skipna=True).compute().item())
        metadata.v_max_global = float(ds['v'].max(skipna=True).compute().item())

        metadata.depth_levels = ds.depth.values.tolist()

        time_coords = pd.to_datetime(ds.time.values)
        metadata.time_steps = len(ds.time)
        metadata.start_date = time_coords[0].isoformat().replace('+00:00', 'Z')
        metadata.end_date = time_coords[-1].isoformat().replace('+00:00', 'Z')

        if len(time_coords) > 1:
            time_delta = time_coords[1] - time_coords[0]
            metadata.step_minutes = time_delta.total_seconds() / 60
        else:
            metadata.step_minutes = 0

        for var_name, var_info in VARIABLES.items():
            if var_name not in ds.variables:
                continue

            variable_metadata = VariableInfo(name=var_info["name"], units=var_info["units"])
            data_array = ds[var_name]

            if "depth" in data_array.dims:
                for i, depth in enumerate(metadata.depth_levels):
                    depth_slice = data_array.isel(depth=i)
                    q05 = float(depth_slice.quantile(0.05, skipna=True).compute().item())
                    q95 = float(depth_slice.quantile(0.95, skipna=True).compute().item())

                    variable_metadata.depth_stats[str(depth)] = DepthStat(vmin=q05, vmax=q95)
            else:
                q05 = float(data_array.quantile(0.05, skipna=True).compute().item())
                q95 = float(data_array.quantile(0.95, skipna=True).compute().item())

                variable_metadata.depth_stats[str(0.0)] = DepthStat(vmin=q05, vmax=q95)

            metadata.variables[var_name] = variable_metadata

        logger.info(f"Metadata calculation for '{dataset_id}' successful")

        metadata_file_path = output_dir / "metadata.json"
        self.__save_metadata(metadata, metadata_file_path)

    def __save_metadata(self, metadata: DatasetMetadata, metadata_file_path: Path):
        metadata_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_file_path, 'w') as f:
            json.dump(metadata.to_dict(), f, indent=4)

    def __replace_nans(self, arr):
        """Helper to replace np.nan with None for valid JSON nulls"""
        arr_list = arr.tolist()

        def recursive_replace(obj):
            if isinstance(obj, list):
                return [recursive_replace(item) for item in obj]
            elif pd.isna(obj):
                return None
            return obj

        return recursive_replace(arr_list)
