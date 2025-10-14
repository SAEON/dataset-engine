import logging
import json
from pathlib import Path

import xarray as xr
import pandas as pd

from .models import DatasetMetadata, VariableInfo, DepthStat

logger = logging.getLogger(__name__)

VARIABLES = {
    "temp": {"name": "Temperature", "units": "Celsius"},
    "u": {"name": "U-component of velocity", "units": "m/s"},
    "v": {"name": "V-component of velocity", "units": "m/s"},
    "salt": {"name": "Salinity", "units": "PSU"},
    "zeta": {"name": "Sea surface height", "units": "m"},
}


def extract_and_save_metadata(dataset_id: str, zarr_path: Path, metadata_file: Path):
    """
    Opens a Zarr store, calculates metadata, populates a structured class instance,
    and saves it to a JSON file.
    """
    logger.info(f"Calculating metadata for dataset '{dataset_id}'")

    try:
        ds = xr.open_zarr(zarr_path, consolidated=True)

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

        save_metadata(dataset_id, metadata, metadata_file)

    except Exception as e:
        logger.exception(f"Error during metadata calculation: {e}")


def save_metadata(dataset_id: str, metadata: DatasetMetadata, metadata_file_path: Path):
    metadata_file_path.parent.mkdir(parents=True, exist_ok=True)

    if metadata_file_path.exists():
        with open(metadata_file_path, 'r') as f:
            all_metadata = json.load(f)
    else:
        all_metadata = {}

    all_metadata[dataset_id] = metadata.to_dict()

    with open(metadata_file_path, 'w') as f:
        json.dump(all_metadata, f, indent=4)


def convert_netcdf_to_zarr(netcdf_dataset_path: Path, zarr_dataset_path: Path):
    """
    Converts a NetCDF file to a Zarr store.
    """
    if not netcdf_dataset_path.exists():
        logger.error(f"Input file not found at '{netcdf_dataset_path}'")
        return

    logger.info(f"Converting Dataset netcdf to Zarr store: {zarr_dataset_path}")
    ds = xr.open_dataset(netcdf_dataset_path, chunks={})
    logger.info(f"Dataset structure: {ds}")
    ds.to_zarr(zarr_dataset_path, mode='w', consolidated=True)
    logger.info(f"Zarr conversion successful!")
