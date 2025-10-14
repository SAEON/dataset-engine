import json
import logging
from pathlib import Path

import numpy as np
import xarray as xr
from fastapi import APIRouter, HTTPException
from fastapi import Response
from fastapi.responses import JSONResponse

DATA_DIR = Path("./data")
METADATA_PATH = DATA_DIR / "datasets_metadata.json"
DATA_VARS = ["temp", "salt", "u", "v", "zeta"]

logger = logging.getLogger(__name__)

ALL_METADATA = {}
DATASETS_CACHE = {}

router = APIRouter()


def on_ocean_dataset_startup():
    global ALL_METADATA
    try:
        with open(METADATA_PATH, 'r') as f:
            ALL_METADATA = json.load(f)
    except FileNotFoundError:
        logger.exception(f"Metadata file not found at '{METADATA_PATH}'.")
        ALL_METADATA = {}
    except Exception as e:
        logger.exception(f"Error during startup reading metadata: {e}")
        ALL_METADATA = {}


def get_dataset(dataset_id: str) -> xr.Dataset:
    if dataset_id in DATASETS_CACHE:
        return DATASETS_CACHE[dataset_id]
    if dataset_id not in ALL_METADATA:
        raise HTTPException(status_code=404, detail=f"Dataset ID '{dataset_id}' not found.")
    zarr_path = DATA_DIR / f"{dataset_id}.zarr"
    if not zarr_path.exists():
        logger.error(f"Error: Zarr store not found at '{zarr_path}' for dataset '{dataset_id}'.")
        raise HTTPException(status_code=404, detail=f"Zarr store for dataset '{dataset_id}' not found on server.")
    try:
        logger.error(f"Opening and caching Zarr store for dataset: '{dataset_id}'")
        ds = xr.open_zarr(zarr_path, consolidated=True)
        DATASETS_CACHE[dataset_id] = ds
        return ds
    except Exception as e:
        logger.error(f"Error opening Zarr store '{zarr_path}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to open Zarr store for dataset '{dataset_id}'.")


@router.get("/")
def read_root():
    return {
        "message": "Zarr Contour Data Server is running.",
        "available_datasets": list(ALL_METADATA.keys())
    }


@router.get("/metadata")
def get_all_metadata():
    return ALL_METADATA


@router.get("/metadata/{dataset_id}")
def get_specific_metadata(dataset_id: str):
    if dataset_id in ALL_METADATA:
        return ALL_METADATA[dataset_id]
    raise HTTPException(status_code=404, detail=f"Dataset ID '{dataset_id}' not found.")


@router.get("/points/{dataset_id}/{time_index}/{depth_index}")
async def get_points_data(dataset_id: str, time_index: int, depth_index: int):
    try:
        ds = get_dataset(dataset_id)
        lon_coords = ds['lon_rho'].values
        lat_coords = ds['lat_rho'].values
        data_slices = {}
        for var in DATA_VARS:
            if var not in ds.variables:
                continue
            data_array = ds[var]
            slicers = {"time": time_index}
            if "depth" in data_array.dims:
                slicers["depth"] = depth_index
            data_slices[var] = data_array.isel(**slicers).values
        lons_flat = lon_coords.flatten()
        lats_flat = lat_coords.flatten()
        flat_data_slices = {var: arr.flatten() for var, arr in data_slices.items()}
        points = []
        if 'temp' in flat_data_slices:
            for i in range(len(lons_flat)):
                if np.isnan(flat_data_slices['temp'][i]):
                    continue
                properties = {var: float(flat_data_slices.get(var, [np.nan])[i]) for var in DATA_VARS}
                points.append({
                    "position": [float(lons_flat[i]), float(lats_flat[i])],
                    "properties": properties
                })
        return JSONResponse(content=points)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Error creating points data for '{dataset_id}': {e}")
        return Response(status_code=500, content=f"Error creating points data: {e}")
