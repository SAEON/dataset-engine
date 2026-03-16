import json
import logging
import os
from pathlib import Path

import numpy as np
import orjson
import xarray as xr
from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import StreamingResponse

DATA_DIR = Path(os.getenv("DATA_DIR", "../data"))
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


@router.get("/points/{dataset_id}/{depth_index}")
async def get_points_data(dataset_id: str, depth_index: int):
    try:
        # Construct the path to the depth_X.json file
        json_path = DATA_DIR / dataset_id / f"depth_{depth_index}.json"
        
        if not json_path.exists():
            raise HTTPException(status_code=404, detail=f"Data for dataset '{dataset_id}' and depth '{depth_index}' not found.")

        # Use a generator to stream the file line by line
        def iterfile():
            with open(json_path, 'rb') as f:
                for line in f:
                    yield line

        return StreamingResponse(
            iterfile(),
            media_type="application/x-ndjson"
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(f"Error serving JSON data: {e}")
        return Response(status_code=500, content=str(e))


@router.get("/grid/{dataset_id}")
async def get_grid_data(dataset_id: str):
    try:
        json_path = DATA_DIR / dataset_id / "grid.json"
        
        if not json_path.exists():
            raise HTTPException(status_code=404, detail=f"Grid data for dataset '{dataset_id}' not found.")

        with open(json_path, 'rb') as f:
            content = f.read()
            
        return Response(
            content=content,
            media_type="application/json"
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(f"Error serving grid data: {e}")
        return Response(status_code=500, content=str(e))
