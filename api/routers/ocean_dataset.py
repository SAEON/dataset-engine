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
router = APIRouter()


def _get_metadata() -> dict:
    try:
        with open(METADATA_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Metadata file not found at '{METADATA_PATH}'.")
        return {}
    except Exception as e:
        logger.exception(f"Error reading metadata: {e}")
        return {}


@router.get("/")
def read_root():
    metadata = _get_metadata()
    return {
        "message": "Zarr Contour Data Server is running.",
        "available_datasets": list(metadata.keys())
    }


@router.get("/metadata")
def get_all_metadata():
    return _get_metadata()


@router.get("/metadata/{dataset_id}")
def get_specific_metadata(dataset_id: str):
    metadata = _get_metadata()
    if dataset_id in metadata:
        return metadata[dataset_id]
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
