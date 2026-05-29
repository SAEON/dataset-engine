import json
import logging
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import StreamingResponse

DATA_DIR = Path(os.getenv("DATA_DIR", "../data"))

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
def read_root():
    return {
        "message": "Zarr Contour Data Server is running."
    }


@router.get("/products")
@router.get("/get_products")
def get_products():
    products_path = DATA_DIR / "products.json"
    if not products_path.exists():
        logger.error(f"products.json not found at '{products_path}'")
        return []
    try:
        with open(products_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.exception(f"Error loading products.json: {e}")
        raise HTTPException(status_code=500, detail="Internal server error reading products.")


@router.get("/metadata/{product_title}/{dataset_id}")
def get_specific_metadata(product_title: str, dataset_id: str):
    json_path = DATA_DIR / product_title / dataset_id / "metadata.json"
    if not json_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Metadata for dataset '{dataset_id}' in product '{product_title}' not found."
        )
    try:
        with open(json_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.exception(f"Error reading metadata for {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error reading metadata.")


@router.get("/grid/{product_title}/{dataset_id}")
async def get_grid_data(product_title: str, dataset_id: str):
    json_path = DATA_DIR / product_title / dataset_id / "grid.json"
    if not json_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Grid data for dataset '{dataset_id}' in product '{product_title}' not found."
        )
    try:
        with open(json_path, 'rb') as f:
            content = f.read()
        return Response(content=content, media_type="application/json")
    except Exception as e:
        logger.exception(f"Error serving grid data for {dataset_id}: {e}")
        return Response(status_code=500, content=str(e))


@router.get("/points/{product_title}/{dataset_id}/{depth_index}")
async def get_points_data(product_title: str, dataset_id: str, depth_index: int):
    json_path = DATA_DIR / product_title / dataset_id / f"depth_{depth_index}.json"
    if not json_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Data for dataset '{dataset_id}' in product '{product_title}' at depth '{depth_index}' not found."
        )
    try:
        def iterfile():
            with open(json_path, 'rb') as f:
                for line in f:
                    yield line

        return StreamingResponse(
            iterfile(),
            media_type="application/x-ndjson"
        )
    except Exception as e:
        logger.exception(f"Error serving JSON data for {dataset_id} at depth {depth_index}: {e}")
        return Response(status_code=500, content=str(e))
