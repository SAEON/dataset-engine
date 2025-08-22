from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from db.models.ocean_dataset_data import BULK_SELECT_SQL, BULK_SELECT_BY_TIME_DEPTH_SQL
from db.models import Dataset, DatasetVariable, VariableThresholds
from .models import DatasetMetadata, Variable, Threshold
from db import Session
from sqlalchemy import text
from somisana.version import VERSION

app = FastAPI(
    title="SOMISANA API",
    description="SOMISANA | SOMISANA Api",
    version=VERSION,
    docs_url='/swagger',
    redoc_url='/docs',
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/data/{time_str}/{depth}")
async def get_data_by_time_and_depth(
        time_str: str,
        depth: float
):
    sql_query = text(BULK_SELECT_BY_TIME_DEPTH_SQL)

    results = Session.execute(
        sql_query,
        {"time": time_str, "depth": depth}
    ).fetchall()

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for time: {time_str} and depth: {depth}")

    results_as_dicts = [{"cell_points": row[0], "temperature": row[1]} for row in results]

    return results_as_dicts


@app.get("/data/")
async def get_data_by_time_and_depth():
    sql_query = text(BULK_SELECT_SQL)

    results = Session.execute(sql_query).fetchall()

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found")

    results_as_dicts = [{"cell_points": row[0], "temperature": row[1]} for row in results]

    return results_as_dicts


@app.get("/dataset_meta/{dataset_id}")
async def get_dataset_metadata(dataset_id: str) -> DatasetMetadata:
    dataset = Session.get(Dataset, dataset_id)

    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    return DatasetMetadata(
        start_date=dataset.start_date,
        end_date=dataset.end_date,
        time_step_minutes=dataset.time_step_minutes,
        variables=[
            Variable(
                name=variable.variable_name,
                thresholds=[
                    Threshold(
                        min_value=threshold.min_value,
                        max_value=threshold.max_value,
                        dependant_value=threshold.dependent_variable_value
                    )
                    for threshold in variable.thresholds
                ]
            )
            for variable in dataset.variables
        ],
    )
