from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from db import Session
from db.models import Dataset
from somisana.version import VERSION
from .models import DatasetMetadata, Variable, Threshold

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
