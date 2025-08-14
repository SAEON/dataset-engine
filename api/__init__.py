from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from db.models.ocean_dataset_data import BULK_SELECT_SQL, BULK_SELECT_BY_TIME_DEPTH_SQL
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

