from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import ocean_dataset
from .routers.ocean_dataset import on_ocean_dataset_startup


@asynccontextmanager
async def lifespan(app: FastAPI):
    on_ocean_dataset_startup()
    yield


app = FastAPI(
    title="SOMISANA Dataset API",
    description="SOMISANA Dataset | SOMISANA Dataset Api",
    docs_url='/swagger',
    redoc_url='/docs',
    lifespan=lifespan
)

app.include_router(ocean_dataset.router, prefix='/ocean_dataset', tags=['Product'])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
