from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import ocean_dataset


app = FastAPI(
    title="SOMISANA Dataset API",
    description="SOMISANA Dataset | SOMISANA Dataset Api",
    docs_url='/swagger',
    redoc_url='/docs',
)

app.include_router(ocean_dataset.router, prefix='/ocean_dataset', tags=['Product'])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
