from datetime import datetime
from pydantic import BaseModel


class Threshold(BaseModel):
    dependant_value: float
    min_value: float
    max_value: float


class Variable(BaseModel):
    name: str
    thresholds: list[Threshold]


class DatasetMetadata(BaseModel):
    start_date: datetime
    end_date: datetime
    time_step_minutes: int
    variables: list[Variable]
