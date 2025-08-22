import datetime

from db import Session
from db.models import DatasetVariable, VariableThresholds, Dataset
from .models import VariableThreshold

YEAR_MONTH_TAG = '<YYYYMM>'
YEAR_MONTH_DAY_TAG = '<YYYYMMDD>'


def parse_ocean_dataset_path(ocean_dataset_path) -> str:
    current_date = datetime.datetime.now()

    current_year = str(current_date.year)
    current_month = f"{current_date.month:02d}"
    current_day = f"{current_date.day:02d}"

    parsed_path = ocean_dataset_path.replace(YEAR_MONTH_TAG, current_year + current_month)
    parsed_path = parsed_path.replace(YEAR_MONTH_DAY_TAG, current_year + current_month + current_day)

    return parsed_path


def insert_variables_and_thresholds(dataset_id: str, temperature_thresholds: dict[float, VariableThreshold],
                                    salinity_thresholds: dict[float, VariableThreshold]):
    temperature_variable = Session.query(DatasetVariable).filter(
        DatasetVariable.dataset_id == dataset_id,
        DatasetVariable.variable_name == 'temperature'
    ).first()

    if not temperature_variable:
        temperature_variable = DatasetVariable()

    temperature_variable.dataset_id = dataset_id
    temperature_variable.variable_name = 'temperature'
    temperature_variable.variable_type = 'layer'

    temperature_variable.save()
    temperature_variable.delete_all_thresholds()

    __save_thresholds(temperature_variable.id, temperature_thresholds)

    salinity_variable = Session.query(DatasetVariable).filter(
        DatasetVariable.dataset_id == dataset_id,
        DatasetVariable.variable_name == 'salinity'
    ).first()

    if not salinity_variable:
        salinity_variable = DatasetVariable()

    salinity_variable.dataset_id = dataset_id
    salinity_variable.variable_name = 'salinity'
    salinity_variable.variable_type = 'layer'

    salinity_variable.save()
    salinity_variable.delete_all_thresholds()

    __save_thresholds(salinity_variable.id, salinity_thresholds)


def __save_thresholds(dataset_variable_id: int, thresholds: dict[float, VariableThreshold]):
    for depth in thresholds:
        VariableThresholds(
            dataset_variable_id=dataset_variable_id,
            min_value=thresholds[depth].min_value,
            max_value=thresholds[depth].max_value,
            dependent_variable_value=depth
        ).save()


def set_dataset_dates(dataset_id: str, start_date: datetime, end_date: datetime, time_step_minutes: int):
    dataset = Session.get(Dataset, dataset_id)
    dataset.start_date = start_date
    dataset.end_date = end_date
    dataset.time_step_minutes = time_step_minutes
    dataset.save()

