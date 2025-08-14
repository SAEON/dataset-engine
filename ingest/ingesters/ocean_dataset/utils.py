import datetime
from .models import VariableThreshold
from db.models import DatasetVariable, VariableThresholds

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
    temperature_variable = DatasetVariable(
        dataset_id=dataset_id,
        variable_name='temperature',
        variable_type='layer',
    ).save()

    __save_thresholds(temperature_variable.id, temperature_thresholds)

    salinity_variable = DatasetVariable(
        dataset_id=dataset_id,
        variable_name='salinity',
        variable_type='layer',
    ).save()

    __save_thresholds(salinity_variable.id, salinity_thresholds)


def __save_thresholds(dataset_variable_id: int, thresholds: dict[float, VariableThreshold]):
    for depth in thresholds:
        VariableThresholds(
            dataset_variable_id=dataset_variable_id,
            min_value=thresholds[depth].min_value,
            max_value=thresholds[depth].max_value,
            dependent_variable_value=depth
        ).save()

def set_dataset_dates(dataset_id: str, start_date: datetime, end_date: datetime, times_step_minutes: int):
    pass