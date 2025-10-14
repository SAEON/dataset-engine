import datetime

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

