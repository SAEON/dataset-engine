import logging

from ingest.dataset_orchestrator import fetch_and_ingest
from db.models import DatasetVariable, ocean_dataset_data
from ingest.ingesters.ocean_dataset.models import VariableThreshold
from ingest.ingesters.ocean_dataset.utils import insert_variables_and_thresholds
from db.utils import switch_tables, BulkInserter

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    # temp_thresholds = {}
    #
    # # The data from the table
    # threshold_data = [
    #     {'depth': -1000.0, 'min_temp': 3.170896053314209, 'max_temp': 4.578857421875},
    #     {'depth': -500.0, 'min_temp': 4.962965965270996, 'max_temp': 10.612008094787598},
    #     {'depth': -100.0, 'min_temp': 8.166056632995605, 'max_temp': 18.228734970092773},
    #     {'depth': -50.0, 'min_temp': 9.25619888305664, 'max_temp': 19.1806697845459},
    #     {'depth': -10.0, 'min_temp': 10.980690956115723, 'max_temp': 19.313135147094727},
    #     {'depth': -5.0, 'min_temp': 10.917180061340332, 'max_temp': 19.29850196838379},
    #     {'depth': 0.0, 'min_temp': 10.625594139099121, 'max_temp': 19.292789459228516},
    # ]
    #
    # # A loop to populate the dictionary
    # for row in threshold_data:
    #     depth = row['depth']
    #     min_temp = row['min_temp']
    #     max_temp = row['max_temp']
    #
    #     temp_thresholds[depth] = VariableThreshold(
    #         variable_name='temperature',
    #         min_value=min_temp,
    #         max_value=max_temp
    #     )
    #
    # salinity_thresholds = {}
    #
    # # The data from the provided table for salinity.
    # salinity_data = [
    #     {'depth': -1000.0, 'min_salinity': 34.37104034423828, 'max_salinity': 34.56215286254883},
    #     {'depth': -500.0, 'min_salinity': 34.289215087890625, 'max_salinity': 34.829891204833984},
    #     {'depth': -100.0, 'min_salinity': 34.58671569824219, 'max_salinity': 35.50516128540039},
    #     {'depth': -50.0, 'min_salinity': 34.55145263671875, 'max_salinity': 35.55046844482422},
    #     {'depth': -10.0, 'min_salinity': 34.34844207763672, 'max_salinity': 35.55000686645508},
    #     {'depth': -5.0, 'min_salinity': 34.22987747192383, 'max_salinity': 35.55014419555664},
    #     {'depth': 0.0, 'min_salinity': 33.859153747558594, 'max_salinity': 35.55027770996094},
    # ]
    #
    # # Iterate through the list of dictionaries and populate the salinity_thresholds dictionary.
    # for row in salinity_data:
    #     depth = row['depth']
    #     min_salinity = row['min_salinity']
    #     max_salinity = row['max_salinity']
    #
    #     # Create a VariableThreshold object and assign it to the dictionary.
    #     salinity_thresholds[depth] = VariableThreshold(
    #         variable_name='salinity',
    #         min_value=min_salinity,
    #         max_value=max_salinity
    #     )
    #
    # insert_variables_and_thresholds('sa_west', temp_thresholds, salinity_thresholds)

    fetch_and_ingest()
