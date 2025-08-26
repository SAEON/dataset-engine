from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Numeric, text, Index
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from db import Base, Session

BULK_SELECT_BY_TIME_DEPTH_SQL = "SELECT cell_points, temperature FROM ocean_dataset_data WHERE date_time = :time AND depth = :depth"

BULK_SELECT_SQL = "SELECT cell_points, temperature FROM ocean_dataset_data"

__BASE_TABLE_NAME = 'ocean_dataset_data'


def get_table_name(dataset_id: str) -> str:
    return f'{dataset_id}_{__BASE_TABLE_NAME}'


def get_temp_table_name(dataset_id: str) -> str:
    return f'{dataset_id}_temp_{__BASE_TABLE_NAME}'


def get_attributes(table_name: str):
    return {
        '__tablename__': table_name,

        '__table_args__': (
            Index(f'{table_name}_idx_date_time_depth', 'date_time', 'depth'),
        ),

        'id': Column(Integer, primary_key=True),
        'dataset_id': Column(String, nullable=False),
        'date_time': Column(DateTime, nullable=False),
        'depth': Column(Numeric, nullable=False),
        'cell_points': Column(Geometry('POLYGON', srid=4326), nullable=False),
        'temperature': Column(Numeric, nullable=False),
        'salinity': Column(Numeric, nullable=False),
        'u_velocity': Column(Numeric, nullable=False),
        'v_velocity': Column(Numeric, nullable=False),
    }
