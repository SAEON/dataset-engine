from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Numeric, text, Index
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from db import Base, Session

TEMP_TABLE_NAME = 'temp_ocean_dataset_data'

TEMP_TABLE_INSERT_SQL = """
    INSERT INTO temp_ocean_dataset_data (
        dataset_id, date_time, depth, cell_points, temperature, salinity, u_velocity, v_velocity
    ) VALUES %s
"""

BULK_SELECT_BY_TIME_DEPTH_SQL = "SELECT cell_points, temperature FROM ocean_dataset_data WHERE date_time = :time AND depth = :depth"

BULK_SELECT_SQL = "SELECT cell_points, temperature FROM ocean_dataset_data"


class OceanDatasetData(Base):
    """
    OceanDatasetData contains the data extracted from ocean datasets
    """
    __tablename__ = 'ocean_dataset_data'

    __table_args__ = (
        Index('ocean_dataset_data_idx_date_time_depth', 'date_time', 'depth'),
    )

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, nullable=False)
    date_time = Column(DateTime, nullable=False)
    depth = Column(Numeric, nullable=False)
    cell_points = Column(Geometry('POLYGON', srid=4326), nullable=False)
    temperature = Column(Numeric, nullable=False)
    salinity = Column(Numeric, nullable=False)
    u_velocity = Column(Numeric, nullable=False)
    v_velocity = Column(Numeric, nullable=False)


class TempOceanDatasetData(Base):
    __tablename__ = 'temp_ocean_dataset_data'

    __table_args__ = (
        Index('temp_ocean_dataset_data_idx_date_time_depth', 'date_time', 'depth'),
    )

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, nullable=False)
    date_time = Column(DateTime, nullable=False)
    depth = Column(Numeric, nullable=False)
    cell_points = Column(Geometry('POLYGON', srid=4326), nullable=False)
    temperature = Column(Numeric, nullable=False)
    salinity = Column(Numeric, nullable=False)
    u_velocity = Column(Numeric, nullable=False)
    v_velocity = Column(Numeric, nullable=False)