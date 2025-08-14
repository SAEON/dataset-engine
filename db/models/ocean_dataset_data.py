from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Numeric
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

from db import Base

INSERT_SQL = """
    INSERT INTO ocean_dataset_data (
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

    id = Column(Integer, primary_key=True)
    # dataset_id = Column(String, ForeignKey('dataset.id'), nullable=False)
    dataset_id = Column(String)
    date_time = Column(DateTime, nullable=False)
    depth = Column(Numeric, nullable=False)
    cell_points = Column(Geometry('POLYGON', srid=4326), nullable=False)
    temperature = Column(Numeric, nullable=False)
    salinity = Column(Numeric, nullable=False)
    u_velocity = Column(Numeric, nullable=False)
    v_velocity = Column(Numeric, nullable=False)

    # dataset = relationship('Dataset', back_populates='ocean_dataset_data')
