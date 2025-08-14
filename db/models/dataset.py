from sqlalchemy import Column, String, Integer, Numeric, DateTime
from sqlalchemy.orm import relationship

from db import Base


class Dataset(Base):
    """
    A Dataset is the header of the dataset data
    """

    __tablename__ = 'dataset'

    id = Column(String, primary_key=True)
    dataset_type = Column(String, nullable=False)
    north_bound = Column(Numeric, nullable=False)
    south_bound = Column(Numeric, nullable=False)
    east_bound = Column(Numeric, nullable=False)
    west_bound = Column(Numeric, nullable=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    time_step_minutes = Column(Integer)

    variables = relationship("DatasetVariable", back_populates="dataset")
