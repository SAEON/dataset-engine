from sqlalchemy import Column, String, Integer, Numeric, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from db import Base, Session


class DatasetVariable(Base):
    """
    A Dataset variable is a variable that contains the relevant data in a dataset
    """

    __tablename__ = 'dataset_variable'

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, ForeignKey('dataset.id'), nullable=False)
    variable_name = Column(String, nullable=False)
    variable_type = Column(String, nullable=False)

    dataset = relationship("Dataset", back_populates="variables")
    thresholds = relationship("VariableThresholds", back_populates="dataset_variable")

    def delete_all_thresholds(self):
        """
        Deletes all VariableThresholds associated with this DatasetVariable.
        """
        for threshold in self.thresholds:
            threshold.delete()


class VariableThresholds(Base):
    """
    Variable Thresholds contain the thresholds for variables in a dataset
    """

    __tablename__ = 'variable_thresholds'

    id = Column(Integer, primary_key=True)
    dataset_variable_id = Column(Integer, ForeignKey('dataset_variable.id'), nullable=False)
    min_value = Column(Numeric, nullable=False)
    max_value = Column(Numeric, nullable=False)
    dependent_variable_value = Column(Numeric, nullable=False)

    dataset_variable = relationship("DatasetVariable", back_populates="thresholds")
