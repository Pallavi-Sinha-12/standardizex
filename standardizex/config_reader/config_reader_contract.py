from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class ConfigReaderContract(ABC):

    @abstractmethod
    def read_source_columns_schema(self) -> DataFrame:
        pass

    @abstractmethod
    def read_new_columns_schema(self) -> DataFrame:
        pass

    @abstractmethod
    def read_column_descriptions_metadata(self) -> dict:
        pass

    @abstractmethod
    def read_column_sequence_order(self) -> list:
        pass
