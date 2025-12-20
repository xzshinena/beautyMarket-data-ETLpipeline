# abstract base class (abc)
import pandas as pd
from abc import ABC, abstractmethod
from config import required_columns

class DataSource(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        pass

    def validate_schema(self, df: pd.DataFrame) -> bool:
        actual_columns = set(df.columns)
        return required_columns.issubset(actual_columns)

    def fetch_n_validate(self) -> pd.DataFrame:
        df = self.fetch()
        
        if not self.validate_schema(df) :
            actual = set(df.columns)
            missing = required_columns - actual
            raise ValueError(
                f"Error from '{self.name}' not having all required columns"
                f"Missing columns : '{missing}'"
            )
        
        return df




