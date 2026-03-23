"""
Module Data source

Data source module must always provide data in type 'dict', with keys of variable names.
"""

from abc import ABC, abstractmethod
import random
from datetime import datetime
import pandas as pd
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class DataSourceBase(ABC):
    """Base class of data source"""
    def __init__(self):
        # Internal variable for property 'all_variable_names'
        # It should be defined during the initialization, e.g. from a configuration file, from inside the class, or
        # from reading parameters of all devices. Using tuple to ensure the elements are immutable.
        self._all_variable_names: tuple[str, ...] = ()

    @abstractmethod
    def read_data(self) -> dict:
        """
        Read data from source

        This method must be implemented in child classes and will be used by the DataLogger to retrieve data.
        """
        pass

    @property
    def all_variable_names(self) -> tuple[str, ...]:
        """
        All possible variable names provided by this data source

        This property returns a tuple containing the names of all variables that this data source can potentially
        provide.
        """
        return self._all_variable_names


class RandomFloatSource(DataSourceBase):
    def __init__(self, size: int = 10, key_missing_rate: float = 0.5, value_missing_rate: float = 0.5):
        """
        Random float source to simulate data generation
        :param size: Number of variables to generate
        :param key_missing_rate: Probability of a key being excluded from the final dictionary
        :param value_missing_rate: Probability of assigning None to a value instead of a random float

        Default variable names are formatted as 'RandData<n>'.
        """
        super().__init__()
        if not (0.0 <= key_missing_rate <= 1.0):
            raise ValueError(f"key_missing_rate '{key_missing_rate}' must be between 0.0 and 1.0")
        if not (0.0 <= value_missing_rate <= 1.0):
            raise ValueError(f"value_missing_rate '{value_missing_rate}' must be between 0.0 and 1.0")

        self.size = size
        self.key_missing_rate = key_missing_rate
        self.value_missing_rate = value_missing_rate
        self._all_variable_names = tuple(f'RandData{n}' for n in range(self.size))  # Define all data names

    def read_data(self) -> dict[str, float]:
        """Generate random data for each variable name, randomly drop some keys, and randomly insert None values"""
        return {
            name: (None if random.random() < self.value_missing_rate else random.uniform(0.0, 100.0))
            for name in self._all_variable_names
            if random.random() >= self.key_missing_rate
        }


class RandomStringSource(RandomFloatSource):
    def __init__(
            self, size: int = 10, str_length: int = 5, key_missing_rate: float = 0.5, value_missing_rate: float = 0.5):
        """
        Random string source to simulate data generation
        :param size: Number of variables to generate
        :param str_length: Length of each random string
        :param key_missing_rate: Probability of a key being excluded from the final dictionary
        :param value_missing_rate: Probability of assigning None to a value instead of a random float

        Default variable names are formatted as 'RandStr<n>'.
        """
        super().__init__(size, key_missing_rate, value_missing_rate)
        self.str_length = str_length
        self._all_variable_names = tuple(f'RandStr{n}' for n in range(self.size))  # Re-define all data names

    def read_data(self) -> dict[str, str]:
        def generate_random_string(length: int) -> str:
            """Generate random string with defined length"""
            chars = '1234567890AaBbCcDdEeFf'
            return ''.join(random.choice(chars) for _ in range(length))

        return {
            name: (None if random.random() < self.value_missing_rate else generate_random_string(self.str_length))
            for name in self._all_variable_names
            if random.random() >= self.key_missing_rate
        }


class RandomBooleanSource(RandomFloatSource):
    def __init__(
            self, size: int = 10, key_missing_rate: float = 0.5, value_missing_rate: float = 0.5):
        """
        Random boolean source to simulate data generation
        :param size: Number of variables to generate
        :param key_missing_rate: Probability of a key being excluded from the final dictionary
        :param value_missing_rate: Probability of assigning None to a value instead of a random float

        Default variable names are formatted as 'RandBool<n>'.
        """
        super().__init__(size, key_missing_rate, value_missing_rate)
        self._all_variable_names = tuple(f'RandBool{n}' for n in range(self.size))  # Re-define all data names

    def read_data(self) -> dict[str, bool]:
        """Generate random data for each variable name, randomly drop some keys, and randomly insert None values"""
        return {
            name: (None if random.random() < self.value_missing_rate else random.choice([True, False]))
            for name in self._all_variable_names
            if random.random() >= self.key_missing_rate
        }


class DataSourceTimeTable(DataSourceBase):
    # Class attribute: supported types of fallback method
    _fallback_methods = ('prior', 'next', 'interpolate')
    # Class attribute: initial keys
    _init_keys = ('fallback_method', 'datetime_column_name', 'datetime_format')

    def __init__(
            self,
            df_timetable: pd.DataFrame,
            fallback_method: str = 'prior',
            **kwargs
    ):
        """
        Data source that provides data based on a schedule (timetable)

        :param df_timetable: Timetable in pandas DataFrame. It should contain a column or index of datetime values,
            which can be defined with kwargs 'datetime_column_name' and 'datetime_format'
        :param fallback_method: Fallback method when target datetime is not in index, supporting 'prior', 'next' and
            'interpolate'
        :param kwargs:
            'datetime_column_name': str: Column name of datetime in pandas DataFrame. Default is 'datetime'.
            'datetime_format': str: Format of datetime in pandas DataFrame. Default is '%Y-%m-%d %H:%M:%S'.
        """
        logger.info("Initializing DataSourceTimeTable ...")
        super().__init__()
        self.df_timetable = df_timetable
        if fallback_method not in self._fallback_methods:
            raise ValueError(
                f"Invalid fallback method '{fallback_method}', it must be one of '{self._fallback_methods}'")
        self.fallback_method = fallback_method
        self.datetime_column_name = kwargs.get('datetime_column_name', 'datetime')
        self.datetime_format = kwargs.get('datetime_format', '%Y-%m-%d %H:%M:%S')

        # Process and check timetable
        self._process_timetable()
        self._check_duplicate_datetime()
        self._check_monotonic_datetime()

        # Extract all variable names
        self._all_variable_names = tuple(self.df_timetable.columns)

    def _process_timetable(self) -> None:
        """Process timetable"""
        if self.datetime_column_name in self.df_timetable.columns:
            # Datetime in columns, convert string in datetime
            self.df_timetable[self.datetime_column_name] = pd.to_datetime(
                self.df_timetable[self.datetime_column_name], format=self.datetime_format)
            # Set datetime to index
            self.df_timetable = self.df_timetable.set_index(self.datetime_column_name)
        elif self.df_timetable.index.name == self.datetime_column_name:
            # Datetime in index, rename the index
            self.df_timetable.index.name = 'datetime'
            # Convert to datetime
            self.df_timetable.index = pd.to_datetime(self.df_timetable.index, format=self.datetime_format)
        else:
            raise ValueError(f"Invalid datetime column name '{self.datetime_column_name}': not found in index or column")

    def _check_duplicate_datetime(self) -> None:
        """Check duplicates of datetime, if duplicates exist, keep the last duplicates"""
        if self.df_timetable.index.duplicated().any():
            logging.warning("Duplicated datetime found, keeping the last values ...")
            self.df_timetable = self.df_timetable[~self.df_timetable.index.duplicated(keep='last')]

    def _check_monotonic_datetime(self) -> None:
        """Check if datetime is monotonic, if not monotonic increasing, rearrange the index"""
        if not self.df_timetable.index.is_monotonic_increasing:
            logging.warning("Datetime is not monotonic increasing, rearranging ...")
            self.df_timetable = self.df_timetable.sort_index()

    def _extract_data_from_timetable(self, dt_target: datetime, fallback_method: str) -> dict:
        """Extract data from the timetable based on the row of specified datetime"""
        # Target datetime found in index
        if dt_target in self.df_timetable.index:
            logging.debug(f"Target datetime '{dt_target}' found in index")
            return self.df_timetable.loc[dt_target].to_dict()

        # Target datetime not found in index
        prior_rows = self.df_timetable[self.df_timetable.index < dt_target]
        next_rows = self.df_timetable[self.df_timetable.index > dt_target]

        # Prior value
        if fallback_method == 'prior' and not prior_rows.empty:
            return prior_rows.iloc[-1].to_dict()

        # Next value
        if fallback_method == 'next' and not next_rows.empty:
            return next_rows.iloc[0].to_dict()

        # Interpolate
        if fallback_method == 'interpolate' and not prior_rows.empty and not next_rows.empty:
            # Concatenate last row of 'prior_rows' with first row of 'next_rows' to df_temp
            df_temp = pd.concat([prior_rows.iloc[[-1]], next_rows.iloc[[0]]])
            # Form a union with dt_target and reindex to generate missing value
            df_temp = df_temp.reindex(df_temp.index.union([dt_target]))
            # Interpolate missing value
            df_temp = df_temp.interpolate(method='time')
            return df_temp.loc[dt_target].to_dict()

        return {}

    def read_data(self) -> dict:
        """Execute by DataLoggerTimeTrigger, extract data from timetable"""
        dt_now = datetime.now().replace(microsecond=0)
        return self._extract_data_from_timetable(dt_target=dt_now, fallback_method=self.fallback_method)

    @classmethod
    def from_csv(cls, file_name: str, **kwargs) -> 'DataSourceTimeTable':
        """
        Create instance of DataSourceTimeTable from csv file

        :param file_name: csv file name
        :param kwargs: kwargs of DataSourceTimeTable and pandas.read_csv
        """
        csv_kwargs = {k: v for k, v in kwargs.items() if k not in DataSourceTimeTable._init_keys}
        cls_kwargs = {k: v for k, v in kwargs.items() if k in DataSourceTimeTable._init_keys}
        # Load csv to df
        df_timetable = pd.read_csv(file_name, **csv_kwargs)
        return  cls(df_timetable=df_timetable, **cls_kwargs)

    @classmethod
    def from_excel(cls, file_name: str, **kwargs) -> 'DataSourceTimeTable':
        """
        Create instance of DataSourceTimeTable from Excel file

        :param file_name: Excel file name
        :param kwargs: kwargs of DataSourceTimeTable and pandas.read_excel
        """
        excel_kwargs = {k: v for k, v in kwargs.items() if k not in DataSourceTimeTable._init_keys}
        cls_kwargs = {k: v for k, v in kwargs.items() if k in DataSourceTimeTable._init_keys}
        # Load excel to df
        df_timetable = pd.read_excel(file_name, **excel_kwargs)
        return cls(df_timetable=df_timetable, **cls_kwargs)
