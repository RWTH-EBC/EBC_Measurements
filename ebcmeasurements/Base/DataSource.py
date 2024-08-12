"""
Module Data source

Data source module must always provide data in type 'dict', with keys of variable names.
"""

from abc import ABC, abstractmethod
import random


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


class RandomDataSource(DataSourceBase):
    def __init__(self, size: int = 10, key_missing_rate: float = 0.5, value_missing_rate: float = 0.5):
        """
        Random data source to simulate data generation
        :param size: Number of variables to generate
        :param key_missing_rate: Probability of a key being excluded from the final dictionary
        :param value_missing_rate: Probability of assigning None to a value instead of a random float
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

    def read_data(self) -> dict:
        """Generate random data for each variable name, randomly drop some keys, and randomly insert None values"""
        return {
            name: (None if random.random() < self.value_missing_rate else random.uniform(0.0, 100.0))
            for name in self._all_variable_names
            if random.random() >= self.key_missing_rate
        }


class RandomStringSource(RandomDataSource):
    def __init__(
            self, size: int = 10, str_length: int = 5, key_missing_rate: float = 0.5, value_missing_rate: float = 0.5):
        """
        Random string source to simulate data generation
        :param size: Number of variables to generate
        :param str_length: Length of each random string
        :param key_missing_rate: Probability of a key being excluded from the final dictionary
        :param value_missing_rate: Probability of assigning None to a value instead of a random float
        """
        super().__init__(size, key_missing_rate, value_missing_rate)
        self.str_length = str_length
        self._all_variable_names = tuple(f'RandStr{n}' for n in range(self.size))  # Re-define all data names

    def read_data(self) -> dict:
        def generate_random_string(length: int) -> str:
            """Generate random string with defined length"""
            chars = '1234567890AaBbCcDdEeFf'
            return ''.join(random.choice(chars) for _ in range(length))

        return {
            name: (None if random.random() < self.value_missing_rate else generate_random_string(self.str_length))
            for name in self._all_variable_names
            if random.random() >= self.key_missing_rate
        }


if __name__ == "__main__":
    random_data_source = RandomDataSource(size=10, key_missing_rate=0.5, value_missing_rate=0.5)
    print(f"All variable names of random data source: {random_data_source.all_variable_names}")
    for _ in range(10):
        print(random_data_source.read_data())

    random_str_source = RandomStringSource(size=10, str_length=8, key_missing_rate=0.5, value_missing_rate=0.5)
    print(f"All variable names of random string source: {random_str_source.all_variable_names}")
    for _ in range(10):
        print(random_str_source.read_data())
