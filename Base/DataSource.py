"""
Module Data source

Data source module must take responsibility for always providing stable data length, even though some or all data are
missing by reading.

Data names will be directly provided from instance itself. These names should be immutable.
"""

from abc import ABC, abstractmethod
import random


class DataSourceBase(ABC):
    def __init__(self):
        self._all_data_names = None  # Internal variable for all_data_names

    @abstractmethod
    def read_data(self) -> list:
        """Read data from source, this method will be used in DataLogger"""
        pass

    @property
    def all_data_names(self) -> list[str]:
        return self._all_data_names


class RandomDataSource(DataSourceBase):
    """Random data source to simulate data generation"""
    def __init__(self, size: int = 2, missing_rate: float = 0.5):
        super().__init__()
        self.size = size
        self.missing_rate = missing_rate
        self._all_data_names = [f'RandData{n}' for n in range(size)]  # Set all data names

    def read_data(self) -> list:
        # Generate a list of random floats
        data = [random.uniform(0.0, 100.0) for _ in range(self.size)]
        # Randomly select indices to be replaced with None
        missing_indices = random.sample(range(self.size), int(self.size * self.missing_rate))
        # Replace selected indices with None
        for index in missing_indices:
            data[index] = None
        return data


class RandomStringSource(DataSourceBase):
    """Random string source to simulate data generation"""
    def __init__(self, size: int = 2, str_length: int = 5):
        super().__init__()
        self.size = size
        self.str_length = str_length
        self._all_data_names = [f'RandStr{n}' for n in range(size)]  # Set all data names

    def read_data(self) -> list:
        def generate_random_string(length: int):
            """Generate random string with defined length"""
            return ''.join(random.choice(['A', 'a', 'B', 'b', 'C', 'c', 'D', 'd', 'E', 'e']) for _ in range(length))

        return [generate_random_string(self.str_length) for _ in range(self.size)]


if __name__ == "__main__":
    random_data_source = RandomDataSource(size=10, missing_rate=0.5)
    print(random_data_source.all_data_names)
    for _ in range(10):
        print(random_data_source.read_data())

    random_str_source = RandomStringSource(size=10, str_length=10)
    print(random_str_source.all_data_names)
    for _ in range(10):
        print(random_str_source.read_data())
