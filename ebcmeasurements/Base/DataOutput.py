"""
Module Data Output

Data output module will always receive data in type 'dict' from data logger, with keys of variable names.
"""

from ebcmeasurements import BASE_DIR
from abc import ABC, abstractmethod
from typing import TypedDict
import csv
import os
import logging.config
# Load logging configuration from file
logging.config.fileConfig(os.path.join(BASE_DIR, 'Base', '_config', 'logging.ini'))
logger = logging.getLogger('DataOutput')


class DataOutputBase(ABC):
    # Class attribute: key's name for the logged time
    key_of_log_time = 'LogTime'

    def __init__(self, log_time_required: bool):
        # Internal variable for property 'all_variable_names'
        # It should be set by a DataLogger instance via property setter
        self._all_variable_names: tuple[str, ...] = ()

        # Internal variable for property 'log_time_required'
        # It should be defined during the initialization
        self._log_time_required = log_time_required

    @abstractmethod
    def log_data(self, data: dict):
        """
        Log data to output

        This method must be implemented in child class and will be used by the DataLogger to log data to the output
        """
        pass

    @staticmethod
    def generate_dir_of_file(file_name: str):
        """Generate a directory to save file if it does not exist"""
        dir_path = os.path.dirname(file_name)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    @property
    def all_variable_names(self) -> tuple[str, ...]:
        """
        All possible variable names of this data output

        This property returns a tuple containing the names of all variables that this data output can potentially
        contain.
        """
        return self._all_variable_names

    @all_variable_names.setter
    def all_variable_names(self, names: tuple[str, ...]):
        self._all_variable_names = names

    @property
    def log_time_required(self) -> bool:
        """If this data output requires log time by data logging"""
        return self._log_time_required


class DataOutputCsv(DataOutputBase):
    class CsvWriterSettings(TypedDict):
        """Typed dict for csv writer settings"""
        delimiter: str

    def __init__(
            self,
            file_name: str,
            csv_writer_settings: dict | None = None
    ):
        """
        Initialize data output instance for csv data
        :param file_name: File name to save csv data with full path
        :param csv_writer_settings: Settings of csv writer, supported keys: 'delimiter', if None, use default settings
        """
        logger.info("Initializing DataOutputCsv ...")

        super().__init__(log_time_required=True)  # csv file always requires log time
        self.file_name = file_name
        self.generate_dir_of_file(self.file_name)  # Generate file path if not exists

        # Set default csv_writer_settings
        self.csv_writer_settings: 'DataOutputCsv.CsvWriterSettings' = {
            'delimiter': ';'  # Delimiter of csv-file
        }

        # Set csv_writer_settings
        if csv_writer_settings is None:
            # Use default csv_writer_settings
            logger.info(f"Using default csv writer settings: {self.csv_writer_settings}")
        else:
            # Check all keys in csv_writer_settings
            for key in csv_writer_settings.keys():
                if key not in self.csv_writer_settings.keys():
                    raise ValueError(f"Invalid key in csv_writer_settings: '{key}'")
            # Update csv_writer_settings
            self.csv_writer_settings.update(csv_writer_settings)
            logger.info(f"Using csv writer settings: {self.csv_writer_settings}")

    def log_data(self, data: dict):
        """Log data to csv"""
        # Create a data dictionary based on the order of all variable names
        reordered_data = {k: data.get(k, None) for k in self._all_variable_names}
        self._append_to_csv(list(reordered_data.values()))  # Append data to csv

    def write_header_line(self):
        """Write header line as the first row of csv, this method must be called by initializing DataLogger"""
        self._write_to_csv(list(self._all_variable_names))

    def _write_to_csv(self, row: list):
        """Write a csv, the existing content in the file is erased as soon as the file is opened"""
        with open(self.file_name, 'w', newline='') as f:
            csv_writer = csv.writer(f, **self.csv_writer_settings)
            csv_writer.writerow(row)

    def _append_to_csv(self, row: list):
        """Append a new line to csv, the existing content in the file is preserved"""
        with open(self.file_name, 'a', newline='') as f:
            csv_writer = csv.writer(f, **self.csv_writer_settings)
            csv_writer.writerow(row)
