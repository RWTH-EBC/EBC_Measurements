"""
Module Data Output

Data output module must take responsibility to check if the data length is same as the headers' length, in order to
avoid an invalid data logging.

Headers will be directly provided from instance itself. They should be immutable.
"""

from abc import ABC, abstractmethod
from typing import TypedDict
import csv
import os
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('DataOutput')


class DataOutputBase(ABC):
    def __init__(self, all_data_names: list[str], time_in_header: bool):
        # Internal variable for all_data_names, achieved by combine the sources' property 'all_data_name'
        self._all_data_names = all_data_names
        # Set if 'Time' be added to headers
        self._time_in_header = time_in_header
        # Generate the header
        self._headers = self._generate_headers()  # Internal variable for headers
        self._length_headers = len(self._headers)

    def _generate_headers(self) -> list[str]:
        """Generate header of output"""
        if self._time_in_header:
            return ['Time'] + self._all_data_names
        else:
            return self._all_data_names

    @abstractmethod
    def _log_data(self, row: list):
        """Log data to output, this method will be used in DataLogger"""
        pass

    def check_and_log_data(self, row: list) -> bool:
        """
        Check data length and log data by same length, skip logging by different lengths
        :return: True for successful logging, False for failed logging
        """
        if self._check_data_length(row):  # Check data length
            self._log_data(row)
            return True
        else:
            logger.error(f"Mismatched data length = {len(row)} and length of headers = {self._length_headers}, "
                         f"skipping logging ...")
            return False

    def _check_data_length(self, row: list) -> bool:
        """Check if the data length same as the length of data names"""
        return len(row) == self._length_headers

    @staticmethod
    def generate_dir_of_file(file_name: str):
        """Generate a directory to save file if it does not exist"""
        dir_path = os.path.dirname(file_name)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    @property
    def time_in_header(self):
        return self._time_in_header

    @property
    def headers(self):
        return self._headers


class DataOutputCsv(DataOutputBase):
    class CsvWriterSettings(TypedDict):
        """Typed dict for csv writer settings"""
        delimiter: str

    # Class attribute: Default setting of csv writer
    _csv_writer_settings_default: 'DataOutputCsv.CsvWriterSettings' = {
        'delimiter': ';'  # Delimiter of csv-file
    }

    def __init__(
            self,
            file_name: str,
            all_data_names: list[str],
            csv_writer_settings: dict[str: str] | None = None
    ):
        """
        Initialize data output instance for csv data
        :param file_name: File name to save csv data with full path
        :param all_data_names: Data names from all sources
        :param csv_writer_settings: Settings of csv writer, supported keys: 'delimiter', if None, use default settings
        """
        logger.info("Initializing DataOutputCsv ...")

        super().__init__(all_data_names=all_data_names, time_in_header=True)  # csv file always needs 'Time' in header
        self.file_name = file_name
        self.generate_dir_of_file(self.file_name)  # Generate file path if not exists

        # Get default csv_writer_settings
        self.csv_writer_settings = self._csv_writer_settings_default.copy()  # Use copy() to avoid change cls attribute

        # Set csv_writer_settings
        if csv_writer_settings is None:
            # Use default csv_writer_settings
            logger.info(f"Using default csv writer settings: {self.csv_writer_settings}")
        else:
            # Check all keys in csv_writer_settings
            for key in csv_writer_settings.keys():
                if key not in self._csv_writer_settings_default.keys():
                    raise ValueError(f"Invalid key in csv_writer_settings: '{key}'")
            # Update csv_writer_settings
            self.csv_writer_settings.update(csv_writer_settings)
            logger.info(f"Using csv writer settings: {self.csv_writer_settings}")

        # Write headers to csv
        self._write_csv_header(self._headers)

    def _log_data(self, row: list):
        """Log data to csv"""
        self._append_to_csv(row)  # Append data to csv

    def _write_csv_header(self, row: list):
        """Write header as the first row of csv, this will erase the file"""
        self._write_to_csv(row)

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

    @property
    def csv_writer_settings_default(self):
        return self._csv_writer_settings_default
