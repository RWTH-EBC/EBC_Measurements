"""
Base module: DataLogger, incl. ABC of DataSource and DataOutput
"""

from abc import ABC, abstractmethod
from typing import TypedDict
import csv
import time
import os
import random
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('DataLogger')





class DataOutputBase(ABC):
    """
    If the output format has data headers, data output module must take responsibility to check if the data length is
    same as the headers' length, to avoid an invalid data logging.

    Data headers should be directly achieved from data source with property 'all_data_names'.
    """
    def __init__(self, all_data_names: list[str], time_in_header: bool):
        self._all_data_names = all_data_names
        self._time_in_header = time_in_header
        # Generate the header
        self._headers = self._generate_headers()
        self._length_headers = len(self._headers)

    def _generate_headers(self) -> list[str]:
        """Generate header of output"""
        if self._time_in_header:
            return ['Time'] + self._all_data_names
        else:
            return self._all_data_names

    @abstractmethod
    def _check_data_length(self, data: list) -> bool:
        """Check if the data length same as the length of data names, must be implemented if output has headers"""
        pass

    @abstractmethod
    def log_data(self, row: list) -> bool:
        """
        Log data to output, this method will be used in DataLogger
        Return True for successful logging, False for failed logging
        """
        pass

    @staticmethod
    def generate_dir_of_file(file_name: str):
        """Generate a directory to save file if it does not exist"""
        dir_path = os.path.dirname(file_name)
        if not os.path.exists(dir_path):
            logger.info(f"Generating dir '{dir_path}' ...")
            os.makedirs(dir_path)

    @property
    def time_in_header(self):
        return self._time_in_header


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
            time_in_header: bool = True,
            csv_writer_settings: dict[str: str] | None = None
    ):
        """
        Initialize data output instance for csv data
        :param file_name: File name of csv data with full path
        :param all_data_names: Data names from all sources
        :param time_in_header: Set 'Time' to the first column of headers
        :param csv_writer_settings: Settings of output, supported 'delimiter'
        """
        logger.info("Initializing DataOutputCsv ...")

        super().__init__(all_data_names=all_data_names, time_in_header=time_in_header)
        self.file_name = file_name
        self.generate_dir_of_file(self.file_name)  # Generate file path

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
                    raise ValueError(f"Invalid key in csv writer settings '{key}'")
            # Update csv_writer_settings
            self.csv_writer_settings.update(csv_writer_settings)
            logger.info(f"Using csv writer settings: {self.csv_writer_settings}")

        # Write headers to csv
        self._write_csv_header(self._headers)

    def log_data(self, row: list) -> bool:
        """Log data to csv"""
        # Check data length
        if self._check_data_length(row):
            self._append_to_csv(row)  # Append data to csv
            return True
        else:
            logger.error(f"Mismatched data length = {len(row)} and length of headers = {self._length_headers}, "
                         f"skipping logging ...")
            return False

    def _check_data_length(self, data: list) -> bool:
        """Check if the data length same as the length of data names"""
        return len(data) == self._length_headers

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


class DataLoggerBase:
    def __init__(
            self,
            data_sources_mapping: dict[str: DataSourceBase],
            data_outputs_mapping: dict[str: DataOutputBase],
    ):
        """
        Initialize data logger instance

        The format of data_sources_mapping is as follows:
        {
            '<sou1_name>': instance1 of class 'DataSourceBase' or child class,
            '<sou2_name>': instance2 of class 'DataSourceBase' or child class,
        }

        The format of data_outputs_mapping is as follows:
        {
            '<output1_name>': instance1 of class 'DataOutputBase' or child class,
            '<output2_name>': instance2 of class 'DataOutputBase' or child class,
        }

        :param data_sources_mapping: Mapping of multiple data sources
        :param data_outputs_mapping: Mapping of multiple data outputs
        """
        logger.info("Initializing DataLoggerBase ...")

        self.data_sources_mapping = data_sources_mapping
        self.data_outputs_mapping = data_outputs_mapping

        # Extract all data sources to a list (of instance(s))
        self.data_sources = list(self.data_sources_mapping.values())

        # Extract all data outputs to a list (of instance(s))
        self.data_outputs = list(self.data_outputs_mapping.values())

    def run_data_logging(
            self,
            interval: int | float,
            duration: int | float | None,
    ):
        """
        Run data logging
        :param interval: Log interval in second
        :param duration: Log duration in second, if None, the duration is infinite
        """
        # Check the input
        if interval <= 0:
            raise ValueError(f"Logging interval '{interval}' should be greater than 0")
        if duration is not None:
            if duration <= 0:
                raise ValueError(f"Logging duration '{duration}' should be 'None' or a value greater than 0")

        # Init time values
        start_time = time.time()
        end_time = None if duration is None else start_time + duration
        next_log_time = start_time  # Init next logging time
        log_count = 0  # Init count of logging

        logger.info(f"Starting data logging at time {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        if end_time is None:
            logger.info("Estimated end time: infinite")
        else:
            logger.info(f"Estimated end time {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")

        # Logging data
        try:
            while end_time is None or time.time() < end_time:
                # Update next logging time
                next_log_time += interval

                # Get timestamp and data from all sources, using 'read_data()' method
                timestamp = self.get_timestamp_now()
                data = [v for source in self.data_sources for v in source.read_data()]

                # Log count
                log_count += 1  # Update log counter
                print(f"Logging count(s): {log_count}")  # Print log counter to console

                # Log data to each output
                for data_output in self.data_outputs:
                    if data_output.time_in_header:
                        # Add timestamp to data
                        data_to_log = self._add_timestamp_to_data(timestamp, data)  # Add timestamp to data
                    else:
                        data_to_log = data
                    # Log data, using 'log_data()' method
                    logger.debug(f"Logging data: {data_to_log} to {data_output}")
                    data_output.log_data(data_to_log)  # Log to all outputs

                # Calculate the time to sleep to maintain the interval
                sleep_time = next_log_time - time.time()
                if sleep_time > 0:
                    logger.debug(f"sleep_time = {sleep_time}")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"sleep_time = {sleep_time} is negative")

            # Finish data logging
            logger.info("Data logging completed")
        except KeyboardInterrupt:
            logger.warning("Data logging stopped manually")

    @staticmethod
    def get_timestamp_now() -> str:
        """Get the timestamp by now"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    @staticmethod
    def _add_timestamp_to_data(timestamp: str, data: list) -> list:
        """Add timestamp to data"""
        return [timestamp] + data


if __name__ == "__main__":
    data_source_1 = RandomDataSource()
    data_source_2 = RandomStringSource()
    data_output_1 = DataOutputCsv(
        file_name='Test/csv_logger_1.csv',
        all_data_names=data_source_1.all_data_names + data_source_2.all_data_names,
        time_in_header=True,
    )
    data_output_2 = DataOutputCsv(
        file_name='Test/csv_logger_2.csv',
        all_data_names=data_source_1.all_data_names + data_source_2.all_data_names,
        time_in_header=False,
        csv_writer_settings={'delimiter': '\t'}
    )

    test_logger = DataLoggerBase(
        data_sources_mapping={
            'Sou1': data_source_1,
            'Sou2': data_source_2,
        },
        data_outputs_mapping={
            'Log1': data_output_1,
            'Log2': data_output_2,
        }
    )
    test_logger.run_data_logging(
        interval=2,
        duration=10
    )
