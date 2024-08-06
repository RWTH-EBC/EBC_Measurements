"""
Base module: DataLogger, incl. DataSource, DataOutput
"""

from abc import ABC, abstractmethod
from typing import TypedDict
import csv
import time
import os
import random
import logging
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger()


class DataSourceBase:
    @abstractmethod
    def read_data(self) -> list:
        """Read data from source, this method will be used in DataLogger"""
        pass


class RandomDataSource(DataSourceBase):
    """Random data source to simulate data generation"""
    def read_data(self) -> list:
        return [random.uniform(-10.0, 10.0), random.uniform(0.0, 100.0)]


class RandomStringSource(DataSourceBase):
    """Random string source to simulate data generation"""
    def read_data(self) -> list:
        def generate_random_string(length=5):
            """Generate random string with defined length"""
            return ''.join(random.choice(['A', 'a', 'B', 'b', 'C', 'c', 'D', 'd']) for _ in range(length))

        return [None, generate_random_string(5)]


class DataOutputBase:
    @abstractmethod
    def log_data(self, row: list):
        """Log data to output, this method will be used in DataLogger"""
        pass

    @staticmethod
    def generate_dir_of_file(file_name: str):
        """Generate a directory to save file if it does not exist"""
        dir_path = os.path.dirname(file_name)
        if not os.path.exists(dir_path):
            logger.info(f"Generating dir '{dir_path}' ...")
            os.makedirs(dir_path)


class DataOutputCsv(DataOutputBase):
    class CsvWriterSettings(TypedDict):
        """Typed dict for csv writer settings"""
        delimiter: str

    # Class attribute: Default setting of csv writer
    _csv_writer_settings_default = {
        'delimiter': ';'  # Delimiter of csv-file
    }

    def __init__(self, file_name: str, csv_writer_settings: dict[str: str] | None = None):
        """
        Initialize data output instance for csv data
        :param file_name: Path with file name of csv data
        :param csv_writer_settings: Settings of csv writer, supported 'delimiter'
        """
        logger.info("Initializing DataOutputCsv ...")

        self.file_name = file_name
        self.generate_dir_of_file(self.file_name)  # Generate file path

        # Get default settings
        self.csv_writer_settings = self._csv_writer_settings_default.copy()  # Use copy() to avoid change cls attribute

        # Set csv_writer_settings
        if csv_writer_settings is None:
            # Use default settings
            logger.info(f"Using default csv writer settings: {self.csv_writer_settings}")
        else:
            # Check all keys in settings
            for key in csv_writer_settings.keys():
                if key not in self._csv_writer_settings_default.keys():
                    raise ValueError(f"Invalid csv writer setting key '{key}'")
            # Update settings
            self.csv_writer_settings.update(csv_writer_settings)
            logger.info(f"Using csv writer settings: {self.csv_writer_settings}")

    def write_csv_header(self, row: list):
        """Write header as the first row of csv"""
        self._write_to_csv(row)

    def log_data(self, row: list):
        """Log data to csv"""
        self._append_to_csv(row)

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
    class DataSourcesHeaders(TypedDict):
        """Typed dict for data sources and headers"""
        source: DataSourceBase
        headers: list[str]

    def __init__(
            self,
            data_sources_headers_mapping: dict[str: 'DataSourcesHeaders'],
            data_outputs_mapping: dict[str: DataOutputBase],
    ):
        """
        Initialize data logger instance

        The length of header from each data source should be same as the data length read from this source

        The format of data_sources_headers_mapping is as follows:
        {
            '<sou1_name>': {
                'source': instance1 of class 'DataSourceBase' or child class,
                'headers': [<var11_header>, <var12_header>, ...],
            },
            '<sou2_name>': {
                'source': instance2 of class 'DataSourceBase',
                'headers': [<var21_header>, <var22_header>, ...],
            },
        }

        The format of data_outputs_mapping is as follows:
        {
            '<output1_name>': instance1 of class 'DataOutputBase' or child class,
            '<output2_name>': instance2 of class 'DataOutputBase' or child class,
        }

        :param data_sources_headers_mapping: Mapping of multiple data sources and headers
        :param data_outputs_mapping: Mapping of multiple outputs
        """
        logger.info("Initializing DataLoggerBase ...")

        self.data_sources_headers_mapping = data_sources_headers_mapping
        self.data_outputs_mapping = data_outputs_mapping

        # Extract all data sources to a list (of instance(s))
        self.data_sources = [subdict['source'] for subdict in self.data_sources_headers_mapping.values()]

        # Extract all headers to a list (of str)
        self.data_headers = [header for subdict in self.data_sources_headers_mapping.values()
                             for header in subdict['headers']]
        self.data_headers_length = len(self.data_headers)  # Length of all data headers

        # Extract all data outputs to a list
        self.data_outputs = list(self.data_outputs_mapping.values())

        # Init outputs -> for new output format, add in this part
        for key, data_output in self.data_outputs_mapping.items():
            # Init csv output
            if isinstance(data_output, DataOutputCsv):
                _csv_header = ['Time'] + self.data_headers  # Generate csv header
                data_output.write_csv_header(_csv_header)  # Write header to csv
            else:
                pass

    def run_data_logging(
            self,
            interval: int | float,
            duration: int | float | None = None,
            add_timestamp: bool = True,
            check_data_length: bool = True
    ):
        """
        Run data logging
        :param interval: Log interval in second
        :param duration: Log duration in second, if None, the duration is infinite
        :param add_timestamp: Add timestamp to output data
        :param check_data_length: Default True, check if the read data length same as the length of headers, if the
            lengths are different, data will not be logged, it is recommended to set it as 'True'
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

                # Check data length if activated
                if check_data_length:
                    valid_data_length = self._check_data_length(data)  # Check if data length same as headers
                else:
                    valid_data_length = True

                # Log data, using 'log_data()' method for each output
                if valid_data_length:
                    log_count += 1  # Update log counter
                    print(f"Logging count(s): {log_count}")  # Print log counter to console
                    # Add timestamp to data
                    if add_timestamp:
                        data_to_log = self._add_timestamp_to_data(timestamp, data)  # Add timestamp to data
                    else:
                        data_to_log = data
                    for data_output in self.data_outputs:
                        logger.debug(f"Logging data: {data_to_log} to {data_output}")
                        data_output.log_data(data_to_log)  # Log to all outputs
                else:
                    logger.error(f"Mismatched data length = {len(data)} with headers length = {len(self.data_headers)}")

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

    def _check_data_length(self, data: list) -> bool:
        """Check if the data length same as the length of headers"""
        return len(data) == len(self.data_headers)

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
    data_output_1 = DataOutputCsv(file_name=os.path.join('Test', 'csv_logger_1.csv'))
    data_output_2 = DataOutputCsv(
        file_name=os.path.join('Test', 'csv_logger_2.csv'),
        csv_writer_settings={'delimiter': '\t'}
    )

    test_logger = DataLoggerBase(
        data_sources_headers_mapping={
            'Sou1': {
                'source': data_source_1,
                'headers': ['value_11', 'value_12']
            },
            'Sou2': {
                'source': data_source_2,
                'headers': ['value_21', 'value_22']
            },
        },
        data_outputs_mapping={
            'Log1': data_output_1,
            'Log2': data_output_2
        }
    )
    test_logger.run_data_logging(
        interval=2,
        duration=10,
        check_data_length=True
    )
