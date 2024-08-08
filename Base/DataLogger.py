"""
Base module: DataLogger, incl. ABC of DataSource and DataOutput
"""

from Base import DataSource, DataOutput
from abc import ABC, abstractmethod
import time
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('DataLogger')


class DataLoggerBase(ABC):
    def __init__(
            self,
            data_sources_mapping: dict[str: DataSource.DataSourceBase],
            data_outputs_mapping: dict[str: DataOutput.DataOutputBase],
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
        self._data_sources_mapping = data_sources_mapping
        self._data_outputs_mapping = data_outputs_mapping

        # Extract all data sources to a list (of instance(s))
        self._data_sources = list(self._data_sources_mapping.values())

        # Extract all data outputs to a list (of instance(s))
        self._data_outputs = list(self._data_outputs_mapping.values())

    @abstractmethod
    def run_data_logging(self, **kwargs):
        """Run data logging"""
        pass

    @property
    def data_sources_mapping(self) -> dict:
        return self._data_sources_mapping

    @property
    def data_sources(self) -> list:
        return self._data_sources

    @property
    def data_outputs_mapping(self) -> dict:
        return self._data_outputs_mapping

    @property
    def data_outputs(self) -> list:
        return self._data_outputs


class DataLoggerTimeTrigger(DataLoggerBase):
    def __init__(
            self,
            data_sources_mapping: dict[str: DataSource.DataSourceBase],
            data_outputs_mapping: dict[str: DataOutput.DataOutputBase],
    ):
        logger.info("Initializing DataLoggerTimeTrigger ...")
        super().__init__(data_sources_mapping, data_outputs_mapping)

    def run_data_logging(self, interval: int | float, duration: int | float | None):
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
                row = [v for source in self._data_sources for v in source.read_data()]

                # Log count
                log_count += 1  # Update log counter
                print(f"Logging count(s): {log_count}")  # Print log counter to console

                # Log data to each output
                for data_output in self._data_outputs:
                    if hasattr(data_output, 'data_output'):
                        data_output = data_output.data_output  # For instance with nested class, e.g. Beckhoff
                    if data_output.time_in_header:
                        # Add timestamp to data
                        row_to_log = self._add_timestamp_to_row(timestamp, row)  # Add timestamp to data
                    else:
                        row_to_log = row
                    # Log data, using 'log_data()' method
                    logger.debug(f"Logging data: {row_to_log} to {data_output}")
                    data_output.check_and_log_data(row_to_log)  # Log to all outputs

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
    def _add_timestamp_to_row(timestamp: str, row: list) -> list:
        """Add timestamp to row"""
        return [timestamp] + row


if __name__ == "__main__":
    data_source_1 = DataSource.RandomDataSource(size=5, missing_rate=0.5)
    data_source_2 = DataSource.RandomStringSource(size=5, str_length=5)
    data_output_1 = DataOutput.DataOutputCsv(file_name='Test/csv_logger_1.csv',
                                             all_variable_names=data_source_1.all_variable_names + data_source_2.all_variable_names)
    data_output_2 = DataOutput.DataOutputCsv(file_name='Test/csv_logger_2.csv',
                                             all_variable_names=data_source_1.all_variable_names + data_source_2.all_variable_names,
                                             csv_writer_settings={'delimiter': '\t'})

    test_logger = DataLoggerTimeTrigger(
        data_sources_mapping={
            'Sou1': data_source_1,
            'Sou2': data_source_2,
        },
        data_outputs_mapping={
            'Log1': data_output_1,
            'Log2': data_output_2,
        }
    )
    print(test_logger.data_sources)
    print(test_logger.data_outputs)
    test_logger.run_data_logging(
        interval=2,
        duration=10
    )
