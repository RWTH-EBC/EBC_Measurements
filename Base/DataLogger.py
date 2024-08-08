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
            data_source_prefix_delimiter: str | None = None
    ):
        """
        Initialize data logger instance

        The format of data_sources_mapping is as follows:
        {
            '<source1_name>': instance1 of DataSource,
            '<source2_name>': instance2 of DataSource,
        }

        The format of data_outputs_mapping is as follows:
        {
            '<output1_name>': instance1 of class DataOutput,
            '<output2_name>': instance2 of class DataOutput,
        }

        :param data_sources_mapping: Mapping of multiple data sources
        :param data_outputs_mapping: Mapping of multiple data outputs
        :param data_source_prefix_delimiter: If not None, source name will be added as prefix to variable names for
        each output, prefix separated from the variable name with the here defined delimiter
        """
        # Extract all data sources and outputs to dict (values as instance(s)), also for nested class, e.g. Beckhoff
        self._data_sources_mapping = {
            k: ds.data_source if hasattr(ds, 'data_source') else ds for k, ds in data_sources_mapping.items()
        }
        self._data_outputs_mapping = {
            k: do.data_output if hasattr(do, 'data_output') else do for k, do in data_outputs_mapping.items()
        }
        # Generate list of data sources and outputs
        self._data_sources = list(self._data_sources_mapping.values())
        self._data_outputs = list(self._data_outputs_mapping.values())

        self._data_source_prefix_delimiter = data_source_prefix_delimiter

        # All variable names from all data sources
        if self._data_source_prefix_delimiter is None:
            data_sources_all_variable_names = tuple(item for ds in self._data_sources for item in ds.all_variable_names)
        else:
            data_sources_all_variable_names = tuple(
                f'{k}{self._data_source_prefix_delimiter}{item}'
                for k, ds in self._data_sources_mapping.items()
                for item in ds.all_variable_names
            )

        # Set all_variable_names for each DataOutput
        for data_output in self._data_outputs:
            if data_output.log_time_required:
                data_output.all_variable_names = (data_output.key_of_log_time,) + data_sources_all_variable_names
            else:
                data_output.all_variable_names = data_sources_all_variable_names

        # Methods for DataOutput that must be initialed
        for data_output in self._data_outputs:
            # Csv output
            if isinstance(data_output, DataOutput.DataOutputCsv):
                data_output.write_header_line()
            else:
                pass

    def read_data_all_sources(self) -> dict:
        """Read data from all data sources"""
        if self._data_source_prefix_delimiter is None:
            return {k: v for ds in self._data_sources for k, v in ds.read_data().items()}
        else:
            return {
                f'{k_ds}{self._data_source_prefix_delimiter}{k}': v
                for k_ds, ds in self._data_sources_mapping.items()
                for k, v in ds.read_data().items()
            }

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
            data_source_prefix_delimiter: str | None = None
    ):
        """Time triggerd data logger"""
        logger.info("Initializing DataLoggerTimeTrigger ...")
        super().__init__(data_sources_mapping, data_outputs_mapping, data_source_prefix_delimiter)

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

                # Get timestamp
                timestamp = self.get_timestamp_now()

                # Get data from all sources
                data = self.read_data_all_sources()

                # Log count
                log_count += 1  # Update log counter
                print(f"Logging count(s): {log_count}")  # Print log counter to console

                # Log data to each output
                for data_output in self._data_outputs:
                    if data_output.log_time_required:
                        # Add timestamp to data
                        data[data_output.key_of_log_time] = timestamp  # Add timestamp to data
                    # Log data, using 'log_data()' method
                    logger.debug(f"Logging data: {data} to {data_output}")
                    data_output.log_data(data)  # Log to output

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


if __name__ == "__main__":
    # Init data sources
    data_source_1 = DataSource.RandomDataSource(size=5, key_missing_rate=0, value_missing_rate=0.5)
    data_source_2 = DataSource.RandomStringSource(size=5, str_length=5, key_missing_rate=0.5, value_missing_rate=0.5)

    # Init outputs
    data_output_1 = DataOutput.DataOutputCsv(file_name='Test/csv_logger_1.csv')
    data_output_2 = DataOutput.DataOutputCsv(file_name='Test/csv_logger_2.csv', csv_writer_settings={'delimiter': '\t'})

    data_logger = DataLoggerTimeTrigger(
        data_sources_mapping={
            'Sou1': data_source_1,
            'Sou2': data_source_2,
        },
        data_outputs_mapping={
            'Log1': data_output_1,
            'Log2': data_output_2,
        },
        data_source_prefix_delimiter='_'
    )
    print(f"Data source(s): {data_logger.data_sources}")
    print(f"Data output(s): {data_logger.data_outputs}")
    data_logger.run_data_logging(
        interval=2,
        duration=10
    )
