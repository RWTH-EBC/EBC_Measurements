"""
Base module: DataLogger, incl. ABC of DataSource and DataOutput
"""

from Base import DataSource, DataOutput, DataSourceOutput
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
            data_rename_mapping: dict[str: dict[str: dict[str: str]]] | None = None,
    ):
        """
        Initialize data logger instance

        The format of data_sources_mapping is as follows:
        {
            '<source1_name>': instance1 of DataSource,
            '<source2_name>': instance2 of DataSource,
            ...
        }

        The format of data_outputs_mapping is as follows:
        {
            '<output1_name>': instance1 of class DataOutput,
            '<output2_name>': instance2 of class DataOutput,
            ...
        }

        The format of data_rename_mapping is as follows:
        {
            '<source1_name>': {
                <'output1_name'>: {
                    <variable_name_in_source1>: <new_variable_name_in_output1>,
                    ...
                },
                <'output2_name'>: {
                    <variable_name_in_source1>: <new_variable_name_in_output2>,
                    ...
                },
            },
            '<source2_name>': {
                <'output1_name'>: {
                    <variable_name_in_source2>: <new_variable_name_in_output1>,
                    ...
                },
                <'output2_name'>: {
                    <variable_name_in_source2>: <new_variable_name_in_output2>,
                    ...
                },
            },
            ...
        }

        :param data_sources_mapping: Mapping of multiple data sources
        :param data_outputs_mapping: Mapping of multiple data outputs
        :param data_rename_mapping: Mapping of rename for data sources and data outputs, None to use default names
            provided by data sources
        """
        # Extract all data sources and outputs to dict (values as instance(s)), also for nested class, e.g. Beckhoff
        self._data_sources_mapping = {
            k: ds.data_source if isinstance(ds, DataSourceOutput.DataSourceOutputBase) else ds
            for k, ds in data_sources_mapping.items()
        }
        self._data_outputs_mapping = {
            k: do.data_output if isinstance(do, DataSourceOutput.DataSourceOutputBase) else do
            for k, do in data_outputs_mapping.items()
        }

        # Check rename mapping of data sources and outputs
        if data_rename_mapping is None:
            self._data_rename_mapping = None
        else:
            # Check data source name
            for ds_name, output_dict in data_rename_mapping.items():
                if ds_name in self._data_sources_mapping.keys():
                    # Check data output name
                    for do_name, mapping in output_dict.items():
                        if do_name in self._data_outputs_mapping.keys():
                            # Check mapping keys
                            for key in mapping.keys():
                                if key not in self._data_sources_mapping[ds_name].all_variable_names:
                                    raise ValueError(f"Invalid variable name '{key}' for data source '{ds_name}' data "
                                                     f"output '{do_name}' for rename mapping")
                        else:
                            raise ValueError(f"Invalid data output name '{do_name}' for rename mapping")
                else:
                    raise ValueError(f"Invalid data source name '{ds_name}' for rename mapping")
            # Checking complete
            self._data_rename_mapping = data_rename_mapping
            logger.info(f"Data rename activated, using mapping: \n{self._data_rename_mapping}")

        # All variable names from all data sources, this will be set to DataOutput
        if self._data_rename_mapping is None:
            # Without rename
            self._all_variable_names_dict = {
                ds_name: {
                    do_name: tuple(ds.all_variable_names)  # Origin names without rename
                    for do_name in self._data_outputs_mapping.keys()
                }
                for ds_name, ds in self._data_sources_mapping.items()
            }
        else:
            # With rename
            self._all_variable_names_dict = {
                ds_name: {
                    do_name: tuple(
                        self._data_rename_mapping.get(ds_name, {}).get(do_name, {}).get(var, var)  # Rename
                        for var in ds.all_variable_names
                    )
                    for do_name in self._data_outputs_mapping.keys()
                }
                for ds_name, ds in self._data_sources_mapping.items()
            }

        # Set all_variable_names for each DataOutput
        for do_name, do in self._data_outputs_mapping.items():
            # Collect variable names from all data sources for the current output
            all_data_sources_all_variable_names = tuple(
                var_name
                for ds_name in self._data_sources_mapping.keys()
                for var_name in self._all_variable_names_dict[ds_name][do_name]
            )

            if do.log_time_required:
                # With key of log time
                do.all_variable_names = (do.key_of_log_time,) + all_data_sources_all_variable_names
            else:
                # Without key of log time, only all variable names
                do.all_variable_names = all_data_sources_all_variable_names

        # Additional methods for DataOutput that must be initialed
        for do in self._data_outputs_mapping.values():
            # Csv output
            if isinstance(do, DataOutput.DataOutputCsv):
                # Write csv header line
                do.write_header_line()
            else:
                pass

    def read_data_all_sources(self) -> dict[str: dict]:
        """Read data from all data sources"""
        return {
            ds_name: ds.read_data()
            for ds_name, ds in self._data_sources_mapping.items()
        }

    def log_data_all_outputs(self, data: dict[str: dict], timestamp: str = None):
        """Log data to all data outputs"""
        for do_name, do in self._data_outputs_mapping.items():
            # Unzip and rename key for the current output
            if self._data_rename_mapping is None:
                unzipped_data = {
                    var: value
                    for ds_name, ds_data in data.items()
                    for var, value in ds_data.items()
                }
            else:
                unzipped_data = {
                    self._data_rename_mapping.get(ds_name, {}).get(do_name, {}).get(var, var): value
                    for ds_name, ds_data in data.items()
                    for var, value in ds_data.items()
                }
            # Add log time as settings
            if do.log_time_required:
                # This data output requires log time
                if timestamp is None:
                    raise ValueError(f"The data output '{do}' requires timestamp but got None")
                else:
                    # Add timestamp to data
                    unzipped_data[do.key_of_log_time] = timestamp
            # Log data to this output
            logger.debug(f"Logging data: {unzipped_data} to {do}")
            do.log_data(unzipped_data)  # Log to output

    @abstractmethod
    def run_data_logging(self, **kwargs):
        """Run data logging"""
        pass

    @property
    def data_sources_mapping(self) -> dict:
        return self._data_sources_mapping

    @property
    def data_outputs_mapping(self) -> dict:
        return self._data_outputs_mapping


class DataLoggerTimeTrigger(DataLoggerBase):
    def __init__(
            self,
            data_sources_mapping: dict[str: DataSource.DataSourceBase],
            data_outputs_mapping: dict[str: DataOutput.DataOutputBase],
            data_rename_mapping: dict[str: dict[str: dict[str: str]]] | None = None,
    ):
        """Time triggerd data logger"""
        logger.info("Initializing DataLoggerTimeTrigger ...")
        super().__init__(data_sources_mapping, data_outputs_mapping, data_rename_mapping)

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
                self.log_data_all_outputs(data, timestamp)

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
        data_rename_mapping={
            'Sou1': {
                'Log1': {'RandData0': 'RandData0InLog1'},
                'Log2': {'RandData1': 'RandData1InLog2', 'RandData2': 'RandData2InLog2'},
            },
            'Sou2': {
                'Log2': {'RandStr0': 'RandStr000'},
            }
        }
    )
    print(f"Data sources mapping: {data_logger.data_sources_mapping}")
    print(f"Data outputs mapping: {data_logger.data_outputs_mapping}")
    data_logger.run_data_logging(
        interval=2,
        duration=10
    )
