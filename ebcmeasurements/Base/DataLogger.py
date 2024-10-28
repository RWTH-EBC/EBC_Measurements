"""
Base module: DataLogger, incl. ABC of DataSource and DataOutput
"""

from ebcmeasurements.Base import DataSource, DataOutput, DataSourceOutput
from abc import ABC, abstractmethod
import time
import logging
# Load logging configuration from file
logger = logging.getLogger(__name__)


class DataLoggerBase(ABC):
    def __init__(
            self,
            data_sources_mapping: dict[str, DataSource.DataSourceBase | DataSourceOutput.DataSourceOutputBase],
            data_outputs_mapping: dict[str, DataOutput.DataOutputBase | DataSourceOutput.DataSourceOutputBase],
            data_rename_mapping: dict[str, dict[str, dict[str, str]]] | None = None,
            **kwargs
    ):
        """
        Initialize data logger instance

        The format of data_sources_mapping is as follows:
        {
            '<source1_name>': <instance1 of DataSource>,
            '<source2_name>': <instance2 of DataSource>,
            ...
        }

        The format of data_outputs_mapping is as follows:
        {
            '<output1_name>': <instance1 of class DataOutput>,
            '<output2_name>': <instance2 of class DataOutput>,
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
        :param kwargs:
            'data_rename_mapping_explicit': bool: Default is False, if set True, all variable keys in rename mapping
            will be checked, if they are available in data source
            'auto_prefix_for_duplicate': bool: Default is True, if set True, all variable names will be prefixed with
            data source name and delimiter if any duplicates of variable names are detected in an output after renaming
            'auto_prefix_delimiter': str: Default is ':', delimiter between data source name and variable name
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
        if data_rename_mapping is not None:
            self._check_data_rename_mapping_input(
                data_rename_mapping=data_rename_mapping,
                explicit=kwargs.get('data_rename_mapping_explicit', False)
            )

        # Init the data rename mapping
        self._data_rename_mapping = self._init_data_rename_mapping(
            data_rename_mapping=data_rename_mapping if data_rename_mapping is not None else {},
        )

        # Find duplicates of variable names after renaming for all data outputs
        self._all_duplicates = self._get_duplicates_in_data_rename_mapping(
            data_rename_mapping=self._data_rename_mapping)

        # Auto prefix for duplicate variable names
        if self._all_duplicates:
            for do_name, dup in self._all_duplicates.items():
                logger.info(
                    f"With the current data rename mapping, duplicates of variable names are detected in data output "
                    f"'{do_name}' with following same variable names from data sources: {dup}")
            if kwargs.get('auto_prefix_for_duplicate', True):
                logger.info(f'Auto-prefixing for duplicate variable names ...')
                self._data_rename_mapping = self._prefix_data_rename_mapping(
                    all_duplicates=self._all_duplicates, delimiter=kwargs.get('auto_prefix_delimiter', ':')
                )
            else:
                logger.warning(
                    f'Auto-prefixing for duplicate variable names deactivated, this may cause error by data logging!')

        # All variable names from all data sources, this will be set to DataOutput
        self._all_variable_names_dict = {
            ds_name: {
                do_name: tuple(mapping.values()) for do_name, mapping in output_dict.items()
            }
            for ds_name, output_dict in self._data_rename_mapping.items()
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

        # Count of logging
        self.log_count = 0

    def _check_data_rename_mapping_input(self, data_rename_mapping: dict, explicit: bool):
        """Check input dict of data rename mapping"""
        def _check_data_source_name(data_source_name):
            """Check if data source name available in data sources"""
            if data_source_name not in self._data_sources_mapping.keys():
                raise ValueError(f"Invalid data source name '{data_source_name}' for rename mapping")

        def _check_data_output_name(data_output_name):
            """Check if data output name available in data outputs"""
            if data_output_name not in self._data_outputs_mapping.keys():
                raise ValueError(f"Invalid data output name '{data_output_name}' for rename mapping")

        def _explicit_check_rename_mapping(data_source_name, rename_mapping):
            """Explicit check if all keys in the rename mapping are available in data source"""
            for key in rename_mapping.keys():
                if key not in self._data_sources_mapping[data_source_name].all_variable_names:
                    raise ValueError(
                        f"Explicit rename mapping check activated: Variable '{key}' not available in data source "
                        f"'{data_source_name}'"
                    )

        # Check data source, data output, and mapping
        for ds_name, output_dict in data_rename_mapping.items():
            _check_data_source_name(ds_name)
            for do_name, mapping in output_dict.items():
                _check_data_output_name(do_name)
                if explicit:
                    _explicit_check_rename_mapping(ds_name, mapping)

    def _init_data_rename_mapping(self, data_rename_mapping: dict) -> dict[str, dict[str, dict[str, str]]]:
        """Init data rename mapping for all data sources to all data outputs, if the rename mapping for a variable
        name is unavailable, use its original name in the data source, the return has the same structure as
        data_rename_mapping"""
        return {
            ds_name: {
                do_name: {
                    var: data_rename_mapping.get(ds_name, {}).get(do_name, {}).get(var, var)
                    for var in ds.all_variable_names
                }
                for do_name in self._data_outputs_mapping.keys()
            }
            for ds_name, ds in self._data_sources_mapping.items()
        }

    def _get_duplicates_in_data_rename_mapping(self, data_rename_mapping: dict) -> dict[str, dict[str, list[str]]]:
        """Get duplicates in data rename mapping for all data outputs"""
        all_duplicates = {}  # {<do_name1>: {<var_rename1>: [<ds_name1>, <ds_name2>, ...], ...}, ...}

        # Search duplicates for each data output
        for do_name in self._data_outputs_mapping.keys():
            var_locs = {}
            for ds_name, output_dict in data_rename_mapping.items():
                for var in output_dict[do_name].values():
                    if var not in var_locs:
                        var_locs[var] = []  # Init an empty list to save locations for this key
                    var_locs[var].append(ds_name)
            # Duplicates for this data output
            do_duplicates = {key: locs for key, locs in var_locs.items() if len(locs) > 1}
            # Save duplicates to overall dict
            if do_duplicates:
                all_duplicates[do_name] = do_duplicates

        return all_duplicates

    def _prefix_data_rename_mapping(
            self,
            all_duplicates: dict[str, dict[str, list[str]]],
            delimiter: str = ':'
    ) -> dict[str, dict[str, dict[str, str]]]:
        """Prefix the data rename mapping for data outputs with duplicate in data sources"""
        return {
            ds_name: {
                do_name: {
                    # Keep the original do_name if no duplicates, else prefix with the ds_name
                    key: var if do_name not in all_duplicates.keys() else f'{ds_name}{delimiter}{var}'
                    for key, var in mapping.items()
                }
                for do_name, mapping in output_dict.items()
            }
            for ds_name, output_dict in self._data_rename_mapping.items()
        }

    def read_data_all_sources(self) -> dict[str, dict]:
        """Read data from all data sources"""
        return {
            ds_name: ds.read_data()
            for ds_name, ds in self._data_sources_mapping.items()
        }

    def log_data_all_outputs(self, data: dict[str, dict], timestamp: str = None):
        """Log data to all data outputs"""
        for do_name, do in self._data_outputs_mapping.items():
            # Unzip and rename key for the current output
            unzipped_data = {
                self._data_rename_mapping[ds_name][do_name][var]: value
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

    @staticmethod
    def get_timestamp_now() -> str:
        """Get the timestamp by now"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())


class DataLoggerTimeTrigger(DataLoggerBase):
    def __init__(
            self,
            data_sources_mapping: dict[str, DataSource.DataSourceBase | DataSourceOutput.DataSourceOutputBase],
            data_outputs_mapping: dict[str, DataOutput.DataOutputBase | DataSourceOutput.DataSourceOutputBase],
            data_rename_mapping: dict[str, dict[str, dict[str, str]]] | None = None,
            **kwargs
    ):
        """Time triggerd data logger"""
        logger.info("Initializing DataLoggerTimeTrigger ...")
        super().__init__(data_sources_mapping, data_outputs_mapping, data_rename_mapping, **kwargs)

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
                self.log_count += 1  # Update log counter
                print(f"TimeTrigger - {hex(id(self))} - Logging count(s): {self.log_count}")  # Print count to console

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
