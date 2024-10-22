"""
Module IcpdasDataSourceOutput: Interface of ICP DAS to DataLogger
"""
from ebcmeasurements.Base import DataSourceOutput, Auxiliary
from ebcmeasurements.Icpdas import IoBase, IoSeries_I87K
import re
import os
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class IcpdasDataSourceOutput(DataSourceOutput.DataSourceOutputBase):
    class IcpdasDataSource(DataSourceOutput.DataSourceOutputBase.SystemDataSource):
        """I/O Series implementation of nested class SystemDataSource"""
        def __init__(self, system: tuple[IoBase.EthernetIoModule, ...]):
            logger.info("Initializing IcpdasDataSource ...")
            super().__init__(system)
            # Set variable names as 'Mo<slot index>Ch<channel index>'
            self._all_variable_names = tuple(f'Mo{m.slot_idx}Ch{ch}' for m in self.system for ch in range(m.io_channel))

        def read_data(self) -> dict:
            data = {}
            # Read data by looping each module
            for m in self.system:
                # Read data for all channels, return is {'Ch0': <value0>, 'Ch1': <value1>, ...}
                module_data = m.read_analog_input_all_channels()
                data.update({f'Mo{m.slot_idx}{k}': v for k, v in module_data.items()})
            return data

    class IcpdasDataOutput(DataSourceOutput.DataSourceOutputBase.SystemDataOutput):
        """I/O Series implementation of nested class SystemDataOutput"""
        def __init__(self, system: tuple[IoBase.EthernetIoModule, ...]):
            logger.info("Initializing IcpdasDataOutput ...")
            super().__init__(system, log_time_required=False)  # No requires of log time
            # Set variable names as 'Mo<slot index>Ch<channel index>'
            self._all_variable_names = tuple(f'Mo{m.slot_idx}Ch{ch}' for m in self.system for ch in range(m.io_channel))
            # Generate a module map to facilitate data output
            self._module_map = {f'Mo{m.slot_idx}': m for m in self.system}

        def log_data(self, data: dict):
            """Log data"""
            if not data:
                logger.debug("No keys available in data, skipping logging ...")
                return

            data_cleaned = self.clean_keys_with_none_values(data)  # Clean none values
            if not data_cleaned:
                logger.info("No more keys after cleaning the data, skipping logging ...")
                return

            # Loop to output data
            for k, v in data.items():
                # Match the module and channel index
                match_res = re.search(pattern=r'Mo(\d+)Ch(\d+)', string=k)
                if not match_res:
                    logger.warning(f"No match for key {k}, it should be in format 'Mo<slot index>Ch<channel index>'")
                    continue

                mo, ch = match_res.groups()
                module_key = f'Mo{mo}'
                if module_key in self._module_map:
                    self._module_map[module_key].output_analog_value_specified_channel(channel=int(ch), data=v)
                else:
                    logger.warning(f"I/O module '{module_key}' is not available as output module")

    def __init__(
            self,
            host: str,
            port: int,
            time_out: float = 0.5,
            io_series: str = 'I-87K',
            output_dir: str | None = None,
            ignore_slots_idx: list[int] = None
    ):
        """
        Initialization of IcpdasDataSourceOutput

        :param host: Host address of I/O unit
        :param port: Port of I/O unit
        :param time_out: Timeout in seconds
        :param io_series: I/O series name, the current version only supports 'I-87K'
        :param output_dir: Output directory to save initialization information
        :param ignore_slots_idx: List of slot indices to be ignored by reading or writing data
        """
        logger.info(f"Initializing IcpdasDataSourceOutput ...")
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.ignore_slots_idx = ignore_slots_idx
        self._all_configs = {}  # Configurations of I/O unit and all I/O modules

        # Create output dir if it is not None
        if self.output_dir is None:
            logger.info(f"No output dir set, initialization information will not be saved")
        else:
            logger.info(f"Initialization information will be saved to: {self.output_dir}")
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

        # Init and connect I/O unit
        super().__init__()
        if io_series == 'I-87K':
            self.io_unit = IoSeries_I87K.IoUnit(self.host, self.port, time_out)  # Init I/O Unit of I-87K
        else:
            raise AttributeError(f"Not supported I/O series: {io_series}")

        # Init all I/O modules
        self.io_modules = self._init_all_modules()

        # Define the attribute 'system'
        self.system = {'I/O unit': self.io_unit, 'I/O modules': self.io_modules}

        # Update configuration info
        self._all_configs.update({'I/O unit': self._get_unit_configuration()})
        self._all_configs.update({
            f'I/O module {m.slot_idx}': self._get_module_configuration(m)
            for m in self.io_modules
        })
        logger.info(f"Configurations of I/O unit and modules: {self._all_configs}")

        # Save configuration to file
        if self.output_dir is not None:
            _file_path = os.path.join(self.output_dir, f'Config_{self.host}.json')
            logger.info(f"Saving configurations of unit with host {self.host} to: {_file_path} ...")
            Auxiliary.dump_json(self._all_configs, _file_path)

    def _init_all_modules(self) -> tuple:
        """Initialize all I/O modules"""
        modules = []
        if isinstance(self.io_unit, IoSeries_I87K.IoUnit):
            # Init for I/O modules of I/O series I-87K
            for slot in range(self.io_unit.io_slot):
                # Get the module name
                address_id = slot + 2  # For ET-87PX series, the slot 0 has address ID of 2
                module_name_rsp = self.io_unit.read_module_name(address_id)  # Get the module name response
                if module_name_rsp is None:
                    # None response due to time out or empty slot
                    continue  # Skip this slot
                else:
                    module_name = module_name_rsp['module_name']
                # Determine the class based on the module name
                cls = IoSeries_I87K.IO_MODULE_MAP[module_name]['cls']
                # Implement the class
                module = cls(io_unit=self.io_unit, address_id=address_id)
                # Append module to the list
                modules.append(module)
            return tuple(modules)
        else:
            raise AttributeError(f"Not supported I/O system: {self.io_unit}")

    def _get_unit_configuration(self) -> dict[str, str | int]:
        """Get the configuration of the I/O unit"""
        return {
            'host': self.io_unit.host,
            'port': self.io_unit.port,
            'address_id': self.io_unit.address_id,
            'name': self.io_unit.name,
            'io_slot': self.io_unit.io_slot,
            'firmware_version': self.io_unit.read_firmware_version(self.io_unit.address_id).get('firmware_version'),
        }

    def _get_module_configuration(self, io_module: IoBase.EthernetIoModule) -> dict[str, str | int]:
        """Get the configuration of an I/O module"""
        config = {
            'address_id': io_module.address_id,
            'slot_idx': io_module.slot_idx,
            'name': self.io_unit.read_module_name(io_module.address_id).get('module_name'),
            'io_type': io_module.io_type,
            'io_channel': io_module.io_channel,
            'firmware_version': self.io_unit.read_firmware_version(io_module.address_id).get('firmware_version'),
        }
        # Update with I/O module status (multiple keys)
        config.update(io_module.read_configuration_status())
        return config

    @property
    def all_configs(self) -> dict[str, dict[str, str | None]]:
        return self._all_configs

    @property
    def data_source(self) -> 'IcpdasDataSourceOutput.IcpdasDataSource':
        """Instance of IcpdasDataSource, initialized on first access"""
        if self._data_source is None:
            # Choose modules with types of 'DI' or 'AI' as data source
            if self.ignore_slots_idx is None:
                system = tuple(m for m in self.io_modules if m.io_type in ['DI', 'AI'])
            else:
                system = tuple(
                    m for m in self.io_modules if m.io_type in ['DI', 'AI'] and m.slot_idx not in self.ignore_slots_idx)
            # Check if system contains any modules
            if not system:
                raise ValueError("No input modules available, unable to initialize data source")
            else:
                # Lazy initialization with properties
                self._data_source = self.IcpdasDataSource(system=system)
        return self._data_source

    @property
    def data_output(self) -> 'IcpdasDataSourceOutput.IcpdasDataOutput':
        """Instance of IcpdasDataOutput, initialized on first access"""
        if self._data_output is None:
            # Choose modules with types of 'DO' or 'AO' as data output
            if self.ignore_slots_idx is None:
                system = tuple(m for m in self.io_modules if m.io_type in ['DO', 'AO'])
            else:
                system = tuple(
                    m for m in self.io_modules if m.io_type in ['DO', 'AO'] and m.slot_idx not in self.ignore_slots_idx)
            # Check if system contains any modules
            if not system:
                raise ValueError("No output modules available, unable to initialize data output")
            else:
                # Lazy initialization with properties
                self._data_output = self.IcpdasDataOutput(system=system)
        return self._data_output
