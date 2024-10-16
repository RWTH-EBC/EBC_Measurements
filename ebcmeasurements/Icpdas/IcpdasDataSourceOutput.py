"""
Module IcpdasDataSourceOutput: Interface of ICP DAS to DataLogger
"""
from ebcmeasurements.Base import DataSourceOutput, Auxiliary
from ebcmeasurements.Icpdas import IoBase, IoSeries_I87K
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
            self._all_variable_names = tuple(f'Mo{m.slot_idx}Ch{ch}' for m in self.system for ch in range(m.io_channel))

        def read_data(self) -> dict:
            data = {}
            for m in self.system:
                slot_idx = m.slot_idx
                module_data = m.read_analog_input_all_channels()  # Read data for all channels
                data.update({
                    f'Mo{slot_idx}{k}': v
                    for k, v in module_data.items()
                })
            return data



    class IcpdasDataOutput(DataSourceOutput.DataSourceOutputBase.SystemDataOutput):
        """I/O Series implementation of nested class SystemDataOutput"""
        def __init__(self, system: IoSeries_I87K.IoUnit, all_output_slots: tuple[str, ...]):
            logger.info("Initializing IcpdasDataOutput ...")
            super().__init__(system, log_time_required=False)  # No requires of log time
            self._all_variable_names = None

        def log_data(self, data: dict):
            """Log data"""
            if data:
                data_cleaned = self.clean_keys_with_none_values(data)  # Clean none values
                if data_cleaned:
                    pass  # TODO: Finish
                else:
                    logger.info("No more keys after cleaning the data, skipping logging ...")
            else:
                logger.debug("No keys available in data, skipping logging ...")

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
        """
        logger.info(f"Initializing IcpdasDataSourceOutput ...")
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.ignore_slots_idx = ignore_slots_idx
        self.all_configs = {}  # Configurations of I/O unit and all I/O modules

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
            self.io_unit = IoSeries_I87K.IoUnit(self.host, self.port, time_out)  # I/O Unit of I-87K
        else:
            raise AttributeError(f"Not supported I/O series: {io_series}")
        # Update configuration info
        self.all_configs.update({'I/O unit': self._get_unit_configuration()})  # Get I/O unit configuration
        self.all_configs.update(self._get_modules_configuration())  # Get I/O modules configuration

        # Init all I/O modules
        self.io_modules = self._init_all_modules()
        self._get_modules_status()  # Get I/O modules status

        logger.info(f"Configurations of I/O unit and modules: {self.all_configs}")

        # Save configuration to file
        if self.output_dir is not None:
            _file_path = os.path.join(self.output_dir, f'Config_{self.host}.json')
            logger.info(f"Saving configurations of unit with host {self.host} to: {_file_path} ...")
            Auxiliary.dump_json(self.all_configs, _file_path)


    def _get_unit_configuration(self) -> dict[str, str | int]:
        """Get the configuration of the I/O unit"""
        name = self.io_unit.read_module_name(1).get('module_name')
        return {
            'address_id': self.io_unit.read_module_name(1).get('address_id'),
            'name': name,
            'firmware_version': self.io_unit.read_firmware_version(1).get('firmware_version'),
            'slot': self.io_unit.specifications[name]['io_slot']
        }

    def _get_modules_configuration(self) -> dict[str, dict[str, str | None]]:
        """Get the configuration of all I/O modules"""
        if isinstance(self.io_unit, IoSeries_I87K.IoUnit):
            adr_slot_offset = 2  # Offset between address ID and slot index: Slot index 0 -> Address ID 2
            return {
                f'I/O module {slot}': {
                    'address_id': self.io_unit.read_module_name(slot + adr_slot_offset).get('address_id'),
                    'name': self.io_unit.read_module_name(slot + adr_slot_offset).get('module_name'),
                    'firmware_version': self.io_unit.read_firmware_version(slot + adr_slot_offset).get(
                        'firmware_version'),
                }
                for slot in range(self.all_configs['I/O unit']['slot'])
            }
        else:
            raise AttributeError(f"Not supported I/O system: {self.io_unit}")

    def _init_all_modules(self) -> tuple:
        """Initialize all I/O modules"""
        modules = []
        if isinstance(self.io_unit, IoSeries_I87K.IoUnit):
            for slot in range(self.all_configs['I/O unit']['slot']):
                # Determine the class
                cls = IoSeries_I87K.IO_MODULE_MAP[self.all_configs[f'I/O module {slot}']['name']]['cls']
                # Implement the class
                module = cls(io_unit=self.io_unit, address_id=self.all_configs[f'I/O module {slot}']['address_id'])
                # Update config to all_configs
                self.all_configs[f'I/O module {slot}'].update({
                    'slot_idx': module.slot_idx,
                    'io_type': module.io_type,
                    'io_channel': module.io_channel,
                })
                modules.append(module)
            return tuple(modules)
        else:
            raise AttributeError(f"Not supported I/O system: {self.io_unit}")

    def _get_modules_status(self):
        """Get the configuration status of all I/O modules"""
        for slot in range(self.all_configs['I/O unit']['slot']):
            status = self.io_modules[slot].read_configuration_status()
            if status is not None:
                self.all_configs[f'I/O module {slot}'].update(status)

    @property
    def data_source(self) -> 'IcpdasDataSourceOutput.IcpdasDataSource':
        """Instance of IcpdasDataSource, initialized on first access"""
        if self._data_source is None:
            if self.ignore_slots_idx is None:
                system = tuple(m for m in self.io_modules if m.io_type in ['DI', 'AI'])
            else:
                system = tuple(
                    m for m in self.io_modules if m.io_type in ['DI', 'AI'] and m.slot_idx not in self.ignore_slots_idx)
            if not system:
                raise ValueError("No input modules available, unable to initialize data source")
            else:
                # Lazy initialization with properties
                self._data_source = self.IcpdasDataSource(system=system)
        return self._data_source


if __name__ == "__main__":
    test = IcpdasDataSourceOutput(
        host='134.130.40.39', port=9999, output_dir='Test')

    print(test.data_source.all_variable_names)
    print(test.data_source.read_data())
