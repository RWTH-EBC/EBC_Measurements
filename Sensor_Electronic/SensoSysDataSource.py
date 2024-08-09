"""
Module SensoSysDataSource: Interface to DataLogger
"""

from Base import DataSource, Auxiliary
from Sensor_Electronic import SensoSysDevices
from typing import TypedDict
from datetime import datetime
import os
import sys
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('SensoSys')


class SensoSysDataSource(DataSource.DataSourceBase):
    class SensoSysConfigs(TypedDict):
        """Typed dict for SensoSys configurations"""
        port: str
        scan_by_file: bool
        time_out: float

    def __init__(self, sensosys_config_file: str | None = None, output_dir: str | None = None):
        """
        Initialize SensoSysDataSource instance
        :param sensosys_config_file: Configure the SensoSys from a json file, if None, start a configuration guidance
            * Notice *: if configuration with file, the connection to COM port will not be checked
            The default configuration file is "_config/SensoSysConfigs_default.json" with following keys:
                "port":
                    COM port for connection
                "scan_by_file":
                    true - only scan devices with id listed in "_config/SensoSysAllDevicesIDs_default.json"
                    false - scan devices from id 0 to 255
                "time_out":
                    Time out for COM port reader, see class SensoSys
        :param output_dir: Output dir to save initialization config and found devices, if None, they will not be saved
        """
        logger.info("Initializing SensoSysDataSource ...")

        super().__init__()
        self.sensosys_config_file = sensosys_config_file
        self.output_dir = output_dir

        # Create output dir if it is not None
        if self.output_dir is None:
            logger.info(f"No output dir set, results of initialization will not be saved")
        else:
            logger.info(f"Results of initialization will be saved to: {self.output_dir}")
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

        # Default configurations
        self.sensosys_configs: 'SensoSysDataSource.SensoSysConfigs' = {
            'port': 'COM1',
            'scan_by_file': False,
            'time_out': 0.05,
        }

        # Scan available COM port(s)
        self.available_ports = self._scan_available_ports()

        # Update the SensoSys configurations
        if self.sensosys_config_file is None:
            # Configuration by guide
            _succeed = self._get_sensosys_configs_by_guide()
            assert _succeed  # Configuration by guide will always return True
            logger.info(f"Succeeded to update SensoSys configurations by user guide")
        else:
            # Configuration by file
            _succeed = self._get_sensosys_configs_by_file(self.sensosys_config_file)
            if _succeed:
                logger.info(f"Succeeded to update SensoSys configurations by file")
            else:
                logger.warning(f"Failed to update SensoSys configurations by file, using default values ...")

        # Init SensoSys
        logger.info(f"Initializing SensoSysDevices with configurations: {self.sensosys_configs}")
        self.sensosys = SensoSysDevices.SensoSys(
            port=self.sensosys_configs['port'],
            time_out=self.sensosys_configs['time_out'],
        )

        # Scan devices
        if self.sensosys_configs['scan_by_file']:
            self.sensosys_devices = self._scan_devices_by_file(file_name='_config/SensoSysAllDevicesIDs_default.json')
        else:
            self.sensosys_devices = self._scan_devices_by_ids()  # Scan by ip from 00 to FF
        logger.info(f"Found SensoSys devices: \n"
                    f"{self.sensosys_devices} \n"
                    f"Number of found devices: {len(self.sensosys_devices)}")

        # Possible quit after scanning
        if len(self.sensosys_devices) == 0:
            logger.error("No devices found, please check the connection, exiting ...")
            sys.exit(0)
        else:
            _continue = self._get_if_continue()
            if not _continue:
                logger.info("Exiting manually ...")
                sys.exit(0)

        # Save scan devices result to file
        if self.output_dir is not None:
            _file_path = os.path.join(self.output_dir, 'FoundDevices.json')
            logger.info(f"Saving found devices to: {_file_path} ...")
            Auxiliary.dump_json(self.sensosys_devices, _file_path)

        # Convert scanned devices to a list of [(id, name, sensor_config), ...] to simplify data reading
        self.sensosys_devices_list = [
            (k, v['instrument_name'], v.get('sensor_config')) for k, v in self.sensosys_devices.items()]

        # Set all_data_names
        self._all_variable_names = self._get_all_variable_names()

    def _get_sensosys_configs_by_guide(self) -> bool:
        """Get SensoSys configurations by a user guide"""
        logger.info("Configuring SensoSys by a user guide ...")
        # Set the COM port
        _pop_devmgmt = self._get_if_pop_system_device_management()  # Get if pop the system device management
        if _pop_devmgmt:
            SensoSysDevices.pop_system_device_management()
        self.sensosys_configs['port'] = self._get_port_name()  # Get the port name by user input
        _port_available = self._check_port_name()  # Check the input port
        if _port_available:
            logger.info(f"Set COM port to '{self.sensosys_configs['port']}'")
        else:
            logger.error(f"The COM port '{self.sensosys_configs['port']}' is unavailable, exiting ...")
            sys.exit(1)

        # Set scan by file
        self.sensosys_configs['scan_by_file'] = self._get_scan_by_file()  # Get the 'scan by file' value

        # Dump the configs to json file
        if self.output_dir is not None:
            _file_path = os.path.join(self.output_dir, 'SensoSysConfigs.json')
            logger.info(f"Saving configurations to: {_file_path} ...")
            Auxiliary.dump_json(self.sensosys_configs, _file_path)

        return True

    def _get_sensosys_configs_by_file(self, file_name: str) -> bool:
        """Get SensoSys configurations by a file"""
        logger.info(f"Configuring SensoSys by file {file_name} ...")
        # Check the file path
        if os.path.isfile(file_name):
            # Check the keys
            configs = Auxiliary.load_json(file_name)
            if set(configs.keys()) == set(self.sensosys_configs.keys()):
                self.sensosys_configs.update(configs)  # Update values of configurations
                return True
            else:
                logger.error(f"Some keys in file {file_name} are invalid or incomplete")
                return False
        else:
            logger.error(f"Invalid file name {file_name}")
            return False

    def _check_port_name(self) -> bool:
        """Check if the port name is in available ports"""
        return self.sensosys_configs['port'] in self.available_ports

    def _scan_devices_by_file(self, file_name: str) -> dict[str: dict]:
        """Scan devices by ids read from a file"""
        ids_str = Auxiliary.load_json(file_name)
        ids = [int(i) for i in ids_str]
        return self._scan_devices(ids=ids)

    def _scan_devices_by_ids(self) -> dict[str: dict]:
        """Scan devices by id from 0 (00) to 255 (FF)"""
        return self._scan_devices(ids=list(range(0, 256)))

    def _scan_devices(self, ids: list[int]) -> dict[str: dict]:
        """Scan devices by ids"""
        available_devices = {}
        for _id in ids:
            logger.info(f"Scanning address ID {_id} ...")
            device_name_response = self.sensosys.read_instrument_name(_id)
            if device_name_response is not None:
                # Get and convert instrument name to upper case
                device_name_response['instrument_name'] = device_name_response['instrument_name'].upper().strip()
                instrument_name = device_name_response['instrument_name']
                logger.info(
                    f"Found device with ID '{_id}', instrument name '{instrument_name}'")

                # Read common device information
                device_responses = device_name_response  # Dict for all responses
                device_responses.update(self.sensosys.read_serial_number(_id))  # Serial number
                device_responses.update(self.sensosys.read_expired_calibration_date(_id))  # Calibration expired data
                device_responses.update(self.sensosys.read_battery_state(_id))  # Battery state

                # Read special device information
                if instrument_name.startswith('ANEMO'):
                    device_responses.update(self.sensosys.senso_anemo_read_configuration(_id))
                    device_responses.update(self.sensosys.senso_anemo_read_indicator(_id))
                elif instrument_name.startswith('THERM'):
                    device_responses.update(self.sensosys.senso_therm_read_configuration(_id))
                    for _ch in range(1, 5):
                        device_responses.update({
                            f'senso_therm_indicator_channel_{_ch}': self.sensosys.senso_therm_read_indicator(
                                _id, _ch).get('senso_therm_indicator')
                        })
                elif instrument_name.startswith('HYGRO'):
                    device_responses.update(self.sensosys.senso_hygbar_read_configuration(_id))
                else:
                    raise ValueError(f"Invalid instrument name '{instrument_name}'")

                # Convert calibration expired date format
                exp_date = device_responses.get('calibration_expired_date')
                date_formats = ['%d-%m-%y', '%d.%m.%y']
                if exp_date is not None:
                    for _fmt in date_formats:
                        try:
                            device_responses['calibration_expired_date'] = datetime.strptime(
                                exp_date, _fmt).strftime('%Y-%m-%d')
                            break  # Exit the loop once the date is successfully parsed
                        except ValueError:
                            continue  # If parsing fails, continue to the next format

                # Update available devices
                available_devices.update({str(_id): device_responses})
        return available_devices

    def _get_all_variable_names(self) -> tuple[str, ...]:
        """Get all measurement parameters for instruments that found"""
        names = ()
        for _id, _name, _sensor_config in self.sensosys_devices_list:
            if _name.startswith('ANEMO'):
                names += (f't_a_{_id}', f'v_{_id}', f'vstar_{_id}')
            elif _name.startswith('THERM'):
                names += (f't_a_{_id}', f't_g_{_id}', f't_w_{_id}', f't_s_{_id}')
            elif _name.startswith('HYGRO'):
                names += tuple(f'{p}_{_id}' for p in self.sensosys.senso_hygbar_sensor_config[_sensor_config]['params'])
            else:
                raise ValueError(f"Invalid instrument name '{_name}'")
        return names

    def read_data(self) -> dict:
        """Read all measurement data for instruments that found"""
        data = {}
        for _id, _name, _sensor_config in self.sensosys_devices_list:
            _id = int(_id)  # Convert str id to int
            if _name.startswith('ANEMO'):
                resp = self.sensosys.senso_anemo_read_measurement_data(_id)
                if resp is None:
                    logger.warning(f"No data received from {_id} - {_name} ...")
                else:
                    data.update({
                        f't_a_{_id}': resp.get('t_a'),
                        f'v_{_id}': resp.get('v'),
                        f'vstar_{_id}': resp.get('v_star'),
                    })
            elif _name.startswith('THERM'):
                resp = self.sensosys.senso_therm_read_temperatures_enabled_channels(_id)
                if resp is None:
                    logger.warning(f"No data received from {_id} - {_name} ...")
                else:
                    data.update({
                        f't_a_{_id}': resp.get('t_a'),
                        f't_g_{_id}': resp.get('t_g'),
                        f't_w_{_id}': resp.get('t_w'),
                        f't_s_{_id}': resp.get('t_s'),
                    })
            elif _name.startswith('HYGRO'):
                resp = self.sensosys.senso_hygbar_read_measurement_data(_id, _sensor_config)
                if resp is None:
                    logger.warning(f"No data received from {_id} - {_name} ...")
                else:
                    data.update({
                        f'{p}_{_id}': resp.get(p)
                        for p in self.sensosys.senso_hygbar_sensor_config[_sensor_config]['params']
                    })
            else:
                raise ValueError(f"Invalid instrument name '{_name}'")
        return data

    @staticmethod
    def _scan_available_ports():
        """Scan available COM ports"""
        logger.info(f"Scanning available COM port(s) ...")
        available_ports = SensoSysDevices.scan_com_ports()
        if available_ports is None:
            logging.error("No available ports found, exiting ...")
            sys.exit()
        else:
            logging.info(f"Found available port(s): {available_ports}")
            return available_ports

    @staticmethod
    def _get_if_pop_system_device_management() -> bool:
        """Get if pop system device management"""
        input_str = input("Open the system device management 'devmgmt.msc' (y/n): ").lower().strip()
        if input_str == 'y':
            return True
        elif input_str == 'n':
            return False
        else:
            logger.error(f"Invalid input '{input_str}', it can only be 'y' or 'n', exiting ...")
            sys.exit(1)

    @staticmethod
    def _get_port_name() -> str:
        """Get the port name with user input str"""
        input_str = input("Enter the port number: COM")
        return f"COM{input_str}"

    @staticmethod
    def _get_scan_by_file() -> bool:
        """Get if scan devices by a file"""
        input_str = input("Scan devices by the file (y) / Scan devices from address ID 0 to 255 (n): ").lower().strip()
        if input_str == 'y':
            return True
        elif input_str == 'n':
            return False
        else:
            logger.error(f"Invalid input '{input_str}', it can only be 'y' or 'n', exiting ...")
            sys.exit(1)

    @staticmethod
    def _get_if_continue() -> bool:
        """Get if continue to run programm"""
        input_str = input("Continue (y/n): ").lower().strip()
        if input_str == 'y':
            return True
        elif input_str == 'n':
            return False
        else:
            logger.error(f"Invalid input '{input_str}', it can only be 'y' or 'n', exiting ...")
            sys.exit(1)


if __name__ == '__main__':
    from Base import DataOutput, DataLogger

    # If configuration from file
    CONFIG_FROM_FILE = True

    # Init SensoSysDataSource
    senso_sys_source = SensoSysDataSource(
        sensosys_config_file=r'_config/SensoSysConfigs_default.json' if CONFIG_FROM_FILE else None,
        output_dir='Test'
    )
    print(f"All data names of senso_sys_source: {senso_sys_source.all_variable_names}")

    # Init csv output
    csv_output = DataOutput.DataOutputCsv(file_name='Test/csv_logger.csv')

    # Init DataLogger
    time_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'senso_sys': senso_sys_source},
        data_outputs_mapping={'csv_output': csv_output},
    )
    # Run DataLogger
    time_logger.run_data_logging(
        interval=1,
        duration=30
    )
