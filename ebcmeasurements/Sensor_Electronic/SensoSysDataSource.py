"""
Module SensoSysDataSource: Interface to DataLogger
"""

from ebcmeasurements.Base import DataSource, Auxiliary
from ebcmeasurements.Sensor_Electronic import SensoSysDevices
from datetime import datetime
import os
import sys
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class SensoSysDataSource(DataSource.DataSourceBase):
    def __init__(
            self,
            port: str | None = None,
            output_dir: str | None = None,
            all_devices_ids: list[int] | None = None,
            time_out: float = 0.1,
    ):
        """
        Initialize SensoSysDataSource instance
        :param port: Port number to connect, if None, start a configuration guidance
        :param output_dir: Output dir to save information of found devices, if None, they will not be saved
        :param all_devices_ids: All possible device's IDs to scan, if None, scan ID from 0 to 255
        :param time_out: Timeout in seconds for serial communication
        """
        logger.info("Initializing SensoSysDataSource ...")

        super().__init__()
        self.port = None
        self.output_dir = output_dir
        self.all_devices_ids = None

        # Create output dir if it is not None
        if self.output_dir is None:
            logger.info(f"No output dir set, initialization information will not be saved")
        else:
            logger.info(f"Initialization information will be saved to: {self.output_dir}")
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

        # Scan available COM port(s)
        self.available_ports = self._scan_available_ports()

        # Set device COM port
        if port is None:
            self.port = self._get_port_by_guide()  # Configuration by guide
        else:
            self.port = port  # Configuration by file

        # Check device COM port
        self._check_port_name()

        # Init SensoSys
        logger.info(f"Initializing SensoSysDevices ...")
        self.sensosys = SensoSysDevices.SensoSys(port=self.port, time_out=time_out)

        # Scan devices
        if all_devices_ids is None:
            self.sensosys_devices = self._scan_devices(ids=list(range(0, 255)))  # Scan by id from 00 to FF
        else:
            self.sensosys_devices = self._scan_devices(ids=all_devices_ids)  # Scan according to input
        self.all_devices_ids = self.sensosys_devices.keys()
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

    def _get_port_by_guide(self) -> str:
        """Get SensoSys configurations by a user guide"""
        logger.info("Configuring SensoSys by a user guide ...")
        # Set the COM port
        _pop_devmgmt = self._get_if_pop_system_device_management()  # Get if pop the system device management
        if _pop_devmgmt:
            SensoSysDevices.pop_system_device_management()
        return self._get_port_name()  # Get the port name by user input

    def _check_port_name(self):
        """Check if the port name is in available ports"""
        if self.port in self.available_ports:
            logger.info(f"Successfully set COM port to '{self.port}'")
        else:
            logger.error(f"The COM port '{self.port}' is unavailable, exiting ...")
            sys.exit(1)

    def _scan_devices(self, ids: list[int]) -> dict[str, dict]:
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
                elif instrument_name.startswith(('HYGRO', 'HIGRO')):
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
            elif _name.startswith(('HYGRO', 'HIGRO')):
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
            elif _name.startswith(('HYGRO', 'HIGRO')):
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
