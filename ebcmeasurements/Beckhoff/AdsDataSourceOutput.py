"""
Module AdsDataSourceOutput: Interface of ADS (Beckhoff TwinCAT) to DataLogger

What is Automation Device Specification (ADS):
    See https://infosys.beckhoff.com/english.php?content=../content/1033/tcinfosys3/11291871243.html&id=
"""

from ebcmeasurements import BASE_DIR
from ebcmeasurements.Base import DataSourceOutput
import pyads
import time
import os
import sys
import logging.config
# Load logging configuration from file
logging.config.fileConfig(os.path.join(BASE_DIR, 'Beckhoff', '_config', 'logging.ini'))
logger = logging.getLogger('ADS')


class AdsDataSourceOutput(DataSourceOutput.DataSourceOutputBase):
    # Class attribute: ADS states
    _ads_states_codes = {
        pyads.ADSSTATE_INVALID: 'Invalid',  # 0
        pyads.ADSSTATE_IDLE: 'Idle',  # 1
        pyads.ADSSTATE_RESET: 'Reset',  # 2
        pyads.ADSSTATE_INIT: 'Init',  # 3
        pyads.ADSSTATE_START: 'Start',  # 4
        pyads.ADSSTATE_RUN: 'Run',  # 5
        pyads.ADSSTATE_STOP: 'Stop',  # 6
        pyads.ADSSTATE_SAVECFG: 'Save cfg',  # 7
        pyads.ADSSTATE_LOADCFG: 'Load cfg',  # 8
        pyads.ADSSTATE_POWERFAILURE: 'Power failure',  # 9
        pyads.ADSSTATE_POWERGOOD: 'Power good',  # 10
        pyads.ADSSTATE_ERROR: 'Error',  # 11
        pyads.ADSSTATE_SHUTDOWN: 'Shut down',  # 12
        pyads.ADSSTATE_SUSPEND: 'Suspend',  # 13
        pyads.ADSSTATE_RESUME: 'Resume',  # 14
        pyads.ADSSTATE_CONFIG: 'Config',  # 15: system is in config mode
        pyads.ADSSTATE_RECONFIG: 'Re-config',  # 16: system should restart in config mode
    }

    # Class attribute: ADS return codes
    # https://infosys.beckhoff.com/content/1033/tc3_ads_intro/374277003.html?id=4954945278371876402
    _ads_return_codes = {
        0: 'No error',
        1: 'Internal error',
        2: 'No real time',
        3: 'Allocation locked – memory error',
        4: 'Mailbox full – the ADS message could not be sent. Reducing the number of ADS messages per cycle will help',
        5: 'Wrong HMSG',
        6: 'Target port not found – ADS server is not started, not reachable or not installed',
        7: 'Target computer not found – AMS route was not found',
        8: 'Unknown command ID',
        9: 'Invalid task ID',
        10: 'No IO',
        11: 'Unknown AMS command',
        12: 'Win32 error',
        13: 'Port not connected',
        14: 'Invalid AMS length',
        15: 'Invalid AMS Net ID',
        16: 'Installation level is too low –TwinCAT 2 license error',
        17: 'No debugging available',
        18: 'Port disabled – TwinCAT system service not started',
        19: 'Port already connected',
        20: 'AMS Sync Win32 error',
        21: 'AMS Sync Timeout',
        22: 'AMS Sync error',
        23: 'No index map for AMS Sync available',
        24: 'Invalid AMS port',
        25: 'No memory',
        26: 'TCP send error',
        27: 'Host unreachable',
        28: 'Invalid AMS fragment',
        29: 'TLS send error – secure ADS connection failed',
        30: 'Access denied – secure ADS access denied',
    }

    class AdsDataSource(DataSourceOutput.DataSourceOutputBase.SystemDataSource):
        """Ads implementation of nested class SystemDataSource"""
        def __init__(self, system: pyads.Connection, all_variable_names: tuple[str, ...]):
            logger.info("Initializing AdsDataSource ...")
            super().__init__(system)
            self._all_variable_names = all_variable_names

        def read_data(self) -> dict:
            return self.system.read_list_by_name(list(self._all_variable_names))

    class AdsDataOutput(DataSourceOutput.DataSourceOutputBase.SystemDataOutput):
        """Ads implementation of nested class SystemDataOutput"""
        def __init__(self, system: pyads.Connection, all_variable_names: tuple[str, ...]):
            logger.info("Initializing AdsDataOutput ...")
            super().__init__(system, log_time_required=False)  # No requires of log time
            self._all_variable_names = all_variable_names

        def log_data(self, data: dict):
            """Log data"""
            def clean_keys_with_none_values(input_dict: dict) -> dict:
                """Clean keys that have none values"""
                keys_of_none_values = [k for k, v in input_dict.items() if v is None]
                if len(keys_of_none_values) > 0:
                    logger.debug(f"Found keys with none values: '{keys_of_none_values}, removing these keys ...")
                    for _k in keys_of_none_values:
                        del input_dict[_k]
                return input_dict

            data_cleaned = clean_keys_with_none_values(data)  # Clean none values
            if len(data_cleaned) > 0:
                self.system.write_list_by_name(data_cleaned)
            else:
                logger.warning("No more keys after cleaning the data, skipping logging ...")

    def __init__(
            self,
            ams_net_id: str,
            ams_net_port: int = pyads.PORT_TC3PLC1,
            source_data_names: list[str] | None = None,
            output_data_names: list[str] | None = None
    ):
        """
        Initialization of AdsDataSourceOutput instance

        Before running the program, routes must be added via TwinCAT or TwinCAT Router UI, see:
        https://infosys.beckhoff.com/content/1033/tc3_system/5211773067.html?id=2762137833336592415

        :param ams_net_id: See package pyads.Connection.ams_net_id
        :param ams_net_port: See package pyads.Connection.ams_net_port
        :param source_data_names: List of source names to be read from PLC, None to deactivate read function
        :param output_data_names: List of output names to be logged to PLC, None to deactivate write function
        """
        logger.info("Initializing AdsDataSourceOutput ...")
        self.ams_net_id = ams_net_id
        self.ams_net_port = ams_net_port
        self._source_data_names = source_data_names
        self._output_data_names = output_data_names

        # Config PLC
        super().__init__()
        self.system = pyads.Connection(self.ams_net_id, self.ams_net_port)

        # Init connection state
        self.plc_connected = False

        # Connect PLC with retries
        self._plc_connect_with_retry(max_retries=5, retry_period=2)
        if self.plc_connected:
            logger.info(f"Connect to PLC successfully")
        else:
            logger.error(f"Connect to PLC failed, exiting ...")
            sys.exit(1)

    def __del__(self):
        """Destructor method to ensure PLC disconnected"""
        if self.system.is_open:
            self._plc_close()
        else:
            logger.info("PLC already disconnected")

    def _plc_connect(self):
        """Try to connect PLC only once"""
        if self.plc_connected:
            # Read PLC state
            plc_state = self._plc_read_state()
            logger.info(f"PLC already connected: ADS state - '{plc_state[0]}', device state - '{plc_state[1]}'")
        else:
            try:
                logger.info(f"Connecting PLC ...")
                # Connect PLC
                self.system.open()

                # Read PLC state
                plc_state = self._plc_read_state()
                logger.info(f"PLC connected: ADS state - '{plc_state[0]}', device state - '{plc_state[1]}'")

                # Update state
                self.plc_connected = True
            except pyads.ADSError:
                logger.warning(f"PLC connection failed")

    def _plc_connect_with_retry(self, max_retries: int = 5, retry_period: int = 2):
        """Connect PLC with multiple retries"""
        attempt = 1
        while attempt <= max_retries:
            logger.info(f"Connecting PLC with attempt(s): {attempt}/{max_retries} ...")
            self._plc_connect()
            if self.plc_connected or attempt == max_retries:
                break
            else:
                attempt += 1
                time.sleep(retry_period)

    def _plc_close(self):
        """Close PLC: close the connection to the TwinCAT message router"""
        logger.info("Disconnecting PLC ...")
        self.system.close()

    def _plc_read_state(self) -> tuple[str, str]:
        """Read the current ADS state and the device state"""
        logger.info("Reading ADS state and device state ...")
        ads_state_int, device_state_int = self.system.read_state()
        return self._ads_states_codes.get(ads_state_int), self._ads_return_codes.get(device_state_int)

    @property
    def data_source(self) -> 'AdsDataSourceOutput.AdsDataSource':
        """Instance of AdsDataSource, initialized on first access"""
        if self._data_source is None:
            if self._source_data_names is None:
                raise ValueError("No values in 'source_data_names', unable to initialize data source")
            else:
                # Lazy initialization with properties
                self._data_source = self.AdsDataSource(
                    system=self.system, all_variable_names=tuple(self._source_data_names))
        return self._data_source

    @property
    def data_output(self) -> 'AdsDataSourceOutput.AdsDataOutput':
        """Instance of AdsDataOutput, initialized on first access"""
        if self._data_output is None:
            if self._output_data_names is None:
                raise ValueError("No values in 'output_data_names', unable to initialize data output")
            else:
                # Lazy initialization with properties
                self._data_output = self.AdsDataOutput(
                    system=self.system, all_variable_names=tuple(self._output_data_names))
        return self._data_output

    @property
    def ads_states_codes(self) -> dict:
        return self._ads_states_codes

    @property
    def ads_return_codes(self) -> dict:
        return self._ads_return_codes


if __name__ == '__main__':
    from ebcmeasurements.Base import DataSource, DataOutput, DataLogger, Auxiliary
    import threading

    # Init ADS for data source and output
    ads_source_output = AdsDataSourceOutput(
        ams_net_id='5.78.127.222.1.1',
        source_data_names=Auxiliary.load_json('_config/AdsReadDataExamples.json')[:20],
        output_data_names=Auxiliary.load_json('_config/AdsWriteDataExamples.json')
    )

    # Init csv output
    csv_output = DataOutput.DataOutputCsv(file_name=os.path.join('Test', 'csv_logger.csv'))

    # Init random source
    random_source = DataSource.RandomDataSource(size=2, key_missing_rate=0.5, value_missing_rate=0.5)

    # Init DataLoggers
    ads_logger_read = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'ads': ads_source_output},
        data_outputs_mapping={'csv_output': csv_output},
        data_rename_mapping={
            'ads': {
                'csv_output': {
                    'GVL_WtrSupPri.stWtrSupPriCtrl[1].fCircSupTempSet': 'FW_CircSupTempSet',
                    'GVL_WtrSupPri.stWtrSupPriCtrl[2].fCircSupTempSet': 'FK_CircSupTempSet',
                }
            }
        }
    )
    ads_logger_write = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'random': random_source},
        data_outputs_mapping={'ads_output': ads_source_output},
        data_rename_mapping={
            'random': {
                'ads_output': {
                    'RandData0': 'GVL_WtrSupPri.stWtrSupPriCtrl[1].fCircSupTempSet',
                    'RandData1': 'GVL_WtrSupPri.stWtrSupPriCtrl[2].fCircSupTempSet',
                }
            }
        }
    )

    # Run DataLoggers
    thread_1 = threading.Thread(
        target=ads_logger_read.run_data_logging,
        args=(2, 30),
        name='PLC to csv',
    )
    thread_2 = threading.Thread(
        target=ads_logger_write.run_data_logging,
        args=(1, 30),
        name='Rnd to PLC',
    )
    thread_1.start()
    time.sleep(0.5)
    thread_2.start()
    thread_1.join()
    thread_2.join()
    print("Both data logging tasks have completed.")
