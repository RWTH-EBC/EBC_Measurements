"""
Module AdsDataSourceOutput: Interface of ADS (Beckhoff TwinCAT) to DataLogger

What is Automation Device Specification (ADS):
    See https://infosys.beckhoff.com/english.php?content=../content/1033/tcinfosys3/11291871243.html&id=
"""

from Base import DataSource, DataOutput
import pyads
import time
import os
import sys
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('ADS')


class AdsDataSourceOutput:
    # Class attribute: ADS states
    _ads_states = {
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

    class AdsDataSource(DataSource.DataSourceBase):
        """Inner class for data source"""
        def __init__(self, plc: pyads.Connection, all_data_names: list[str]):
            super().__init__()
            self.plc = plc  # Instance of plc in main class
            self._all_data_names = all_data_names

        def read_data(self) -> list:
            return list(self.plc.read_list_by_name(self._all_data_names).values())

    class AdsDataOutput(DataOutput.DataOutputBase):
        """Inner class for data output"""
        def __init__(self, plc: pyads.Connection, all_data_names: list[str]):
            super().__init__(all_data_names=all_data_names, time_in_header=False)  # Without time header
            self.plc = plc  # Instance of plc in main class

        def _log_data(self, row: list):
            """Log data"""
            def clean_keys_with_none_values(input_dict: dict) -> dict:
                """Clean keys that have none values"""
                keys_of_none_values = [k for k, v in input_dict.items() if v is None]
                if len(keys_of_none_values) > 0:
                    logger.debug(f"Found keys with none values: '{keys_of_none_values}, removing these keys ...")
                    for _k in keys_of_none_values:
                        del input_dict[_k]
                return input_dict

            write_dict = dict(zip(self._headers, row))  # Zip headers with data
            write_dict_cleaned = clean_keys_with_none_values(write_dict)  # Clean none values
            if len(write_dict_cleaned) > 0:
                self.plc.write_list_by_name(write_dict_cleaned)
            else:
                logger.warning("No more keys after cleaning the write dict, skipping logging ...")

    def __init__(
            self,
            ams_net_id: str,
            ams_net_port: int = pyads.PORT_TC3PLC1,
            source_data_names: list[str] | None = None,
            output_data_names: list[str] | None = None
    ):
        """TODO: Describe add router"""
        logger.info("Initializing AdsDataSourceOutput ...")
        self.ams_net_id = ams_net_id
        self.ams_net_port = ams_net_port
        self._source_data_names = source_data_names
        self._output_data_names = output_data_names

        # Config PLC
        self.plc = pyads.Connection(self.ams_net_id, self.ams_net_port)

        # Init connection state
        self.plc_connected = False

        # Init inner classes
        # Data source
        if self._source_data_names is None:
            self._data_source = None
        else:
            logger.info("Initializing AdsDataSource ...")
            self._data_source = self.AdsDataSource(self.plc, self._source_data_names)
        # Data output
        if self._output_data_names is None:
            self._data_output = None
        else:
            logger.info("Initializing AdsDataOutput ...")
            self._data_output = self.AdsDataOutput(self.plc, self._output_data_names)

        # Connect PLC with retries
        self._plc_connect_with_retry(max_retries=5, retry_period=2)
        if self.plc_connected:
            logger.info(f"Connect to PLC successfully")
        else:
            logger.error(f"Connect to PLC failed, exiting ...")
            sys.exit(1)

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
                self.plc.open()

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
        self.plc.close()

    def _plc_read_state(self) -> tuple[str, str]:
        """Read the current ADS state and the device state"""
        logger.info("Reading ADS state and device state ...")
        _ads_state_int, _device_state_int = self.plc.read_state()
        return self._ads_states.get(_ads_state_int), self._ads_return_codes.get(_device_state_int)

    def read_data(self) -> list | None:
        if isinstance(self._data_source, self.AdsDataSource):
            if self.plc_connected:
                return self._data_source.read_data()
            else:
                logger.warning(f"PLC disconnected, reconnecting ...")
                self._plc_connect()
        else:
            raise AttributeError("No data source exists, please check the initialization")

    def log_data(self, row: list):
        if isinstance(self._data_output, self.AdsDataOutput):
            if self.plc_connected:
                return self._data_output.check_and_log_data(row)
            else:
                logger.warning(f"PLC disconnected, reconnecting ...")
                self._plc_connect()
        else:
            raise AttributeError("No data output exists, please check the initialization")

    @property
    def data_source(self):
        return self._data_source

    @property
    def data_output(self):
        return self._data_output

    @property
    def ads_states(self):
        return self._ads_states

    @property
    def ads_return_codes(self):
        return self._ads_return_codes


if __name__ == '__main__':
    from Base import Auxiliary, DataLogger

    # Init ADS
    ads_source_output = AdsDataSourceOutput(
        ams_net_id='5.78.127.222.1.1',
        source_data_names=Auxiliary.load_json('_config/AdsReadDataExamples.json')[:20],
        output_data_names=Auxiliary.load_json('_config/AdsWriteDataExamples.json')
    )

    # Init csv output
    csv_output = DataLogger.DataOutputCsv(
        file_name=os.path.join('Test', 'csv_logger.csv'),
        all_data_names=ads_source_output.data_source.all_data_names
    )

    # Init random source
    random_source = DataSource.RandomDataSource(size=2, missing_rate=0.5)

    # Init DataLoggers
    test_logger_read = DataLogger.DataLoggerBase(
        data_sources_mapping={
            'ads': {
                'source': ads_source_output,
                'headers': ads_source_output.get_all_read_data_names(),
            }
        },
        data_outputs_mapping={
            'csv_output': csv_output
        },
    )
    test_logger_write = DataLogger.DataLoggerBase(
        data_sources_mapping={
            'random': {
                'source': random_source,
                'headers': Auxiliary.load_json('_config/AdsWriteDataExamples.json'),
            }
        },
        data_outputs_mapping={
            'ads_output': ads_source_output
        },
    )
    # Run DataLoggers
    test_logger_read.run_data_logging(
        interval=1,
        duration=10
    )
    test_logger_write.run_data_logging(
        interval=1,
        duration=None,
        with_timestamp=False
    )
