"""
Module AdsDataSourceOutput: Interface of ADS (Beckhoff TwinCAT) to DataLogger

What is Automation Device Specification (ADS):
    See https://infosys.beckhoff.com/english.php?content=../content/1033/tcinfosys3/11291871243.html&id=
"""

from Base import DataLogger, Auxiliary
from typing import TypedDict
import pyads
import os
import logging
import logging.config
# Load logging configuration from file
logging.config.fileConfig(r'_config/logging.ini')
logger = logging.getLogger('ADS')


class AdsDataSourceOutput(DataLogger.DataSourceBase, DataLogger.DataOutputBase):
    def __init__(self, ams_net_id: str, ams_net_port: int = pyads.PORT_TC3PLC1):
        logger.info("Initializing AdsDataSourceOutput ...")
        self.ams_net_id = ams_net_id
        self.ams_net_port = ams_net_port
        self.plc = pyads.Connection(self.ams_net_id, self.ams_net_port)

        # Connect to plc
        self._plc_open()

    def __del__(self):
        """Destructor to ensure the PLC disconnection"""
        self._plc_close()

    def _plc_open(self):
        """Open PLC: connect to TwinCAT message router"""
        logger.info("Connecting PLC ...")
        self.plc.open()

    def _plc_close(self):
        """Close PLC: close the connection to the TwinCAT message router"""
        logger.info("Disconnecting PLC ...")
        self.plc.close()

    def _plc_read_state(self):
        """Read the current ADS-state and the machine-state"""
        ads_state_int, device_state_int = self.plc.read_state()

    def read_data(self) -> list:
        pass

    def log_data(self, row: list):
        pass




if __name__ == '__main__':
    test_ads = AdsDataSourceOutput('')
