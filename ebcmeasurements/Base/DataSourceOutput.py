"""
Module Data source output

Data source output is for system that contains source (for read data) and output (for log data) via one system
interface, e.g. Beckhoff PLC (ADS interface), MQTT (client interface)
"""

from abc import ABC
from ebcmeasurements.Base import DataSource, DataOutput
from typing import Optional
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class DataSourceOutputBase(ABC):
    """Base class of data source output"""
    class SystemDataSource(DataSource.DataSourceBase, ABC):
        """Nested class for data source operation"""
        def __init__(self, system: Optional[object]):
            super().__init__()
            self.system = system

    class SystemDataOutput(DataOutput.DataOutputBase, ABC):
        """Nested class for data output operation"""
        def __init__(self, system: Optional[object], log_time_required: Optional[bool]):
            super().__init__(log_time_required)
            self.system = system

    def __init__(self):
        self.system: object = None  # System of data source and data output
        self._data_source: Optional[DataSourceOutputBase.SystemDataSource] = None
        self._data_output: Optional[DataSourceOutputBase.SystemDataOutput] = None

    @property
    def data_source(self) -> 'DataSourceOutputBase.SystemDataSource':
        """Instance of SystemDataSource, initialized on first access"""
        if self._data_source is None:
            # Lazy initialization with properties
            self._data_source = self.SystemDataSource(system=self.system)
        return self._data_source

    @property
    def data_output(self) -> 'DataSourceOutputBase.SystemDataOutput':
        """Instance of SystemDataOutput, initialized on first access"""
        if self._data_output is None:
            # Lazy initialization with properties
            self._data_output = self.SystemDataOutput(system=self.system, log_time_required=None)
        return self._data_output
