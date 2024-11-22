from .Base import Auxiliary, DataLogger, DataOutput, DataSource
from .Icpdas import IcpdasDataSourceOutput
from .Mqtt import MqttDataSourceOutput, MqttTheThingsNetwork
from .Sensor_Electronic import SensoSysDataSource
import logging

try:
    from .Beckhoff import AdsDataSourceOutput
except (ImportError, FileNotFoundError) as e:
    # If TwinCAT is not installed in system, it will cause an error as 'TcAdsDll.dll' does not exist. See:
    # https://github.com/stlehmann/pyads/issues/105
    # https://stackoverflow.com/questions/76305160/windows-10-python-pyads-library-error-could-not-find-module-tcadsdll-dll
    logging.warning(
        f"TwinCAT not installed in the system, 'Beckhoff' submodule will not be available. Original error: {e}")

# Configure the root logger with a default leve and format
logging.basicConfig(
    level=logging.INFO,  # Set the default logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def set_logging_level(level: str):
    """
    Set the logging level for all loggers in this package.
    :param level: The desired logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = level.upper()
    try:
        numeric_level = getattr(logging, level)
        # Set the logging level for the root logger and all child loggers
        logging.getLogger().setLevel(numeric_level)
        for logger_name in [
            'ebcmeasurements.Base',
            'ebcmeasurements.Beckhoff',
            'ebcmeasurements.Icpdas',
            'ebcmeasurements.Mqtt',
            'ebcmeasurements.Sensor_Electronic',
        ]:
            logging.getLogger(logger_name).setLevel(numeric_level)
        print(f"Logging level set to {level} for all modules.")
    except AttributeError:
        print(f"Invalid logging level: {level}. Use DEBUG, INFO, WARNING, ERROR, or CRITICAL.")
