"""
Example 03: Measuring system of Sensor Electronic
Manufacturer website: http://sensor-electronic.pl/

This example shows the measuring API of Sensor Electronic.

NOTICE: The measuring system must be connected to the PC in order to run this example.
"""
from ebcmeasurements.Base import DataOutput, DataLogger
from ebcmeasurements.Sensor_Electronic import SensoSysDataSource


def e03_sensor_electronic():
    # Firstly, implement the measuring system as a data source.
    senso_sys_source = SensoSysDataSource.SensoSysDataSource(
        port=None,  # port: Use 'None' to start the configuration guidance, it is also possible to input the port
        # directly to skip the guidance, e.g. 'COM6'
        output_dir=r'Results/e03_sensor_electronic',  # Path to save initialization information, if not 'None',
        # a JSON file will be saved that includes information about the found devices
        all_devices_ids=None,  # Use 'None' to scan device IDs, it is also possible to input the IDs directly to skip
        # the scanning, e.g. [41, 42, 43]
    )

    # After initialization, all variable names are stored in the property 'all_variable_names'
    print(f"All variable names of data source: {senso_sys_source.all_variable_names}")

    # For this example, implement a csv data output to log data
    data_output = DataOutput.DataOutputCsv(file_name=r'Results/e03_sensor_electronic/e03_sensor_electronic.csv')

    # Now, implement and run the data logger
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'senso_sys': senso_sys_source},
        data_outputs_mapping={'csv_output': data_output},
    )
    data_logger.run_data_logging(interval=1, duration=10)


if __name__ == "__main__":
    e03_sensor_electronic()
    print("\nExample 03: That's it!")
