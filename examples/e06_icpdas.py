"""
Example 06: ICP DAS I/O units and I/O modules
Manufacturer website: https://www.icpdas.com/

This example shows the API of ICP DAS system

NOTICE 1: Hardware (I/O units with I/O modules) are necessary to run this example
NOTICE 2: The package now only supports I-87K system
"""
from ebcmeasurements import DataOutput, DataLogger, IcpdasDataSourceOutput


def e05_icpdas():
    # Before running this example, please verify the connectivity to I/O units

    # Firstly, implement the ICP DAS system as a data source-output. In this example, the system uses only the data
    # source feature.
    icpdas_source_1 = IcpdasDataSourceOutput.IcpdasDataSourceOutput(
        host='134.130.40.38',
        port=9999,
        output_dir=r'Results/e06_icpdas',
    )
    icpdas_source_2 = IcpdasDataSourceOutput.IcpdasDataSourceOutput(
        host='134.130.40.39',
        port=9999,
        output_dir=r'Results/e06_icpdas',
        ignore_slots_idx=[4, 5, 6, 7],  # It is possible to ignore slots to exclude them from data logging
    )

    # The following part shows how to log values to a csv file
    csv_output = DataOutput.DataOutputCsv(file_name=r'Results/e06_icpdas.csv')

    # Implement data logger to log data
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'Sou1': icpdas_source_1, 'Sou2': icpdas_source_2},
        data_outputs_mapping={'Out': csv_output},
    )
    data_logger.run_data_logging(interval=1, duration=10)


if __name__ == '__main__':
    e05_icpdas()
    print("\nExample 05: That's it!")
