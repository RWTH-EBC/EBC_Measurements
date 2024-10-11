"""
Example 02: Multiple data sources and data outputs

As demonstrated in 'e01_basic_structure_example.py', it is possible to use a single data logger to log data to multiple
outputs. In fact, the data logger can also collect data from multiple data sources. However, it should be noted that
variable names may have duplicates across different sources, which can lead to ambiguity in the logged data. This
example shows how to address this issue.
"""
from ebcmeasurements.Base import DataSource, DataOutput, DataLogger


def e02_multiple_sources_outputs():
    # Firstly, two random data sources are implemented for this example.
    data_source_1 = DataSource.RandomDataSource(size=2, key_missing_rate=0, value_missing_rate=0)
    data_source_2 = DataSource.RandomDataSource(size=2, key_missing_rate=0, value_missing_rate=0)

    # Then, also implement two data outputs.
    data_output_a = DataOutput.DataOutputCsv(file_name=r'Results/e02_csv_output_A.csv')
    data_output_b = DataOutput.DataOutputCsv(file_name=r'Results/e02_csv_output_B.csv')

    # If check the variable names of both data sources, it will be found that they are same.
    print(f"Variable names for data_source_1: {data_source_1.all_variable_names}")
    print(f"Variable names for data_source_2: {data_source_2.all_variable_names}")

    # In this case, if the data logger logs data from both sources simultaneously, warnings are issued to indicate that
    # there are duplicate variable names for each output.
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'Sou1': data_source_1, 'Sou2': data_source_2},
        data_outputs_mapping={'OutA': data_output_a, 'OutB': data_output_b},
    )
    data_logger.run_data_logging(interval=1, duration=5)

    # To avoid ambiguity, the data logger has automatically prefixed data source names to the variable names in the
    # file header.
    input("Please check both files of logged data. Press Enter to continue...")

    # It is possible to customize the rename mapping for variable names from each data source to each data output.
    data_logger_with_rename_1 = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'Sou1': data_source_1, 'Sou2': data_source_2},
        data_outputs_mapping={'OutA': data_output_a, 'OutB': data_output_b},
        data_rename_mapping={
            'Sou1': {  # Rename variables in data source 'Sou1'
                'OutA': {  # Rename variables when log the data from 'Sou1' to 'OutA'
                    'RandData0': 'RandData0_S1',  # Rename 'RandData0' from Sou1 to 'RandData0_S1' when log to 'OutA'
                    'RandData1': 'RandData1_S1',  # Rename 'RandData1' from Sou1 to 'RandData1_S1' when log to 'OutA'
                },
            },
            'Sou2': {  # Rename variables in data source 'Sou2'
                'OutA': {  # Rename variables when log the data from 'Sou1' to 'OutA'
                    'RandData0': 'RandData0_S2',  # Rename 'RandData0' from Sou2 to 'RandData0_S2' when log to 'OutA'
                    'RandData1': 'RandData1_S2',  # Rename 'RandData1' from Sou2 to 'RandData1_S2' when log to 'OutA'
                },
            },
        }
    )
    data_logger_with_rename_1.run_data_logging(interval=1, duration=5)

    # As a result, the prefix function was automatically deactivated for 'OutA', as the variables have been renamed to
    # eliminate any ambiguity. However, for 'OutB', the prefix remains because the variables were not renamed for this
    # output and duplicates still exist.
    input("Please check both files of logged data. Press Enter to continue...")

    # It should be noted that the rename mapping must eliminate all duplicates for a data output. If it does not, a
    # prefix will still be added to all variables logging to this output.
    data_logger_with_rename_2 = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'Sou1': data_source_1, 'Sou2': data_source_2},
        data_outputs_mapping={'OutA': data_output_a, 'OutB': data_output_b},
        data_rename_mapping={
            'Sou1': {  # Rename variables in data source 'Sou1'
                'OutA': {  # Rename variables when log the data from 'Sou1' to 'OutA'
                    'RandData0': 'RandData0_S1',  # Rename 'RandData0' from Sou1 to 'RandData0_S1' when log to 'OutA'
                    'RandData1': 'RandData1_S1',  # Rename 'RandData1' from Sou1 to 'RandData1_S1' when log to 'OutA'
                },
                'OutB': {
                    'RandData0': 'RandData0_S1',  # Only rename the variable 'RandData0'
                }
            },
            'Sou2': {  # Rename variables in data source 'Sou2'
                'OutA': {  # Rename variables when log the data from 'Sou1' to 'OutA'
                    'RandData0': 'RandData0_S2',  # Rename 'RandData0' from Sou2 to 'RandData0_S2' when log to 'OutA'
                    'RandData1': 'RandData1_S2',  # Rename 'RandData1' from Sou2 to 'RandData1_S2' when log to 'OutA'
                },
                'OutB': {
                    'RandData0': 'RandData0_S2',  # Only rename the variable 'RandData0'
                }
            },
        }
    )
    data_logger_with_rename_2.run_data_logging(interval=1, duration=5)

    # As a result, in 'OutB', data source names were prefixed to all variable names due to duplicate data source name
    # 'RandData1'.
    print("Please check both files of logged data.")


if __name__ == '__main__':
    e02_multiple_sources_outputs()
    print("\nExample 02: That's it!")
