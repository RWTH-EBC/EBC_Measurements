"""
Example 04: Beckhoff system
Manufacturer website: https://www.beckhoff.com/

This example shows the API of Beckhoff system

NOTICE: Load the project "EBC Measurements TwinCAT" to a Beckhoff PLC in order to run this example
"""
from ebcmeasurements.Base import DataSource, DataOutput, DataLogger
from ebcmeasurements.Beckhoff import AdsDataSourceOutput


def e04_beckhoff():
    # Before running this example, ADS route must be added, PLC must be connected and in "run" mode

    # Firstly, implement the PLC as a data source-output. This type indicates that data can be read from or written
    # to PLC.
    ads_source_output = AdsDataSourceOutput.AdsDataSourceOutput(
        ams_net_id='5.59.199.202.1.1',  # Adjust this AMS id for the PLC
        source_data_names=['GVL_Test.bTestRead', 'GVL_Test.fTestRead'],
        output_data_names=['GVL_Test.bTestWrite', 'GVL_Test.fTestWrite', 'GVL_Test.sTestWrite'],
    )

    # The following part shows how to read data from the PLC and log it to a csv file.
    # Implement a csv output to log data
    ads_to_csv_output = DataOutput.DataOutputCsv(file_name=r'Results/e04_beckhoff.csv')

    # Implement data logger to log data to csv file
    logger_to_csv = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'ads': ads_source_output},
        data_outputs_mapping={'ads_TO_csv': ads_to_csv_output},
        data_rename_mapping={
            'ads': {
                'ads_TO_csv': {
                    'GVL_Test.bTestRead': 'bTestRead',
                    'GVL_Test.fTestRead': 'fTestRead',
                }
            }
        }
    )
    logger_to_csv.run_data_logging(interval=1, duration=10)

    # The following part shows how to write data to the PLC.
    # Implement different data sources for boolean, real, and string values
    bool_to_ads_source = DataSource.RandomBooleanSource(size=1, key_missing_rate=0, value_missing_rate=0.2)
    real_to_ads_source = DataSource.RandomDataSource(size=1, key_missing_rate=0, value_missing_rate=0.2)
    string_to_ads_source = DataSource.RandomStringSource(size=1, key_missing_rate=0, value_missing_rate=0.2)

    # Implement data logger to log data to PLC
    logger_to_ads = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={
            'bool': bool_to_ads_source,
            'real': real_to_ads_source,
            'string': string_to_ads_source,
        },
        data_outputs_mapping={'TO_ads': ads_source_output},
        data_rename_mapping={
            'bool': {'TO_ads': {'RandBool0': 'GVL_Test.bTestWrite'}},
            'real': {'TO_ads': {'RandData0': 'GVL_Test.fTestWrite'}},
            'string': {'TO_ads': {'RandStr0': 'GVL_Test.sTestWrite'}},
        }
    )
    logger_to_ads.run_data_logging(interval=1, duration=10)

    # To read and write data simultaneously, threading can be utilized.
    import threading
    thread_read = threading.Thread(
        target=logger_to_csv.run_data_logging,
        args=(1, 20),
        name='to_csv',
    )
    thread_write = threading.Thread(
        target=logger_to_ads.run_data_logging,
        args=(1, 20),
        name='to_ads',
    )
    thread_read.start()
    thread_write.start()
    thread_read.join()
    thread_write.join()


if __name__ == '__main__':
    e04_beckhoff()
    print("\nExample 04: That's it!")
