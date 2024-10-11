"""
Example 01: Understand the basic structure of EBC_Measurements

As introduced in 'README.md', data logging is the process of acquiring data over time from sources, and storing them
in one or multiple outputs. This example shows the basic structure of this process. To configurate this process, three
steps are usually necessary:
1. Configuration of data source(s)
2. Configuration of data output(s)
3. Configuration of data logger(s)

"""
from ebcmeasurements.Base import DataSource, DataOutput, DataLogger


def e01_basic_structure_example():
    # This example uses a simple data source that generates random data to simulate measurements from a sensor or
    # measurement device.
    test_random_data_source = DataSource.RandomDataSource(size=5, key_missing_rate=0, value_missing_rate=0)

    # This random data source is a passive data source, similar to most sensors and measurement devices used in
    # experiments: Data will be acquired only upon request. In EBC_Measurements, the method "read_data()" is
    # internally used to request data from a data source.
    for req in range(5):
        data = test_random_data_source.read_data()
        # Use print to check the acquired data
        print(f"Random data source, request {req}: {data}")

    # In practice, keys and values may be missing during a measurement. This can be simulated by setting the missing
    # rates of the random data source.
    test_random_data_source_missing = DataSource.RandomDataSource(size=5, key_missing_rate=0.5, value_missing_rate=0.5)
    for req in range(5):
        data = test_random_data_source_missing.read_data()
        # Use print to check the acquired data
        print(f"Random data source (missing rate activated), request {req}: {data}")

    # By the way, it is possible to use a random string source to generate string data.
    test_random_string_source = DataSource.RandomStringSource(
        size=5, str_length=10, key_missing_rate=0, value_missing_rate=0
    )
    for req in range(5):
        data = test_random_string_source.read_data()
        # Use print to check the acquired data
        print(f"Random string source, request {req}: {data}")

    # Now, the following part configures a simple data logging process
    # Step 1: Configuration data source(s)
    # Here uses a single random data source to generate data
    random_data_source = DataSource.RandomDataSource(size=10, key_missing_rate=0.2, value_missing_rate=0.2)

    # Step 2: Configuration data output(s)
    # Here uses two csv outputs to log data. The file path will be created automatically if it is not available.
    csv_data_output_1 = DataOutput.DataOutputCsv(file_name=r'Results/e01_csv_output_1.csv')
    csv_data_output_2 = DataOutput.DataOutputCsv(
        file_name=r'Results/e01_csv_output_2.csv',
        csv_writer_settings={'delimiter': '\t'},  # Default delimiter is ';', it can be changed by this parameter
    )

    # Step 3: Configuration of data logger(s) Here uses a periodic-triggered (time-triggered) data logger to log data
    # from 'random_data_source' to both 'csv_data_output_1' and 'csv_data_output_2' simultaneously.
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'Sou': random_data_source},  # Mapping of data sources: {<name>: <instance>}
        data_outputs_mapping={
            'Out1': csv_data_output_1, 'Out2': csv_data_output_2},  # Mapping of data outputs: {<name>: <instance>}
    )
    # Now, run this data logger
    data_logger.run_data_logging(interval=1, duration=5)

    # After logging, you can check both csv files under examples/Results: They contain the same data requested from
    # the random data source, but with different delimiters.


if __name__ == '__main__':
    e01_basic_structure_example()
    print("\nExample 01: That's it!")
