"""
Example 08: Timetable as data source

This example shows the setup of a data source based on timetable, with different fallback methods and file resources.
"""

from ebcmeasurements import DataSource, DataOutput, DataLogger
import os
import pandas as pd

def e08_timetable():
    def generate_timetable(duration: int = 60, interval: int = 1) -> pd.DataFrame:
        """
        Generate a timetable with a given duration and interval

        The timetable contains two columns of values, with values in 'Val A' and 'Val B' sequentially increase and
        decrease, respectively.

        :param duration: Duration of timetable (in seconds)
        :param interval: Interval of timetable (in seconds)
        """
        start_time = pd.Timestamp.now().replace(microsecond=0)
        end_time = start_time + pd.Timedelta(seconds=duration)
        df_index = pd.date_range(start=start_time, end=end_time, freq=f'{interval}s', name='datetime')
        df_data = {
            'Val A': list(range(0, len(df_index), 1)),
            'Val B': list(range(len(df_index), 0, -1)),
        }
        df = pd.DataFrame(df_data, index=df_index)
        return df

    # Firstly, generate a timetable for the upcoming 20 seconds, with interval of 5 seconds
    df_timetable = generate_timetable(duration=20, interval=5)
    print(f"Generated timetable:\n{df_timetable}")

    # Then, load this timetable into data source, with 'prior' fallback method
    timetable_source = DataSource.DataSourceTimeTable(df_timetable=df_timetable, fallback_method='prior')
    print(f"All variable names of data source: {timetable_source.all_variable_names}")

    # To observe the data source output, log the data to a csv file
    output_dir = r'Results/e08_timetable'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    data_output = DataOutput.DataOutputCsv(file_name=os.path.join(output_dir, 'e08_1_timetable_output.csv'))
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'timetable': timetable_source},
        data_outputs_mapping={'csv_output': data_output},
    )

    # Now, run the data logger for 30 seconds, sampling every 1 second
    data_logger.run_data_logging(duration=30, interval=1)

    # Save the original timetable to a csv file for comparison
    df_timetable.to_csv(os.path.join(output_dir, 'e08_1_timetable_orig.csv'))

    # After logging, you can check the results under example/Results/e08_timetable: Since the fallback method is 'prior',
    # output values align with the nearest past timestamp.


    # Compare different fallback methods by establishing three separate timetable data sources
    input("Press Enter to continue ...")
    df_timetable = generate_timetable(duration=20, interval=5)
    print(f"Generated timetable:\n{df_timetable}")
    timetable_source_prior = DataSource.DataSourceTimeTable(df_timetable=df_timetable, fallback_method='prior')
    timetable_source_next = DataSource.DataSourceTimeTable(df_timetable=df_timetable, fallback_method='next')
    timetable_source_interpolate = DataSource.DataSourceTimeTable(df_timetable=df_timetable, fallback_method='interpolate')

    data_output = DataOutput.DataOutputCsv(file_name=os.path.join(output_dir, 'e08_2_timetable_comparison_output.csv'))
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={
            'timetable_prior': timetable_source_prior,
            'timetable_next': timetable_source_next,
            'timetable_interpolate': timetable_source_interpolate,
        },
        data_outputs_mapping={'csv_output': data_output},
    )
    data_logger.run_data_logging(duration=30, interval=1)

    # Save the original timetable to a csv file for comparison
    df_timetable.to_csv(os.path.join(output_dir, 'e08_2_timetable_comparison_orig.csv'))

    # After logging, you can check the results in the newly generated csv files: The 'next' and 'interpolate' fallback
    # methods process values differently, and both stop providing values once the last timestamp is exceeded.


    # It is also possible to load timetable directly from csv or Excel files
    input("Press Enter to continue ...")
    df_timetable = generate_timetable(duration=20, interval=5)
    # Rename the index to 'dt' and convert datetime to non-standard datetime format '%Y%m%d_%H%M%S'
    df_timetable.index.name = 'dt'
    df_timetable.index = df_timetable.index.strftime('%Y%m%d_%H%M%S')
    print(f"Generated timetable:\n{df_timetable}")
    print(f"Saving timetable as csv and Excel to {output_dir} ...")
    # Save timetable into csv and Excel files
    df_timetable.to_csv(os.path.join(output_dir, 'e08_3_timetable_file.csv'), sep=';', index=True)
    df_timetable.to_excel(os.path.join(output_dir, 'e08_3_timetable_file.xlsx'), sheet_name='timetable', index=True)

    # Now, load the timetable data sources from the saved files, the column name and datetime format must be explicitly
    # defined since they differ from the defaults.
    timetable_source_csv = DataSource.DataSourceTimeTable.from_csv(
        file_name=os.path.join(output_dir, 'e08_3_timetable_file.csv'),
        fallback_method='prior',
        datetime_column_name='dt',
        datetime_format='%Y%m%d_%H%M%S',
        sep=';',
    )
    timetable_source_excel = DataSource.DataSourceTimeTable.from_excel(
        file_name=os.path.join(output_dir, 'e08_3_timetable_file.xlsx'),
        fallback_method='prior',
        datetime_column_name='dt',
        datetime_format='%Y%m%d_%H%M%S',
    )

    # Log the loaded timetable to csv file as output
    data_output = DataOutput.DataOutputCsv(file_name=os.path.join(output_dir, 'e08_3_timetable_file_output.csv'))
    data_logger = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'timetable_csv': timetable_source_csv, 'timetable_excel': timetable_source_excel},
        data_outputs_mapping={'csv_output': data_output},
    )
    data_logger.run_data_logging(duration=30, interval=1)

    # After logging, you can check the results in files: The timetable was successfully loaded from both csv and Excel.


if __name__ == '__main__':
    e08_timetable()
    print("\nExample 08: That's it!")
