"""
Example 05: MQTT
Organisation website: https://mqtt.org/

This example shows the API of MQTT

NOTICE: Internet connection is necessary to run this example; Install a MQTT explorer (http://mqtt-explorer.com/) to
monitor data by subscribing topic 'ebc_measurements/#'
"""
from ebcmeasurements.Base import DataSource, DataOutput, DataLogger
from ebcmeasurements.Mqtt import MqttDataSourceOutput


def e05_mqtt():
    # Firstly, implement a MQTT source-output. This type indicates that data can be read from (subscribe) or written
    # to (publish) to MQTT broker.
    mqtt_source_output = MqttDataSourceOutput.MqttDataSourceOutput(
        broker="test.mosquitto.org",
        subscribe_topics=['ebc_measurements/valA', 'ebc_measurements/valB'],
        publish_topics=['ebc_measurements/valA', 'ebc_measurements/valB', 'ebc_measurements/valC'],
    )

    # The following part shows how to publish values to MQTT broker
    # Implement a random data source
    data_to_mqtt_source = DataSource.RandomDataSource(size=3, key_missing_rate=0.2, value_missing_rate=0.2)

    # Implement data logger to publish data to MQTT
    logger_publish = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'TO_mqtt': data_to_mqtt_source},
        data_outputs_mapping={'mqtt': mqtt_source_output},
        data_rename_mapping={
            'TO_mqtt': {
                'mqtt': {
                    'RandData0': 'ebc_measurements/valA',
                    'RandData1': 'ebc_measurements/valB',
                    'RandData2': 'ebc_measurements/valC',
                }
            }
        }
    )
    logger_publish.run_data_logging(interval=1, duration=5)

    # The following part shows how to subscribe to values from the MQTT broker. There are two options for configuring
    # the subscription: periodic-trigger and on-message-trigger.

    # To configure the periodic-trigger logger, the process is similar to that of configuring a normal logger.
    output_periodic = DataOutput.DataOutputCsv(r'Results/e05_mqtt_periodic.csv')  # Save log data to a csv file
    logger_periodic = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'mqtt': mqtt_source_output},
        data_outputs_mapping={'csv_periodic': output_periodic},
    )

    # The configuration of the on-message trigger must be based on an MQTT source-output instance.
    mqtt_source_on_msg = MqttDataSourceOutput.MqttDataSourceOutput(
        broker="test.mosquitto.org",
        subscribe_topics=['ebc_measurements/valA'],  # Only subscribe 'valA' for on-message trigger
    )
    output_on_msg = DataOutput.DataOutputCsv(r'Results/e05_mqtt_on_msg.csv')  # Save log data to another csv file
    # Configuration dict for on-message-logger
    config_logger_on_msg = mqtt_source_on_msg.MqttDataLoggerConfig(
        data_outputs_mapping={'csv_on_msg': output_on_msg},
        data_rename_mapping={
            'csv_on_msg': {
                'ebc_measurements/valA': 'renamed_valA',  # Renaming is possible
            }
        }
    )
    # Set configuration to MQTT instance by using property setter
    mqtt_source_on_msg.data_logger = config_logger_on_msg

    # Now, use threading to run multiple instances. The on-message logger does not require a separate thread,
    # as it operates in the main thread without method.
    import threading
    thread_publish = threading.Thread(
        target=logger_publish.run_data_logging,
        args=(2, 30),
        name='Thread publish',
    )
    thread_periodic = threading.Thread(
        target=logger_periodic.run_data_logging,
        args=(1, 30),
        name='Thread subscribe periodic',
    )
    thread_publish.start()
    thread_periodic.start()
    thread_publish.join()
    thread_periodic.join()

    # Compare both csv files: In the file with periodic-trigger, data was logged every second as configured. An empty
    # row was logged if the values were already recorded (so that the buffer was cleared) or if no values were
    # available. In the file with on-message-trigger, data was logged only when a publication was detected.


if __name__ == '__main__':
    e05_mqtt()
    print("\nExample 05: That's it!")
