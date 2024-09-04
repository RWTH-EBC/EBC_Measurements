"""
Module MqttDataSourceOutput: Interface of MQTT client to DataLogger
"""
from ebcmeasurements.Base import DataSourceOutput
import paho.mqtt.client as mqtt
import time
import os
import sys
import logging.config
# Load logging configuration from file
logger = logging.getLogger(__name__)


class MqttDataSourceOutput(DataSourceOutput.DataSourceOutputBase):
    class MqttDataSource(DataSourceOutput.DataSourceOutputBase.SystemDataSource):
        """MQTT implementation of nested class SystemDataSource"""
        def __init__(self, system: mqtt.Client, all_topics: tuple[str, ...]):
            logger.info("Initializing MqttDataSource ...")
            super().__init__(system)
            self._all_variable_names = all_topics
            self._data_buffer = {}

        def synchronize_data_buffer(self, data: dict[str, float]):
            self._data_buffer.update(data)

        def read_data(self) -> dict:
            data = self._data_buffer.copy()  # Copy the current data buffer
            self._data_buffer.clear()  # Clear the data buffer
            return data

        @property
        def topics_list(self) -> list[tuple[str, int]]:
            qos = 0
            return list(zip(self._all_variable_names, [qos] * len(self._all_variable_names)))

    class MqttDataOutput(DataSourceOutput.DataSourceOutputBase.SystemDataOutput):
        """MQTT implementation of nested class SystemDataOutput"""
        def __init__(self, system: mqtt.Client, all_topics: tuple[str, ...]):
            logger.info("Initializing AdsDataOutput ...")
            super().__init__(system, log_time_required=False)  # No requires of log time
            self._all_variable_names = all_topics

        def log_data(self, data: dict):
            if data:
                data_cleaned = self.clean_keys_with_none_values(data)  # Clean none values
                if data_cleaned:
                    for topic, value in data_cleaned.items():
                        self.system.publish(topic, value)
                else:
                    logger.info("No more keys after cleaning the data, skipping logging ...")
            else:
                logger.debug("No keys available in data, skipping logging ...")

    def __init__(
            self,
            broker: str,
            port: int = 1883,
            keepalive: int = 60,
            username: str = None,
            password: str = None,
            subscribe_topics: list[str] | None = None,
            publish_topics: list[str] | None = None
    ):
        logger.info("Initializing MqttDataSourceOutput ...")
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        self._subscribe_topics = subscribe_topics
        self._publish_topics = publish_topics

        # Config MQTT
        super().__init__()
        self.system = mqtt.Client()

        # Init DataSource
        if self._subscribe_topics is not None:
            self._data_source = self.MqttDataSource(system=self.system, all_topics=tuple(self._subscribe_topics))
        else:
            self._data_source = None

        # Set username and password if provided
        if username and password:
            self.system.username_pw_set(username, password)

        # Assign callback functions
        self.system.on_connect = self.on_connect
        self.system.on_message = self.on_message
        self.system.on_publish = self.on_publish
        self.system.on_disconnect = self.on_disconnect

        # Connect to the broker
        self._mqtt_connect_with_retry(max_retries=5, retry_period=2)
        if self.system.is_connected():
            logger.info("Connect to MQTT broker successfully")
        else:
            logger.error("Connect to MQTT broker failed, exiting ...")
            sys.exit(1)

    def __del__(self):
        """Destructor method to ensure MQTT disconnected"""
        if self.system.is_connected():
            self._mqtt_stop()
        else:
            logger.info("MQTT broker already disconnected")

    def _mqtt_connect(self):
        """Try to connect to MQTT broker only once"""
        if self.system.is_connected():
            logger.info(f"MQTT broker already connected: {self.broker}")
        else:
            try:
                logger.info(f"Connecting to broker: {self.broker} ...")
                self.system.connect(self.broker, self.port, self.keepalive)  # Connect MQTT
                self._mqtt_start()  # Start network
            except Exception as e:
                logger.warning(f"Failed to connect to MQTT broker '{self.broker}', port '{self.port}': {e}")

    def _mqtt_connect_with_retry(self, max_retries: int = 5, retry_period: int = 2):
        """Connect MQTT with multiple retries"""
        attempt = 1
        while attempt <= max_retries:
            logger.info(f"Connecting to broker with attempt(s): {attempt}/{max_retries} ...")
            self._mqtt_connect()
            time.sleep(1)  # Wait for one second to synchronise connection state
            if self.system.is_connected() or attempt == max_retries:
                break
            else:
                attempt += 1
                time.sleep(retry_period)

    def _mqtt_start(self):
        """Start the network loop"""
        logger.info("Starting network loop ...")
        self.system.loop_start()

    def _mqtt_stop(self):
        """Stop the network loop and disconnect the broker"""
        logger.info("Stopping network loop ...")
        self.system.loop_stop()
        self.system.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to {self.broker} with result code {rc}")
            # Subscribe to multiple topics for data source
            if self._data_source is not None:
                self.system.subscribe(self.data_source.topics_list)
        else:
            logger.warning(f"Connection failed with result code {rc}")
            self._mqtt_connect_with_retry(max_retries=100, retry_period=10)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8")
        logger.debug(f"Received message '{payload}' on topic '{topic}' with QoS {msg.qos}")
        # Synchronize data buffer of data source
        if self._data_source is not None:
            self._data_source.synchronize_data_buffer({topic: float(payload)})

    def on_publish(self, client, userdata, mid):
        logger.debug(f"Message published with mid: {mid}")

    def on_disconnect(self, client, userdata, rc):
        logger.info("Disconnected from the broker")
        if rc != 0:
            logger.warning("Unexpected disconnection. Attempting to reconnect...")
            self._mqtt_connect_with_retry(max_retries=100, retry_period=10)

    @property
    def data_source(self) -> 'MqttDataSourceOutput.MqttDataSource':
        """Instance of MqttDataSource"""
        if self._data_source is None:
            raise ValueError("Unable to access data source, due to missing values in 'subscribe_topics'")
        return self._data_source


if __name__ == '__main__':
    from ebcmeasurements.Base import DataSource, DataOutput, DataLogger

    # Init MQTT
    mqtt_source_output = MqttDataSourceOutput(
        broker="hil.ebc-team-kap.osc.eonerc.rwth-aachen.de",
        subscribe_topics=[
            "rkl/GVL_PwrSupMon.stPwrSupMonCC[1,1].fVoltage",
            "rkl/GVL_PwrSupMon.stPwrSupMonCC[2,1].fVoltage",
        ]
    )

    # Init csv output
    csv_output = DataOutput.DataOutputCsv(file_name=os.path.join('Test', 'csv_logger.csv'))

    # Init DataLoggers
    mqtt_logger_read = DataLogger.DataLoggerTimeTrigger(
        data_sources_mapping={'mqtt': mqtt_source_output},
        data_outputs_mapping={'csv_output': csv_output},
        data_rename_mapping={
            'mqtt': {
                'csv_output': {
                    'rkl/GVL_PwrSupMon.stPwrSupMonCC[1,1].fVoltage': 'PwrSupMonCC11',
                }
            }
        }
    )
    mqtt_logger_read.run_data_logging(interval=2, duration=None)
