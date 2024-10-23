"""
Module MqttDataSourceOutput: Interface of MQTT client to DataLogger
"""
from ebcmeasurements.Base import DataOutput, DataSourceOutput, DataLogger
import paho.mqtt.client as mqtt
from typing import TypedDict
import threading
import time
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

        def mqtt_subscribe(self):
            qos = 0
            self.system.subscribe(list(zip(self._all_variable_names, [qos] * len(self._all_variable_names))))

        def synchronize_data_buffer(self, data: dict[str, float]):
            self._data_buffer.update(data)

        def read_data(self) -> dict:
            """Execute by DataLoggerTimeTrigger, read data from buffer updated in the last period and clean"""
            data = self._data_buffer.copy()  # Copy the current data buffer
            self._data_buffer.clear()  # Clear the data buffer
            return data

    class MqttDataOutput(DataSourceOutput.DataSourceOutputBase.SystemDataOutput):
        """MQTT implementation of nested class SystemDataOutput"""
        def __init__(self, system: mqtt.Client, all_topics: tuple[str, ...]):
            logger.info("Initializing MqttDataOutput ...")
            super().__init__(system, log_time_required=False)  # No requires of log time
            self._all_variable_names = all_topics

        def log_data(self, data: dict):
            if data:
                data_cleaned = self.clean_keys_with_none_values(data)  # Clean none values
                if data_cleaned:
                    if self.system.is_connected():
                        for topic, value in data_cleaned.items():
                            self.system.publish(topic, value)
                    else:
                        logger.warning("Unable to publish the data due to disconnection")
                else:
                    logger.info("No more keys after cleaning the data, skipping logging ...")
            else:
                logger.debug("No keys available in data, skipping logging ...")

    class MqttDataLoggerConfig(TypedDict):
        """Typed dict for logger configuration of nested class MqttDataLogger """
        data_outputs_mapping: dict[str, DataOutput.DataOutputBase]
        data_rename_mapping: dict[str, dict[str, str]] | None

    class MqttDataLogger(DataLogger.DataLoggerBase):
        """MQTT implementation of nested class MqttDataLogger, triggerd by 'on_message'"""
        def __init__(
                self,
                data_source,
                data_outputs_mapping: dict[str: DataOutput.DataOutputBase],
                data_rename_mapping: dict[str: dict[str: str]] | None = None,
        ):
            """MQTT 'on message' triggerd data logger"""
            logger.info("Initializing MqttDataLogger ...")
            self.data_source_name = str(id(data_source))  # Get ID as data source name
            self.log_count = 0  # Init count of logging
            super().__init__(
                data_sources_mapping={self.data_source_name: data_source},
                data_outputs_mapping=data_outputs_mapping,
                data_rename_mapping=
                {self.data_source_name: data_rename_mapping} if data_rename_mapping is not None else None
            )

        def run_data_logging(self, data):
            # Logging data
            timestamp = self.get_timestamp_now()  # Get timestamp

            # Log count
            self.log_count += 1  # Update log counter
            print(f"MQTT - Logging count(s): {self.log_count}")  # Print log counter to console

            # Log data to each output
            self.log_data_all_outputs({self.data_source_name: data}, timestamp)

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
        """
        Initialization of MqttDataSourceOutput instance

        :param broker: See package paho.mqtt.client
        :param port: See package paho.mqtt.client
        :param keepalive: See package paho.mqtt.client
        :param username: See package paho.mqtt.client
        :param password: See package paho.mqtt.client
        :param subscribe_topics: List of topics to be subscribed from MQTT broker, None to deactivate subscribe function
        :param publish_topics: List of topics to be published to MQTT broker, None to deactivate publish function
        """
        logger.info("Initializing MqttDataSourceOutput ...")
        self.broker = broker
        self.port = port
        self.keepalive = keepalive

        # Config MQTT
        super().__init__()
        self.system = mqtt.Client()
        # Set username and password if provided
        if username and password:
            self.system.username_pw_set(username, password)

        # Init DataSource
        if subscribe_topics is not None:
            self._data_source = self.MqttDataSource(system=self.system, all_topics=tuple(subscribe_topics))
        else:
            self._data_source = None

        # Init DataOutput
        if publish_topics is not None:
            self._data_output = self.MqttDataOutput(system=self.system, all_topics=tuple(publish_topics))
        else:
            self._data_output = None

        # Init DataLogger
        self._data_logger = None

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
        self.mqtt_stop()

    def _mqtt_connect(self):
        """Try to connect to MQTT broker only once"""
        if self.system.is_connected():
            logger.info(f"MQTT broker already connected: {self.broker}")
        else:
            try:
                logger.info(f"Connecting to broker: {self.broker} ...")
                self.system.connect(self.broker, self.port, self.keepalive)  # Connect MQTT
                mqtt_thread = threading.Thread(target=self._mqtt_loop_forever)
                mqtt_thread.start()
                logger.info(f"MQTT loop started")
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

    def _mqtt_loop_forever(self):
        """Run the network loop forever"""
        logger.info("Starting network loop forever ...")
        self.system.loop_forever()

    def mqtt_stop(self):
        """Stop the network loop and disconnect the broker"""
        logger.info("Stopping network loop and disconnecting ...")
        self.system.loop_stop()
        self.system.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to {self.broker} with result code {rc}")
            # Subscribe to multiple topics for data source
            if self._data_source is not None:
                self._data_source.mqtt_subscribe()
        else:
            logger.warning(f"Connection failed with result code {rc}")
            self._mqtt_connect_with_retry(max_retries=100, retry_period=10)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8")
        logger.debug(f"Received message '{payload}' on topic '{topic}' with QoS {msg.qos}")
        data = {topic: float(payload)}
        if self._data_source is not None:
            self._data_source.synchronize_data_buffer(data)  # Synchronize data buffer of data source
        if self._data_logger is not None:
            self._data_logger.run_data_logging(data)  # Trigger MQTT data logger

    def on_publish(self, client, userdata, mid):
        logger.debug(f"Message published with mid: {mid}")

    def on_disconnect(self, client, userdata, rc):
        logger.info(f"Disconnected from the broker {rc}")
        if rc != 0:
            logger.warning("Unexpected disconnection. Attempting to reconnect...")
            self._mqtt_connect_with_retry(max_retries=100, retry_period=10)

    @property
    def data_source(self) -> 'MqttDataSourceOutput.MqttDataSource':
        """Instance of MqttDataSource"""
        if self._data_source is None:
            raise AttributeError("Data source unavailable, due to missing values in 'subscribe_topics'")
        return self._data_source

    @property
    def data_output(self) -> 'MqttDataSourceOutput.MqttDataOutput':
        """Instance of MqttDataOutput"""
        if self._data_output is None:
            raise AttributeError("Data output unavailable, due to missing values in 'publish_topics'")
        return self._data_output

    @property
    def data_logger(self) -> 'MqttDataSourceOutput.MqttDataLogger':
        """MQTT data logger"""
        return self._data_logger

    @data_logger.setter
    def data_logger(self, config: 'MqttDataSourceOutput.MqttDataLoggerConfig'):
        """Set MQTT data logger"""
        self._data_logger = self.MqttDataLogger(
            self.data_source, config['data_outputs_mapping'], config.get('data_rename_mapping'))
