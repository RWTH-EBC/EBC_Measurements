"""
Example 07: TheThingsNetwork (TTN)
Organisation website: https://www.thethingsnetwork.org/

This example shows the API of TTN

NOTICE: Registered IoT-device in the TTN and internet connection are necessary to run this example
"""
from ebcmeasurements import DataOutput, MqttTheThingsNetwork


def e07_ttn():
    # This example uses the devices with following IDs, for each device, various variable names are chosen to be logged
    devices_dict = {
        'eui-a8...a': ['temperature', 'humidity', 'light', 'vdd'],
        'eui-a8...b': ['temperature', 'humidity'],
        'eui-a8...c': ['temperature'],
    }

    # Firstly, implement the MQTT system to connect the service in TTN. In this example, the system uses only the data
    # source feature.
    ttn_source_output = MqttTheThingsNetwork.MqttTheThingsNetwork(
        username='...@ttn',
        password='NNSXS.L5VPR...',
        device_ids=list(devices_dict.keys()),
        device_uplink_payload_variable_names=devices_dict,
    )

    # Then, set data output to a csv file
    csv_output = DataOutput.DataOutputCsv(r'Results/e07_ttn.csv')

    # This example uses an On-Message-triggered data logger
    ttn_source_output.activate_on_msg_data_logger(
        data_outputs_mapping={'csv': csv_output}, data_rename_mapping=None
    )


if __name__ == '__main__':
    e07_ttn()
    input('Press Enter to exit the main thread ...')  # Use an input to keep the main thread running
    print("\nExample 07: That's it!")
