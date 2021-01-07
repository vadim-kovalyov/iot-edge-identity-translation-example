import logging
import os
import sys
import time

from ruuvitag_sensor.ruuvi import RuuviTagSensor

import mqtt_client

# A map of sensors' MAC to device id.
# All devices need to be pre-created in IoT Hub.
mac_to_device_id = {"AA:2C:6A:1E:59:3D": "sensor_1"}

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server.
    """
    logging.info("Upstream client connected with result code "+str(rc))


def publish_upstream(client, mac, payload):
    """ The callback for when a message is received from a sensor.
    """
    if mac in mac_to_device_id:
        device_id = mac_to_device_id[mac]
        logging.info(f"publishing message for {device_id} ({mac}).")
        client.publish(
            f"$iothub/{device_id}/messages/events", payload, qos=1)
    else:
        logging.warning("ignoring message from an unknown sensor.")


def main():
    logging.info("Protocol Translation Module (PTM) for Python")

    gateway_hostname = os.environ["IOTEDGE_GATEWAYHOSTNAME"]

    # create a client from environment variables.
    client = mqtt_client.create_from_environment()
    client.on_connect = on_connect
    client.connect(gateway_hostname, port=8883)

    def callback(data):
        publish_upstream(client, data[0], data[1])

    # listening for sensors data.
    logging.info("listening for sensors data.")
    RuuviTagSensor.get_datas(callback)

    # start the mqtt client loop.
    client.loop_forever()
    logging.info("exiting.")


if __name__ == "__main__":
    main()
