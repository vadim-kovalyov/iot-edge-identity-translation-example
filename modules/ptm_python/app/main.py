import logging
import os
import sys
import time

from ruuvitag_sensor.ruuvi import RuuviTagSensor

import module_client

# A map of sensors' MAC to device id.
# All devices need to be pre-created in IoT Hub.
authorized_devices = [('FE:36:EA:1E:62:AF', 'sensor_1'),
                      ('E4:F2:81:30:8D:52', 'sensor_3')]

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server.
    """
    logging.info(f"Upstream client connected with result code {str(rc)}")

    # code 4 means bad username or password
    # which in our case means expired token.
    if rc == 4:
        logging.info(f"Trying to refresh the SAS token.")
        client.refresh()


def publish_upstream(client, mac, payload):
    """ The callback for when a message is received from a sensor.
    """

    # This loop to check for authorized devices should not be needed given that it should already be enforced by the ruuvitag library
    is_authorized_device = False
    for sensor in authorized_devices:
        if mac == sensor[0]:
            is_authorized_device = True
            logging.info(
                f"publishing message for {sensor[1]} [{mac}]: {str(payload)}.")
            client.publish(
                f"$iothub/{sensor[1]}/messages/events", str(payload), qos=1)
    if(is_authorized_device == False):
        logging.warning(
            f"ignoring message from an unknown sensor (MAC address: {mac}).")


def main():
    logging.info("Azure IoT Edge Protocol Translation Module (PTM) Sample")

    gateway_hostname = os.environ["IOTEDGE_GATEWAYHOSTNAME"]

    # create a client from environment variables.
    client = module_client.create_from_environment()
    client.on_connect = on_connect
    client.connect(gateway_hostname, port=8883)

    def callback(data):
        publish_upstream(client, data[0], data[1])

    # start the mqtt client loop.
    logging.info("starting mqtt client loop.")
    client.loop_start()

    # listening for sensors data.
    logging.info("listening for sensors data.")
    RuuviTagSensor.get_datas(callback, [sensor[0]
                                        for sensor in authorized_devices])

    logging.info("exiting.")


if __name__ == "__main__":
    main()
