import time
import os
import sys
import mqtt_client
from ruuvitag_sensor.ruuvi import RuuviTagSensor

# A map of sensors' MAC to device id.
# All devices need to be pre-created in IoT Hub.
mac_to_device_id = {"AA:2C:6A:1E:59:3D": "device_id"}


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Upstream client connected with result code "+str(rc))


# The callback for when a message is received from a sensor.
def publish_upstream(client, mac, payload):
    if mac in mac_to_device_id:
        device_id = mac_to_device_id[mac]
        print("publishing message for {device_id} ({mac}).".format(
            device_id=device_id, mac=mac))
        client.publish(
            "$iothub/{device_id}/messages/events".format(device_id=device_id), payload, qos=1)
    else:
        print("ignoring message from an unknown sensor.")


def main():
    try:
        if not sys.version >= "3.5.3":
            raise Exception(
                "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version)
        print("Protocol Translation Module (PTM) for Python")

        gateway_hostname = os.environ["IOTEDGE_GATEWAYHOSTNAME"]

        # create a client from environment variables.
        client = mqtt_client.create_from_environment()
        client.on_connect = on_connect
        client.connect(gateway_hostname, port=8883)

        def callback(data):
            publish_upstream(client, data[0], data[1])

        # listening for sensors data.
        print("listening for sensors data.")
        RuuviTagSensor.get_datas(callback)

        # start the mqtt client loop.
        client.loop_forever()
        print("exiting.")

    except Exception as e:
        print("Unexpected error %s " % e)
        raise


if __name__ == "__main__":
    main()
