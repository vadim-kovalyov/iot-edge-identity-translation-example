import time
import os
import sys
import mqtt_client


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
        client.on_message = on_message
        client.connect(gateway_hostname, port=8883)

        print("client connected")
        print("listening for events")

        # start the client loop.
        client.loop_forever()

    except Exception as e:
        print("Unexpected error %s " % e)
        raise


if __name__ == "__main__":
    main()


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("/command")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
