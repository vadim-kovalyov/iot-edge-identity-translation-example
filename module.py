import os
import mqtt_client

print("Starting module")

gateway_hostname = os.environ["IOTEDGE_GATEWAYHOSTNAME"]

client = mqtt_client.create_from_environment()
client.on_connect = on_connect
client.connect(gateway_hostname, port=8883)

print("client connected")
print("listening for events")

client.loop_forever()


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("/command")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
