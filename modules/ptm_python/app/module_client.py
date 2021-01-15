"""This module contains a wrapper for paho.mqtt.client and a helper methods 
to create one from edge module environmet.
"""

import logging
import os
import ssl

import paho.mqtt.client as mqtt
import edge.sastoken as auth
import edge.edge_hsm as edge_hsm


class ModuleClient:
    def __init__(self, hostname: str, device_id: str,
                 module_id: str, module_generation_id: str,
                 workload_uri: str, api_version: str, sastoken_ttl: int):
        self._username = f"{hostname}/{device_id}/{module_id}/?api-version={api_version}"

        # Use an HSM for authentication in the general case
        hsm = edge_hsm.IoTEdgeHsm(
            module_id=module_id,
            generation_id=module_generation_id,
            workload_uri=workload_uri,
            api_version=api_version,
        )

        # Create SasToken
        uri = _form_sas_uri(hostname=hostname,
                            device_id=device_id, module_id=module_id)

        self._token = auth.RenewableSasToken(
            uri, hsm, ttl=sastoken_ttl)

        # Create TLS context
        server_verification_cert = hsm.get_certificate()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_verify_locations(cadata=server_verification_cert)

        # Create mqtt client
        self._mqtt_client = mqtt.Client(client_id=f"{device_id}/{module_id}")
        self._mqtt_client.username_pw_set(self._username, str(self._token))
        self._mqtt_client.tls_set_context(context)

    def refresh(self):
        self._token.refresh()
        self._mqtt_client.username_pw_set(self._username, str(self._token))

    def connect(self, host: str, port: int = 1883, keepalive: int = 60):
        """Connect to a remote broker.
        """
        self._mqtt_client.connect(host, port, keepalive)

    def publish(self, topic: str, payload=None, qos: int = 0, retain: bool = False):
        """Publish a message on a topic.
        """
        self._mqtt_client.publish(topic, payload, qos, retain)

    @property
    def on_connect(self):
        """If implemented, called when the broker responds to our connection
        request."""
        return self._mqtt_client.on_connect

    @on_connect.setter
    def on_connect(self, func):
        """ Define the connect callback implementation. See paho.mqtt.client.on_connect
        """

        # wrap on_connect to replace mqtt_client with module_client.
        def callback(client, userdata, flags, rc):
            func(self, userdata, flags, rc)

        self._mqtt_client.on_connect = callback

    def loop_forever(self):
        """This function call loop_forever() on inner mqtt client. See paho.mqtt.client.loop_forever
        """
        self._mqtt_client.loop_forever()

    def loop_start(self):
        """This function call loop_start() on inner mqtt client. See paho.mqtt.client.loop_start
        """
        self._mqtt_client.loop_start()


def create_from_environment(sastoken_ttl: int = 30) -> ModuleClient:
    """Creates a paho.mqtt.client from edge module environmet. The returned object
    has proper authentication context (username and password) already set.

    :param int sastoken_ttl: The time to live (in seconds) for the created SasToken used for
        authentication. Default is 3600 seconds (1 hour)
    """

    # Get the Edge container variables
    hostname = os.environ["IOTEDGE_IOTHUBHOSTNAME"]
    device_id = os.environ["IOTEDGE_DEVICEID"]
    module_id = os.environ["IOTEDGE_MODULEID"]
    module_generation_id = os.environ["IOTEDGE_MODULEGENERATIONID"]
    workload_uri = os.environ["IOTEDGE_WORKLOADURI"]
    api_version = os.environ["IOTEDGE_APIVERSION"]

    client = ModuleClient(hostname, device_id, module_id,
                          module_generation_id, workload_uri, api_version, sastoken_ttl)
    return client


def _form_sas_uri(hostname, device_id, module_id) -> str:
    return f"{hostname}/devices/{device_id}/modules/{module_id}"
