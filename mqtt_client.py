"""This module contains a helper methods to create paho.mqtt.client from
edge module environmet.
"""

import ssl
import abc
import logging
import threading
import os
import io
import time
import paho.mqtt.client as mqtt
from azure.iot.device.iothub import edge_hsm
from azure.iot.device.common.auth import sastoken as auth


def create_from_environment(**kwargs):
    """Creates a paho.mqtt.client from edge module environmet. The returned object
    has proper authentication context (username and password) already set.

    :param int sastoken_ttl: The time to live (in seconds) for the created SasToken used for
        authentication. Default is 3600 seconds (1 hour)
    """

    # Get the Edge container variables
    hostname = os.environ["IOTEDGE_IOTHUBHOSTNAME"]
    device_id = os.environ["IOTEDGE_DEVICEID"]
    module_id = os.environ["IOTEDGE_MODULEID"]
    gateway_hostname = os.environ["IOTEDGE_GATEWAYHOSTNAME"]
    module_generation_id = os.environ["IOTEDGE_MODULEGENERATIONID"]
    workload_uri = os.environ["IOTEDGE_WORKLOADURI"]
    api_version = os.environ["IOTEDGE_APIVERSION"]

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
    token_ttl = kwargs.get("sastoken_ttl", 3600)
    try:
        token = auth.RenewableSasToken(
            uri, hsm, ttl=token_ttl)
    except auth.SasTokenError as e:
        new_err = ValueError(
            "Could not create a SasToken using the values provided, or in the Edge environment"
        )
        new_err.__cause__ = e
        raise new_err

    # Create TLS context
    try:
        server_verification_cert = hsm.get_certificate()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_verify_locations(ca_data=server_verification_cert)
    except edge_hsm.IoTEdgeError as e:
        new_err = OSError(
            "Unexpected failure getting server verification certificate")
        new_err.__cause__ = e
        raise new_err

    # Create mqtt client
    client = mqtt.Client()
    client.tls_set_context(context)
    client.username_pw_set(
        "{hostname}/{device_id}/{module_id}/?api-version={api_version}", token)
    client.connect(gateway_hostname, port=8883)

    return client


def _form_sas_uri(hostname, device_id, module_id=None):
    if module_id:
        return "{hostname}/devices/{device_id}/modules/{module_id}".format(
            hostname=hostname, device_id=device_id, module_id=module_id
        )
    else:
        return "{hostname}/devices/{device_id}".format(hostname=hostname, device_id=device_id)
