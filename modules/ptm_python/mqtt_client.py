"""This module contains a helper methods to create paho.mqtt.client from
edge module environmet.
"""

import logging
import os
import ssl

import paho.mqtt.client as mqtt
from azure.iot.device.common.auth import sastoken as auth
from azure.iot.device.iothub import edge_hsm


def create_from_environment(sastoken_ttl=3600):
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
    try:
        token = auth.RenewableSasToken(
            uri, hsm, ttl=sastoken_ttl)
    except auth.SasTokenError as e:
        raise ValueError(
            "Could not create a SasToken using the values provided, or in the Edge environment"
        ) from e

    # Create TLS context
    try:
        server_verification_cert = hsm.get_certificate()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_verify_locations(cadata=server_verification_cert)
    except edge_hsm.IoTEdgeError as e:
        new_err = OSError(
            "Unexpected failure getting server verification certificate")
        new_err.__cause__ = e
        raise new_err

    # Create mqtt client
    client = mqtt.Client(
        client_id=f"{device_id}/{module_id}")
    client.tls_set_context(context)
    client.username_pw_set(
        f"{hostname}/{device_id}/{module_id}/?api-version={api_version}", str(token))

    return client


def _form_sas_uri(hostname, device_id, module_id=None):
    if module_id:
        return f"{hostname}/devices/{device_id}/modules/{module_id}"
    else:
        return f"{hostname}/devices/{device_id}"
