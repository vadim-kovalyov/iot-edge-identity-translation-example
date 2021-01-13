# Azure IoT Edge Protocol Translation Module
This repository contains code for a sample protocol translation module (PTM) running on an [IoT Edge device](https://azure.microsoft.com/en-us/services/iot-edge/).

A PTM is a module that receives data from downstream devices over a protocol, provides an Azure IoT Hub device identity to each downstream devices and forward their to IoT Hub using IoT Edge's upstream protocol. To learn more about IoT Edge gateway patterns, please see [this documentation](https://docs.microsoft.com/azure/iot-edge/iot-edge-as-gateway?view=iotedge-2018-06).

In this sample, a [Raspberry Pi](https://www.raspberrypi.org/) collects data from several [RuuviTag temperature sensors](https://ruuvi.com/) over Bluetooth Low Energy (BLE LE) and forward this data to the cloud.

## Pre-requisites
### Hardware

To run this sample, you need the following hardware:

- **Raspberry Pi 3**: Set up Azure IoT Edge on a Raspberry Pi 3 with Bluetooth connectivity  by following these [instructions to set up the hardware](https://blog.jongallant.com/2017/11/raspberrypi-setup/) and installing *IoT Edge 1.2-RC1 or above*. From a terminal on your Raspberry Pi, run the following commands (alternatively, you can see the full installation tutorial [here](https://docs.microsoft.com/en-us/azure/iot-edge/how-to-install-iot-edge?view=iotedge-2018-06&tabs=linux)):

    ```bash
    sudo apt-get update
    sudo apt-get install moby-engine
    sudo wget -O libiothsm-std_1.2.0_rc1_armhf.deb "https://github.com/Azure/azure-iotedge/releases/download/1.2.0-rc1/libiothsm-std_1.2.0_rc1-1-1_debian9_armhf.deb"
    sudo wget -O iotedge_1.2.0_rc1_armhf.deb "https://github.com/Azure/azure-iotedge/releases/download/1.2.0-rc1/iotedge_1.2.0_rc1-1_debian9_armhf.deb"
    sudo dpkg -i libiothsm-std_1.2.0_rc1_armhf.deb
    sudo dpkg -i iotedge_1.2.0_rc1_armhf.deb
    ```

- **A couple of RuuviTags sensors**: have a few of [these sensors](https://ruuvi.com/) and take note of their MAC addresses. You can find their MAC addresses by using Ruuvi Station's phone application from [Ruuvi's website](https://ruuvi.com/).

### Services

To run this sample, you must have the following Azure services:

- **Azure IoT Hub**: This is your Cloud gateway to remotely manage your IoT Edge devices. You can use the free sku for this sample. In your IoT Hub, register your Raspberry Pi as an IoT Edge device and register your RuuviTag sensors as IoT devices (not IoT Edge devices) parented to your IoT Edge device. Finally, provision your IoT Edge device by copying its connection string to your device in the IoT Edge configuration file located at `/etc/iotedge/config.yaml`. For more information about registering and manually provisioning devices, see [this documentation for IoT Edge devices](https://docs.microsoft.com/en-us/azure/iot-edge/how-to-manual-provision-symmetric-key?view=iotedge-2018-06&tabs=azure-portal%2Clinux) and [this one for IoT devices parented to a gateway device](https://docs.microsoft.com/azure/iot-edge/how-to-authenticate-downstream-device?view=iotedge-2020-11#symmetric-key-authentication).
- **Azure Container Registry**: This is where you host your containers (e.g. IoT Edge modules). Deployment manifests refer to this container registry for the IoT Edge devices to download their images.You can use the free sku for this sample.

### Tooling

You need the following development tools to do IoT Edge development in general and to run this sample:

- **Visual Studio Code**: IoT Edge development environment. [Download it from here](https://code.visualstudio.com/).
- **Visual Studio Code: Azure IoT Edge Extension**: An extension that connects to your IoT Hub and lets you manage your IoT Devices and IoT Edge Devices right from VS Code. A must-have for IoT Edge development. [Download it from here](https://marketplace.visualstudio.com/items?itemName=vsciot-vscode.azure-iot-edge). Once installed, connect it to your IoT Hub.
- **Docker**: Have docker running to be able to compile IoT Edge modules which are Docker containers. You can [install Docker from here](https://docs.docker.com/get-docker/).

To learn more about this development environment, check out [this tutorial](https://docs.microsoft.com/en-us/azure/iot-edge/how-to-deploy-modules-vscode).

## Description of the solution

### Modules

This sample is made of one custom module:

- **Protocol Translation Module (PTM)**: the PTM periodically collects data from registered RuuviTag sensors over Bluetooth and forward this data to the IoT Edge Hub MQTT broker on the telemetry channel of each RuuviTag sensor registered in IoT Hub. This module does not use [Azure IoT Device SDKs](https://docs.microsoft.com/azure/iot-hub/iot-hub-devguide-sdks#azure-iot-hub-device-sdks) but instead directly uses [Paho](https://pypi.org/project/paho-mqtt/) as a MQTT client.

### Data flow

In this sample, data is collected by the PTM module over Bluetooth from the sensors thanks to [this RuuviTag python library](https://github.com/ttu/ruuvitag-sensor) and sent to the edgeHub MQTT broker on the IoT Hub telemetry topic of each sensor. All messages received by the edgeHub are then forwarded to IoT Hub via a [route](https://docs.microsoft.com/azure/iot-edge/module-composition?view=iotedge-2020-11#declare-routes). The upstream protocol used between the IoT Edge device and IoT Hub is AMQP to multiplex all the upstream connections needed (one per sensor).

### Authorization

Several authorization mechanisms are used in this sample:

- **MQTT broker authorization policy**:  The PTM is authorized to connect to the EdgeHub MQTT broker and to publish messages on each sensor IoT Hub telemetry topic. You can have a look at this authorization policy via the [deployment manifest](deployment.template.json), inthe edgeHub twin section.
- **SAS token**: To send any messages to the edgeHub, a module must first get a token signed by the IoT Edge runtime. The Azure IoT Device SDKs typically take care of this step, but since they are not used here, the PTM gets a token itself by querying [the IoT Edge Workload API](https://github.com/Azure/iotedge/blob/c0bad527da979fc0d8d1c810474e5078dfee83ca/edgelet/workload/README.md). It also takes care of refreshing this token whenever needed.
- **Parent/Child relationships**: For the edgeHub to send data to IoT Hub on behalf of child devices, the current IoT Hub APIs require a parent/child relationship to be registered in IoT Hub. This is why you must register each sensor device as a child of the IoT Edge device in IoT Hub the pre-requisites.

## Get started
From your mac or PC:
1. Clone this repository
2. Register the MAC addresses and associated IoT hub device names of your RuuviTag sensors by editing the `./modules/ptm_python/main.py` file and assigning to the `mac_to_device_id` variable the list of MAC addresses, device ids pairs in the format `{"MAC_address_sensor_1": "device_id_sensor_1", "MAC_address_sensor_2": "device_id_sensor_2", ...}`
3. Update the `.env` file with the values for your container registry
4. Build the entire solution by right-clicking on the `deployment.template.json` file and select `Build and push IoT Edge Solution`
5. Deploy the solution to your device by right-clicking on the `config/deployment.json` file, select `Create Deployment for Single device` and choose your IoT Edge device
6. Monitor the messages being sent to the Cloud by right-clicking on one of your sensor device registered in IoT hub from the VS Code IoT Edge Extension and select `Start Monitoring D2C Message`

Note: To stop Device to Cloud (D2C) monitoring, use the `Azure IoT Hub: Stop monitoring D2C messages` command from the Command Palette (Ctrl+Shift+P).
