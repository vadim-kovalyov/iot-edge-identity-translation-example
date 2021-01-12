# Iot Edge Identity Translation Sample Module
This repository contains code for a sample module that can perform identity translation scenarios.

## Pre-requisites <TODO: Clean up section>
- Have a Raspberry pi. <TODO: add guide to setup a raspberry pi>
- Install IoT Edge RC 1.2.

```
    sudo wget -O libiothsm-std_1.2.0_rc1_armhf.deb "https://github.com/Azure/azure-iotedge/releases/download/1.2.0-rc1/libiothsm-std_1.2.0_rc1-1-1_debian9_armhf.deb"
    sudo wget -O iotedge_1.2.0_rc1_armhf.deb "https://github.com/Azure/azure-iotedge/releases/download/1.2.0-rc1/iotedge_1.2.0_rc1-1_debian9_armhf.deb"
    sudo dpkg -i libiothsm-std_1.2.0_rc1_armhf.deb
    sudo dpkg -i iotedge_1.2.0_rc1_armhf.deb
```

## Building
You can build a module container and deploy it to your IoT Edge runtime.
```

```

## Running
To run the module - deploy it to your IoT Edge runtime. Here is an example of deployment manifest. 
NONE: Identity translation requires edgeHub version > 1.2
```

```