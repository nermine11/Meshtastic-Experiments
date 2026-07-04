# Meshtastic-Experiments

This repository contains experiments conducted with **Meshtastic**, an open-source, decentralized mesh networking platform based on LoRa technology.

## Overview

The goal of this project is to evaluate Meshtastic's capabilities for low-power, long-range wireless communication and assess its suitability for real-world scenarios. The experiments were implemented using **Seeed Studio XIAO nRF52840** nodes and focused on **Disaster Response** and **Hiking** use cases.

## Hardware

- Seeed Studio XIAO nRF52840
- Meshtastic firmware

## Features

- Configured and deployed a decentralized LoRa mesh network.
- Implemented automated experiment orchestration using Python.
- Added firmware support for experiment configuration and statistics collection.
- Evaluated packet delivery and network behavior under different deployment scenarios.

## Experiment Scenarios

### 1. Disaster Response

**Network topology**

- Devices **1–3**: Routers
- Devices **4–9**: End nodes
- Device **10**: Controller connected to the computer

**Workflow**

1. Clear statistics on all nodes.
2. Wait for confirmation from each device.
3. Retry every 10 seconds (maximum 3 retries).
4. Configure packet generation (PKGEN) on all end nodes.
5. Nodes wait 10 minutes before starting transmission.

| Devices | Destination | Reply | Period | Packets |
|---------|-------------|-------|--------|---------|
| 4–8 | Node 9 | Yes | 60 s | 100 |
| 9 | Broadcast | No | 60 s | 100 |

After **2 hours**, the controller retrieves statistics from every node, retrying requests until a response is received (maximum 10 retries).

---

### 2. Hiking

**Network topology**

- Devices **1–10**: End nodes
- Device **10**: Controller connected to the computer

The controller performs the same workflow:
- Clear statistics
- Configure PKGEN
- Wait for transmissions
- Retrieve experiment statistics

Traffic pattern:

| Source | Destination |
|--------|-------------|
| 1 | 2 |
| 2 | 3 |
| 3 | 4 |
| 4 | 5 |
| 5 | 6 |
| 6 | 7 |
| 7 | 8 |
| 8 | 9 |
| 9 | 1 |

All transmissions:
- Reply enabled
- Period: **60 s**
- Packets: **100**

## Repository Structure

### Firmware

The firmware includes an **Experiment Module** responsible for:

- Clearing node statistics
- Configuring packet generation
- Collecting experiment statistics
- Handling controller requests

Relevant files:

- `firmware/src/modules/ExperimentModule.cpp`
- `firmware/src/modules/ExperimentModule.h`

### Controller

A Python controller automates the experiments by:

- Configuring all nodes
- Managing retries and timeouts
- Starting experiments
- Collecting statistics
- Exporting experiment results

**Controller script**

`controller.py`

## Technologies

- Meshtastic
- LoRa
- Seeed Studio XIAO nRF52840
- Python
- C++
- Embedded Systems
- Wireless Mesh Networking
