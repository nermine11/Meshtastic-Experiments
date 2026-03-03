import meshtastic.serial_interface
from pubsub import pub
from queue import Queue
from dataclasses import dataclass
import time
import json
import sys
import os
import random
from enum import Enum, auto

# Variables initialization
PortNum_PRIVATE_APP             = 256
NB_NODES                        = 6
NODENUM_BROADCAST               = 0xffffffff
HOUR                            = 3600
STATS_INTERVAL                  = 900         #15 mins
WAIT_PHASE_TIMEOUT              = 60          # safety timeout per state
MAX_RETRIES                     = 1
class State(Enum):
    IDLE                        = auto()  
    WAIT_CONFIG                 = auto()
    WAIT_STATS                  = auto()
    WAIT_STATS_CLEARED          = auto()

class Commands(Enum):
    PKGEN_CONFIG_REQ        = 0X01 
    STATS_REQ               = 0X02
    CLEAR_STATS_REQ         = 0X03

class (Enum):
    STATS_RESP              = 0X04
    PKGEN_CONFIG_RESP       = 0X05
    CLEAR_STATS_RESP        = 0X06

# --------- stats structure ----------
@dataclass
class LinkStats:
    """ Stats between each two nodes Link ."""
    sent_dms:int                = 0
    received_dms:int            = 0
    received_broadcasts:int     = 0
    rtt                         = 0 
@dataclass
class ExperimentStats:
    """ Stats between each two nodes Link ."""
    timestamp:int                  = 0
    sent_broadcasts:int            = 0
    network_stats: dict            = {}

# --------- Collector Controller ----------
class CollectorController():
    def __init__(self):
        self.nodes = {
        1227105360, 3214103652, 1833769890, 
        2458335390, 871882989, 2446794159}
        self.interface                      = meshtastic.serial_interface.SerialInterface()
        self.received_packets               = Queue()
        self.reset()
        pub.subscribe(self.on_receive, "meshtastic.receive.text")
    
    #----------initialization-----------------------
    def reset(self):
        self.state                           = State.IDLE
        self.pkgen_config_responsed_nodes    = set()
        self.stats_responded_nodes           = set()
        self.stats_cleared_nodes             = set()
        self.retries                         = 0
        self.pkgen_start_time                = time.monotonic()
        self.phase_start_time                = time.monotonic()
        self.stats_start_time                = time.monotonic()
        self.cleared_nodes                   = set(self.nodes)
        self.sent_broadcasts                 = {}
        self.network_stats                   = {}
        for A in self.nodes:
            self.network_stats[A]   = {}
            self.sent_broadcasts[A] = 0
            for B in self.nodes:
                if B == A:
                    continue
                self.network_stats[A][B] = LinkStats()


    def process_stats(self, packet):
        payload = packet["decoded"]["payload"]
        A       = packet["fromId"]
        offset = 1
        self.sent_broadcasts[A] = int.from_bytes(payload[offset: offset + 4], 'little')
        offset +=4
        for _ in range(NB_NODES - 1):
            node_id = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4
            if(node_id == A):
                continue
            self.network_stats[A][node_id].sent_dms = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4
            self.network_stats[A][node_id].received_dms = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4
            self.network_stats[A][node_id].received_broadcasts = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4
            self.network_stats[A][node_id].rtt = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4

