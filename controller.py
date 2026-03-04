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
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d %H:%M:%S'
)
# Variables initialization
PortNum_PRIVATE_APP             = 256
NODENUM_BROADCAST               = 0xffffffff
RETRY_TIMEOUT                   = 10         # timeout to retry
MAX_RETRIES                     = 3

class State(Enum):
    IDLE                        = auto()  
    WAIT_CONFIG_RESP            = auto()
    WAIT_STATS                  = auto()
    WAIT_STATS_CLEARED          = auto()

class PacketType(Enum):
    PKGEN_CONFIG_REQ        = 0X01 
    PKGEN_CONFIG_RESP       = 0X02
    #PKGEN_DATA              = 0X03
    PKGEN_REPLY             = 0X04
    #STATS_GET_REQ           = 0X05
    STATS_GET_RESP          = 0X06
    STATS_CLEAR_REQ         = 0X03
    STATS_CLEAR_RESP        = 0X05

# --------- stats structure ----------
@dataclass
class LinkStats:
    """ Stats between each two nodes Link ."""
    nodeid:int                              = 0
    num_pkgen_data_sent:int                 = 0
    num_pkgen_reply_received:int            = 0
    num_pkgen_data_received_dm:int          = 0
    num_pkgen_data_received_broadcasts:int  = 0  
    rtt:int                                 = 0

# --------- Collector Controller ----------
class CollectorController():
    def __init__(self):
        self.devices = [
        0x52e99376, 0x49242450, 0xbf935464,
        0x6d4d1ba2, 0x9287389e, 0x33f7e0ed, 
        0x91d71daf, 0x7aa01783, 0x59d388e5, 
        0x31c0c4f1
        ]
        self.nodes = [
        0x6d4d1ba2, 0x9287389e, 0x33f7e0ed, 
        0x91d71daf, 0x7aa01783, 0x59d388e5,
        ]
        self.interface                      = meshtastic.serial_interface.SerialInterface()
        self.received_packets               = Queue()
        self.reset()
        pub.subscribe(self.on_receive, "meshtastic.receive")

     #----------initialization-----------------------
    def reset(self) -> None:
        self.state                           = State.IDLE
        self.retry_timeout                   = 0
        self.retries                         = 0
        self.stats_cleared_nodes             = set()
        self.num_sent_broadcasts_by_node     = {}
        self.network_stats                   = {}
        for A in self.nodes:
            self.network_stats[A]   = {}
            self.num_sent_broadcasts_by_node[A] = 0
            for B in self.nodes:
                if B == A:
                    continue
                self.network_stats[A][B] = LinkStats()

    # -----------helper functions--------------------
    def convert_id_to_hex(self, id):
        if(id.startswith("!")):
            return int(id[1:], 16)
        return id

    # -----------Commands--------------------
    def clear_stats_request(self, destination: int) -> None:
        """ Sends a clear_stats_request to the destination"""
        payload = bytes([PacketType.STATS_CLEAR_REQ.value])
        self.interface.sendData(data = payload,
                destinationId=destination,
                portNum= PortNum_PRIVATE_APP,
                wantAck=False)
        logging.info("Sent clear request to 0x%x ", destination)

    # -----------PROCESS INCOMING PACKETS--------------------

    def on_receive(self, packet, interface) -> None:
        """Callback invoked when a packet arrives"""
        portnum = packet["decoded"].get("portnum")
        if not(packet["decoded"]) or portnum != "PRIVATE_APP":
            return
        self.received_packets.put(packet)

    def process_all_packets(self) -> None:
        """ Process all packets currently in the queue """
        while not self.received_packets.empty():
            packet = self.received_packets.get()
            self.process_packet(packet)

    def process_packet(self, packet) -> None:
        """ processes the packets by calling the corresponding function"""
        # we received stats_response
        payload = packet["decoded"].get("payload")
        if(not payload or len(payload) < 1):
            return
        pkttype = payload[0]
        if pkttype == PacketType.CLEAR_STATS_RESP.value:
            packet_from = self.convert_id_to_hex(packet["fromId"])
            self.stats_cleared_nodes.add(packet_from)
            logging.info("Received clear response from 0x%x", packet_from)
        else:
            logging.info("Unknown command", pkttype)

    # -----------Timers--------------------
    def check_retry_timeout(self) -> bool:
        """ check if it is time to stop waiting for responses from all nodes"""
        current_time = time.monotonic()
        return current_time - self.retry_timeout >= RETRY_TIMEOUT

    # -----------Update Stats--------------------
    def clear_stats_broadcast(self) -> None:
        """ Sends request to clear the stats of all nodes """
        self.clear_stats_request(NODENUM_BROADCAST)
        self.retry_timeout = time.monotonic() 
        self.state = State.WAIT_STATS_CLEARED

    def clear_stats_unicast(self, destinations: set) ->None:
        """ Sends request to clear the stats of non cleared noded """
        for node in destinations:
            logging.info("Resend clear request to 0x%x", node)
            self.clear_stats_request(node)
        self.retry_timeout = time.monotonic()

    def handle_wait_stats_cleared_state(self, now: float) -> None:
        """ Handles the WAIT_STATS_CLEARED state """
        # All nodes have been cleared
        if self.stats_cleared_nodes == set(self.nodes):
            logging.info("All nodes cleared, resetting my stats now")
            self.reset()
            return
        #Not all nodes answered so retry again asking to clear stats after timeout
        if self.check_retry_timeout() and self.retries < MAX_RETRIES:
            self.retries += 1
            logging.info("Not All nodes cleared, num retries left %d ", MAX_RETRIES - self.retries )
            # who did not answer
            non_cleared_nodes = set(self.nodes) - self.stats_cleared_nodes
            self.clear_stats_unicast(non_cleared_nodes)
            return
        if self.retries >= MAX_RETRIES :
            logging.warning(" Max number of retries exceeded, exiting the program...")
            exit()

    def update_state_machine(self) -> None:
        """ Updates the states and call corresponding function """
        now = time.monotonic()
        if self.state == State.WAIT_STATS_CLEARED:
            self.handle_wait_stats_cleared_state(now)

    """ ----------MAIN loop---------------"""
    def run(self) -> None:
        while True:
            self.process_all_packets()
            self.update_state_machine()


# --------------RUN--------------------
if(__name__ == "__main__"):
    controller = CollectorController()
    controller.clear_stats_broadcast()
    controller.run()    