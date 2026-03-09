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

# -------------------------------------------------------
# Logging configuration
# -------------------------------------------------------

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d %H:%M:%S'
)
# -------------------------------------------------------
# Global constants
# -------------------------------------------------------

PortNum_PRIVATE_APP             = 256
NODENUM_BROADCAST               = 0xffffffff
RETRY_TIMEOUT                   = 300         # timeout to retry to check
MAX_RETRIES                     = 3
MAX_RETRIES_STATS               = 10
NUMPKT                          = 50
PERIOD                          = 60
STATS_TIMEOUT                   = NUMPKT * PERIOD + 600 + 600     
CONTROLLER_NODE                 = 0x31c0c4f1
LEADER_NODE                     = 0x59d388e5
DISASTER_RESPONSE               = "disaster_response"
HIKING                          = "hiking"

# -------------------------------------------------------
# Experiment states
# -------------------------------------------------------

class State(Enum):
    IDLE                        = auto()
    WAIT_STATS_CLEARED          = auto()
    WAIT_PKGEN_RESP             = auto()
    ASK_FOR_STATS               = auto()
    WAIT_STATS_RESP             = auto()
    STATS_CLEARED               = auto()
    WAIT_TO_ASK_FOR_STATS       = auto()
    SAVE_TO_JSON                = auto()

# -------------------------------------------------------
# Packet types used in the protocol
# -------------------------------------------------------s

class PacketType(Enum):
    PKGEN_CONFIG_REQ        = 0X01 
    PKGEN_CONFIG_RESP       = 0X02
    PKGEN_DATA              = 0X03
    PKGEN_REPLY             = 0X04
    STATS_GET_REQ           = 0X05
    STATS_GET_RESP          = 0X06
    STATS_CLEAR_REQ         = 0X07
    STATS_CLEAR_RESP        = 0X08

# -------------------------------------------------------
# Data structure representing link statistics
# -------------------------------------------------------

@dataclass
class LinkStats:
    """ Stats between each two nodes Link ."""
    num_pkgen_data_sent:int                 = 0
    num_pkgen_reply_received:int            = 0
    num_pkgen_data_received_dm:int          = 0
    num_pkgen_data_received_broadcasts:int  = 0  
    rtt:int                                 = 0

# -------------------------------------------------------
# Collector Controller
# -------------------------------------------------------

class CollectorController():
    # ---------------------------------------------------
    # Constructor
    # --------------------------------------------------
    def __init__(self, scenario):
        self.devices = [
        0x52e99376, 0x49242450, 0xbf935464,
        0x6d4d1ba2, 0x9287389e, 0x33f7e0ed, 
        0x91d71daf, 0x7aa01783, 0x59d388e5,
        0x31c0c4f1]
        self.scenario   = scenario
        # nodes participating in the experiment
        self.nodes      = self.devices[3:9]
        # Meshtastic serial interface
        self.interface  = meshtastic.serial_interface.SerialInterface()
        # initialize internal variables
        self.reset()
        # subscribe to incoming packets
        pub.subscribe(self.on_receive, "meshtastic.receive")


    # ---------------------------------------------------
    # Reset experiment variables
    # ---------------------------------------------------

    def reset(self) -> None:
        """
        Initializes or resets all experiment state variables.

        This clears statistics, response tracking,
        retry counters and timeouts.
        """
        self.received_packets                = Queue()
        self.state                           = State.IDLE
        self.retry_timeout                   = 0
        self.stats_timeout                   = time.monotonic() + STATS_TIMEOUT

        self.retries                         = 0
        self.stats_cleared_nodes             = set()
        self.pkgen_responded_nodes           = set()
        self.stats_responded_nodes           = set()
        self.sent_pkgen_cmdids               = {}
        self.num_sent_broadcasts_by_node     = {}
        self.network_stats                   = {}
        
        for A in self.nodes:
            self.network_stats[A]   = {}
            self.num_sent_broadcasts_by_node[A] = 0
            self.sent_pkgen_cmdids[A] = -1
            for B in self.nodes:
                if B == A:
                    continue
                self.network_stats[A][B] = LinkStats()

    # ---------------------------------------------------
    # Convert node ID string to integer
    # ---------------------------------------------------

    def convert_id_to_hex(self, id):
        if(id.startswith("!")):
            return int(id[1:], 16)
        return id

    # ---------------------------------------------------
    # Packet receive callback
    # ---------------------------------------------------
    def on_receive(self, packet, interface) -> None:
        """Callback invoked when a packet arrives"""
        decoded = packet.get("decoded")
        if not decoded:
            return
        portnum = decoded.get("portnum")
        if portnum != "PRIVATE_APP":
            return
        print("got packet",packet)
        print(time.monotonic())
        self.received_packets.put(packet)

    # ---------------------------------------------------
    # Process received packets
    # ---------------------------------------------------
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
        packet_source = self.convert_id_to_hex(packet["fromId"])
        if pkttype == PacketType.STATS_CLEAR_RESP.value:
            self.stats_cleared_nodes.add(packet_source)
            logging.info("Received clear response from 0x%x", packet_source) 
        elif pkttype == PacketType.PKGEN_CONFIG_RESP.value:
            logging.info("Received PKGEN response %d from 0x%x", payload[1], 
            packet_source)    
            if packet_source in self.sent_pkgen_cmdids and payload[1] == self.sent_pkgen_cmdids[packet_source]:
                self.pkgen_responded_nodes.add(packet_source)
            else:
                logging.warning("Unexpected cmdid from 0x%x should be %d", packet_source, 
                self.sent_pkgen_cmdids[packet_source])
        elif pkttype == PacketType.STATS_GET_RESP.value:
            logging.info("Received stats %d from 0x%x", payload[1], 
            packet_source)    
            self.stats_responded_nodes.add(packet_source)
            self.process_stats(packet)
        else:
            logging.info("Unknown command %s", pkttype)

    # ---------------------------------------------------
    # Send requests
    # ---------------------------------------------------

    def send_clear_stats_request(self, destination: int) -> None:
        """ Sends a clear_stats_request to the destination"""
        payload = bytes([PacketType.STATS_CLEAR_REQ.value])
        self.interface.sendData(data = payload,
                destinationId=destination,
                portNum= PortNum_PRIVATE_APP,
                wantAck=False)
        logging.info("Sent clear request to 0x%x ", destination)

    def send_pkgen_request(self, destination: int , cmdid: int,
    pkgen_destination:int, do_reply: bool,
    period: int, numpkt:int)-> None:
        """  PKGEN_CONFIG_REQUEST packet """
        payload     = bytearray()
        # 1 byte : pkttype
        payload.append(PacketType.PKGEN_CONFIG_REQ.value)
        # 1 byte : cmdid
        payload.append(cmdid)
        #  4 bytes: pkgen_destination
        payload += (pkgen_destination.to_bytes(4, 'little'))
        #  1 byte: do_reply
        if do_reply :
            do_reply_byte = 0x01
        else:
            do_reply_byte = 0x00
        payload.append(do_reply_byte)
        #  2 bytes: period
        payload += (period.to_bytes(2, 'little'))
        #  2 bytes: numpkt
        payload += (numpkt.to_bytes(2, 'little'))
        # send the data
        self.interface.sendData(data = bytes(payload),
                destinationId=destination,
                portNum= PortNum_PRIVATE_APP,
                wantAck=False)
        logging.info("Sent pkgen request to 0x%x ", destination)

    def send_pkgen_request_disaster_response(self, period: int, numpkt:int)-> None:
        cmdid = 0
        for node in self.nodes:
            cmdid += 1
            # leader node broadcasts 
            if node == LEADER_NODE:
                self.send_pkgen_request(node, cmdid, NODENUM_BROADCAST, False,            
                period, numpkt)
            else:
                # other nodes send DMs to the leader node
                self.send_pkgen_request(node, cmdid, LEADER_NODE, True,
                period, numpkt)
            self.sent_pkgen_cmdids[node] = cmdid   # save the cmdid of each node

    def send_stats_request(self, destination):
        """ send STATS_GET_REQ"""
        payload = bytes([PacketType.STATS_GET_REQ.value])
        self.interface.sendData(data = payload,
            destinationId=destination,
            portNum= PortNum_PRIVATE_APP,
            wantAck=False)

    # ---------------------------------------------------
    # Retry Timer
    # ---------------------------------------------------

    def check_retry_timeout(self) -> bool:
        """ check if it is time to stop waiting for responses from all nodes"""
        current_time = time.monotonic()
        return current_time - self.retry_timeout >= RETRY_TIMEOUT

    # ---------------------------------------------------
    # Clear stats
    # ---------------------------------------------------

    def send_clear_stats_to_all(self) -> None:
        """ Sends request to clear the stats of all nodes """
        for node in self.nodes:
            self.send_clear_stats_request(node)
        self.retry_timeout = time.monotonic()             # set timeout in case we have to retry
        self.state = State.WAIT_STATS_CLEARED             # change state 

    def resend_clear_stats(self, destinations: set) -> None:
        """ Resends request to clear the stats of non cleared noded """
        for node in destinations:
            logging.info("Resend clear request to 0x%x", node)
            self.send_clear_stats_request(node)

    def handle_wait_stats_cleared_state(self) -> bool:
        """ Handles the WAIT_STATS_CLEARED state """
        # All nodes have been cleared
        if self.stats_cleared_nodes == set(self.nodes):
            logging.info("All nodes cleared")
            self.reset()
            return True
        #Not all nodes answered so retry again asking to clear stats after timeout
        if self.check_retry_timeout() and self.retries < MAX_RETRIES:
            self.retries += 1
            logging.info("Not All nodes cleared, num retries left %d ", MAX_RETRIES - self.retries )
            # who did not answer
            non_cleared_nodes = set(self.nodes) - self.stats_cleared_nodes
            self.resend_clear_stats(non_cleared_nodes)
            self.retry_timeout = time.monotonic()
            return False
        if self.retries >= MAX_RETRIES :
            logging.warning(" Max number of retries exceeded, exiting the program...")
            exit()

    # ---------------------------------------------------
    # PKGEN
    # ---------------------------------------------------

    def resend_pkgen(self, destinations: set, destination:int, do_reply: bool,
    period: int, numpkt:int) -> None:
        for node in destinations:
            cmdid = self.sent_pkgen_cmdids[node]
            self.send_pkgen_request(node, cmdid, destination,
            do_reply, period, numpkt)

    def handle_wait_pkgen_resp_state(self) -> bool:
        """ Configure PKGEN on all nodes (not routers) """
        # All nodes sent back PKGEN_CONFIG_RESP
        if self.pkgen_responded_nodes == set(self.nodes):
            logging.info("All nodes sent back PKGEN_CONFIG_RESP")
            return True
        #Not all nodes answered so retry again sending PKGEN
        if self.check_retry_timeout() and self.retries < MAX_RETRIES:
            self.retries += 1
            logging.info("Not All nodes sent back PKGEN_CONFIG_RESP, num retries left %d ", MAX_RETRIES - self.retries )
            non_responded_nodes = set(self.nodes) - self.pkgen_responded_nodes
            self.resend_pkgen(non_responded_nodes, LEADER_NODE, True, PERIOD, NUMPKT)
            self.retry_timeout = time.monotonic()
            return False
        if self.retries >= MAX_RETRIES :
            logging.warning(" Max number of retries exceeded, exiting the program...")
            exit()

    # ---------------------------------------------------
    # STATS
    # ---------------------------------------------------
    def process_stats(self, packet)-> None:
        """ Save the received stats locally """
        payload = packet["decoded"]["payload"]
        # A is the node sending this stat packet
        A       = self.convert_id_to_hex(packet["fromId"])
        # 1 byte pkktype
        offset = 1
        # 2 bytes num_broadcasts_sent
        self.num_sent_broadcasts_by_node[A] = int.from_bytes(payload[offset: offset + 2], 'little')
        offset +=2
        for _ in range(len(self.devices) -1):
            nodeid = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4
            # make sure the nested dictionary exists
            if A not in self.network_stats:
                self.network_stats[A] = {}
            if nodeid not in self.network_stats[A]:
                self.network_stats[A][nodeid] = LinkStats()
            # 2 bytes num_pkgen_data_sent
            self.network_stats[A][nodeid].num_pkgen_data_sent = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2
            # 2 bytes num_pkgen_reply_received  
            self.network_stats[A][nodeid].num_pkgen_reply_received = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2        
            # 2 bytes   num_pkgen_data_received_dm   
            self.network_stats[A][nodeid].num_pkgen_data_received_dm = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2                
            # 2 bytes  num_pkgen_data_received_broadcasts
            self.network_stats[A][nodeid].num_pkgen_data_received_broadcasts = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2
            #2 bytes rtt                
            self.network_stats[A][nodeid].rtt = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2
        logging.info("=== Stats processing complete ===")

    def send_stats_request_to_all(self):
        """ send STATS_GET_REQ"""
        for node in self.nodes:
            logging.info("send stats request to 0x%x", node)
            self.send_stats_request(node)

    def resend_stats_request(self, destinations:set)-> None:
        """ Resends get_stats_req to the nodes that did not respond """
        for node in destinations:
            logging.info("Resend stats request to 0x%x", node)
            self.send_stats_request(node)

    def handle_wait_stats_resp(self) -> bool:
        """ Handles the WAIT_STATS_RESP state """
        # All nodes sent STATS_GET_RESP
        if self.stats_responded_nodes == set(self.nodes):
            logging.info("All nodes sent their stats")
            return True
        #Not all nodes answered so retry again asking to send stats after timeout
        if self.check_retry_timeout() and self.retries < MAX_RETRIES_STATS:
            self.retries += 1
            logging.info("Not All nodes sent their, num retries left %d ", MAX_RETRIES_STATS - self.retries )
            # who did not answer
            no_stats_nodes = set(self.nodes) - self.stats_responded_nodes
            self.resend_stats_request(no_stats_nodes)
            self.retry_timeout = time.monotonic()
            return False
        if self.retries >= MAX_RETRIES_STATS :
            logging.warning(" Max number of retries exceeded, exiting the program...")
            exit()

    # ---------------------------------------------------
    # State machine update
    # ---------------------------------------------------
    
    def update_state_machine(self) -> None:
        """ Updates the states and call corresponding function """
        if self.state == State.WAIT_STATS_CLEARED:
            if self.handle_wait_stats_cleared_state():
                self.state = State.STATS_CLEARED 
                self.retries = 0
        elif self.state == State.STATS_CLEARED:
            if self.scenario == DISASTER_RESPONSE :
                self.send_pkgen_request_disaster_response(PERIOD, NUMPKT)
                self.retry_timeout = time.monotonic()
                self.state = State.WAIT_PKGEN_RESP
        elif self.state == State.WAIT_PKGEN_RESP:
            if self.handle_wait_pkgen_resp_state():
                self.retries = 0
                self.state = State.WAIT_TO_ASK_FOR_STATS
        elif self.state == State.WAIT_TO_ASK_FOR_STATS:
            # Wait before requesting stats
            if time.monotonic() >= self.stats_timeout :
                logging.info("time to send stats")
                self.state = State.ASK_FOR_STATS 
        elif self.state == State.ASK_FOR_STATS:
            self.send_stats_request_to_all()
            self.retry_timeout = time.monotonic()
            self.retries = 0
            self.state   =  State.WAIT_STATS_RESP
        elif self.state == State.WAIT_STATS_RESP:
            if self.handle_wait_stats_resp():
                self.retries = 0
                self.state = State.SAVE_TO_JSON
        elif self.state == State.SAVE_TO_JSON:
            self.save_to_json("stats.json")
            exit()
    
    # ---------------------------------------------------
    # Save to json
    # ---------------------------------------------------

    def save_to_json(self, filename) -> None:
        """
            Save collected stats to a JSON file.
            Format:
            {
                "node_id": {
                    "num_broadcasts_sent": int,
                    "links": [
                        {
                            "neighbor": neighbor_id,
                            "num_pkgen_data_sent": int,
                            "num_pkgen_reply_received": int,
                            "num_pkgen_data_received_dm": int,
                            "num_pkgen_data_received_broadcasts": int,
                            "rtt": int
                        },
                        ...
                    ]
                },
                ...
            }
        """
        output = {}
        for node in self.nodes:
            node_entry = {
                "num_broadcasts_sent": self.num_sent_broadcasts_by_node.get(node, 0),
                "links": []
            }
            if node in self.network_stats:
                for neighbor, stats in self.network_stats[node].items():
                    # stats is a LinkStats object
                    link = {
                        "neighbor": neighbor,
                        "num_pkgen_data_sent": stats.num_pkgen_data_sent,
                        "num_pkgen_reply_received": stats.num_pkgen_reply_received,
                        "num_pkgen_data_received_dm": stats.num_pkgen_data_received_dm,
                        "num_pkgen_data_received_broadcasts": stats.num_pkgen_data_received_broadcasts,
                        "rtt": stats.rtt
                    }
                    node_entry["links"].append(link)
                output[str(node)] = node_entry
        # Save to Json
        with open(filename, "w") as f:
            json.dump(output, f, indent=4)
        logging.info(f"Stats saved to {filename}")

    # ---------------------------------------------------
    # MAIN loop
    # ---------------------------------------------------
    def run(self) -> None:
        while True:
            self.process_all_packets()
            self.update_state_machine()
            time.sleep(0.05)

# -------------------------------------------------------
# Program entry point
# -------------------------------------------------------
if(__name__ == "__main__"):
    if(len(sys.argv) < 2):
        print("Choose Scenario: disaster_response or hiking")
    scenario   = sys.argv[1]
    controller = CollectorController(scenario)
    controller.send_clear_stats_to_all()
    controller.run()    