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
RETRY_TIMEOUT                   = 10         # timeout to retry to check
MAX_RETRIES                     = 3
MAX_RETRIES_STATS               = 10
NUMPKT                          = 1
PERIOD                          = 10
STATS_TIMEOUT                   = NUMPKT * PERIOD + 180     
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
    rtt:float                               = 0

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
        self.nodes = self.devices
        if scenario == DISASTER_RESPONSE:
            self.nodes      = self.devices[3:9]
        elif scenario == HIKING:
            self.nodes = self.devices[0:9]
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
        self.stats_cleared_nodes             = set()
        self.pkgen_responded_nodes           = set()
        self.stats_responded_nodes           = set()
        self.sent_pkgen_cmdids               = {}
        self.num_sent_broadcasts_by_node     = {}
        self.network_stats                   = {}
        
        self.clear_node_index                = 0
        self.clear_retries                   = 0

        self.pkgen_node_index                = 0
        self.pkgen_retries                   = 0

        self.stats_node_index                = 0
        self.stats_retries                   = 0

        for A in self.devices:
            self.network_stats[A]   = {}
            self.num_sent_broadcasts_by_node[A] = 0
            self.sent_pkgen_cmdids[A] = -1
            for B in self.devices:
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
        payload = packet["decoded"].get("payload")
        if(not payload or len(payload) < 1):
            return
        pkttype = payload[0]
        if pkttype in(
            PacketType.STATS_CLEAR_RESP.value,
            PacketType.PKGEN_CONFIG_RESP.value,
            PacketType.STATS_GET_RESP.value
        ):
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

    def _send_clear_stats_to_current_node(self) -> None:
        """ Sends request to clear the stats of all nodes """
        node = self.nodes[self.clear_node_index]
        self.send_clear_stats_request(node)
        self.retry_timeout = time.monotonic()

    def send_clear_stats_to_all(self)->None:
        """ Start sequential clear process with first node """
        self.clear_node_index = 0
        self.clear_retries    = 0
        self._send_clear_stats_to_current_node()
        self.state = State.WAIT_STATS_CLEARED

    def handle_wait_stats_cleared_state(self) -> bool:
        """ Handles the WAIT_STATS_CLEARED state """
        # All nodes have been cleared
        if self.clear_node_index >= len(self.nodes):
            logging.info("All nodes cleared")
            self.reset()
            return True
        current_node = self.nodes[self.clear_node_index]
        # current node to be cleared has been cleared, so move on to clear next node
        if current_node in self.stats_cleared_nodes:
            self.clear_node_index +=1
            self.clear_retries     =0
            if self.clear_node_index < len(self.nodes):
                self._send_clear_stats_to_current_node()
            return False
        # current_node did not answer so retry again asking to clear stats after timeout
        elif self.check_retry_timeout():
            if self.clear_retries < MAX_RETRIES:
                self.clear_retries += 1
                logging.info("node 0x%x did not clear, num retries left %d ",current_node, MAX_RETRIES - self.clear_retries)
                # who did not answer
                self._send_clear_stats_to_current_node()
                return False
            elif self.clear_retries >= MAX_RETRIES :
                logging.warning(" Max number of retries exceeded, exiting the program...")
                sys.exit()
        else:
            return False

    # ---------------------------------------------------
    # PKGEN
    # ---------------------------------------------------

    def _send_pkgen_request_to_current_node_disaster_response(self)-> None:
        """ Send PKGEN request to current node """
        node = self.nodes[self.pkgen_node_index]
        # cmdid is node index
        cmdid = self.pkgen_node_index
        self.sent_pkgen_cmdids[node] = cmdid   # save the cmdid of each node
        if node == LEADER_NODE:
            self.send_pkgen_request(node, cmdid, NODENUM_BROADCAST, False,            
            PERIOD, NUMPKT)
        else:
            # other nodes send DMs to the leader node
            self.send_pkgen_request(node, cmdid, LEADER_NODE, True,
            PERIOD, NUMPKT)
        self.retry_timeout = time.monotonic()

    def _send_pkgen_request_to_current_node_hiking(self)-> None:
        """ Send PKGEN request to current node """
        node = self.nodes[self.pkgen_node_index]
        # cmdid is node index
        cmdid = self.pkgen_node_index
        self.sent_pkgen_cmdids[node] = cmdid   # save the cmdid of each node
        i = self.pkgen_node_index
        if(i == len(self.nodes) - 1):
            self.send_pkgen_request(node, cmdid, self.nodes[0], True,            
            PERIOD, NUMPKT)
        else:
            self.send_pkgen_request(node, cmdid, self.nodes[i + 1], True,            
            PERIOD, NUMPKT)  
        self.retry_timeout = time.monotonic()

    def send_pkgen_request_per_scenario(self)->None:
        """ Start sequential clear process with first node """
        self.pkgen_node_index = 0
        self.pkgen_retries    = 0
        if self.scenario == DISASTER_RESPONSE:
            self._send_pkgen_request_to_current_node_disaster_response()
        elif self.scenario == HIKING:
            self._send_pkgen_request_to_current_node_hiking()
    
    def handle_wait_pkgen_resp_state(self) -> bool:
        """ Handles the WAIT_STATS_CLEARED state """
        # All nodes have been cleared
        if self.pkgen_node_index >= len(self.nodes):
            logging.info("All nodes sent back PKGEN_CONFIG_RESP")
            return True
        current_node = self.nodes[self.pkgen_node_index]
        # current node to be cleared has been cleared, so move on to clear next node
        if current_node in self.pkgen_responded_nodes:
            self.pkgen_node_index +=1
            self.pkgen_retries     =0
            if self.pkgen_node_index < len(self.nodes):
                if self.scenario == DISASTER_RESPONSE:
                    self._send_pkgen_request_to_current_node_disaster_response()
                elif self.scenario == HIKING:
                    self._send_pkgen_request_to_current_node_hiking()
            return False
        # current_node did not answer so retry again asking to clear stats after timeout
        if self.check_retry_timeout():
            if self.pkgen_retries < MAX_RETRIES:
                self.pkgen_retries += 1
                logging.info("node 0x%x did not send PKGEN_CONFIG_RESP, num retries left %d ",current_node, MAX_RETRIES - self.pkgen_retries)
                # send again
                if self.pkgen_node_index < len(self.nodes):
                    if self.scenario == DISASTER_RESPONSE:
                        self._send_pkgen_request_to_current_node_disaster_response()
                    elif self.scenario == HIKING:
                        self._send_pkgen_request_to_current_node_hiking()
                    return False
            elif self.pkgen_retries >= MAX_RETRIES :
                logging.warning(" Max number of retries exceeded, exiting the program...")
                sys.exit()
        else:
            return False

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
            rtt_sum = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2
            count = self.network_stats[A][nodeid].num_pkgen_reply_received
            self.network_stats[A][nodeid].rtt = rtt_sum / count if count > 0 else 0.0
        logging.info("=== Stats processing complete ===")

    def _send_stats_request_to_current_node(self) -> None:
        """ send STATS_GET_REQ"""
        node = self.nodes[self.stats_node_index]
        self.send_stats_request(node)
        self.retry_timeout = time.monotonic()
        logging.info("send stats request to 0x%x", node)

    def send_stats_request_to_all(self):
        """ send STATS_GET_REQ"""
        self.stats_node_index = 0
        self.stats_retries    = 0
        self._send_stats_request_to_current_node()

    def handle_wait_stats_resp(self) -> bool:
        """ Handles the WAIT_STATS_RESP state """
        # All nodes have been cleared
        if self.stats_node_index >= len(self.nodes):
            logging.info("All nodes sent their stats")
            return True
        current_node = self.nodes[self.stats_node_index]
        # current node to be cleared has been cleared, so move on to clear next node
        if current_node in self.stats_responded_nodes:
            self.stats_node_index +=1
            self.stats_retries     =0
            if self.stats_node_index < len(self.nodes):
                self._send_stats_request_to_current_node()
            return False
        # current_node did not answer so retry again asking to clear stats after timeout
        if self.check_retry_timeout():
            if self.stats_retries < MAX_RETRIES:
                self.stats_retries += 1
                logging.info("node 0x%x did not send stats, num retries left %d ",current_node, MAX_RETRIES - self.stats_retries)
                # send STATS_REQ again
                self._send_stats_request_to_current_node()
                return False
            elif self.stats_retries >= MAX_RETRIES :
                logging.warning(" Max number of retries exceeded, exiting the program...")
                sys.exit()
        else:
            return False

    # ---------------------------------------------------
    # State machine update
    # ---------------------------------------------------
    
    def update_state_machine(self) -> None:
        """ Updates the states and call corresponding function """
        if self.state == State.WAIT_STATS_CLEARED:
            if self.handle_wait_stats_cleared_state():
                self.state = State.STATS_CLEARED 
        elif self.state == State.STATS_CLEARED:
            self.send_pkgen_request_per_scenario()
            self.state = State.WAIT_PKGEN_RESP
        elif self.state == State.WAIT_PKGEN_RESP:
            if self.handle_wait_pkgen_resp_state():
                self.state = State.WAIT_TO_ASK_FOR_STATS
        elif self.state == State.WAIT_TO_ASK_FOR_STATS:
            # Wait before requesting stats
            if time.monotonic() >= self.stats_timeout :
                logging.info("time to send stats")
                self.state = State.ASK_FOR_STATS 
        elif self.state == State.ASK_FOR_STATS:
            self.send_stats_request_to_all()
            self.retry_timeout = time.monotonic()
            self.state   =  State.WAIT_STATS_RESP
        elif self.state == State.WAIT_STATS_RESP:
            if self.handle_wait_stats_resp():
                self.state = State.SAVE_TO_JSON
        elif self.state == State.SAVE_TO_JSON:
            self.save_to_json("data/stats.json")
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
        for node in self.devices:
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
            try:
                self.process_all_packets()
                self.update_state_machine()
                time.sleep(0.05)
            except Exception as e:
                logging.error(f"Error in run() : {e}", exc_info=True)

# -------------------------------------------------------
# Program entry point
# -------------------------------------------------------
if(__name__ == "__main__"):
    if(len(sys.argv) < 2):
        print("Choose Scenario: disaster_response or hiking")
    scenario   = sys.argv[1]
    controller = CollectorController(scenario)
    print(controller.received_packets.qsize())
    controller.send_clear_stats_to_all()
    controller.run()    