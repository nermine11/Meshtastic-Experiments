import meshtastic.serial_interface
from pubsub import pub
from dataclasses import dataclass
import time
import json
import sys
import logging
from enum import Enum, auto

# -------------------------------------------------------
# Logging configuration
# -------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d %H:%M:%S'
)

# -------------------------------------------------------
# Global constants used in the experiment
# -------------------------------------------------------

PortNum_PRIVATE_APP = 256
NODENUM_BROADCAST = 0xffffffff

RETRY_TIMEOUT = 300
MAX_RETRIES = 3
MAX_RETRIES_STATS = 10

NUMPKT = 100
PERIOD = 60

STATS_TIMEOUT = NUMPKT * PERIOD + 600 + 1800

CONTROLLER_NODE = 0x31c0c4f1
LEADER_NODE = 0x59d388e5

DISASTER_RESPONSE = "disaster_response"
HIKING = "hiking"

# -------------------------------------------------------
# Experiment states
# -------------------------------------------------------

class State(Enum):
    """Represents the current state of the experiment controller"""

    IDLE = auto()
    WAIT_STATS_CLEARED = auto()
    WAIT_PKGEN_RESP = auto()
    ASK_FOR_STATS = auto()
    WAIT_STATS_RESP = auto()
    STATS_CLEARED = auto()
    WAIT_TO_ASK_FOR_STATS = auto()
    SAVE_TO_JSON = auto()


# -------------------------------------------------------
# Packet types used in the private protocol
# -------------------------------------------------------

class PacketType(Enum):
    PKGEN_CONFIG_REQ = 0X01
    PKGEN_CONFIG_RESP = 0X02
    PKGEN_DATA = 0X03
    PKGEN_REPLY = 0X04
    STATS_GET_REQ = 0X05
    STATS_GET_RESP = 0X06
    STATS_CLEAR_REQ = 0X07
    STATS_CLEAR_RESP = 0X08


# -------------------------------------------------------
# Data structure representing link statistics
# -------------------------------------------------------

@dataclass
class LinkStats:
    """Statistics describing traffic between two nodes"""

    num_pkgen_data_sent: int = 0
    num_pkgen_reply_received: int = 0
    num_pkgen_data_received_dm: int = 0
    num_pkgen_data_received_broadcasts: int = 0
    rtt: int = 0


# -------------------------------------------------------
# Collector Controller
# -------------------------------------------------------

class CollectorController():

    # ---------------------------------------------------
    # Constructor
    # ---------------------------------------------------

    def __init__(self, scenario):
        """
        Initializes the collector controller.

        Creates the serial interface, loads node list,
        initializes experiment variables, and subscribes
        to Meshtastic packet events.
        """

        self.devices = [
            0x52e99376, 0x49242450, 0xbf935464,
            0x6d4d1ba2, 0x9287389e, 0x33f7e0ed,
            0x91d71daf, 0x7aa01783, 0x59d388e5,
            0x31c0c4f1]

        self.scenario = scenario

        # nodes participating in experiment
        self.nodes = self.devices[3:9]

        # Meshtastic serial interface
        self.interface = meshtastic.serial_interface.SerialInterface()

        # initialize internal variables
        self.reset()

        # subscribe to incoming packets
        pub.subscribe(self.on_receive, "meshtastic.receive")


    # ---------------------------------------------------
    # Reset experiment variables
    # ---------------------------------------------------

    def reset(self):
        """
        Initializes or resets all experiment state variables.

        This clears statistics, response tracking,
        retry counters and timeouts.
        """

        self.state = State.IDLE
        self.retry_timeout = 0
        self.stats_timeout = 0
        self.retries = 0

        self.stats_cleared_nodes = set()
        self.pkgen_responded_nodes = set()
        self.stats_responded_nodes = set()

        self.sent_pkgen_cmdids = {}
        self.num_sent_broadcasts_by_node = {}
        self.network_stats = {}

        for A in self.nodes:

            self.network_stats[A] = {}
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
        """
        Converts Meshtastic node ID format (!xxxx)
        into an integer node number.
        """

        if id.startswith("!"):
            return int(id[1:], 16)
        return id


    # ---------------------------------------------------
    # Packet receive callback
    # ---------------------------------------------------

    def on_receive(self, packet, interface):
        """
        Callback triggered automatically whenever
        a packet is received from the radio.

        This function filters packets and processes
        them immediately.

        NOTE: This replaces the old queue-based
        polling architecture.
        """

        decoded = packet.get("decoded")

        if not decoded:
            return

        portnum = decoded.get("portnum")

        if portnum != "PRIVATE_APP":
            return

        # process packet immediately
        self.process_packet(packet)

        # update experiment state machine
        self.update_state_machine()


    # ---------------------------------------------------
    # Process a received packet
    # ---------------------------------------------------

    def process_packet(self, packet):
        """
        Parses the payload of an incoming packet
        and updates the controller state accordingly.

        Handles:
        - stats clear responses
        - pkgen configuration responses
        - statistics responses
        """

        payload = packet["decoded"].get("payload")

        if not payload or len(payload) < 1:
            return

        pkttype = payload[0]
        packet_source = self.convert_id_to_hex(packet["fromId"])

        if pkttype == PacketType.STATS_CLEAR_RESP.value:

            self.stats_cleared_nodes.add(packet_source)
            logging.info("Received clear response from 0x%x", packet_source)

        elif pkttype == PacketType.PKGEN_CONFIG_RESP.value:

            logging.info("Received PKGEN response from 0x%x", packet_source)

            if payload[1] == self.sent_pkgen_cmdids[packet_source]:
                self.pkgen_responded_nodes.add(packet_source)

        elif pkttype == PacketType.STATS_GET_RESP.value:

            logging.info("Received stats from 0x%x", packet_source)

            self.stats_responded_nodes.add(packet_source)

            self.process_stats(packet)


    # ---------------------------------------------------
    # Process statistics packet
    # ---------------------------------------------------

    def process_stats(self, packet):
        """
        Extracts link statistics from a received
        STATS_GET_RESP packet and stores them locally.
        """

        payload = packet["decoded"]["payload"]

        A = self.convert_id_to_hex(packet["fromId"])

        offset = 1

        self.num_sent_broadcasts_by_node[A] = int.from_bytes(payload[offset: offset + 2], 'little')
        offset += 2

        for _ in range(len(self.nodes) - 1):

            nodeid = int.from_bytes(payload[offset: offset + 4], 'little')
            offset += 4

            if nodeid == A:
                continue

            if nodeid not in self.network_stats[A]:
                self.network_stats[A][nodeid] = LinkStats()

            stats = self.network_stats[A][nodeid]

            stats.num_pkgen_data_sent = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2

            stats.num_pkgen_reply_received = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2

            stats.num_pkgen_data_received_dm = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2

            stats.num_pkgen_data_received_broadcasts = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2

            stats.rtt = int.from_bytes(payload[offset: offset + 2], 'little')
            offset += 2

        logging.info("Stats processing complete")


    # ---------------------------------------------------
    # Timer handler
    # ---------------------------------------------------

    def check_timers(self):
        """
        Periodically checks timeout conditions.

        This replaces the old polling-based
        state machine execution.
        """

        if self.state == State.WAIT_STATS_CLEARED:
            self.handle_wait_stats_cleared_state()

        elif self.state == State.WAIT_PKGEN_RESP:
            self.handle_wait_pkgen_resp_state()

        elif self.state == State.WAIT_TO_ASK_FOR_STATS:

            if time.monotonic() >= self.stats_timeout:
                self.state = State.ASK_FOR_STATS

        elif self.state == State.WAIT_STATS_RESP:
            self.handle_wait_stats_resp()


    # ---------------------------------------------------
    # State machine update
    # ---------------------------------------------------

    def update_state_machine(self):
        """
        Updates the experiment state machine.

        Transitions between experiment phases such as:
        clearing stats, configuring packet generation,
        collecting statistics, and saving results.
        """

        if self.state == State.STATS_CLEARED:

            if self.scenario == DISASTER_RESPONSE:
                self.send_pkgen_request_disaster_response(PERIOD, NUMPKT)
                self.retry_timeout = time.monotonic()
                self.state = State.WAIT_PKGEN_RESP

        elif self.state == State.ASK_FOR_STATS:

            self.send_stats_request_to_all()
            self.retry_timeout = time.monotonic()
            self.retries = 0
            self.state = State.WAIT_STATS_RESP

        elif self.state == State.SAVE_TO_JSON:

            self.save_to_json("stats.json")
            exit()


    # ---------------------------------------------------
    # Save experiment results
    # ---------------------------------------------------

    def save_to_json(self, filename):
        """
        Saves collected network statistics
        into a JSON file for later analysis.
        """

        output = {}

        for node in self.nodes:

            node_entry = {
                "num_broadcasts_sent": self.num_sent_broadcasts_by_node.get(node, 0),
                "links": []
            }

            for neighbor, stats in self.network_stats[node].items():

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

        with open(filename, "w") as f:
            json.dump(output, f, indent=4)

        logging.info(f"Stats saved to {filename}")


    # ---------------------------------------------------
    # Main controller loop
    # ---------------------------------------------------

    def run(self):
        """
        Main loop of the controller.

        This loop no longer processes packets.
        It only checks timers periodically.

        Packet processing happens immediately
        in the on_receive() callback.
        """

        while True:

            self.check_timers()

            # small sleep prevents CPU busy loop
            time.sleep(0.05)


# -------------------------------------------------------
# Program entry point
# -------------------------------------------------------

if __name__ == "__main__":

    while len(sys.argv) < 2:
        print("Choose Scenario: disaster_response or hiking")

    scenario = sys.argv[1]

    controller = CollectorController(scenario)

    controller.send_clear_stats_to_all()

    controller.run()